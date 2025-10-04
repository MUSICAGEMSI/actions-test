from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os
import requests
import time
import json
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Set, Dict
import re
from collections import Counter

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzJv9YlseCXdvXwi0OOpxh-Q61rmCly2kMUBEtcv5VSyPEKdcKg7MAVvIgDYSM1yWpV/exec'

# PARÂMETROS OTIMIZADOS
RANGE_INICIO = 1
RANGE_FIM = 1000000
NUM_WORKERS = 100  # Aumentado drasticamente
BATCH_SIZE = 1000  # Processa em batches
CONCURRENT_REQUESTS = 50  # Requisições simultâneas por worker
TIMEOUT = 3  # Timeout reduzido

print(f"🎓 COLETOR DE IDs - ALUNOS DE HORTOLÂNDIA (OTIMIZADO)")
print(f"📊 Range: {RANGE_INICIO:,} - {RANGE_FIM:,}")
print(f"🚀 Workers: {NUM_WORKERS} | Concurrent: {CONCURRENT_REQUESTS}")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

# Cache global
ids_igrejas_hortolandia = set()
alunos_encontrados = []
lock = asyncio.Lock()
requisicoes_totais = 0

def buscar_ids_igrejas_hortolandia() -> Set[int]:
    """Busca IDs das igrejas de Hortolândia do Google Sheets"""
    print("📥 Buscando IDs das igrejas de Hortolândia...")
    
    try:
        params = {"acao": "listar_ids_hortolandia"}
        response = requests.get(URL_APPS_SCRIPT, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            ids = set(data.get('ids', []))
            print(f"✅ {len(ids)} IDs carregados: {sorted(list(ids))}")
            return ids
        else:
            print(f"⚠️ Erro: Status {response.status_code}")
            return set()
    except Exception as e:
        print(f"❌ Erro: {e}")
        return set()

def extrair_igreja_id(html: str) -> int:
    """Extrai ID da igreja do HTML"""
    match = re.search(r'igreja_selecionada\s*\((\d+)\)', html)
    return int(match.group(1)) if match else None

def extrair_nome_aluno(html: str) -> str:
    """Extrai nome do aluno do HTML"""
    match = re.search(r'name="nome"[^>]*value="([^"]+)"', html)
    return match.group(1).strip() if match else ""

async def verificar_aluno_async(session: aiohttp.ClientSession, aluno_id: int, semaphore: asyncio.Semaphore) -> Dict:
    """Verifica um aluno de forma assíncrona"""
    global requisicoes_totais
    
    async with semaphore:
        try:
            url = f"https://musical.congregacao.org.br/grp_musical/editar/{aluno_id}"
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as resp:
                requisicoes_totais += 1
                
                if resp.status == 200:
                    html = await resp.text()
                    
                    if 'igreja_selecionada' in html:
                        igreja_id = extrair_igreja_id(html)
                        
                        if igreja_id and igreja_id in ids_igrejas_hortolandia:
                            nome = extrair_nome_aluno(html)
                            return {
                                'id_aluno': aluno_id,
                                'id_igreja': igreja_id,
                                'nome': nome
                            }
        except asyncio.TimeoutError:
            pass
        except Exception:
            pass
    
    return None

async def processar_batch_async(ids_batch: List[int], cookies: dict, worker_id: int) -> List[Dict]:
    """Processa um batch de IDs de forma assíncrona"""
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS, limit_per_host=CONCURRENT_REQUESTS)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT, connect=TIMEOUT)
    
    async with aiohttp.ClientSession(
        cookies=cookies,
        connector=connector,
        timeout=timeout,
        headers={'User-Agent': 'Mozilla/5.0'}
    ) as session:
        
        tasks = [verificar_aluno_async(session, aluno_id, semaphore) for aluno_id in ids_batch]
        resultados = await asyncio.gather(*tasks, return_exceptions=True)
        
        alunos = [r for r in resultados if isinstance(r, dict) and r is not None]
        
        if alunos:
            print(f"✅ Worker {worker_id}: {len(alunos)} alunos encontrados em {len(ids_batch)} IDs")
            for aluno in alunos[:3]:  # Mostra primeiros 3
                print(f"   → ID {aluno['id_aluno']} | Igreja {aluno['id_igreja']} | {aluno['nome'][:40]}")
        
        return alunos

def processar_batch_sync(ids_batch: List[int], cookies: dict, worker_id: int) -> List[Dict]:
    """Wrapper síncrono para executar batch assíncrono"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        resultado = loop.run_until_complete(processar_batch_async(ids_batch, cookies, worker_id))
        loop.close()
        return resultado
    except Exception as e:
        print(f"❌ Worker {worker_id}: Erro - {e}")
        return []

def executar_coleta_paralela(cookies: dict, range_inicio: int, range_fim: int, num_workers: int) -> List[Dict]:
    """Executa coleta paralela ultra otimizada"""
    total_ids = range_fim - range_inicio + 1
    
    # Dividir em batches
    batches = []
    for i in range(range_inicio, range_fim + 1, BATCH_SIZE):
        fim_batch = min(i + BATCH_SIZE - 1, range_fim)
        batches.append(list(range(i, fim_batch + 1)))
    
    print(f"📦 {len(batches)} batches de {BATCH_SIZE} IDs")
    print(f"🚀 Iniciando coleta com {num_workers} workers...\n")
    
    todos_alunos = []
    tempo_inicio = time.time()
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        
        for idx, batch in enumerate(batches):
            future = executor.submit(processar_batch_sync, batch, cookies, idx)
            futures.append(future)
        
        # Processar resultados conforme chegam
        for idx, future in enumerate(as_completed(futures)):
            try:
                alunos = future.result(timeout=300)
                todos_alunos.extend(alunos)
                
                progresso = ((idx + 1) / len(batches)) * 100
                tempo_decorrido = time.time() - tempo_inicio
                velocidade = requisicoes_totais / tempo_decorrido if tempo_decorrido > 0 else 0
                
                if (idx + 1) % 10 == 0 or (idx + 1) == len(batches):
                    print(f"📊 Progresso: {progresso:.1f}% | {len(todos_alunos)} alunos | {velocidade:.0f} req/s | {tempo_decorrido/60:.1f} min")
                
            except Exception as e:
                print(f"❌ Batch {idx}: Erro - {e}")
    
    return todos_alunos

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def salvar_alunos_json(alunos: List[Dict], arquivo: str = "alunos_hortolandia.json"):
    """Salva alunos em JSON"""
    try:
        with open(arquivo, 'w', encoding='utf-8') as f:
            json.dump({
                "alunos": alunos,
                "total": len(alunos),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }, f, indent=2, ensure_ascii=False)
        print(f"💾 Dados salvos: {arquivo}")
    except Exception as e:
        print(f"❌ Erro ao salvar: {e}")

def enviar_para_sheets(alunos: List[Dict], tempo_exec: float):
    """Envia dados para Google Sheets"""
    if not alunos:
        print("⚠️ Nenhum aluno para enviar")
        return False
    
    print(f"\n📤 Enviando {len(alunos)} alunos para Google Sheets...")
    
    # Preparar dados
    dados_formatados = []
    for aluno in alunos:
        dados_formatados.append([
            aluno['id_aluno'],
            aluno['id_igreja'],
            aluno['nome']
        ])
    
    payload = {
        "tipo": "alunos_hortolandia",
        "headers": ["ID_Aluno", "ID_Igreja", "Nome_Aluno"],
        "dados": dados_formatados,
        "resumo": {
            "total_alunos": len(alunos),
            "igrejas_monitoradas": len(ids_igrejas_hortolandia),
            "range": f"{RANGE_INICIO}-{RANGE_FIM}",
            "tempo_min": round(tempo_exec/60, 2),
            "velocidade_req_s": round(requisicoes_totais/tempo_exec, 1),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
        
        if response.status_code == 200:
            print("✅ Enviado com sucesso!")
            return True
        else:
            print(f"⚠️ Status {response.status_code}: {response.text[:200]}")
            return False
    except Exception as e:
        print(f"❌ Erro no envio: {e}")
        return False

def main():
    global ids_igrejas_hortolandia
    
    tempo_inicio = time.time()
    
    # Buscar IDs das igrejas
    ids_igrejas_hortolandia = buscar_ids_igrejas_hortolandia()
    
    if not ids_igrejas_hortolandia:
        print("❌ Nenhum ID de igreja encontrado. Abortando...")
        return
    
    print("\n🔐 Realizando login...")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        pagina.set_extra_http_headers({'User-Agent': 'Mozilla/5.0'})
        
        try:
            pagina.goto(URL_INICIAL, timeout=15000)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado!\n")
        except Exception as e:
            print(f"❌ Erro no login: {e}")
            navegador.close()
            return
        
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
    
    # Executar coleta otimizada
    print("🎓 Iniciando busca ultra otimizada...")
    print(f"🏛️ Monitorando {len(ids_igrejas_hortolandia)} igrejas\n")
    
    alunos = executar_coleta_paralela(cookies_dict, RANGE_INICIO, RANGE_FIM, NUM_WORKERS)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'='*70}")
    print(f"🏁 COLETA FINALIZADA!")
    print(f"{'='*70}")
    print(f"🎓 Alunos encontrados: {len(alunos)}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    print(f"📊 IDs verificados: {requisicoes_totais:,}")
    print(f"⚡ Velocidade: {requisicoes_totais/tempo_total:.1f} req/s")
    print(f"{'='*70}")
    
    if alunos:
        # Ordenar por ID
        alunos.sort(key=lambda x: x['id_aluno'])
        
        # Primeiros 10 alunos
        print(f"\n📋 Primeiros 10 alunos:")
        for i, a in enumerate(alunos[:10], 1):
            print(f"   {i}. ID {a['id_aluno']} | Igreja {a['id_igreja']} | {a['nome'][:50]}")
        
        if len(alunos) > 10:
            print(f"   ... e mais {len(alunos) - 10} alunos")
        
        # Distribuição por igreja
        print(f"\n📊 Distribuição por igreja:")
        dist = Counter([a['id_igreja'] for a in alunos])
        for igreja_id, qtd in dist.most_common():
            print(f"   Igreja {igreja_id}: {qtd} alunos")
        
        # Salvar e enviar
        salvar_alunos_json(alunos)
        enviar_para_sheets(alunos, tempo_total)
    else:
        print("⚠️ Nenhum aluno encontrado")
    
    print(f"\n🎯 Processo finalizado!")

if __name__ == "__main__":
    main()
