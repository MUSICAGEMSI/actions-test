from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os
import requests
import time
import json
import asyncio
import aiohttp
from typing import List, Set, Dict
import re
from collections import Counter

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzJv9YlseCXdvXwi0OOpxh-Q61rmCly2kMUBEtcv5VSyPEKdcKg7MAVvIgDYSM1yWpV/exec'

# PARÂMETROS ULTRA OTIMIZADOS
RANGE_INICIO = 1
RANGE_FIM = 1000000
CONCURRENT_REQUESTS = 500  # Máximo de requisições simultâneas
TIMEOUT = 2  # Timeout agressivo
CHUNK_SIZE = 5000  # Chunks grandes para processamento

print(f"🚀 COLETOR ULTRA-RÁPIDO - ALUNOS DE HORTOLÂNDIA")
print(f"📊 Range: {RANGE_INICIO:,} - {RANGE_FIM:,}")
print(f"⚡ Concurrent: {CONCURRENT_REQUESTS} | Timeout: {TIMEOUT}s")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

# Cache global
ids_igrejas_hortolandia = set()
alunos_encontrados = []
requisicoes_totais = 0
requisicoes_sucesso = 0

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
    global requisicoes_totais, requisicoes_sucesso
    
    async with semaphore:
        try:
            url = f"https://musical.congregacao.org.br/grp_musical/editar/{aluno_id}"
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as resp:
                requisicoes_totais += 1
                
                if resp.status == 200:
                    html = await resp.text()
                    requisicoes_sucesso += 1
                    
                    if 'igreja_selecionada' in html:
                        igreja_id = extrair_igreja_id(html)
                        
                        if igreja_id and igreja_id in ids_igrejas_hortolandia:
                            nome = extrair_nome_aluno(html)
                            return {
                                'id_aluno': aluno_id,
                                'id_igreja': igreja_id,
                                'nome': nome
                            }
        except:
            pass
    
    return None

async def processar_todos_ids_async(ids_lista: List[int], cookies: dict) -> List[Dict]:
    """Processa TODOS os IDs em uma única sessão assíncrona massiva"""
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    # Configuração ultra agressiva
    connector = aiohttp.TCPConnector(
        limit=CONCURRENT_REQUESTS,
        limit_per_host=CONCURRENT_REQUESTS,
        ttl_dns_cache=300,
        force_close=False,
        enable_cleanup_closed=True
    )
    
    timeout = aiohttp.ClientTimeout(total=TIMEOUT, connect=1, sock_read=TIMEOUT)
    
    async with aiohttp.ClientSession(
        cookies=cookies,
        connector=connector,
        timeout=timeout,
        headers={
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'text/html,application/xhtml+xml',
            'Connection': 'keep-alive'
        }
    ) as session:
        
        print(f"🚀 Lançando {len(ids_lista):,} requisições simultâneas...")
        
        # Dividir em chunks para não sobrecarregar memória
        alunos_total = []
        chunks = [ids_lista[i:i + CHUNK_SIZE] for i in range(0, len(ids_lista), CHUNK_SIZE)]
        
        tempo_inicio = time.time()
        
        for idx_chunk, chunk in enumerate(chunks, 1):
            tasks = [verificar_aluno_async(session, aluno_id, semaphore) for aluno_id in chunk]
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            alunos_chunk = [r for r in resultados if isinstance(r, dict) and r is not None]
            alunos_total.extend(alunos_chunk)
            
            tempo_decorrido = time.time() - tempo_inicio
            progresso = (idx_chunk / len(chunks)) * 100
            velocidade = requisicoes_totais / tempo_decorrido if tempo_decorrido > 0 else 0
            
            print(f"📊 Chunk {idx_chunk}/{len(chunks)} ({progresso:.1f}%) | "
                  f"{len(alunos_total)} alunos | "
                  f"{velocidade:.0f} req/s | "
                  f"{requisicoes_sucesso:,}/{requisicoes_totais:,} OK")
            
            if alunos_chunk:
                print(f"   ✅ +{len(alunos_chunk)} alunos neste chunk")
        
        return alunos_total

def executar_coleta_ultra_rapida(cookies: dict, range_inicio: int, range_fim: int) -> List[Dict]:
    """Executa coleta com máxima velocidade possível"""
    ids_lista = list(range(range_inicio, range_fim + 1))
    
    print(f"\n🎓 Iniciando varredura ultra-rápida...")
    print(f"🏛️ Monitorando {len(ids_igrejas_hortolandia)} igrejas")
    print(f"🔍 Verificando {len(ids_lista):,} IDs\n")
    
    tempo_inicio = time.time()
    
    # Executar tudo em uma única chamada assíncrona
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        alunos = loop.run_until_complete(processar_todos_ids_async(ids_lista, cookies))
    finally:
        loop.close()
    
    tempo_total = time.time() - tempo_inicio
    
    return alunos, tempo_total

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
    
    dados_formatados = [[a['id_aluno'], a['id_igreja'], a['nome']] for a in alunos]
    
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
            "taxa_sucesso": f"{(requisicoes_sucesso/requisicoes_totais*100):.1f}%",
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
    
    tempo_inicio_total = time.time()
    
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
    
    # Executar coleta ultra-rápida
    alunos, tempo_coleta = executar_coleta_ultra_rapida(cookies_dict, RANGE_INICIO, RANGE_FIM)
    
    tempo_total = time.time() - tempo_inicio_total
    
    print(f"\n{'='*70}")
    print(f"🏁 COLETA FINALIZADA!")
    print(f"{'='*70}")
    print(f"🎓 Alunos encontrados: {len(alunos)}")
    print(f"⏱️ Tempo coleta: {tempo_coleta:.1f}s ({tempo_coleta/60:.1f} min)")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    print(f"📊 Requisições: {requisicoes_totais:,} ({requisicoes_sucesso:,} OK)")
    print(f"⚡ Velocidade: {requisicoes_totais/tempo_coleta:.1f} req/s")
    print(f"✅ Taxa sucesso: {(requisicoes_sucesso/requisicoes_totais*100):.1f}%")
    print(f"{'='*70}")
    
    if alunos:
        alunos.sort(key=lambda x: x['id_aluno'])
        
        print(f"\n📋 Primeiros 10 alunos:")
        for i, a in enumerate(alunos[:10], 1):
            print(f"   {i}. ID {a['id_aluno']} | Igreja {a['id_igreja']} | {a['nome'][:50]}")
        
        if len(alunos) > 10:
            print(f"   ... e mais {len(alunos) - 10} alunos")
        
        print(f"\n📊 Distribuição por igreja:")
        dist = Counter([a['id_igreja'] for a in alunos])
        for igreja_id, qtd in dist.most_common():
            print(f"   Igreja {igreja_id}: {qtd} alunos")
        
        salvar_alunos_json(alunos)
        enviar_para_sheets(alunos, tempo_coleta)
    else:
        print("⚠️ Nenhum aluno encontrado")
    
    print(f"\n🎯 Processo finalizado!")

if __name__ == "__main__":
    main()
