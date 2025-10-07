from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os
import sys
import requests
import time
import json
import concurrent.futures
from typing import List, Set, Dict, Optional, Tuple
import re

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzl1l143sg2_S5a6bOQy6WqWATMDZpSglIyKUp3OVZtycuHXQmGjisOpzffHTW5TvyK/exec'

# Parâmetros de busca inteligente
ID_MINIMO = 1
ID_MAXIMO = 1000000
NUM_THREADS = 25
TAMANHO_AMOSTRA = 100  # Quantos IDs verificar em cada etapa da busca binária

print(f"🎓 COLETOR INTELIGENTE - ALUNOS DE HORTOLÂNDIA")
print(f"🔍 Range total: {ID_MINIMO:,} - {ID_MAXIMO:,}")
print(f"🧵 Threads: {NUM_THREADS}")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

def buscar_ids_igrejas_hortolandia() -> Set[int]:
    """Busca os IDs das igrejas de Hortolândia do Google Sheets"""
    print("📥 Buscando IDs das igrejas de Hortolândia do Google Sheets...")
    
    try:
        params = {"acao": "listar_ids_hortolandia"}
        response = requests.get(URL_APPS_SCRIPT, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            ids = set(data.get('ids', []))
            print(f"✅ {len(ids)} IDs de igrejas carregados: {sorted(list(ids))}")
            return ids
        else:
            print(f"⚠️ Erro ao buscar IDs: Status {response.status_code}")
            return set()
            
    except Exception as e:
        print(f"❌ Erro ao buscar IDs das igrejas: {e}")
        return set()

def extrair_igreja_selecionada(html_content: str) -> Optional[int]:
    """Extrai o ID da igreja_selecionada do HTML"""
    if not html_content:
        return None
    
    match = re.search(r'igreja_selecionada\s*\((\d+)\)', html_content)
    if match:
        return int(match.group(1))
    
    match = re.search(r'igreja_selecionada\((\d+)\)', html_content)
    if match:
        return int(match.group(1))
    
    return None

def extrair_nome_aluno(html_content: str) -> str:
    """Extrai o nome do aluno do HTML"""
    if not html_content:
        return ""
    
    match = re.search(r'name="nome"[^>]*value="([^"]+)"', html_content)
    if match:
        return match.group(1).strip()
    
    return ""

def verificar_aluno_existe(session, aluno_id: int, ids_igrejas: Set[int]) -> Optional[Dict]:
    """
    Verifica se um aluno existe e se é de Hortolândia
    Retorna dict com dados se for de Hortolândia, None caso contrário
    """
    try:
        url = f"https://musical.congregacao.org.br/grp_musical/editar/{aluno_id}"
        resp = session.get(url, timeout=10)
        
        if resp.status_code == 200 and 'igreja_selecionada' in resp.text:
            igreja_id = extrair_igreja_selecionada(resp.text)
            
            if igreja_id and igreja_id in ids_igrejas:
                nome_aluno = extrair_nome_aluno(resp.text)
                return {
                    'id_aluno': aluno_id,
                    'id_igreja': igreja_id,
                    'nome': nome_aluno
                }
        
        return None
    except:
        return None

def buscar_primeiro_aluno_hortolandia(session, ids_igrejas: Set[int], inicio: int, fim: int) -> Optional[int]:
    """
    Usa busca binária para encontrar o PRIMEIRO aluno de Hortolândia no range
    """
    print(f"\n🔍 Buscando PRIMEIRO aluno de Hortolândia entre {inicio:,} e {fim:,}...")
    
    primeiro_encontrado = None
    
    while inicio <= fim:
        meio = (inicio + fim) // 2
        
        # Verificar uma amostra ao redor do meio
        ids_amostra = list(range(max(1, meio - TAMANHO_AMOSTRA//2), 
                                 min(ID_MAXIMO, meio + TAMANHO_AMOSTRA//2) + 1))
        
        encontrados_na_amostra = []
        
        print(f"   Verificando amostra no ID ~{meio:,}...", end=" ")
        
        for id_teste in ids_amostra:
            resultado = verificar_aluno_existe(session, id_teste, ids_igrejas)
            if resultado:
                encontrados_na_amostra.append(id_teste)
            time.sleep(0.05)
        
        if encontrados_na_amostra:
            menor_id = min(encontrados_na_amostra)
            print(f"✅ Encontrado! Menor ID: {menor_id:,}")
            primeiro_encontrado = menor_id
            # Buscar mais à esquerda
            fim = menor_id - TAMANHO_AMOSTRA - 1
        else:
            print(f"❌ Nada encontrado")
            # Buscar à direita
            inicio = meio + TAMANHO_AMOSTRA + 1
        
        if inicio > fim:
            break
    
    if primeiro_encontrado:
        # Fazer uma verificação refinada ao redor do primeiro encontrado
        print(f"\n🎯 Refinando busca ao redor de {primeiro_encontrado:,}...")
        for id_teste in range(max(1, primeiro_encontrado - 500), primeiro_encontrado):
            resultado = verificar_aluno_existe(session, id_teste, ids_igrejas)
            if resultado:
                primeiro_encontrado = id_teste
                print(f"   ✅ Encontrado ID ainda menor: {primeiro_encontrado:,}")
            time.sleep(0.05)
    
    return primeiro_encontrado

def buscar_ultimo_aluno_hortolandia(session, ids_igrejas: Set[int], inicio: int, fim: int) -> Optional[int]:
    """
    Usa busca binária para encontrar o ÚLTIMO aluno de Hortolândia no range
    """
    print(f"\n🔍 Buscando ÚLTIMO aluno de Hortolândia entre {inicio:,} e {fim:,}...")
    
    ultimo_encontrado = None
    
    while inicio <= fim:
        meio = (inicio + fim) // 2
        
        # Verificar uma amostra ao redor do meio
        ids_amostra = list(range(max(1, meio - TAMANHO_AMOSTRA//2), 
                                 min(ID_MAXIMO, meio + TAMANHO_AMOSTRA//2) + 1))
        
        encontrados_na_amostra = []
        
        print(f"   Verificando amostra no ID ~{meio:,}...", end=" ")
        
        for id_teste in ids_amostra:
            resultado = verificar_aluno_existe(session, id_teste, ids_igrejas)
            if resultado:
                encontrados_na_amostra.append(id_teste)
            time.sleep(0.05)
        
        if encontrados_na_amostra:
            maior_id = max(encontrados_na_amostra)
            print(f"✅ Encontrado! Maior ID: {maior_id:,}")
            ultimo_encontrado = maior_id
            # Buscar mais à direita
            inicio = maior_id + TAMANHO_AMOSTRA + 1
        else:
            print(f"❌ Nada encontrado")
            # Buscar à esquerda
            fim = meio - TAMANHO_AMOSTRA - 1
        
        if inicio > fim:
            break
    
    if ultimo_encontrado:
        # Fazer uma verificação refinada ao redor do último encontrado
        print(f"\n🎯 Refinando busca ao redor de {ultimo_encontrado:,}...")
        for id_teste in range(ultimo_encontrado + 1, min(ID_MAXIMO, ultimo_encontrado + 500)):
            resultado = verificar_aluno_existe(session, id_teste, ids_igrejas)
            if resultado:
                ultimo_encontrado = id_teste
                print(f"   ✅ Encontrado ID ainda maior: {ultimo_encontrado:,}")
            time.sleep(0.05)
    
    return ultimo_encontrado

class ColetorAlunosHortolandia:
    def __init__(self, session, thread_id: int, ids_igrejas: Set[int]):
        self.session = session
        self.thread_id = thread_id
        self.ids_igrejas = ids_igrejas
        self.alunos_encontrados: List[Dict] = []
        self.requisicoes_feitas = 0
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
    
    def coletar_batch_alunos(self, ids_batch: List[int]) -> List[Dict]:
        """Verifica um batch de IDs de alunos e retorna os que são de Hortolândia"""
        for aluno_id in ids_batch:
            try:
                url = f"https://musical.congregacao.org.br/grp_musical/editar/{aluno_id}"
                
                resp = self.session.get(url, headers=self.headers, timeout=10)
                self.requisicoes_feitas += 1
                
                if resp.status_code == 200:
                    html = resp.text
                    
                    if 'igreja_selecionada' in html:
                        igreja_id = extrair_igreja_selecionada(html)
                        
                        if igreja_id and igreja_id in self.ids_igrejas:
                            nome_aluno = extrair_nome_aluno(html)
                            
                            aluno_data = {
                                'id_aluno': aluno_id,
                                'id_igreja': igreja_id,
                                'nome': nome_aluno
                            }
                            
                            self.alunos_encontrados.append(aluno_data)
                            print(f"✅ T{self.thread_id}: Aluno {aluno_id} | Igreja {igreja_id} | {nome_aluno[:40]}")
                
                time.sleep(0.08)
                
                if self.requisicoes_feitas % 500 == 0:
                    print(f"📊 T{self.thread_id}: {self.requisicoes_feitas:,} requisições | {len(self.alunos_encontrados)} alunos encontrados")
                
            except Exception as e:
                if "timeout" in str(e).lower():
                    print(f"⏱️ T{self.thread_id}: Timeout no ID {aluno_id}")
                continue
        
        return self.alunos_encontrados

def executar_coleta_paralela_alunos(session, ids_igrejas: Set[int], range_inicio: int, range_fim: int, num_threads: int) -> List[Dict]:
    """Executa coleta paralela de alunos de Hortolândia"""
    total_ids = range_fim - range_inicio + 1
    ids_per_thread = total_ids // num_threads
    
    print(f"📈 Dividindo {total_ids:,} IDs em {num_threads} threads ({ids_per_thread:,} IDs/thread)")
    
    thread_ranges = []
    for i in range(num_threads):
        inicio = range_inicio + (i * ids_per_thread)
        fim = inicio + ids_per_thread - 1
        
        if i == num_threads - 1:
            fim = range_fim
            
        thread_ranges.append(list(range(inicio, fim + 1)))
    
    todos_alunos = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        coletores = [ColetorAlunosHortolandia(session, i, ids_igrejas) for i in range(num_threads)]
        
        futures = []
        for i, ids_thread in enumerate(thread_ranges):
            future = executor.submit(coletores[i].coletar_batch_alunos, ids_thread)
            futures.append((future, i))
        
        for future, thread_id in futures:
            try:
                alunos_thread = future.result(timeout=3600)
                todos_alunos.extend(alunos_thread)
                coletor = coletores[thread_id]
                print(f"✅ Thread {thread_id}: {len(alunos_thread)} alunos | {coletor.requisicoes_feitas:,} requisições")
            except Exception as e:
                print(f"❌ Thread {thread_id}: Erro - {e}")
    
    return todos_alunos

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def salvar_alunos_em_arquivo(alunos: List[Dict], nome_arquivo: str = "alunos_hortolandia.json"):
    """Salva os dados dos alunos em arquivo JSON"""
    try:
        with open(nome_arquivo, 'w', encoding='utf-8') as f:
            json.dump({
                "alunos": alunos,
                "total": len(alunos),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Dados salvos em: {nome_arquivo}")
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo: {e}")

def enviar_alunos_para_sheets(alunos: List[Dict], tempo_execucao: float, ids_igrejas: Set[int], range_inicio: int, range_fim: int):
    """Envia os dados dos alunos para Google Sheets via Apps Script"""
    if not alunos:
        print("⚠️ Nenhum aluno para enviar")
        return False
    
    print(f"\n📤 Enviando {len(alunos)} alunos para Google Sheets...")
    
    relatorio = [["ID_ALUNO", "ID_IGREJA", "NOME_ALUNO"]]
    
    for aluno in alunos:
        relatorio.append([
            str(aluno['id_aluno']),
            str(aluno['id_igreja']),
            aluno['nome']
        ])
    
    payload = {
        "tipo": "alunos_hortolandia",
        "relatorio_formatado": relatorio,
        "metadata": {
            "total_alunos": len(alunos),
            "total_igrejas_monitoradas": len(ids_igrejas),
            "range_inicio": range_inicio,
            "range_fim": range_fim,
            "tempo_execucao_min": round(tempo_execucao/60, 2),
            "threads_utilizadas": NUM_THREADS,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "ids_igrejas": sorted(list(ids_igrejas))
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=180)
        
        if response.status_code == 200:
            print("✅ Dados dos alunos enviados com sucesso para Google Sheets!")
            print(f"📄 Resposta: {response.text[:150]}")
            return True
        else:
            print(f"⚠️ Status HTTP: {response.status_code}")
            print(f"📄 Resposta: {response.text[:200]}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Timeout ao enviar para Google Sheets (>180s)")
        return False
    except Exception as e:
        print(f"❌ Erro ao enviar para Google Sheets: {e}")
        return False

def main():
    tempo_inicio = time.time()
    
    # Buscar IDs das igrejas de Hortolândia
    ids_igrejas = buscar_ids_igrejas_hortolandia()
    
    if not ids_igrejas:
        print("❌ Nenhum ID de igreja encontrado. Abortando...")
        return
    
    print("\n🔐 Realizando login...")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        try:
            pagina.goto(URL_INICIAL)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado com sucesso!")
            
        except Exception as e:
            print(f"❌ Erro no login: {e}")
            navegador.close()
            return
        
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
    
    # Criar sessão requests otimizada
    session = requests.Session()
    session.cookies.update(cookies_dict)
    
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=NUM_THREADS + 5,
        pool_maxsize=NUM_THREADS + 5,
        max_retries=2
    )
    session.mount('https://', adapter)
    
    print("\n" + "="*70)
    print("🧠 FASE 1: DESCOBERTA INTELIGENTE DE RANGE")
    print("="*70)
    
    # Buscar primeiro aluno
    primeiro_id = buscar_primeiro_aluno_hortolandia(session, ids_igrejas, ID_MINIMO, ID_MAXIMO)
    
    if not primeiro_id:
        print("\n❌ Nenhum aluno de Hortolândia encontrado no range completo!")
        return
    
    print(f"\n🎯 PRIMEIRO aluno encontrado: ID {primeiro_id:,}")
    
    # Buscar último aluno (começando depois do primeiro)
    ultimo_id = buscar_ultimo_aluno_hortolandia(session, ids_igrejas, primeiro_id, ID_MAXIMO)
    
    if not ultimo_id:
        ultimo_id = primeiro_id
    
    print(f"🎯 ÚLTIMO aluno encontrado: ID {ultimo_id:,}")
    
    # Adicionar margem de segurança
    margem = 1000
    range_inicio = max(1, primeiro_id - margem)
    range_fim = min(ID_MAXIMO, ultimo_id + margem)
    
    print(f"\n📊 Range otimizado com margem de {margem}: {range_inicio:,} - {range_fim:,}")
    print(f"⚡ Economia: verificar apenas {range_fim - range_inicio + 1:,} IDs ao invés de {ID_MAXIMO:,}!")
    print(f"🎉 Redução de {100 * (1 - (range_fim - range_inicio + 1) / ID_MAXIMO):.1f}% no trabalho!")
    
    print("\n" + "="*70)
    print("🎓 FASE 2: COLETA COMPLETA NO RANGE OTIMIZADO")
    print("="*70)
    
    # Executar coleta paralela no range otimizado
    alunos_hortolandia = executar_coleta_paralela_alunos(session, ids_igrejas, range_inicio, range_fim, NUM_THREADS)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'='*70}")
    print(f"🏁 COLETA DE ALUNOS FINALIZADA!")
    print(f"{'='*70}")
    print(f"🎓 Alunos de Hortolândia encontrados: {len(alunos_hortolandia)}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    print(f"📈 Range otimizado: {range_inicio:,} - {range_fim:,} ({range_fim - range_inicio + 1:,} IDs)")
    
    if alunos_hortolandia:
        print(f"⚡ Velocidade: {(range_fim - range_inicio + 1)/tempo_total:.2f} IDs verificados/segundo")
        print(f"\n📋 Primeiros 10 alunos encontrados:")
        
        for i, aluno in enumerate(sorted(alunos_hortolandia, key=lambda x: x['id_aluno'])[:10]):
            print(f"   {i+1}. ID: {aluno['id_aluno']} | Igreja: {aluno['id_igreja']} | {aluno['nome'][:50]}")
        
        if len(alunos_hortolandia) > 10:
            print(f"   ... e mais {len(alunos_hortolandia) - 10} alunos")
        
        # Estatísticas por igreja
        print(f"\n📊 Distribuição por igreja:")
        from collections import Counter
        distribuicao = Counter([a['id_igreja'] for a in alunos_hortolandia])
        for igreja_id, qtd in distribuicao.most_common():
            print(f"   Igreja {igreja_id}: {qtd} alunos")
        
        # Salvar em arquivo
        salvar_alunos_em_arquivo(alunos_hortolandia)
        
        # Enviar para Google Sheets
        enviar_alunos_para_sheets(alunos_hortolandia, tempo_total, ids_igrejas, range_inicio, range_fim)
    
    else:
        print("⚠️ Nenhum aluno de Hortolândia foi encontrado neste range")
    
    print(f"\n🎯 Processo finalizado!")

if __name__ == "__main__":
    main()
