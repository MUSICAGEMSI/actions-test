from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import os
import requests
import time
import json
import concurrent.futures
from typing import List, Dict
import re
from datetime import datetime
import threading
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from queue import Queue
import httpx

# ========================================
# CONFIGURAÇÕES ULTRA OTIMIZADAS
# ========================================

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

# OTIMIZAÇÕES AGRESSIVAS PARA 10 MINUTOS
NUM_THREADS = 50  # Aumentado drasticamente
TIMEOUT_REQUEST = 15  # Reduzido
DELAY_ENTRE_REQ = 0.01  # Praticamente zero
BATCH_SIZE = 100  # Processar em lotes
MAX_RETRIES = 2  # Menos tentativas

print(f"🚀 COLETOR DE LIÇÕES - VERSÃO ULTRA RÁPIDA")
print(f"🎯 META: Processar 2000 alunos em ~10 minutos")
print(f"🧵 Threads: {NUM_THREADS}")
print(f"⏱️  Timeout: {TIMEOUT_REQUEST}s")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

# Locks e contadores thread-safe
print_lock = threading.Lock()
stats_lock = threading.Lock()
global_stats = {
    'processados': 0,
    'erros': 0,
    'sem_dados': 0,
    'com_dados': 0,
    'tempo_inicio': None,
    'ultimo_log': 0
}

def safe_print(msg):
    with print_lock:
        print(msg)

def update_stats(tipo: str):
    with stats_lock:
        global_stats[tipo] += 1

# ========================================
# BUSCAR ALUNOS
# ========================================

def buscar_alunos_hortolandia() -> List[Dict]:
    print("📥 Buscando lista de alunos...")
    
    try:
        params = {"acao": "listar_ids_alunos"}
        response = requests.get(URL_APPS_SCRIPT, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('sucesso'):
                alunos = data.get('alunos', [])
                print(f"✅ {len(alunos)} alunos carregados")
                return alunos
            else:
                print(f"⚠️ Erro: {data.get('erro')}")
                return []
        else:
            print(f"⚠️ HTTP {response.status_code}")
            return []
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        return []

# ========================================
# EXTRAÇÃO OTIMIZADA
# ========================================

def extrair_dados_completos(soup, id_aluno: int, nome_aluno: str) -> Dict:
    """Extração otimizada com try-except mínimo"""
    
    dados = {
        'mts_individual': [],
        'mts_grupo': [],
        'msa_individual': [],
        'msa_grupo': [],
        'provas': [],
        'hinario_individual': [],
        'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [],
        'escalas_grupo': []
    }
    
    try:
        # MTS
        aba_mts = soup.find('div', {'id': 'mts'})
        if aba_mts:
            # Individual
            tab = aba_mts.find('table', {'id': 'datatable1'})
            if tab:
                tbody = tab.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            dados['mts_individual'].append([
                                id_aluno, nome_aluno,
                                cols[0].get_text(strip=True),
                                cols[1].get_text(strip=True),
                                cols[2].get_text(strip=True),
                                cols[3].get_text(strip=True),
                                cols[4].get_text(strip=True),
                                cols[5].get_text(strip=True),
                                cols[6].get_text(strip=True) if len(cols) > 6 else ""
                            ])
            
            # Grupo
            tab_g = aba_mts.find('table', {'id': 'datatable_mts_grupo'})
            if tab_g:
                tbody = tab_g.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            dados['mts_grupo'].append([
                                id_aluno, nome_aluno,
                                cols[0].get_text(strip=True),
                                cols[1].get_text(strip=True),
                                cols[2].get_text(strip=True)
                            ])
        
        # MSA
        aba_msa = soup.find('div', {'id': 'msa'})
        if aba_msa:
            # Individual
            tab = aba_msa.find('table', {'id': 'datatable1'})
            if tab:
                tbody = tab.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            dados['msa_individual'].append([
                                id_aluno, nome_aluno,
                                cols[0].get_text(strip=True),
                                cols[1].get_text(strip=True),
                                cols[2].get_text(strip=True),
                                cols[3].get_text(strip=True),
                                cols[4].get_text(strip=True),
                                cols[5].get_text(strip=True),
                                cols[6].get_text(strip=True)
                            ])
            
            # Grupo
            tab_g = aba_msa.find('table', {'id': 'datatable_mts_grupo'})
            if tab_g:
                tbody = tab_g.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            texto = cols[0].get_text(strip=True)
                            fases_m = re.search(r'de\s+([\d.]+)\s+até\s+([\d.]+)', texto)
                            pag_m = re.search(r'de\s+(\d+)\s+até\s+(\d+)', texto)
                            dados['msa_grupo'].append([
                                id_aluno, nome_aluno,
                                fases_m.group(1) if fases_m else "",
                                fases_m.group(2) if fases_m else "",
                                pag_m.group(1) if pag_m else "",
                                pag_m.group(2) if pag_m else "",
                                cols[1].get_text(strip=True),
                                cols[2].get_text(strip=True)
                            ])
        
        # PROVAS
        aba_provas = soup.find('div', {'id': 'provas'})
        if aba_provas:
            tab = aba_provas.find('table', {'id': 'datatable2'})
            if tab:
                tbody = tab.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 5:
                            dados['provas'].append([
                                id_aluno, nome_aluno,
                                cols[0].get_text(strip=True),
                                cols[1].get_text(strip=True),
                                cols[2].get_text(strip=True),
                                cols[3].get_text(strip=True),
                                cols[4].get_text(strip=True)
                            ])
        
        # HINÁRIO
        aba_hin = soup.find('div', {'id': 'hinario'})
        if aba_hin:
            # Individual
            tab = aba_hin.find('table', {'id': 'datatable4'})
            if tab:
                tbody = tab.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            dados['hinario_individual'].append([
                                id_aluno, nome_aluno,
                                cols[0].get_text(strip=True),
                                cols[1].get_text(strip=True),
                                cols[2].get_text(strip=True),
                                cols[3].get_text(strip=True),
                                cols[4].get_text(strip=True),
                                cols[5].get_text(strip=True),
                                cols[6].get_text(strip=True) if len(cols) > 6 else ""
                            ])
            
            # Grupo
            todas_tabs = aba_hin.find_all('table')
            for tab in todas_tabs:
                if tab.get('id') != 'datatable4':
                    tbody = tab.find('tbody')
                    if tbody:
                        for linha in tbody.find_all('tr'):
                            cols = linha.find_all('td')
                            if len(cols) >= 3:
                                dados['hinario_grupo'].append([
                                    id_aluno, nome_aluno,
                                    cols[0].get_text(strip=True),
                                    cols[1].get_text(strip=True),
                                    cols[2].get_text(strip=True)
                                ])
                        break
        
        # MÉTODOS
        aba_met = soup.find('div', {'id': 'metodos'})
        if aba_met:
            tab = aba_met.find('table', {'id': 'datatable3'})
            if tab:
                tbody = tab.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            dados['metodos'].append([
                                id_aluno, nome_aluno,
                                cols[0].get_text(strip=True),
                                cols[1].get_text(strip=True),
                                cols[2].get_text(strip=True),
                                cols[3].get_text(strip=True),
                                cols[4].get_text(strip=True),
                                cols[5].get_text(strip=True),
                                cols[6].get_text(strip=True) if len(cols) > 6 else ""
                            ])
        
        # ESCALAS
        aba_esc = soup.find('div', {'id': 'escalas'})
        if aba_esc:
            todas_tabs = aba_esc.find_all('table')
            
            # Individual (primeira)
            if len(todas_tabs) > 0:
                tbody = todas_tabs[0].find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 6:
                            dados['escalas_individual'].append([
                                id_aluno, nome_aluno,
                                cols[0].get_text(strip=True),
                                cols[1].get_text(strip=True),
                                cols[2].get_text(strip=True),
                                cols[3].get_text(strip=True),
                                cols[4].get_text(strip=True),
                                cols[5].get_text(strip=True) if len(cols) > 5 else ""
                            ])
            
            # Grupo (segunda)
            if len(todas_tabs) > 1:
                tbody = todas_tabs[1].find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            dados['escalas_grupo'].append([
                                id_aluno, nome_aluno,
                                cols[0].get_text(strip=True),
                                cols[1].get_text(strip=True),
                                cols[2].get_text(strip=True)
                            ])
    
    except:
        pass  # Silenciar erros individuais para velocidade
    
    return dados

# ========================================
# WORKER OTIMIZADO COM HTTPX
# ========================================

def worker_coletar_aluno_httpx(aluno: Dict, client: httpx.Client) -> Dict:
    """Worker ultra otimizado com httpx (mais rápido que requests)"""
    
    id_aluno = aluno['id_aluno']
    nome_aluno = aluno['nome']
    
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'text/html',
        'Connection': 'keep-alive'
    }
    
    try:
        url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
        resp = client.get(url, headers=headers, timeout=TIMEOUT_REQUEST)
        
        if resp.status_code != 200 or len(resp.text) < 1000 or "login" in resp.text.lower():
            update_stats('erros')
            update_stats('processados')
            return None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        dados = extrair_dados_completos(soup, id_aluno, nome_aluno)
        
        total = sum(len(v) for v in dados.values())
        
        if total > 0:
            update_stats('com_dados')
        else:
            update_stats('sem_dados')
        
        update_stats('processados')
        
        # Micro delay
        time.sleep(DELAY_ENTRE_REQ)
        
        return dados
        
    except:
        update_stats('erros')
        update_stats('processados')
        return None

# ========================================
# COLETA PARALELA ULTRA OTIMIZADA
# ========================================

def executar_coleta_ultra_rapida(cookies_dict: Dict, alunos: List[Dict], num_threads: int):
    """Coleta com httpx para máxima performance"""
    
    print(f"\n🚀 Iniciando coleta ULTRA RÁPIDA...")
    print(f"🎯 Meta: {len(alunos)} alunos em ~10 minutos")
    
    todos_dados = {
        'mts_individual': [],
        'mts_grupo': [],
        'msa_individual': [],
        'msa_grupo': [],
        'provas': [],
        'hinario_individual': [],
        'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [],
        'escalas_grupo': []
    }
    
    global_stats['tempo_inicio'] = time.time()
    total_alunos = len(alunos)
    
    # Criar client httpx com configurações otimizadas
    limits = httpx.Limits(
        max_keepalive_connections=num_threads,
        max_connections=num_threads * 2,
        keepalive_expiry=30
    )
    
    # Converter cookies para httpx
    cookies_httpx = httpx.Cookies()
    for k, v in cookies_dict.items():
        cookies_httpx.set(k, v)
    
    with httpx.Client(
        cookies=cookies_httpx,
        limits=limits,
        timeout=TIMEOUT_REQUEST,
        http2=True  # HTTP/2 para melhor performance
    ) as client:
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submeter todas as tarefas
            futures = {
                executor.submit(worker_coletar_aluno_httpx, aluno, client): aluno 
                for aluno in alunos
            }
            
            # Processar resultados
            for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
                try:
                    resultado = future.result(timeout=TIMEOUT_REQUEST + 5)
                    
                    if resultado:
                        for key in todos_dados.keys():
                            todos_dados[key].extend(resultado[key])
                    
                    # Log otimizado (a cada 25 alunos)
                    if i % 25 == 0:
                        with stats_lock:
                            proc = global_stats['processados']
                            com_d = global_stats['com_dados']
                            sem_d = global_stats['sem_dados']
                            erros = global_stats['erros']
                            tempo_dec = time.time() - global_stats['tempo_inicio']
                        
                        velocidade = proc / tempo_dec if tempo_dec > 0 else 0
                        restantes = total_alunos - proc
                        tempo_est = restantes / velocidade if velocidade > 0 else 0
                        pct = (proc / total_alunos) * 100
                        
                        safe_print(
                            f"📊 {proc}/{total_alunos} ({pct:.1f}%) | "
                            f"✅ {com_d} | ⚪ {sem_d} | ❌ {erros} | "
                            f"⚡ {velocidade:.1f}/s | ⏱️  {tempo_est/60:.1f}min"
                        )
                
                except:
                    pass
    
    return todos_dados

# ========================================
# GERAR RESUMO OTIMIZADO
# ========================================

def gerar_resumo_alunos(alunos: List[Dict], todos_dados: Dict) -> List[List]:
    """Resumo otimizado com menos operações"""
    resumo = []
    
    # Pre-computar contagens
    contagens = {
        'mts_i': {},
        'mts_g': {},
        'msa_i': {},
        'msa_g': {},
        'prov': {},
        'hin_i': {},
        'hin_g': {},
        'met': {},
        'esc_i': {},
        'esc_g': {}
    }
    
    # Contar tudo de uma vez
    for x in todos_dados['mts_individual']:
        id_a = int(x[0])
        contagens['mts_i'][id_a] = contagens['mts_i'].get(id_a, 0) + 1
    
    for x in todos_dados['mts_grupo']:
        id_a = int(x[0])
        contagens['mts_g'][id_a] = contagens['mts_g'].get(id_a, 0) + 1
    
    for x in todos_dados['msa_individual']:
        id_a = int(x[0])
        contagens['msa_i'][id_a] = contagens['msa_i'].get(id_a, 0) + 1
    
    for x in todos_dados['msa_grupo']:
        id_a = int(x[0])
        contagens['msa_g'][id_a] = contagens['msa_g'].get(id_a, 0) + 1
    
    for x in todos_dados['provas']:
        id_a = int(x[0])
        contagens['prov'][id_a] = contagens['prov'].get(id_a, 0) + 1
    
    for x in todos_dados['hinario_individual']:
        id_a = int(x[0])
        contagens['hin_i'][id_a] = contagens['hin_i'].get(id_a, 0) + 1
    
    for x in todos_dados['hinario_grupo']:
        id_a = int(x[0])
        contagens['hin_g'][id_a] = contagens['hin_g'].get(id_a, 0) + 1
    
    for x in todos_dados['metodos']:
        id_a = int(x[0])
        contagens['met'][id_a] = contagens['met'].get(id_a, 0) + 1
    
    for x in todos_dados['escalas_individual']:
        id_a = int(x[0])
        contagens['esc_i'][id_a] = contagens['esc_i'].get(id_a, 0) + 1
    
    for x in todos_dados['escalas_grupo']:
        id_a = int(x[0])
        contagens['esc_g'][id_a] = contagens['esc_g'].get(id_a, 0) + 1
    
    # Calcular médias de provas
    medias = {}
    for x in todos_dados['provas']:
        id_a = int(x[0])
        try:
            nota = float(str(x[3]).replace(',', '.'))
            if id_a not in medias:
                medias[id_a] = []
            medias[id_a].append(nota)
        except:
            pass
    
    # Gerar resumo
    for aluno in alunos:
        id_aluno = int(aluno['id_aluno'])
        
        media = 0
        if id_aluno in medias:
            media = round(sum(medias[id_aluno]) / len(medias[id_aluno]), 2)
        
        resumo.append([
            id_aluno,
            aluno['nome'],
            aluno['id_igreja'],
            contagens['mts_i'].get(id_aluno, 0),
            contagens['mts_g'].get(id_aluno, 0),
            contagens['msa_i'].get(id_aluno, 0),
            contagens['msa_g'].get(id_aluno, 0),
            contagens['prov'].get(id_aluno, 0),
            media,
            contagens['hin_i'].get(id_aluno, 0),
            contagens['hin_g'].get(id_aluno, 0),
            contagens['met'].get(id_aluno, 0),
            contagens['esc_i'].get(id_aluno, 0),
            contagens['esc_g'].get(id_aluno, 0),
            "N/A",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ])
    
    return resumo

# ========================================
# ENVIAR PARA SHEETS
# ========================================

def enviar_dados_para_sheets(alunos: List[Dict], todos_dados: Dict, tempo: float):
    print(f"\n📤 Enviando para Google Sheets...")
    
    resumo = gerar_resumo_alunos(alunos, todos_dados)
    
    payload = {
        "tipo": "licoes_alunos",
        "resumo": resumo,
        "mts_individual": todos_dados['mts_individual'],
        "mts_grupo": todos_dados['mts_grupo'],
        "msa_individual": todos_dados['msa_individual'],
        "msa_grupo": todos_dados['msa_grupo'],
        "provas": todos_dados['provas'],
        "hinario_individual": todos_dados['hinario_individual'],
        "hinario_grupo": todos_dados['hinario_grupo'],
        "metodos": todos_dados['metodos'],
        "escalas_individual": todos_dados['escalas_individual'],
        "escalas_grupo": todos_dados['escalas_grupo'],
        "metadata": {
            "total_alunos_processados": len(alunos),
            "tempo_execucao_min": round(tempo/60, 2),
            "threads_utilizadas": NUM_THREADS,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=300)
        
        if response.status_code == 200:
            print("✅ Enviado com sucesso!")
            return True
        else:
            print(f"⚠️ Status: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False

def salvar_backup_local(alunos: List[Dict], todos_dados: Dict):
    try:
        nome = f"licoes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(nome, 'w', encoding='utf-8') as f:
            json.dump({
                "alunos": alunos,
                "dados": todos_dados,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Backup: {nome}")
    except Exception as e:
        print(f"❌ Erro backup: {e}")

def extrair_cookies_playwright(pagina):
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

# ========================================
# MAIN
# ========================================

def main():
    tempo_inicio = time.time()
    
    alunos = buscar_alunos_hortolandia()
    
    if not alunos:
        print("❌ Nenhum aluno encontrado")
        return
    
    print(f"\n🎓 {len(alunos)} alunos para processar")
    print(f"🎯 Meta: ~{len(alunos)/200:.1f} minutos ({len(alunos)/200*60:.0f} segundos)")
    print("\n🔐 Realizando login...")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        try:
            pagina.goto(URL_INICIAL, timeout=30000)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=15000)
            
            time.sleep(2)
            pagina.goto("https://musical.congregacao.org.br/licoes", timeout=15000)
            
            if "login" in pagina.url.lower():
                print("❌ Login falhou")
                navegador.close()
                return
            
            print("✅ Login OK!")
            
        except Exception as e:
            print(f"❌ Erro login: {e}")
            navegador.close()
            return
        
        cookies_dict = extrair_cookies_playwright(pagina)
        print(f"🍪 {len(cookies_dict)} cookies capturados")
        navegador.close()
    
    print(f"\n{'='*70}")
    print(f"🚀 INICIANDO COLETA ULTRA RÁPIDA")
    print(f"{'='*70}")
    
    todos_dados = executar_coleta_ultra_rapida(cookies_dict, alunos, NUM_THREADS)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'='*70}")
    print(f"🏁 CONCLUÍDO EM {tempo_total/60:.1f} MINUTOS!")
    print(f"{'='*70}")
    print(f"⚡ Velocidade média: {len(alunos)/tempo_total:.1f} alunos/s")
    
    total_reg = sum(len(v) for v in todos_dados.values())
    
    print(f"\n📦 {total_reg:,} registros coletados")
    print(f"   MTS Ind: {len(todos_dados['mts_individual']):,}")
    print(f"   MTS Grp: {len(todos_dados['mts_grupo']):,}")
    print(f"   MSA Ind: {len(todos_dados['msa_individual']):,}")
    print(f"   MSA Grp: {len(todos_dados['msa_grupo']):,}")
    print(f"   Provas: {len(todos_dados['provas']):,}")
    print(f"   Hinário Ind: {len(todos_dados['hinario_individual']):,}")
    print(f"   Hinário Grp: {len(todos_dados['hinario_grupo']):,}")
    print(f"   Métodos: {len(todos_dados['metodos']):,}")
    print(f"   Escalas Ind: {len(todos_dados['escalas_individual']):,}")
    print(f"   Escalas Grp: {len(todos_dados['escalas_grupo']):,}")
    
    salvar_backup_local(alunos, todos_dados)
    
    if total_reg > 0:
        enviar_dados_para_sheets(alunos, todos_dados, tempo_total)
        print(f"\n✅ SUCESSO TOTAL!")
    else:
        print(f"\n⚠️ Nenhum dado coletado")
    
    print(f"\n{'='*70}\n")

if __name__ == "__main__":
    main()
