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

# ========================================
# CONFIGURAÇÕES OTIMIZADAS E ESTÁVEIS
# ========================================

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

NUM_THREADS = 15  # Reduzido para evitar bloqueios
TIMEOUT_REQUEST = 20  # Aumentado para maior confiabilidade
DELAY_ENTRE_REQ = 0.1  # Pequeno delay para não sobrecarregar

print(f"🎓 COLETOR DE LIÇÕES - VERSÃO ESTÁVEL")
print(f"🧵 Threads: {NUM_THREADS}")
print(f"⏱️  Timeout: {TIMEOUT_REQUEST}s")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

# Locks e contadores
print_lock = threading.Lock()
stats_lock = threading.Lock()
global_stats = {
    'processados': 0,
    'erros': 0,
    'sem_dados': 0,
    'com_dados': 0,
    'tempo_inicio': None
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
    print("📥 Buscando lista de alunos do Google Sheets...")
    
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
                print(f"⚠️ Erro na resposta: {data.get('erro')}")
                return []
        else:
            print(f"⚠️ Erro HTTP: Status {response.status_code}")
            return []
            
    except Exception as e:
        print(f"❌ Erro ao buscar alunos: {e}")
        return []

# ========================================
# FUNÇÕES DE EXTRAÇÃO
# ========================================

def extrair_dados_completos(soup, id_aluno: int, nome_aluno: str) -> Dict:
    """Extrai todos os dados de um aluno de uma vez"""
    
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
        # MTS Individual
        aba_mts = soup.find('div', {'id': 'mts'})
        if aba_mts:
            tab = aba_mts.find('table', {'id': 'datatable1'})
            if tab and tab.find('tbody'):
                for linha in tab.find('tbody').find_all('tr'):
                    cols = linha.find_all('td')
                    if len(cols) >= 7:
                        dados['mts_individual'].append([id_aluno, nome_aluno] + 
                            [c.get_text(strip=True) for c in cols[:7]])
            
            # MTS Grupo
            tab_g = aba_mts.find('table', {'id': 'datatable_mts_grupo'})
            if tab_g and tab_g.find('tbody'):
                for linha in tab_g.find('tbody').find_all('tr'):
                    cols = linha.find_all('td')
                    if len(cols) >= 3:
                        dados['mts_grupo'].append([id_aluno, nome_aluno] + 
                            [c.get_text(strip=True) for c in cols[:3]])
        
        # MSA Individual
        aba_msa = soup.find('div', {'id': 'msa'})
        if aba_msa:
            tab = aba_msa.find('table', {'id': 'datatable1'})
            if tab and tab.find('tbody'):
                for linha in tab.find('tbody').find_all('tr'):
                    cols = linha.find_all('td')
                    if len(cols) >= 7:
                        dados['msa_individual'].append([id_aluno, nome_aluno] + 
                            [c.get_text(strip=True) for c in cols[:7]])
            
            # MSA Grupo
            tab_g = aba_msa.find('table', {'id': 'datatable_mts_grupo'})
            if tab_g and tab_g.find('tbody'):
                for linha in tab_g.find('tbody').find_all('tr'):
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
        
        # Provas
        aba_provas = soup.find('div', {'id': 'provas'})
        if aba_provas:
            tab = aba_provas.find('table', {'id': 'datatable2'})
            if tab and tab.find('tbody'):
                for linha in tab.find('tbody').find_all('tr'):
                    cols = linha.find_all('td')
                    if len(cols) >= 5:
                        dados['provas'].append([id_aluno, nome_aluno] + 
                            [c.get_text(strip=True) for c in cols[:5]])
        
        # Hinário Individual
        aba_hin = soup.find('div', {'id': 'hinario'})
        if aba_hin:
            tab = aba_hin.find('table', {'id': 'datatable4'})
            if tab and tab.find('tbody'):
                for linha in tab.find('tbody').find_all('tr'):
                    cols = linha.find_all('td')
                    if len(cols) >= 7:
                        dados['hinario_individual'].append([id_aluno, nome_aluno] + 
                            [c.get_text(strip=True) for c in cols[:7]])
            
            # Hinário Grupo
            tab_g = aba_hin.find('table', {'id': 'datatable_hinos_grupo'})
            if tab_g and tab_g.find('tbody'):
                for linha in tab_g.find('tbody').find_all('tr'):
                    cols = linha.find_all('td')
                    if len(cols) >= 3:
                        dados['hinario_grupo'].append([id_aluno, nome_aluno] + 
                            [c.get_text(strip=True) for c in cols[:3]])
        
        # Métodos
        aba_met = soup.find('div', {'id': 'metodos'})
        if aba_met:
            tab = aba_met.find('table', {'id': 'datatable3'})
            if tab and tab.find('tbody'):
                for linha in tab.find('tbody').find_all('tr'):
                    cols = linha.find_all('td')
                    if len(cols) >= 7:
                        dados['metodos'].append([id_aluno, nome_aluno] + 
                            [c.get_text(strip=True) for c in cols[:7]])
        
        # Escalas Individual
        aba_esc = soup.find('div', {'id': 'escalas'})
        if aba_esc:
            tab = aba_esc.find('table', {'id': 'datatable4'})
            if tab and tab.find('tbody'):
                for linha in tab.find('tbody').find_all('tr'):
                    cols = linha.find_all('td')
                    if len(cols) >= 6:
                        dados['escalas_individual'].append([id_aluno, nome_aluno] + 
                            [c.get_text(strip=True) for c in cols[:6]])
            
            # Escalas Grupo
            tab_g = aba_esc.find('table', {'id': 'datatable_escalas_grupo'})
            if tab_g and tab_g.find('tbody'):
                for linha in tab_g.find('tbody').find_all('tr'):
                    cols = linha.find_all('td')
                    if len(cols) >= 3:
                        dados['escalas_grupo'].append([id_aluno, nome_aluno] + 
                            [c.get_text(strip=True) for c in cols[:3]])
    
    except Exception as e:
        pass
    
    return dados

# ========================================
# CRIAR SESSÃO COM RETRY
# ========================================

def criar_sessao(cookies_dict: Dict) -> requests.Session:
    """Cria sessão com retry automático"""
    session = requests.Session()
    session.cookies.update(cookies_dict)
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=5,
        pool_maxsize=5
    )
    
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session

# ========================================
# WORKER
# ========================================

def worker_coletar_aluno(aluno: Dict, cookies_dict: Dict) -> Dict:
    """Coleta dados de um único aluno"""
    
    id_aluno = aluno['id_aluno']
    nome_aluno = aluno['nome']
    
    session = criar_sessao(cookies_dict)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html',
        'Connection': 'keep-alive'
    }
    
    try:
        url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
        resp = session.get(url, headers=headers, timeout=TIMEOUT_REQUEST)
        
        # Validar resposta
        if resp.status_code != 200:
            update_stats('erros')
            return None
        
        if len(resp.text) < 1000:
            update_stats('erros')
            return None
        
        if "login" in resp.text.lower():
            update_stats('erros')
            return None
        
        # Parsear HTML
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Extrair todos os dados
        dados = extrair_dados_completos(soup, id_aluno, nome_aluno)
        
        # Calcular total
        total = sum(len(v) for v in dados.values())
        
        if total > 0:
            update_stats('com_dados')
        else:
            update_stats('sem_dados')
        
        update_stats('processados')
        
        # Delay entre requisições
        time.sleep(DELAY_ENTRE_REQ)
        
        return dados
        
    except requests.exceptions.Timeout:
        update_stats('erros')
        update_stats('processados')
        return None
    except Exception as e:
        update_stats('erros')
        update_stats('processados')
        return None
    finally:
        session.close()

# ========================================
# COLETA PARALELA
# ========================================

def executar_coleta_paralela(cookies_dict: Dict, alunos: List[Dict], num_threads: int):
    """Executa coleta paralela"""
    
    print(f"\n🚀 Iniciando coleta com {num_threads} threads...")
    
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
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submeter todas as tarefas
        futures = {
            executor.submit(worker_coletar_aluno, aluno, cookies_dict.copy()): aluno 
            for aluno in alunos
        }
        
        # Processar resultados conforme completam
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                resultado = future.result(timeout=TIMEOUT_REQUEST + 5)
                
                if resultado:
                    # Consolidar dados
                    for key in todos_dados.keys():
                        todos_dados[key].extend(resultado[key])
                
                # Log de progresso a cada 50 alunos
                if i % 50 == 0:
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
                    
                    safe_print(f"\n📊 {proc}/{total_alunos} ({pct:.1f}%) | "
                             f"✅ {com_d} | ⚪ {sem_d} | ❌ {erros} | "
                             f"⚡ {velocidade:.2f}/s | ⏱️  {tempo_est/60:.1f}min")
                
            except Exception as e:
                pass
    
    return todos_dados

# ========================================
# FUNÇÕES AUXILIARES
# ========================================

def gerar_resumo_alunos(alunos: List[Dict], todos_dados: Dict) -> List[List]:
    resumo = []
    
    for aluno in alunos:
        id_aluno = aluno['id_aluno']
        nome = aluno['nome']
        id_igreja = aluno['id_igreja']
        
        t_mts_i = sum(1 for x in todos_dados['mts_individual'] if x[0] == id_aluno)
        t_mts_g = sum(1 for x in todos_dados['mts_grupo'] if x[0] == id_aluno)
        t_msa_i = sum(1 for x in todos_dados['msa_individual'] if x[0] == id_aluno)
        t_msa_g = sum(1 for x in todos_dados['msa_grupo'] if x[0] == id_aluno)
        t_prov = sum(1 for x in todos_dados['provas'] if x[0] == id_aluno)
        t_hin_i = sum(1 for x in todos_dados['hinario_individual'] if x[0] == id_aluno)
        t_hin_g = sum(1 for x in todos_dados['hinario_grupo'] if x[0] == id_aluno)
        t_met = sum(1 for x in todos_dados['metodos'] if x[0] == id_aluno)
        t_esc_i = sum(1 for x in todos_dados['escalas_individual'] if x[0] == id_aluno)
        t_esc_g = sum(1 for x in todos_dados['escalas_grupo'] if x[0] == id_aluno)
        
        provas = [float(x[3]) for x in todos_dados['provas'] 
                 if x[0] == id_aluno and x[3].replace('.','').isdigit()]
        media = round(sum(provas) / len(provas), 2) if provas else 0
        
        resumo.append([
            id_aluno, nome, id_igreja,
            t_mts_i, t_mts_g, t_msa_i, t_msa_g,
            t_prov, media, t_hin_i, t_hin_g,
            t_met, t_esc_i, t_esc_g, "N/A",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ])
    
    return resumo

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
            
            time.sleep(3)
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
        print(f"🍪 {len(cookies_dict)} cookies")
        navegador.close()
    
    print(f"\n{'='*70}")
    print(f"🚀 INICIANDO COLETA")
    print(f"{'='*70}")
    
    todos_dados = executar_coleta_paralela(cookies_dict, alunos, NUM_THREADS)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'='*70}")
    print(f"🏁 CONCLUÍDO!")
    print(f"{'='*70}")
    print(f"⏱️  Tempo: {tempo_total/60:.1f} min")
    print(f"⚡ Velocidade: {len(alunos)/tempo_total:.2f} alunos/s")
    
    total_reg = sum(len(v) for v in todos_dados.values())
    
    print(f"\n📦 {total_reg} registros coletados")
    print(f"   MTS Ind: {len(todos_dados['mts_individual'])}")
    print(f"   MTS Grp: {len(todos_dados['mts_grupo'])}")
    print(f"   MSA Ind: {len(todos_dados['msa_individual'])}")
    print(f"   MSA Grp: {len(todos_dados['msa_grupo'])}")
    print(f"   Provas: {len(todos_dados['provas'])}")
    print(f"   Hinário Ind: {len(todos_dados['hinario_individual'])}")
    print(f"   Hinário Grp: {len(todos_dados['hinario_grupo'])}")
    print(f"   Métodos: {len(todos_dados['metodos'])}")
    print(f"   Escalas Ind: {len(todos_dados['escalas_individual'])}")
    print(f"   Escalas Grp: {len(todos_dados['escalas_grupo'])}")
    
    salvar_backup_local(alunos, todos_dados)
    
    if total_reg > 0:
        enviar_dados_para_sheets(alunos, todos_dados, tempo_total)
        print(f"\n✅ SUCESSO!")
    else:
        print(f"\n⚠️ Nenhum dado coletado")
    
    print(f"\n{'='*70}\n")

if __name__ == "__main__":
    main()
