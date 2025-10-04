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
from urllib3.util.retry import Retry

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

# Configuração otimizada
NUM_THREADS = 30
TIMEOUT_REQUEST = 12
MAX_RETRIES = 3

print("COLETOR DE LIÇÕES - SESSÕES PERSISTENTES")
print(f"Threads: {NUM_THREADS} | Timeout: {TIMEOUT_REQUEST}s")

if not EMAIL or not SENHA:
    print("ERRO: Credenciais não definidas")
    exit(1)

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

def buscar_alunos_hortolandia() -> List[Dict]:
    print("\nBuscando alunos...")
    try:
        response = requests.get(URL_APPS_SCRIPT, params={"acao": "listar_ids_alunos"}, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get('sucesso'):
                alunos = data.get('alunos', [])
                print(f"OK: {len(alunos)} alunos\n")
                return alunos
        return []
    except Exception as e:
        print(f"Erro: {e}")
        return []

def fazer_login() -> Dict:
    print("Fazendo login...")
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        try:
            pagina.goto(URL_INICIAL, timeout=30000)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=15000)
            time.sleep(2)
            
            if "login" in pagina.url.lower():
                navegador.close()
                raise Exception("Login falhou")
            
            cookies = pagina.context.cookies()
            cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}
            navegador.close()
            print("Login OK\n")
            return cookies_dict
        except Exception as e:
            navegador.close()
            raise Exception(f"Erro no login: {e}")

def extrair_dados_completos(soup, id_aluno: int, nome_aluno: str) -> Dict:
    dados = {
        'mts_individual': [], 'mts_grupo': [],
        'msa_individual': [], 'msa_grupo': [],
        'provas': [],
        'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [], 'escalas_grupo': []
    }
    
    try:
        # MTS
        aba_mts = soup.find('div', {'id': 'mts'})
        if aba_mts:
            tabelas = aba_mts.find_all('table', class_='table')
            if len(tabelas) > 0:
                tbody = tabelas[0].find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            dados['mts_individual'].append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:7]])
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            dados['mts_grupo'].append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:3]])
        
        # MSA
        aba_msa = soup.find('div', {'id': 'msa'})
        if aba_msa:
            tabelas = aba_msa.find_all('table', class_='table')
            if len(tabelas) > 0:
                tbody = tabelas[0].find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            dados['msa_individual'].append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:7]])
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            texto = cols[0].get_text(strip=True)
                            fases_m = re.search(r'de\s+([\d.]+)\s+até\s+([\d.]+)', texto)
                            pag_m = re.search(r'de\s+(\d+)\s+até\s+(\d+)', texto)
                            clave_m = re.search(r'Clave\(s\):\s*(.+?)(?:\s*$)', texto)
                            dados['msa_grupo'].append([
                                id_aluno, nome_aluno,
                                fases_m.group(1) if fases_m else "",
                                fases_m.group(2) if fases_m else "",
                                pag_m.group(1) if pag_m else "",
                                pag_m.group(2) if pag_m else "",
                                clave_m.group(1) if clave_m else "",
                                cols[1].get_text(strip=True),
                                cols[2].get_text(strip=True)
                            ])
        
        # PROVAS
        aba_provas = soup.find('div', {'id': 'provas'})
        if aba_provas:
            tabela = aba_provas.find('table', class_='table')
            if tabela:
                tbody = tabela.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 5:
                            dados['provas'].append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:5]])
        
        # HINÁRIO
        aba_hin = soup.find('div', {'id': 'hinario'})
        if aba_hin:
            tabelas = aba_hin.find_all('table', class_='table')
            if len(tabelas) > 0:
                tbody = tabelas[0].find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            dados['hinario_individual'].append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:7]])
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            dados['hinario_grupo'].append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:3]])
        
        # MÉTODOS
        aba_met = soup.find('div', {'id': 'metodos'})
        if aba_met:
            tabela = aba_met.find('table', class_='table')
            if tabela:
                tbody = tabela.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            dados['metodos'].append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:7]])
        
        # ESCALAS
        aba_esc = soup.find('div', {'id': 'escalas'})
        if aba_esc:
            tabelas = aba_esc.find_all('table', class_='table')
            if len(tabelas) > 0:
                tbody = tabelas[0].find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 6:
                            dados['escalas_individual'].append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:6]])
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            dados['escalas_grupo'].append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:3]])
    except Exception:
        pass
    
    return dados

def criar_sessao_persistente(cookies_dict: Dict) -> requests.Session:
    """Sessão HTTP com keep-alive e pool otimizado"""
    session = requests.Session()
    session.cookies.update(cookies_dict)
    
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=10,
        pool_maxsize=50,
        pool_block=False
    )
    
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html',
        'Connection': 'keep-alive'
    })
    
    return session

# Pool de sessões thread-safe
class SessionPool:
    def __init__(self, cookies_dict: Dict, size: int):
        self.sessions = [criar_sessao_persistente(cookies_dict) for _ in range(size)]
        self.lock = threading.Lock()
        self.index = 0
    
    def get_session(self):
        with self.lock:
            session = self.sessions[self.index]
            self.index = (self.index + 1) % len(self.sessions)
            return session
    
    def close_all(self):
        for session in self.sessions:
            session.close()

session_pool = None

def worker_coletar_aluno(aluno: Dict) -> Dict:
    """Worker usando pool de sessões persistentes"""
    id_aluno = aluno['id_aluno']
    nome_aluno = aluno['nome']
    
    session = session_pool.get_session()
    
    for tentativa in range(MAX_RETRIES):
        try:
            url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
            resp = session.get(url, timeout=TIMEOUT_REQUEST)
            
            if resp.status_code != 200 or len(resp.text) < 1000:
                if tentativa < MAX_RETRIES - 1:
                    time.sleep(0.5)
                    continue
                update_stats('erros')
                return None
            
            if "login" in resp.text.lower():
                if tentativa < MAX_RETRIES - 1:
                    time.sleep(1)
                    continue
                update_stats('erros')
                return None
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            dados = extrair_dados_completos(soup, id_aluno, nome_aluno)
            
            total = sum(len(v) for v in dados.values())
            if total > 0:
                update_stats('com_dados')
            else:
                update_stats('sem_dados')
            
            update_stats('processados')
            return dados
            
        except Exception:
            if tentativa < MAX_RETRIES - 1:
                time.sleep(0.5)
                continue
    
    update_stats('erros')
    update_stats('processados')
    return None

def executar_coleta_paralela(alunos: List[Dict], num_threads: int, cookies_dict: Dict):
    global session_pool
    
    print(f"Iniciando coleta com {num_threads} threads...\n")
    
    # Criar pool de sessões
    session_pool = SessionPool(cookies_dict, num_threads)
    
    todos_dados = {
        'mts_individual': [], 'mts_grupo': [],
        'msa_individual': [], 'msa_grupo': [],
        'provas': [],
        'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [], 'escalas_grupo': []
    }
    
    global_stats['tempo_inicio'] = time.time()
    total_alunos = len(alunos)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {executor.submit(worker_coletar_aluno, aluno): aluno for aluno in alunos}
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                resultado = future.result(timeout=TIMEOUT_REQUEST + 10)
                
                if resultado:
                    for key in todos_dados.keys():
                        todos_dados[key].extend(resultado[key])
                
                if i % 50 == 0 or i == total_alunos:
                    with stats_lock:
                        proc = global_stats['processados']
                        com_d = global_stats['com_dados']
                        sem_d = global_stats['sem_dados']
                        erros = global_stats['erros']
                        tempo = time.time() - global_stats['tempo_inicio']
                    
                    vel = proc / tempo if tempo > 0 else 0
                    rest = total_alunos - proc
                    tempo_est = rest / vel if vel > 0 else 0
                    pct = (proc / total_alunos) * 100
                    taxa_erro = (erros / proc * 100) if proc > 0 else 0
                    
                    safe_print(
                        f"[{proc}/{total_alunos}] {pct:.1f}% | "
                        f"OK:{com_d} Vazio:{sem_d} Erro:{erros}({taxa_erro:.1f}%) | "
                        f"{vel:.1f}/s | Resta:{tempo_est/60:.1f}min"
                    )
            except Exception:
                pass
    
    session_pool.close_all()
    return todos_dados

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
        
        provas = []
        for p in todos_dados['provas']:
            if p[0] == id_aluno:
                try:
                    nota_str = str(p[3]).replace(',', '.').strip()
                    if nota_str and nota_str.replace('.', '').replace('-', '').isdigit():
                        nota = float(nota_str)
                        if 0 <= nota <= 10:
                            provas.append(nota)
                except:
                    pass
        
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
    print(f"\nEnviando para Google Sheets...")
    
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
            print("Dados enviados com sucesso!")
            return True
        else:
            print(f"Erro: Status {response.status_code}")
            return False
    except Exception as e:
        print(f"Erro ao enviar: {e}")
        return False

def main():
    tempo_inicio = time.time()
    
    alunos = buscar_alunos_hortolandia()
    if not alunos:
        print("Erro: Nenhum aluno encontrado")
        return
    
    print(f"Total: {len(alunos)} alunos")
    
    cookies_dict = fazer_login()
    
    print("="*60)
    print(f"INICIANDO COLETA DE {len(alunos)} ALUNOS")
    print("="*60)
    
    todos_dados = executar_coleta_paralela(alunos, NUM_THREADS, cookies_dict)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'='*60}")
    print(f"CONCLUÍDO EM {tempo_total/60:.1f} MINUTOS")
    print(f"{'='*60}")
    
    total_reg = sum(len(v) for v in todos_dados.values())
    print(f"\nTotal: {total_reg} registros coletados")
    
    for k, v in todos_dados.items():
        if len(v) > 0:
            print(f"  {k}: {len(v)}")
    
    if total_reg > 0:
        enviar_dados_para_sheets(alunos, todos_dados, tempo_total)
        print(f"\nSUCESSO!\n")
    else:
        print(f"\nNenhum dado coletado\n")

if __name__ == "__main__":
    main()
