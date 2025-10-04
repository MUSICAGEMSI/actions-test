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

# ========================================
# CONFIGURAÇÕES ALTA PERFORMANCE
# ========================================

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

NUM_THREADS = 80
TIMEOUT_REQUEST = 12
BATCH_SIZE = 500
MAX_RETRIES = 2
POOL_CONNECTIONS = 20
POOL_MAXSIZE = 100

print(f"═══════════════════════════════════════════════")
print(f"  COLETOR DE LIÇÕES - ALTA PERFORMANCE")
print(f"═══════════════════════════════════════════════")
print(f"  Threads: {NUM_THREADS}")
print(f"  Timeout: {TIMEOUT_REQUEST}s")
print(f"  Batch: {BATCH_SIZE} alunos")
print(f"═══════════════════════════════════════════════\n")

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
    'tempo_inicio': None,
    'pausar': False
}

def safe_print(msg):
    with print_lock:
        print(msg)

def update_stats(tipo: str):
    with stats_lock:
        global_stats[tipo] += 1
        # Circuit breaker: pausar se taxa de erro > 30%
        if global_stats['processados'] > 50:
            taxa_erro = global_stats['erros'] / global_stats['processados']
            if taxa_erro > 0.3:
                global_stats['pausar'] = True

# ========================================
# BUSCAR ALUNOS
# ========================================

def buscar_alunos_hortolandia() -> List[Dict]:
    print("Buscando lista de alunos...")
    
    try:
        params = {"acao": "listar_ids_alunos"}
        response = requests.get(URL_APPS_SCRIPT, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('sucesso'):
                alunos = data.get('alunos', [])
                print(f"OK: {len(alunos)} alunos carregados\n")
                return alunos
            else:
                print(f"Erro: {data.get('erro')}")
                return []
        else:
            print(f"Erro HTTP: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"Erro ao buscar alunos: {e}")
        return []

# ========================================
# EXTRAÇÃO DE DADOS
# ========================================

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

# ========================================
# SESSÃO HTTP
# ========================================

def criar_sessao(cookies_dict: Dict) -> requests.Session:
    session = requests.Session()
    session.cookies.update(cookies_dict)
    
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=0.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=POOL_CONNECTIONS,
        pool_maxsize=POOL_MAXSIZE,
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

# ========================================
# WORKER
# ========================================

def worker_coletar_aluno(aluno: Dict, cookies_dict: Dict) -> Dict:
    if global_stats['pausar']:
        return None
    
    id_aluno = aluno['id_aluno']
    nome_aluno = aluno['nome']
    session = criar_sessao(cookies_dict)
    
    try:
        url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
        resp = session.get(url, timeout=TIMEOUT_REQUEST)
        
        if resp.status_code != 200 or len(resp.text) < 1000 or "login" in resp.text.lower():
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
        update_stats('erros')
        update_stats('processados')
        return None
    finally:
        session.close()

# ========================================
# COLETA PARALELA COM BATCHING
# ========================================

def executar_coleta_paralela(cookies_dict: Dict, alunos: List[Dict], num_threads: int):
    print(f"Iniciando coleta com {num_threads} threads paralelas...\n")
    
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
    
    # Dividir em batches
    for batch_num in range(0, len(alunos), BATCH_SIZE):
        batch = alunos[batch_num:batch_num + BATCH_SIZE]
        batch_size = len(batch)
        
        print(f"\n--- BATCH {batch_num//BATCH_SIZE + 1}: Processando {batch_size} alunos ---")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = {
                executor.submit(worker_coletar_aluno, aluno, cookies_dict): aluno 
                for aluno in batch
            }
            
            for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
                try:
                    resultado = future.result(timeout=TIMEOUT_REQUEST + 5)
                    
                    if resultado:
                        for key in todos_dados.keys():
                            todos_dados[key].extend(resultado[key])
                    
                    if i % 50 == 0 or i == batch_size:
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
                            f"[{proc}/{total_alunos}] {pct:.1f}% | "
                            f"OK:{com_d} Vazio:{sem_d} Erro:{erros} | "
                            f"{velocidade:.1f}/s | Resta:{tempo_est:.0f}s"
                        )
                
                except Exception:
                    pass
        
        # Checkpoint a cada batch
        if batch_num > 0 and batch_num % BATCH_SIZE == 0:
            salvar_checkpoint(alunos[:batch_num + BATCH_SIZE], todos_dados)
    
    return todos_dados

# ========================================
# RESUMO E ENVIO
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
        
        provas = []
        for p in todos_dados['provas']:
            if p[0] == id_aluno:
                try:
                    nota_str = str(p[3]).replace(',', '.').strip()
                    if nota_str and nota_str.replace('.', '').isdigit():
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
            print("OK: Dados enviados com sucesso!")
            return True
        else:
            print(f"Erro: Status {response.status_code}")
            return False
            
    except Exception as e:
        print(f"Erro ao enviar: {e}")
        return False

def salvar_checkpoint(alunos: List[Dict], todos_dados: Dict):
    try:
        nome = f"checkpoint_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(nome, 'w', encoding='utf-8') as f:
            json.dump({"alunos": alunos, "dados": todos_dados}, f, indent=2, ensure_ascii=False)
        safe_print(f"Checkpoint salvo: {nome}")
    except:
        pass

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
        print("Erro: Nenhum aluno encontrado")
        return
    
    print(f"Total: {len(alunos)} alunos\n")
    print("Realizando login...")
    
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
                print("Erro: Login falhou")
                navegador.close()
                return
            
            print("Login OK!\n")
            
        except Exception as e:
            print(f"Erro no login: {e}")
            navegador.close()
            return
        
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
    
    print(f"{'═'*60}")
    print(f"  INICIANDO COLETA DE {len(alunos)} ALUNOS")
    print(f"{'═'*60}\n")
    
    todos_dados = executar_coleta_paralela(cookies_dict, alunos, NUM_THREADS)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'═'*60}")
    print(f"  CONCLUÍDO EM {tempo_total/60:.1f} MINUTOS")
    print(f"{'═'*60}")
    
    total_reg = sum(len(v) for v in todos_dados.values())
    print(f"\nTotal: {total_reg} registros coletados")
    
    for k, v in todos_dados.items():
        if len(v) > 0:
            print(f"  {k}: {len(v)}")
    
    if total_reg > 0:
        enviar_dados_para_sheets(alunos, todos_dados, tempo_total)
        print(f"\nSUCESSO!\n")
    else:
        print(f"\nAviso: Nenhum dado coletado\n")

if __name__ == "__main__":
    main()
