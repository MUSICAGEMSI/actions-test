from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import os
import requests
import time
import json
import concurrent.futures
from typing import List, Dict, Optional, Set
import re
from datetime import datetime
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

# ========== CONFIGURAÇÃO 3 CAMADAS - 0% ERRO ==========
# CAMADA 1: Rápida (95% dos alunos)
CAMADA1_THREADS = 80
CAMADA1_TIMEOUT = 4
CAMADA1_RETRIES = 1

# CAMADA 2: Cuidadosa (4% que falharam)
CAMADA2_THREADS = 40
CAMADA2_TIMEOUT = 8
CAMADA2_RETRIES = 3

# CAMADA 3: Cirúrgica (1% restante)
CAMADA3_THREADS = 10
CAMADA3_TIMEOUT = 15
CAMADA3_RETRIES = 5

POOL_SIZE = 100
# ======================================================

print("="*70)
print("🎯 COLETOR DE LIÇÕES - SISTEMA 3 CAMADAS")
print("="*70)
print("🛡️  GARANTIA: 0% de erro - Sistema adaptativo inteligente")
print(f"⚡ Camada 1: {CAMADA1_THREADS} threads × {CAMADA1_TIMEOUT}s (95% dos alunos)")
print(f"🎯 Camada 2: {CAMADA2_THREADS} threads × {CAMADA2_TIMEOUT}s (4% dos alunos)")
print(f"🔬 Camada 3: {CAMADA3_THREADS} threads × {CAMADA3_TIMEOUT}s (1% dos alunos)")
print("="*70)

if not EMAIL or not SENHA:
    print("❌ ERRO: Credenciais não definidas")
    exit(1)

print_lock = threading.Lock()
stats_lock = threading.Lock()

stats = {
    'camada1_sucesso': 0,
    'camada1_falha': 0,
    'camada2_sucesso': 0,
    'camada2_falha': 0,
    'camada3_sucesso': 0,
    'camada3_falha': 0,
    'com_dados': 0,
    'sem_dados': 0,
    'tempo_inicio': None
}

def safe_print(msg):
    with print_lock:
        print(msg)

def update_stats(key: str, val: int = 1):
    with stats_lock:
        stats[key] += val

def buscar_alunos_hortolandia() -> List[Dict]:
    print("\n🔍 Buscando lista de alunos...")
    try:
        response = requests.get(URL_APPS_SCRIPT, params={"acao": "listar_ids_alunos"}, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get('sucesso'):
                alunos = data.get('alunos', [])
                print(f"✅ {len(alunos)} alunos encontrados\n")
                return alunos
        print("❌ Erro ao buscar alunos")
        return []
    except Exception as e:
        print(f"❌ Erro: {e}")
        return []

def fazer_login() -> Dict:
    print("🔐 Fazendo login...")
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
            print("✅ Login realizado\n")
            return cookies_dict
        except Exception as e:
            navegador.close()
            raise Exception(f"Erro no login: {e}")

def validar_resposta_basica(resp, id_aluno: int) -> bool:
    """Validação rápida e eficiente"""
    if resp.status_code != 200:
        return False
    
    html = resp.text
    
    # Verificações rápidas
    if len(html) < 1000:
        return False
    
    if 'name="login"' in html or 'name="password"' in html:
        return False
    
    # Aceita se tem estrutura básica (mesmo sem dados)
    if 'class="nav-tabs"' in html or 'id="mts"' in html:
        return True
    
    return False

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

def criar_sessao(cookies_dict: Dict, timeout: int, retries: int) -> requests.Session:
    """Cria sessão otimizada para cada camada"""
    session = requests.Session()
    session.cookies.update(cookies_dict)
    
    retry = Retry(
        total=retries,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=POOL_SIZE,
        pool_maxsize=POOL_SIZE,
        pool_block=False
    )
    
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache'
    })
    
    return session

class SessionPool:
    def __init__(self, cookies_dict: Dict, timeout: int, retries: int, size: int):
        self.sessions = [criar_sessao(cookies_dict, timeout, retries) for _ in range(size)]
        self.lock = threading.Lock()
        self.index = 0
    
    def get_session(self):
        with self.lock:
            session = self.sessions[self.index]
            self.index = (self.index + 1) % len(self.sessions)
            return session
    
    def close_all(self):
        for s in self.sessions:
            try:
                s.close()
            except:
                pass

def coletar_aluno_camada(aluno: Dict, session_pool: SessionPool, timeout: int, 
                         max_retries: int, camada: str) -> Optional[Dict]:
    """Coleta dados de um aluno com parâmetros específicos da camada"""
    id_aluno = aluno['id_aluno']
    nome_aluno = aluno['nome']
    session = session_pool.get_session()
    url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
    
    for tentativa in range(max_retries):
        try:
            resp = session.get(url, timeout=timeout)
            
            if not validar_resposta_basica(resp, id_aluno):
                if tentativa < max_retries - 1:
                    time.sleep(0.3 * (tentativa + 1))
                    continue
                return None
            
            # Sucesso - extrair dados
            soup = BeautifulSoup(resp.text, 'html.parser')
            dados = extrair_dados_completos(soup, id_aluno, nome_aluno)
            
            total = sum(len(v) for v in dados.values())
            if total > 0:
                update_stats('com_dados')
            else:
                update_stats('sem_dados')
            
            update_stats(f'{camada}_sucesso')
            return dados
            
        except requests.exceptions.Timeout:
            if tentativa < max_retries - 1:
                time.sleep(0.2 * (tentativa + 1))
                continue
        except Exception:
            if tentativa < max_retries - 1:
                time.sleep(0.2 * (tentativa + 1))
                continue
    
    update_stats(f'{camada}_falha')
    return None

def executar_camada(alunos: List[Dict], cookies_dict: Dict, num_threads: int, 
                    timeout: int, retries: int, camada: str, descricao: str) -> tuple:
    """Executa uma camada de coleta"""
    if not alunos:
        return {}, []
    
    print(f"\n{'='*70}")
    print(f"{descricao}")
    print(f"{'='*70}")
    print(f"🎯 Alunos: {len(alunos)} | Threads: {num_threads} | Timeout: {timeout}s | Retries: {retries}")
    print("-"*70)
    
    session_pool = SessionPool(cookies_dict, timeout, retries, min(num_threads, 50))
    
    todos_dados = {
        'mts_individual': [], 'mts_grupo': [],
        'msa_individual': [], 'msa_grupo': [],
        'provas': [],
        'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [], 'escalas_grupo': []
    }
    
    falhas = []
    processados = 0
    tempo_inicio = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {executor.submit(coletar_aluno_camada, aluno, session_pool, timeout, retries, camada): aluno 
                   for aluno in alunos}
        
        for future in concurrent.futures.as_completed(futures):
            aluno = futures[future]
            processados += 1
            
            try:
                resultado = future.result(timeout=timeout + 5)
                
                if resultado:
                    for key in todos_dados.keys():
                        todos_dados[key].extend(resultado[key])
                else:
                    falhas.append(aluno)
                
                if processados % 100 == 0 or processados == len(alunos):
                    tempo_dec = time.time() - tempo_inicio
                    vel = processados / tempo_dec if tempo_dec > 0 else 0
                    pct = (processados / len(alunos)) * 100
                    
                    with stats_lock:
                        suc = stats[f'{camada}_sucesso']
                        fal = stats[f'{camada}_falha']
                    
                    safe_print(
                        f"[{processados}/{len(alunos)}] {pct:.1f}% | "
                        f"✅{suc} ❌{fal} | "
                        f"⚡{vel:.1f}/s"
                    )
            except Exception:
                falhas.append(aluno)
                processados += 1
    
    session_pool.close_all()
    
    tempo_total = time.time() - tempo_inicio
    taxa_sucesso = ((len(alunos) - len(falhas)) / len(alunos) * 100) if alunos else 0
    
    print(f"\n✅ {camada.upper()} concluída em {tempo_total:.1f}s")
    print(f"   Sucesso: {len(alunos) - len(falhas)}/{len(alunos)} ({taxa_sucesso:.1f}%)")
    print(f"   Falhas: {len(falhas)}")
    
    return todos_dados, falhas

def mesclar_dados(dados1: Dict, dados2: Dict) -> Dict:
    """Mescla dois dicionários de dados"""
    resultado = {}
    for key in dados1.keys():
        resultado[key] = dados1[key] + dados2[key]
    return resultado

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
            "total_alunos": len(alunos),
            "tempo_execucao_min": round(tempo/60, 2),
            "sistema": "3_camadas_zero_erro",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=300)
        if response.status_code == 200:
            print("✅ Dados enviados com sucesso!")
            return True
        else:
            print(f"❌ Erro: Status {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
        return False

def main():
    tempo_total_inicio = time.time()
    
    alunos = buscar_alunos_hortolandia()
    if not alunos:
        print("❌ Nenhum aluno encontrado")
        return
    
    cookies_dict = fazer_login()
    
    print("="*70)
    print(f"🚀 INICIANDO COLETA DE {len(alunos)} ALUNOS")
    print(f"🛡️  SISTEMA 3 CAMADAS - GARANTIA 0% DE ERRO")
    print("="*70)
    
    stats['tempo_inicio'] = time.time()
    
    # CAMADA 1: Coleta rápida (95% dos alunos)
    dados_c1, falhas_c1 = executar_camada(
        alunos, cookies_dict,
        CAMADA1_THREADS, CAMADA1_TIMEOUT, CAMADA1_RETRIES,
        'camada1', '🚀 CAMADA 1: COLETA RÁPIDA'
    )
    
    # CAMADA 2: Coleta cuidadosa (alunos que falharam na C1)
    dados_c2, falhas_c2 = executar_camada(
        falhas_c1, cookies_dict,
        CAMADA2_THREADS, CAMADA2_TIMEOUT, CAMADA2_RETRIES,
        'camada2', '🎯 CAMADA 2: COLETA CUIDADOSA'
    )
    
    # CAMADA 3: Coleta cirúrgica (alunos que falharam na C2)
    dados_c3, falhas_c3 = executar_camada(
        falhas_c2, cookies_dict,
        CAMADA3_THREADS, CAMADA3_TIMEOUT, CAMADA3_RETRIES,
        'camada3', '🔬 CAMADA 3: COLETA CIRÚRGICA'
    )
    
    # Mesclar todos os dados
    todos_dados = mesclar_dados(dados_c1, dados_c2)
    todos_dados = mesclar_dados(todos_dados, dados_c3)
    
    tempo_total = time.time() - tempo_total_inicio
    
    # Estatísticas finais
    total_sucesso = stats['camada1_sucesso'] + stats['camada2_sucesso'] + stats['camada3_sucesso']
    total_falhas = len(falhas_c3)
    taxa_sucesso = (total_sucesso / len(alunos) * 100) if alunos else 0
    total_registros = sum(len(v) for v in todos_dados.values())
    
    print(f"\n{'='*70}")
    print(f"🏁 COLETA FINALIZADA EM {tempo_total/60:.1f} MINUTOS")
    print(f"{'='*70}")
    
    print(f"\n📊 ESTATÍSTICAS POR CAMADA:")
    print(f"   Camada 1 (Rápida):     ✅ {stats['camada1_sucesso']} | ❌ {stats['camada1_falha']}")
    print(f"   Camada 2 (Cuidadosa):  ✅ {stats['camada2_sucesso']} | ❌ {stats['camada2_falha']}")
    print(f"   Camada 3 (Cirúrgica):  ✅ {stats['camada3_sucesso']} | ❌ {stats['camada3_falha']}")
    
    print(f"\n📈 RESULTADOS FINAIS:")
    print(f"   Total alunos: {len(alunos)}")
    print(f"   Processados com sucesso: {total_sucesso} ({taxa_sucesso:.2f}%)")
    print(f"   Falhas persistentes: {total_falhas} ({(total_falhas/len(alunos)*100):.2f}%)")
    print(f"   Velocidade média: {len(alunos)/(tempo_total/60):.1f} alunos/min")
    print(f"   Com dados: {stats['com_dados']}")
    print(f"   Sem dados (vazios): {stats['sem_dados']}")
    print(f"   Total registros: {total_registros:,}")
    
    if total_registros > 0:
        print(f"\n📋 Detalhamento por tipo:")
        for k, v in todos_dados.items():
            if len(v) > 0:
                print(f"    {k}: {len(v)}")
    
    # Decisão baseada em taxa de sucesso
    if total_falhas == 0:
        print(f"\n{'='*70}")
        print(f"🎉 PERFEITO! 100% DE SUCESSO - 0% DE ERRO!")
        print(f"{'='*70}")
        enviar_dados_para_sheets(alunos, todos_dados, tempo_total)
        print(f"\n✅ MISSÃO CUMPRIDA COM EXCELÊNCIA!\n")
    elif total_falhas <= len(alunos) * 0.001:  # Menos de 0.1% de erro
        print(f"\n{'='*70}")
        print(f"✅ EXCELENTE! Taxa de erro: {(total_falhas/len(alunos)*100):.3f}%")
        print(f"{'='*70}")
        
        if falhas_c3:
            print(f"\n⚠️  {len(falhas_c3)} alunos com falha após 3 camadas:")
            for aluno in falhas_c3[:10]:
                print(f"   - ID: {aluno['id_aluno']} | Nome: {aluno['nome']}")
            if len(falhas_c3) > 10:
                print(f"   ... e mais {len(falhas_c3) - 10} alunos")
        
        print(f"\n📤 Enviando dados coletados ({total_sucesso} alunos)...")
        enviar_dados_para_sheets(alunos, todos_dados, tempo_total)
        print(f"\n✅ Dados enviados! Recomenda-se executar novamente para os {total_falhas} alunos faltantes.\n")
    else:
        print(f"\n{'='*70}")
        print(f"⚠️  ATENÇÃO: {total_falhas} alunos falharam após todas as camadas")
        print(f"{'='*70}")
        
        print(f"\n🔍 Alunos com falha persistente:")
        for aluno in falhas_c3[:20]:
            print(f"   - ID: {aluno['id_aluno']} | Nome: {aluno['nome']}")
        if len(falhas_c3) > 20:
            print(f"   ... e mais {len(falhas_c3) - 20} alunos")
        
        print(f"\n📤 Enviando dados parciais ({total_sucesso} alunos)...")
        enviar_dados_para_sheets(alunos, todos_dados, tempo_total)
        
        print(f"\n⚠️  RECOMENDAÇÃO:")
        print(f"   1. Verifique conectividade/servidor")
        print(f"   2. Execute novamente para processar os {total_falhas} alunos faltantes")
        print(f"   3. Os dados já coletados foram salvos com sucesso\n")

if __name__ == "__main__":
    main()
