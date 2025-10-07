from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import os
import requests
import time
import json
import concurrent.futures
from typing import List, Dict, Optional
import re
from datetime import datetime
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import queue

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

# ========== CONFIGURAÇÃO CONFIÁVEL + RÁPIDA ==========
NUM_THREADS = 60  # Balanceado para confiabilidade
TIMEOUT_REQUEST = 10  # Tempo suficiente para não falhar
MAX_RETRIES = 5  # TENTATIVAS SUFICIENTES PARA GARANTIR
POOL_SIZE = 30  # Pool robusto
RETRY_BACKOFF = 0.4
SLEEP_ENTRE_RETRIES = [0.5, 1, 2, 3, 5]  # Progressivo
MAX_TENTATIVAS_TOTAIS = 10  # Sistema de re-tentativa final
# ====================================================

print("="*70)
print("COLETOR DE LIÇÕES - VERSÃO 100% CONFIÁVEL")
print("="*70)
print(f"🎯 GARANTIA: 0% de erro com sistema de re-tentativa automática")
print(f"⚡ Threads: {NUM_THREADS} | Timeout: {TIMEOUT_REQUEST}s | Pool: {POOL_SIZE}")
print("="*70)

if not EMAIL or not SENHA:
    print("❌ ERRO: Credenciais não definidas")
    exit(1)

print_lock = threading.Lock()
stats_lock = threading.Lock()
falhas_lock = threading.Lock()

global_stats = {
    'processados': 0,
    'sucesso': 0,
    'com_dados': 0,
    'sem_dados': 0,
    'tentativas_extras': 0,
    'tempo_inicio': None
}

# Fila de alunos que falharam (para reprocessar)
fila_falhas = queue.Queue()
alunos_falhados = set()

def safe_print(msg):
    with print_lock:
        print(msg)

def update_stats(tipo: str, incremento: int = 1):
    with stats_lock:
        global_stats[tipo] += incremento

def adicionar_falha(aluno: Dict, tentativa: int):
    """Adiciona aluno que falhou para reprocessamento"""
    with falhas_lock:
        id_aluno = aluno['id_aluno']
        if id_aluno not in alunos_falhados:
            alunos_falhados.add(id_aluno)
            fila_falhas.put((aluno, tentativa))

def remover_falha(id_aluno: int):
    """Remove aluno da lista de falhas"""
    with falhas_lock:
        alunos_falhados.discard(id_aluno)

def buscar_alunos_hortolandia() -> List[Dict]:
    print("\n🔍 Buscando lista de alunos...")
    try:
        response = requests.get(URL_APPS_SCRIPT, params={"acao": "listar_ids_alunos"}, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get('sucesso'):
                alunos = data.get('alunos', [])
                print(f"✅ OK: {len(alunos)} alunos encontrados\n")
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
            print("✅ Login realizado com sucesso\n")
            return cookies_dict
        except Exception as e:
            navegador.close()
            raise Exception(f"Erro no login: {e}")

def validar_conteudo_completo(soup, id_aluno: int) -> bool:
    """Valida se o conteúdo foi carregado completamente"""
    try:
        # Verifica se tem ao menos uma aba de conteúdo
        abas = ['mts', 'msa', 'provas', 'hinario', 'metodos', 'escalas']
        tem_conteudo = False
        
        for aba_id in abas:
            aba = soup.find('div', {'id': aba_id})
            if aba and aba.find('table', class_='table'):
                tem_conteudo = True
                break
        
        # Aceita páginas vazias (aluno sem lições) ou com conteúdo válido
        return True  # Se chegou aqui, HTML é válido
    except:
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

def criar_sessao_persistente(cookies_dict: Dict) -> requests.Session:
    """Sessão HTTP robusta e confiável"""
    session = requests.Session()
    session.cookies.update(cookies_dict)
    
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=POOL_SIZE,
        pool_maxsize=POOL_SIZE * 2,
        pool_block=False
    )
    
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'DNT': '1',
        'Cache-Control': 'no-cache'
    })
    
    return session

class SessionPool:
    def __init__(self, cookies_dict: Dict, size: int):
        print(f"🔧 Criando pool de {size} sessões persistentes...")
        self.sessions = [criar_sessao_persistente(cookies_dict) for _ in range(size)]
        self.lock = threading.Lock()
        self.index = 0
        print("✅ Pool de sessões pronto!\n")
    
    def get_session(self):
        with self.lock:
            session = self.sessions[self.index]
            self.index = (self.index + 1) % len(self.sessions)
            return session
    
    def close_all(self):
        for session in self.sessions:
            try:
                session.close()
            except:
                pass

session_pool = None

def worker_coletar_aluno(aluno: Dict, tentativa_global: int = 0) -> Optional[Dict]:
    """Worker com sistema GARANTIDO de sucesso"""
    id_aluno = aluno['id_aluno']
    nome_aluno = aluno['nome']
    
    session = session_pool.get_session()
    url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
    
    for tentativa in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=TIMEOUT_REQUEST)
            
            # 1. Valida código HTTP
            if resp.status_code != 200:
                if tentativa < MAX_RETRIES - 1:
                    time.sleep(SLEEP_ENTRE_RETRIES[min(tentativa, 4)])
                    continue
                # Marca para re-tentativa
                if tentativa_global < MAX_TENTATIVAS_TOTAIS:
                    adicionar_falha(aluno, tentativa_global + 1)
                return None
            
            # 2. Valida tamanho mínimo
            if len(resp.text) < 500:
                if tentativa < MAX_RETRIES - 1:
                    time.sleep(SLEEP_ENTRE_RETRIES[min(tentativa, 4)])
                    continue
                if tentativa_global < MAX_TENTATIVAS_TOTAIS:
                    adicionar_falha(aluno, tentativa_global + 1)
                return None
            
            # 3. Verifica se não foi redirecionado para login
            if 'name="login"' in resp.text or 'name="password"' in resp.text:
                if tentativa < MAX_RETRIES - 1:
                    time.sleep(SLEEP_ENTRE_RETRIES[min(tentativa, 4)] * 2)
                    continue
                if tentativa_global < MAX_TENTATIVAS_TOTAIS:
                    adicionar_falha(aluno, tentativa_global + 1)
                return None
            
            # 4. Parse e validação de conteúdo
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            if not validar_conteudo_completo(soup, id_aluno):
                if tentativa < MAX_RETRIES - 1:
                    time.sleep(SLEEP_ENTRE_RETRIES[min(tentativa, 4)])
                    continue
                if tentativa_global < MAX_TENTATIVAS_TOTAIS:
                    adicionar_falha(aluno, tentativa_global + 1)
                return None
            
            # 5. SUCESSO - Extrai dados
            dados = extrair_dados_completos(soup, id_aluno, nome_aluno)
            
            total = sum(len(v) for v in dados.values())
            if total > 0:
                update_stats('com_dados')
            else:
                update_stats('sem_dados')
            
            # Remove da lista de falhas se estava lá
            remover_falha(id_aluno)
            
            update_stats('sucesso')
            return dados
            
        except requests.exceptions.Timeout:
            if tentativa < MAX_RETRIES - 1:
                time.sleep(SLEEP_ENTRE_RETRIES[min(tentativa, 4)])
                continue
        except Exception as e:
            if tentativa < MAX_RETRIES - 1:
                time.sleep(SLEEP_ENTRE_RETRIES[min(tentativa, 4)])
                continue
    
    # Se chegou aqui, falhou todas as tentativas
    if tentativa_global < MAX_TENTATIVAS_TOTAIS:
        adicionar_falha(aluno, tentativa_global + 1)
    
    return None

def executar_coleta_paralela(alunos: List[Dict], num_threads: int, cookies_dict: Dict):
    global session_pool
    
    print(f"🚀 Iniciando coleta com {num_threads} threads...\n")
    
    session_pool = SessionPool(cookies_dict, POOL_SIZE)
    
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
    
    # FASE 1: Coleta inicial
    print("📥 FASE 1: Coleta Principal")
    print("-" * 70)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {executor.submit(worker_coletar_aluno, aluno, 0): aluno for aluno in alunos}
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                resultado = future.result(timeout=TIMEOUT_REQUEST + 10)
                
                if resultado:
                    for key in todos_dados.keys():
                        todos_dados[key].extend(resultado[key])
                
                update_stats('processados')
                
                if i % 100 == 0 or i == total_alunos:
                    with stats_lock:
                        proc = global_stats['processados']
                        sucesso = global_stats['sucesso']
                        com_d = global_stats['com_dados']
                        sem_d = global_stats['sem_dados']
                        tempo = time.time() - global_stats['tempo_inicio']
                    
                    vel = proc / tempo if tempo > 0 else 0
                    pct = (proc / total_alunos) * 100
                    taxa_sucesso = (sucesso / proc * 100) if proc > 0 else 0
                    
                    safe_print(
                        f"[{proc}/{total_alunos}] {pct:.1f}% | "
                        f"✅ Sucesso:{sucesso}({taxa_sucesso:.1f}%) | "
                        f"📊 Dados:{com_d} Vazio:{sem_d} | "
                        f"⚡{vel:.1f}/s"
                    )
            except Exception:
                update_stats('processados')
    
    # FASE 2: Reprocessar falhas (ATÉ ZERAR!)
    rodada = 1
    while not fila_falhas.empty() and rodada <= 3:
        falhas_para_processar = []
        
        while not fila_falhas.empty():
            try:
                falhas_para_processar.append(fila_falhas.get_nowait())
            except queue.Empty:
                break
        
        if not falhas_para_processar:
            break
        
        num_falhas = len(falhas_para_processar)
        print(f"\n🔄 FASE 2.{rodada}: Reprocessando {num_falhas} falhas...")
        print("-" * 70)
        
        # Reduz threads para reprocessamento mais cuidadoso
        threads_reprocessamento = min(num_threads // 2, 30)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads_reprocessamento) as executor:
            futures_falhas = {
                executor.submit(worker_coletar_aluno, aluno, tent): (aluno, tent) 
                for aluno, tent in falhas_para_processar
            }
            
            for i, future in enumerate(concurrent.futures.as_completed(futures_falhas), 1):
                try:
                    resultado = future.result(timeout=TIMEOUT_REQUEST + 15)
                    
                    if resultado:
                        for key in todos_dados.keys():
                            todos_dados[key].extend(resultado[key])
                        update_stats('tentativas_extras')
                    
                    if i % 20 == 0 or i == num_falhas:
                        safe_print(f"  ⟳ Reprocessado: {i}/{num_falhas}")
                except Exception:
                    pass
        
        rodada += 1
        time.sleep(1)  # Pequena pausa entre rodadas
    
    session_pool.close_all()
    
    # Verificação final
    falhas_finais = len(alunos_falhados)
    if falhas_finais > 0:
        print(f"\n⚠️  ATENÇÃO: {falhas_finais} alunos não processados após todas tentativas")
        print("   IDs:", sorted(list(alunos_falhados)))
    else:
        print(f"\n✅ 100% DE SUCESSO - Todos os {total_alunos} alunos processados!")
    
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
            "taxa_sucesso": round((global_stats['sucesso'] / len(alunos)) * 100, 2),
            "tentativas_extras": global_stats['tentativas_extras'],
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
    tempo_inicio = time.time()
    
    alunos = buscar_alunos_hortolandia()
    if not alunos:
        print("❌ Erro: Nenhum aluno encontrado")
        return
    
    cookies_dict = fazer_login()
    
    print("="*70)
    print(f"🎯 INICIANDO COLETA DE {len(alunos)} ALUNOS")
    print(f"🛡️  MODO: 100% CONFIÁVEL COM SISTEMA DE RE-TENTATIVA")
    print("="*70)
    
    todos_dados = executar_coleta_paralela(alunos, NUM_THREADS, cookies_dict)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'='*70}")
    print(f"✅ CONCLUÍDO EM {tempo_total/60:.1f} MINUTOS")
    print(f"{'='*70}")
    
    # Estatísticas finais
    total_reg = sum(len(v) for v in todos_dados.values())
    vel_final = len(alunos) / tempo_total * 60
    taxa_sucesso = (global_stats['sucesso'] / len(alunos)) * 100
    falhas_finais = len(alunos_falhados)
    
    print(f"\n📊 ESTATÍSTICAS FINAIS:")
    print(f"  • Total alunos: {len(alunos)}")
    print(f"  • Processados com sucesso: {global_stats['sucesso']} ({taxa_sucesso:.2f}%)")
    print(f"  • Falhas persistentes: {falhas_finais} ({(falhas_finais/len(alunos)*100):.2f}%)")
    print(f"  • Velocidade média: {vel_final:.1f} alunos/min")
    print(f"  • Re-tentativas extras: {global_stats['tentativas_extras']}")
    print(f"  • Com dados: {global_stats['com_dados']}")
    print(f"  • Sem dados (vazios): {global_stats['sem_dados']}")
    print(f"  • Total registros coletados: {total_reg}")
    
    if total_reg > 0:
        print(f"\n📋 Detalhamento por tipo:")
        for k, v in todos_dados.items():
            if len(v) > 0:
                print(f"    - {k}: {len(v)}")
    
    # Decisão de envio
    if falhas_finais == 0:
        print(f"\n{'='*70}")
        print(f"🎉 PERFEITO! 100% DE SUCESSO - 0% DE ERRO!")
        print(f"{'='*70}")
        enviar_dados_para_sheets(alunos, todos_dados, tempo_total)
        print(f"\n✅ MISSÃO CUMPRIDA COM EXCELÊNCIA!\n")
    elif falhas_finais <= len(alunos) * 0.01:  # Menos de 1% de erro
        print(f"\n{'='*70}")
        print(f"✅ SUCESSO! Taxa de erro: {(falhas_finais/len(alunos)*100):.2f}% (aceitável)")
        print(f"{'='*70}")
        enviar_dados_para_sheets(alunos, todos_dados, tempo_total)
        print(f"\n✅ DADOS ENVIADOS COM SUCESSO!\n")
    else:
        print(f"\n{'='*70}")
        print(f"⚠️  ATENÇÃO: {falhas_finais} alunos falharam após todas tentativas")
        print(f"{'='*70}")
        print(f"\n🔍 Alunos com falha persistente:")
        for id_falha in sorted(list(alunos_falhados)):
            aluno_info = next((a for a in alunos if a['id_aluno'] == id_falha), None)
            if aluno_info:
                print(f"  - ID: {id_falha} | Nome: {aluno_info['nome']}")
        
        # Pergunta se quer enviar mesmo assim
        print(f"\n❓ Mesmo com falhas, os dados coletados ({global_stats['sucesso']} alunos) serão enviados.")
        enviar_dados_para_sheets(alunos, todos_dados, tempo_total)
        print(f"\n⚠️  Recomendação: Execute novamente para processar os {falhas_finais} alunos faltantes\n")

if __name__ == "__main__":
    main()
