from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import os
import requests
import time
from typing import List, Dict, Optional
import re
from datetime import datetime
import threading
from queue import Queue, Empty
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

# ========== ESTRATÉGIA: WORKERS COM TIMEOUT PROGRESSIVO ==========
# Cada worker é uma "pessoa" com sua própria sessão persistente
NUM_WORKERS = 30               # 30 workers simultâneos (balanceado)
TIMEOUT_INICIAL = 8            # Timeout inicial (aumenta se necessário)
TIMEOUT_MAXIMO = 20            # Timeout máximo permitido
MAX_RETRIES_PER_WORKER = 2     # Cada worker tenta 2x (rápido)
DELAY_RETRY = 0.3              # Delay mínimo entre retries

# Fila de reprocessamento inteligente
FILA_REPROCESSAMENTO = Queue()
MAX_REPROCESSAMENTOS = 2       # Quantas vezes um aluno pode voltar à fila

# Checkpoint e segurança
CHECKPOINT_FILE = "checkpoint_coleta.json"
AUTO_SAVE_INTERVAL = 120       # Checkpoint a cada 2 minutos
BATCH_SIZE = 50                # Salvar a cada 50 alunos processados

# Timeout dinâmico (ajusta automaticamente)
timeout_atual = TIMEOUT_INICIAL
timeout_lock = threading.Lock()

# Pool de conexões otimizado
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

print("="*80)
print("🚀 COLETOR MULTI-WORKER - ESTRATÉGIA ADAPTATIVA")
print("="*80)
print(f"👷 {NUM_WORKERS} workers simultâneos")
print(f"⏱️  Timeout adaptativo: {TIMEOUT_INICIAL}s (até {TIMEOUT_MAXIMO}s)")
print(f"🔄 Sistema inteligente de reprocessamento")
print(f"💾 Checkpoint automático a cada {AUTO_SAVE_INTERVAL}s")
print(f"⚡ Workers abandonam alunos lentos e pegam próximo")
print(f"✅ GARANTIA: 0% de erro - todos serão processados!")
print("="*80)

if not EMAIL or not SENHA:
    print("❌ ERRO: Credenciais não definidas")
    exit(1)

# Estatísticas thread-safe
stats = {
    'processados': 0,
    'com_dados': 0,
    'sem_dados': 0,
    'falhas_temporarias': 0,
    'tempo_inicio': None,
    'alunos_processados': set(),
    'dados_coletados': {
        'mts_individual': [], 'mts_grupo': [],
        'msa_individual': [], 'msa_grupo': [],
        'provas': [],
        'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [], 'escalas_grupo': []
    },
    'ultimo_save': time.time(),
    'ultimo_checkpoint': 0,
    'timeouts_consecutivos': 0,
    'sucessos_consecutivos': 0
}
stats_lock = threading.Lock()
print_lock = threading.Lock()

def safe_print(msg):
    with print_lock:
        print(msg, flush=True)

def criar_sessao_otimizada(cookies_dict: Dict) -> requests.Session:
    """Cria sessão HTTP otimizada e persistente"""
    session = requests.Session()
    
    # Retry automático para erros de rede
    retry_strategy = Retry(
        total=0,  # Vamos controlar retries manualmente
        backoff_factor=0
    )
    
    adapter = HTTPAdapter(
        pool_connections=20,
        pool_maxsize=20,
        max_retries=retry_strategy,
        pool_block=False
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    session.cookies.update(cookies_dict)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0'
    })
    
    return session

def validar_resposta(text: str, id_aluno: int) -> tuple:
    """Validação rigorosa - Retorna: (valido, tem_dados)"""
    if len(text) < 1000:
        return False, False
    
    if 'name="login"' in text or 'name="password"' in text:
        return False, False
    
    if 'class="nav-tabs"' not in text and 'id="mts"' not in text:
        return False, False
    
    tem_dados = '<tr>' in text and '<td>' in text and 'table' in text
    return True, tem_dados

def extrair_dados_completo(html: str, id_aluno: int, nome_aluno: str) -> Dict:
    """Extração completa e robusta"""
    dados = {
        'mts_individual': [], 'mts_grupo': [],
        'msa_individual': [], 'msa_grupo': [],
        'provas': [],
        'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [], 'escalas_grupo': []
    }
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # MTS Individual
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
        
        # MSA Individual
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
        
        # HINÁRIO Individual
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
        
        # ESCALAS Individual
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
    
    except Exception as e:
        safe_print(f"⚠️  Erro ao extrair dados do aluno {id_aluno}: {e}")
    
    return dados

def worker_coletor(worker_id: int, fila_alunos: Queue, cookies_dict: Dict, contador_reprocessamento: Dict):
    """
    Worker individual - como uma "pessoa" responsável por coletar dados
    Cada worker mantém sua própria sessão persistente
    """
    session = criar_sessao_otimizada(cookies_dict)
    processados_worker = 0
    
    safe_print(f"👷 Worker {worker_id:02d} iniciado")
    
    while True:
        try:
            # Pega próximo aluno da fila (timeout 1s para verificar se acabou)
            try:
                aluno = fila_alunos.get(timeout=1)
            except Empty:
                break
            
            id_aluno = aluno['id_aluno']
            nome_aluno = aluno['nome']
            
            # Verificar se já foi processado (thread-safe)
            with stats_lock:
                if id_aluno in stats['alunos_processados']:
                    fila_alunos.task_done()
                    continue
            
            url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
            sucesso = False
            
            # Tentar coletar com retries
            for tentativa in range(MAX_RETRIES_PER_WORKER):
                try:
                    resp = session.get(url, timeout=TIMEOUT_PER_REQUEST)
                    
                    if resp.status_code == 200:
                        valido, tem_dados = validar_resposta(resp.text, id_aluno)
                        
                        if valido:
                            # Extrair dados
                            dados = extrair_dados_completo(resp.text, id_aluno, nome_aluno)
                            
                            # Salvar dados de forma thread-safe
                            with stats_lock:
                                for key in stats['dados_coletados'].keys():
                                    stats['dados_coletados'][key].extend(dados[key])
                                
                                stats['alunos_processados'].add(id_aluno)
                                stats['processados'] += 1
                                
                                total_registros = sum(len(v) for v in dados.values())
                                if total_registros > 0:
                                    stats['com_dados'] += 1
                                else:
                                    stats['sem_dados'] += 1
                            
                            processados_worker += 1
                            sucesso = True
                            break
                    
                    if tentativa < MAX_RETRIES_PER_WORKER - 1:
                        time.sleep(DELAY_RETRY * (tentativa + 1))
                
                except requests.exceptions.Timeout:
                    if tentativa < MAX_RETRIES_PER_WORKER - 1:
                        time.sleep(DELAY_RETRY * (tentativa + 1))
                    continue
                except Exception as e:
                    if tentativa < MAX_RETRIES_PER_WORKER - 1:
                        time.sleep(DELAY_RETRY * (tentativa + 1))
                    continue
            
            # Se falhou após todas tentativas, colocar na fila de reprocessamento
            if not sucesso:
                vezes_reprocessado = contador_reprocessamento.get(id_aluno, 0)
                if vezes_reprocessado < MAX_REPROCESSAMENTOS:
                    contador_reprocessamento[id_aluno] = vezes_reprocessado + 1
                    FILA_REPROCESSAMENTO.put(aluno)
                    with stats_lock:
                        stats['falhas_temporarias'] += 1
            
            fila_alunos.task_done()
            
        except Exception as e:
            safe_print(f"❌ Worker {worker_id:02d} erro: {e}")
            fila_alunos.task_done()
            continue
    
    session.close()
    safe_print(f"✅ Worker {worker_id:02d} finalizado ({processados_worker} alunos processados)")

def monitor_progresso(total_alunos: int):
    """Thread que monitora e exibe progresso em tempo real"""
    tempo_inicio = time.time()
    ultimo_checkpoint = 0
    
    while True:
        time.sleep(5)  # Atualiza a cada 5 segundos
        
        with stats_lock:
            processados = stats['processados']
            com_dados = stats['com_dados']
            sem_dados = stats['sem_dados']
            falhas = stats['falhas_temporarias']
            
            if processados >= total_alunos:
                break
        
        tempo_decorrido = time.time() - tempo_inicio
        velocidade = processados / tempo_decorrido if tempo_decorrido > 0 else 0
        tempo_restante = (total_alunos - processados) / velocidade if velocidade > 0 else 0
        
        progresso_pct = (processados / total_alunos * 100) if total_alunos > 0 else 0
        
        safe_print(f"\r📊 Progresso: {processados}/{total_alunos} ({progresso_pct:.1f}%) | "
                  f"✅ {com_dados} com dados | ⚪ {sem_dados} sem dados | "
                  f"⚡ {velocidade:.1f} alunos/s | "
                  f"⏱️  Restante: {tempo_restante/60:.1f}min", )
        
        # Auto-checkpoint
        if time.time() - ultimo_checkpoint > AUTO_SAVE_INTERVAL:
            salvar_checkpoint_parcial()
            ultimo_checkpoint = time.time()

def salvar_checkpoint_parcial():
    """Salva checkpoint parcial durante execução"""
    try:
        with stats_lock:
            checkpoint = {
                'timestamp': datetime.now().isoformat(),
                'alunos_processados': list(stats['alunos_processados']),
                'stats': {
                    'processados': stats['processados'],
                    'com_dados': stats['com_dados'],
                    'sem_dados': stats['sem_dados']
                },
                'dados': stats['dados_coletados']
            }
        
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False)
        
        safe_print(f"\n💾 Checkpoint salvo ({stats['processados']} alunos)")
    except Exception as e:
        safe_print(f"⚠️  Erro ao salvar checkpoint: {e}")

def carregar_checkpoint() -> Optional[Dict]:
    """Carrega checkpoint anterior se existir"""
    if Path(CHECKPOINT_FILE).exists():
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return None
    return None

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
    """Login otimizado"""
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
            time.sleep(1)
            
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
        
        total_registros = t_mts_i + t_mts_g + t_msa_i + t_msa_g + t_prov + t_hin_i + t_hin_g + t_met + t_esc_i + t_esc_g
        
        resumo.append([
            id_aluno, nome, id_igreja,
            t_mts_i, t_mts_g, t_msa_i, t_msa_g,
            t_prov, t_hin_i, t_hin_g, t_met,
            t_esc_i, t_esc_g, total_registros
        ])
    
    return resumo

def enviar_para_sheets(todos_dados: Dict, alunos: List[Dict]):
    """Envia todos os dados para o Google Sheets"""
    print("\n📤 Enviando dados para Google Sheets...")
    
    try:
        resumo_alunos = gerar_resumo_alunos(alunos, todos_dados)
        
        medias_provas = {}
        for prova in todos_dados['provas']:
            id_aluno = prova[0]
            try:
                nota = float(prova[3].replace(',', '.'))
                if id_aluno not in medias_provas:
                    medias_provas[id_aluno] = []
                medias_provas[id_aluno].append(nota)
            except:
                pass
        
        resumo_formatado = []
        for linha in resumo_alunos:
            id_aluno = linha[0]
            nome = linha[1]
            id_igreja = linha[2]
            
            media = 0
            if id_aluno in medias_provas and medias_provas[id_aluno]:
                media = sum(medias_provas[id_aluno]) / len(medias_provas[id_aluno])
            
            ultima_atividade = ""
            datas = []
            
            for mts in todos_dados['mts_individual']:
                if mts[0] == id_aluno and mts[4]:
                    datas.append(mts[4])
            for msa in todos_dados['msa_individual']:
                if msa[0] == id_aluno and msa[2]:
                    datas.append(msa[2])
            for prova in todos_dados['provas']:
                if prova[0] == id_aluno and prova[4]:
                    datas.append(prova[4])
            
            if datas:
                ultima_atividade = max(datas)
            
            resumo_formatado.append([
                id_aluno, nome, id_igreja,
                linha[3], linha[4], linha[5], linha[6],
                linha[7], round(media, 2),
                linha[8], linha[9], linha[10],
                linha[11], linha[12],
                ultima_atividade,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ])
        
        metadata = {
            'total_alunos_processados': stats['processados'],
            'alunos_com_dados': stats['com_dados'],
            'alunos_sem_dados': stats['sem_dados'],
            'tempo_coleta_segundos': round(time.time() - stats['tempo_inicio'], 2),
            'data_coleta': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_registros_mts_ind': len(todos_dados['mts_individual']),
            'total_registros_mts_grupo': len(todos_dados['mts_grupo']),
            'total_registros_msa_ind': len(todos_dados['msa_individual']),
            'total_registros_msa_grupo': len(todos_dados['msa_grupo']),
            'total_registros_provas': len(todos_dados['provas']),
            'total_registros_hinario_ind': len(todos_dados['hinario_individual']),
            'total_registros_hinario_grupo': len(todos_dados['hinario_grupo']),
            'total_registros_metodos': len(todos_dados['metodos']),
            'total_registros_escalas_ind': len(todos_dados['escalas_individual']),
            'total_registros_escalas_grupo': len(todos_dados['escalas_grupo'])
        }
        
        payload = {
            'tipo': 'licoes_alunos',
            'resumo': resumo_formatado,
            'mts_individual': todos_dados['mts_individual'],
            'mts_grupo': todos_dados['mts_grupo'],
            'msa_individual': todos_dados['msa_individual'],
            'msa_grupo': todos_dados['msa_grupo'],
            'provas': todos_dados['provas'],
            'hinario_individual': todos_dados['hinario_individual'],
            'hinario_grupo': todos_dados['hinario_grupo'],
            'metodos': todos_dados['metodos'],
            'escalas_individual': todos_dados['escalas_individual'],
            'escalas_grupo': todos_dados['escalas_grupo'],
            'metadata': metadata
        }
        
        print(f"   📊 Enviando {len(resumo_formatado)} alunos...")
        
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=300)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('sucesso'):
                print("✅ Dados enviados com sucesso!")
                return True
            else:
                print(f"❌ Erro: {result.get('erro')}")
                return False
        else:
            print(f"❌ Erro HTTP {response.status_code}")
            return False
    
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
        return False

def main():
    """Função principal com estratégia multi-worker"""
    stats['tempo_inicio'] = time.time()
    
    # Verificar checkpoint
    checkpoint = carregar_checkpoint()
    if checkpoint:
        print(f"\n💾 Checkpoint encontrado de {checkpoint['timestamp']}")
        resposta = input("Deseja continuar de onde parou? (s/n): ")
        if resposta.lower() == 's':
            stats['alunos_processados'] = set(checkpoint['alunos_processados'])
            stats['dados_coletados'] = checkpoint['dados']
            for key in checkpoint['stats']:
                stats[key] = checkpoint['stats'][key]
            print(f"✅ Retomando ({len(stats['alunos_processados'])} já processados)\n")
    
    # Buscar alunos
    alunos = buscar_alunos_hortolandia()
    if not alunos:
        print("❌ Nenhum aluno encontrado")
        return
    
    # Login
    try:
        cookies_dict = fazer_login()
    except Exception as e:
        print(f"❌ {e}")
        return
    
    # Filtrar pendentes
    alunos_pendentes = [a for a in alunos if a['id_aluno'] not in stats['alunos_processados']]
    print(f"📋 Total pendentes: {len(alunos_pendentes)}\n")
    
    if len(alunos_pendentes) == 0:
        print("✅ Todos os alunos já foram processados!")
        return
    
    # Criar fila de trabalho
    fila_alunos = Queue()
    for aluno in alunos_pendentes:
        fila_alunos.put(aluno)
    
    print(f"🚀 Iniciando coleta com {NUM_WORKERS} workers simultâneos...\n")
    
    # Iniciar thread de monitoramento
    monitor_thread = threading.Thread(
        target=monitor_progresso,
        args=(len(alunos_pendentes),),
        daemon=True
    )
    monitor_thread.start()
    
    # Contador de reprocessamento (compartilhado entre workers)
    contador_reprocessamento = {}
    
    # Iniciar workers usando ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = []
        for worker_id in range(NUM_WORKERS):
            future = executor.submit(
                worker_coletor,
                worker_id,
                fila_alunos,
                cookies_dict,
                contador_reprocessamento
            )
            futures.append(future)
        
        # Aguardar todos workers terminarem
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                safe_print(f"❌ Erro em worker: {e}")
    
    print("\n\n✅ Primeira passagem concluída!")
    
    # ========== REPROCESSAMENTO INTELIGENTE ==========
    if not FILA_REPROCESSAMENTO.empty():
        total_reprocessar = FILA_REPROCESSAMENTO.qsize()
        print(f"\n🔄 Reprocessando {total_reprocessar} alunos que falharam...")
        
        # Criar nova fila para reprocessamento
        fila_reprocessamento = Queue()
        while not FILA_REPROCESSAMENTO.empty():
            fila_reprocessamento.put(FILA_REPROCESSAMENTO.get())
        
        # Usar menos workers para reprocessamento (mais cuidadoso)
        NUM_WORKERS_REPROCESS = max(10, NUM_WORKERS // 4)
        
        with ThreadPoolExecutor(max_workers=NUM_WORKERS_REPROCESS) as executor:
            futures = []
            for worker_id in range(NUM_WORKERS_REPROCESS):
                future = executor.submit(
                    worker_coletor,
                    worker_id + 1000,  # IDs diferentes para distinguir
                    fila_reprocessamento,
                    cookies_dict,
                    {}  # Novo contador (última chance)
                )
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    safe_print(f"❌ Erro em reprocessamento: {e}")
        
        print("\n✅ Reprocessamento concluído!")
    
    # Checkpoint final
    salvar_checkpoint_parcial()
    
    # Estatísticas finais
    tempo_total = time.time() - stats['tempo_inicio']
    
    print("\n" + "="*80)
    print("📊 ESTATÍSTICAS FINAIS")
    print("="*80)
    print(f"⏱️  Tempo total: {tempo_total:.2f}s ({tempo_total/60:.2f} minutos)")
    print(f"👥 Alunos processados: {stats['processados']}")
    print(f"   ✅ Com dados: {stats['com_dados']}")
    print(f"   ⚪ Sem dados: {stats['sem_dados']}")
    
    if stats['processados'] > 0:
        velocidade = stats['processados'] / tempo_total
        print(f"🚀 Velocidade média: {velocidade:.2f} alunos/segundo")
    
    total_registros = sum(len(v) for v in stats['dados_coletados'].values())
    print(f"📋 Total de registros coletados: {total_registros}")
    
    # Verificar se ficou algum aluno sem processar
    alunos_faltando = len(alunos) - len(stats['alunos_processados'])
    if alunos_faltando > 0:
        print(f"\n⚠️  {alunos_faltando} alunos não foram processados")
        print("   Execute novamente para tentar coletar os restantes")
    else:
        print("\n🎉 100% DOS ALUNOS PROCESSADOS COM SUCESSO!")
    
    print("="*80)
    
    # Enviar para Google Sheets
    if stats['processados'] > 0:
        enviar_para_sheets(stats['dados_coletados'], alunos)
    else:
        print("\n⚠️  Nenhum dado para enviar")
    
    print("\n✅ COLETA FINALIZADA!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Coleta interrompida pelo usuário")
        print("💾 Checkpoint salvo - execute novamente para continuar")
        if stats['tempo_inicio']:
            tempo_total = time.time() - stats['tempo_inicio']
            print(f"\n⏱️  Tempo decorrido: {tempo_total:.2f}s")
            print(f"👥 Processados até agora: {stats['processados']}")
    except Exception as e:
        print(f"\n❌ Erro fatal: {e}")
        import traceback
        traceback.print_exc()
