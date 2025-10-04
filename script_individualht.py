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
# CONFIGURAÇÕES
# ========================================

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

NUM_THREADS = 15
TIMEOUT_REQUEST = 20
DELAY_ENTRE_REQ = 0.1
LIMITE_ALUNOS_TESTE = 100  # 🧪 MODO TESTE: Processar apenas os primeiros 100 alunos

print(f"🎓 COLETOR DE LIÇÕES - VERSÃO CORRIGIDA")
print(f"🧵 Threads: {NUM_THREADS}")
print(f"🧪 MODO TESTE: Apenas {LIMITE_ALUNOS_TESTE} alunos")

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
# EXTRAÇÃO COM DEBUGGING
# ========================================

def extrair_dados_completos(soup, id_aluno: int, nome_aluno: str) -> Dict:
    """Extrai todos os dados de um aluno - VERSÃO CORRIGIDA"""
    
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
        # ============================================
        # MTS INDIVIDUAL E GRUPO
        # ============================================
        aba_mts = soup.find('div', {'id': 'mts'})
        if aba_mts:
            # MTS Individual
            tab_mts_ind = aba_mts.find('table', {'id': 'datatable1'})
            if tab_mts_ind:
                tbody = tab_mts_ind.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            dados['mts_individual'].append([
                                id_aluno, 
                                nome_aluno,
                                cols[0].get_text(strip=True),  # Módulo
                                cols[1].get_text(strip=True),  # Lições
                                cols[2].get_text(strip=True),  # Data Lição
                                cols[3].get_text(strip=True),  # Autorizante
                                cols[4].get_text(strip=True),  # Data Cadastro
                                cols[5].get_text(strip=True),  # Data Alteração
                                cols[6].get_text(strip=True) if len(cols) > 6 else ""  # Obs
                            ])
            
            # MTS Grupo - CORRIGIDO
            tab_mts_grp = aba_mts.find('table', {'id': 'datatable_mts_grupo'})
            if tab_mts_grp:
                tbody = tab_mts_grp.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            dados['mts_grupo'].append([
                                id_aluno,
                                nome_aluno,
                                cols[0].get_text(strip=True),  # Páginas
                                cols[1].get_text(strip=True),  # Observações
                                cols[2].get_text(strip=True)   # Data Lição
                            ])
        
        # ============================================
        # MSA INDIVIDUAL E GRUPO
        # ============================================
        aba_msa = soup.find('div', {'id': 'msa'})
        if aba_msa:
            # MSA Individual
            tab_msa_ind = aba_msa.find('table', {'id': 'datatable1'})
            if tab_msa_ind:
                tbody = tab_msa_ind.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            dados['msa_individual'].append([
                                id_aluno,
                                nome_aluno,
                                cols[0].get_text(strip=True),  # Data Lição
                                cols[1].get_text(strip=True),  # Fases
                                cols[2].get_text(strip=True),  # Páginas
                                cols[3].get_text(strip=True),  # Lições
                                cols[4].get_text(strip=True),  # Claves
                                cols[5].get_text(strip=True),  # Observações
                                cols[6].get_text(strip=True)   # Autorizante
                            ])
            
            # MSA Grupo
            tab_msa_grp = aba_msa.find('table', {'id': 'datatable_mts_grupo'})
            if tab_msa_grp:
                tbody = tab_msa_grp.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            texto = cols[0].get_text(strip=True)
                            fases_m = re.search(r'de\s+([\d.]+)\s+até\s+([\d.]+)', texto)
                            pag_m = re.search(r'de\s+(\d+)\s+até\s+(\d+)', texto)
                            dados['msa_grupo'].append([
                                id_aluno,
                                nome_aluno,
                                fases_m.group(1) if fases_m else "",
                                fases_m.group(2) if fases_m else "",
                                pag_m.group(1) if pag_m else "",
                                pag_m.group(2) if pag_m else "",
                                cols[1].get_text(strip=True),
                                cols[2].get_text(strip=True)
                            ])
        
        # ============================================
        # PROVAS
        # ============================================
        aba_provas = soup.find('div', {'id': 'provas'})
        if aba_provas:
            tab_provas = aba_provas.find('table', {'id': 'datatable2'})
            if tab_provas:
                tbody = tab_provas.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 5:
                            dados['provas'].append([
                                id_aluno,
                                nome_aluno,
                                cols[0].get_text(strip=True),  # Módulo/Fases
                                cols[1].get_text(strip=True),  # Nota
                                cols[2].get_text(strip=True),  # Data Prova
                                cols[3].get_text(strip=True),  # Autorizante
                                cols[4].get_text(strip=True)   # Data Cadastro
                            ])
        
        # ============================================
        # HINÁRIO INDIVIDUAL E GRUPO - CORRIGIDO
        # ============================================
        aba_hinario = soup.find('div', {'id': 'hinario'})
        if aba_hinario:
            # Hinário Individual
            tab_hin_ind = aba_hinario.find('table', {'id': 'datatable4'})
            if tab_hin_ind:
                tbody = tab_hin_ind.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            dados['hinario_individual'].append([
                                id_aluno,
                                nome_aluno,
                                cols[0].get_text(strip=True),  # Hino
                                cols[1].get_text(strip=True),  # Voz
                                cols[2].get_text(strip=True),  # Data Aula
                                cols[3].get_text(strip=True),  # Autorizante
                                cols[4].get_text(strip=True),  # Data Cadastro
                                cols[5].get_text(strip=True),  # Data Alteração
                                cols[6].get_text(strip=True) if len(cols) > 6 else ""
                            ])
            
            # Hinário Grupo - CORRIGIDO: buscar todas as tabelas
            todas_tabelas_hin = aba_hinario.find_all('table')
            for tab in todas_tabelas_hin:
                # Pular se for a tabela individual
                if tab.get('id') == 'datatable4':
                    continue
                    
                tbody = tab.find('tbody')
                if tbody:
                    linhas = tbody.find_all('tr')
                    for linha in linhas:
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            dados['hinario_grupo'].append([
                                id_aluno,
                                nome_aluno,
                                cols[0].get_text(strip=True),  # Hinos
                                cols[1].get_text(strip=True),  # Observações
                                cols[2].get_text(strip=True)   # Data Lição
                            ])
                    break  # Encontrou a tabela de grupo
        
        # ============================================
        # MÉTODOS - CORRIGIDO
        # ============================================
        aba_metodos = soup.find('div', {'id': 'metodos'})
        if aba_metodos:
            tab_metodos = aba_metodos.find('table', {'id': 'datatable3'})
            if tab_metodos:
                tbody = tab_metodos.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 7:
                            dados['metodos'].append([
                                id_aluno,
                                nome_aluno,
                                cols[0].get_text(strip=True),  # Páginas
                                cols[1].get_text(strip=True),  # Lição
                                cols[2].get_text(strip=True),  # Método
                                cols[3].get_text(strip=True),  # Data Lição
                                cols[4].get_text(strip=True),  # Autorizante
                                cols[5].get_text(strip=True),  # Data Cadastro
                                cols[6].get_text(strip=True) if len(cols) > 6 else ""
                            ])
        
        # ============================================
        # ESCALAS INDIVIDUAL E GRUPO - CORRIGIDO
        # ============================================
        aba_escalas = soup.find('div', {'id': 'escalas'})
        if aba_escalas:
            todas_tabelas_esc = aba_escalas.find_all('table')
            
            # Primeira tabela = Individual
            if len(todas_tabelas_esc) > 0:
                tab_esc_ind = todas_tabelas_esc[0]
                tbody = tab_esc_ind.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 6:
                            dados['escalas_individual'].append([
                                id_aluno,
                                nome_aluno,
                                cols[0].get_text(strip=True),  # Escala
                                cols[1].get_text(strip=True),  # Data
                                cols[2].get_text(strip=True),  # Autorizante
                                cols[3].get_text(strip=True),  # Data Cadastro
                                cols[4].get_text(strip=True),  # Data Alteração
                                cols[5].get_text(strip=True) if len(cols) > 5 else ""
                            ])
            
            # Segunda tabela = Grupo
            if len(todas_tabelas_esc) > 1:
                tab_esc_grp = todas_tabelas_esc[1]
                tbody = tab_esc_grp.find('tbody')
                if tbody:
                    for linha in tbody.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            dados['escalas_grupo'].append([
                                id_aluno,
                                nome_aluno,
                                cols[0].get_text(strip=True),  # Escala
                                cols[1].get_text(strip=True),  # Observações
                                cols[2].get_text(strip=True)   # Data Lição
                            ])
    
    except Exception as e:
        safe_print(f"⚠️ Erro ao extrair dados do aluno {id_aluno}: {e}")
    
    return dados

# ========================================
# CRIAR SESSÃO
# ========================================

def criar_sessao(cookies_dict: Dict) -> requests.Session:
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
        
        if resp.status_code != 200:
            update_stats('erros')
            return None
        
        if len(resp.text) < 1000:
            update_stats('erros')
            return None
        
        if "login" in resp.text.lower():
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
        futures = {
            executor.submit(worker_coletar_aluno, aluno, cookies_dict.copy()): aluno 
            for aluno in alunos
        }
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                resultado = future.result(timeout=TIMEOUT_REQUEST + 5)
                
                if resultado:
                    for key in todos_dados.keys():
                        todos_dados[key].extend(resultado[key])
                
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
# GERAR RESUMO - CORRIGIDO
# ========================================

def gerar_resumo_alunos(alunos: List[Dict], todos_dados: Dict) -> List[List]:
    resumo = []
    
    for aluno in alunos:
        id_aluno = int(aluno['id_aluno'])  # GARANTIR QUE É INT
        nome = aluno['nome']
        id_igreja = aluno['id_igreja']
        
        # Contar lições comparando INT com INT
        t_mts_i = sum(1 for x in todos_dados['mts_individual'] if int(x[0]) == id_aluno)
        t_mts_g = sum(1 for x in todos_dados['mts_grupo'] if int(x[0]) == id_aluno)
        t_msa_i = sum(1 for x in todos_dados['msa_individual'] if int(x[0]) == id_aluno)
        t_msa_g = sum(1 for x in todos_dados['msa_grupo'] if int(x[0]) == id_aluno)
        t_prov = sum(1 for x in todos_dados['provas'] if int(x[0]) == id_aluno)
        t_hin_i = sum(1 for x in todos_dados['hinario_individual'] if int(x[0]) == id_aluno)
        t_hin_g = sum(1 for x in todos_dados['hinario_grupo'] if int(x[0]) == id_aluno)
        t_met = sum(1 for x in todos_dados['metodos'] if int(x[0]) == id_aluno)
        t_esc_i = sum(1 for x in todos_dados['escalas_individual'] if int(x[0]) == id_aluno)
        t_esc_g = sum(1 for x in todos_dados['escalas_grupo'] if int(x[0]) == id_aluno)
        
        # Calcular média das provas
        provas = []
        for x in todos_dados['provas']:
            if int(x[0]) == id_aluno:
                try:
                    nota_str = str(x[3]).replace(',', '.')
                    nota = float(nota_str)
                    provas.append(nota)
                except:
                    pass
        
        media = round(sum(provas) / len(provas), 2) if provas else 0
        
        # Última atividade
        datas = []
        for categoria in todos_dados.values():
            for reg in categoria:
                if int(reg[0]) == id_aluno and len(reg) > 2:
                    # Tentar extrair data de qualquer campo
                    for campo in reg[2:]:
                        if isinstance(campo, str) and '/' in campo:
                            datas.append(campo)
        
        ultima_ativ = max(datas) if datas else "N/A"
        
        resumo.append([
            id_aluno, nome, id_igreja,
            t_mts_i, t_mts_g, t_msa_i, t_msa_g,
            t_prov, media, t_hin_i, t_hin_g,
            t_met, t_esc_i, t_esc_g, ultima_ativ,
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
    
    # 🧪 LIMITAR PARA TESTE
    total_original = len(alunos)
    alunos = alunos[:LIMITE_ALUNOS_TESTE]
    
    print(f"\n🎓 Total de alunos disponíveis: {total_original}")
    print(f"🧪 Processando apenas: {len(alunos)} alunos (modo teste)")
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
