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
from queue import Queue
import urllib3

# Desabilitar warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========================================
# CONFIGURAÇÕES OTIMIZADAS
# ========================================

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

NUM_THREADS = 50  # Aumentado significativamente
TIMEOUT_REQUEST = 8  # Timeout mais agressivo
BATCH_SIZE = 100  # Lotes para progresso

print(f"🚀 COLETOR DE LIÇÕES - ULTRA OTIMIZADO")
print(f"🧵 Threads: {NUM_THREADS}")
print(f"⏱️  Timeout: {TIMEOUT_REQUEST}s")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

# Locks e contadores globais
print_lock = threading.Lock()
stats_lock = threading.Lock()
global_stats = {
    'processados': 0,
    'erros': 0,
    'sem_dados': 0,
    'com_dados': 0
}

def safe_print(msg):
    """Print thread-safe"""
    with print_lock:
        print(msg)

def update_stats(tipo: str):
    """Atualiza estatísticas globais"""
    with stats_lock:
        global_stats[tipo] += 1

# ========================================
# BUSCAR LISTA DE ALUNOS
# ========================================

def buscar_alunos_hortolandia() -> List[Dict]:
    """Busca a lista de alunos de Hortolândia do Google Sheets"""
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
# FUNÇÕES DE EXTRAÇÃO OTIMIZADAS
# ========================================

def extrair_mts_individual(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    dados = []
    try:
        aba = soup.find('div', {'id': 'mts'})
        if not aba:
            return dados
        tabela = aba.find('table', {'id': 'datatable1'})
        if not tabela:
            return dados
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        for linha in tbody.find_all('tr'):
            cols = linha.find_all('td')
            if len(cols) >= 7:
                dados.append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:7]])
    except:
        pass
    return dados

def extrair_mts_grupo(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    dados = []
    try:
        aba = soup.find('div', {'id': 'mts'})
        if not aba:
            return dados
        tabela = aba.find('table', {'id': 'datatable_mts_grupo'})
        if not tabela:
            return dados
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        for linha in tbody.find_all('tr'):
            cols = linha.find_all('td')
            if len(cols) >= 3:
                dados.append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:3]])
    except:
        pass
    return dados

def extrair_msa_individual(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    dados = []
    try:
        aba = soup.find('div', {'id': 'msa'})
        if not aba:
            return dados
        tabela = aba.find('table', {'id': 'datatable1'})
        if not tabela:
            return dados
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        for linha in tbody.find_all('tr'):
            cols = linha.find_all('td')
            if len(cols) >= 7:
                dados.append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:7]])
    except:
        pass
    return dados

def extrair_msa_grupo(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    dados = []
    try:
        aba = soup.find('div', {'id': 'msa'})
        if not aba:
            return dados
        tabela = aba.find('table', {'id': 'datatable_mts_grupo'})
        if not tabela:
            return dados
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        for linha in tbody.find_all('tr'):
            cols = linha.find_all('td')
            if len(cols) >= 3:
                texto = cols[0].get_text(strip=True)
                fases_match = re.search(r'de\s+([\d.]+)\s+até\s+([\d.]+)', texto)
                paginas_match = re.search(r'de\s+(\d+)\s+até\s+(\d+)', texto)
                
                dados.append([
                    id_aluno, nome_aluno,
                    fases_match.group(1) if fases_match else "",
                    fases_match.group(2) if fases_match else "",
                    paginas_match.group(1) if paginas_match else "",
                    paginas_match.group(2) if paginas_match else "",
                    cols[1].get_text(strip=True),
                    cols[2].get_text(strip=True)
                ])
    except:
        pass
    return dados

def extrair_provas(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    dados = []
    try:
        aba = soup.find('div', {'id': 'provas'})
        if not aba:
            return dados
        tabela = aba.find('table', {'id': 'datatable2'})
        if not tabela:
            return dados
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        for linha in tbody.find_all('tr'):
            cols = linha.find_all('td')
            if len(cols) >= 5:
                dados.append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:5]])
    except:
        pass
    return dados

def extrair_hinario_individual(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    dados = []
    try:
        aba = soup.find('div', {'id': 'hinario'})
        if not aba:
            return dados
        tabela = aba.find('table', {'id': 'datatable4'})
        if not tabela:
            return dados
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        for linha in tbody.find_all('tr'):
            cols = linha.find_all('td')
            if len(cols) >= 7:
                dados.append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:7]])
    except:
        pass
    return dados

def extrair_hinario_grupo(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    dados = []
    try:
        aba = soup.find('div', {'id': 'hinario'})
        if not aba:
            return dados
        tabela = aba.find('table', {'id': 'datatable_hinos_grupo'})
        if not tabela:
            return dados
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        for linha in tbody.find_all('tr'):
            cols = linha.find_all('td')
            if len(cols) >= 3:
                dados.append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:3]])
    except:
        pass
    return dados

def extrair_metodos(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    dados = []
    try:
        aba = soup.find('div', {'id': 'metodos'})
        if not aba:
            return dados
        tabela = aba.find('table', {'id': 'datatable3'})
        if not tabela:
            return dados
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        for linha in tbody.find_all('tr'):
            cols = linha.find_all('td')
            if len(cols) >= 7:
                dados.append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:7]])
    except:
        pass
    return dados

def extrair_escalas_individual(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    dados = []
    try:
        aba = soup.find('div', {'id': 'escalas'})
        if not aba:
            return dados
        tabela = aba.find('table', {'id': 'datatable4'})
        if not tabela:
            return dados
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        for linha in tbody.find_all('tr'):
            cols = linha.find_all('td')
            if len(cols) >= 6:
                dados.append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:6]])
    except:
        pass
    return dados

def extrair_escalas_grupo(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    dados = []
    try:
        aba = soup.find('div', {'id': 'escalas'})
        if not aba:
            return dados
        tabela = aba.find('table', {'id': 'datatable_escalas_grupo'})
        if not tabela:
            return dados
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        for linha in tbody.find_all('tr'):
            cols = linha.find_all('td')
            if len(cols) >= 3:
                dados.append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:3]])
    except:
        pass
    return dados

# ========================================
# WORKER ULTRA-OTIMIZADO
# ========================================

def worker_coletar(queue: Queue, cookies_dict: Dict, results: Dict, thread_id: int):
    """Worker otimizado para processar alunos"""
    
    # Criar sessão local
    session = requests.Session()
    session.cookies.update(cookies_dict.copy())
    
    # Adapter otimizado
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=2,
        pool_block=False
    )
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html',
        'Connection': 'keep-alive'
    }
    
    local_data = {
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
    
    processados_local = 0
    
    while True:
        try:
            aluno = queue.get_nowait()
        except:
            break
        
        id_aluno = aluno['id_aluno']
        nome_aluno = aluno['nome']
        
        try:
            url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
            resp = session.get(url, headers=headers, timeout=TIMEOUT_REQUEST)
            
            if resp.status_code == 200 and len(resp.text) > 1000:
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                # Extrair todos os dados
                mts_i = extrair_mts_individual(soup, id_aluno, nome_aluno)
                mts_g = extrair_mts_grupo(soup, id_aluno, nome_aluno)
                msa_i = extrair_msa_individual(soup, id_aluno, nome_aluno)
                msa_g = extrair_msa_grupo(soup, id_aluno, nome_aluno)
                provs = extrair_provas(soup, id_aluno, nome_aluno)
                hin_i = extrair_hinario_individual(soup, id_aluno, nome_aluno)
                hin_g = extrair_hinario_grupo(soup, id_aluno, nome_aluno)
                metod = extrair_metodos(soup, id_aluno, nome_aluno)
                esc_i = extrair_escalas_individual(soup, id_aluno, nome_aluno)
                esc_g = extrair_escalas_grupo(soup, id_aluno, nome_aluno)
                
                # Armazenar
                local_data['mts_individual'].extend(mts_i)
                local_data['mts_grupo'].extend(mts_g)
                local_data['msa_individual'].extend(msa_i)
                local_data['msa_grupo'].extend(msa_g)
                local_data['provas'].extend(provs)
                local_data['hinario_individual'].extend(hin_i)
                local_data['hinario_grupo'].extend(hin_g)
                local_data['metodos'].extend(metod)
                local_data['escalas_individual'].extend(esc_i)
                local_data['escalas_grupo'].extend(esc_g)
                
                total = sum([len(mts_i), len(mts_g), len(msa_i), len(msa_g), 
                            len(provs), len(hin_i), len(hin_g), len(metod), 
                            len(esc_i), len(esc_g)])
                
                if total > 0:
                    update_stats('com_dados')
                else:
                    update_stats('sem_dados')
                
                update_stats('processados')
                processados_local += 1
                
                # Log a cada 10 alunos
                if processados_local % 10 == 0:
                    with stats_lock:
                        total_proc = global_stats['processados']
                    safe_print(f"⚡ T{thread_id:02d}: {processados_local} alunos | Global: {total_proc}")
                
            else:
                update_stats('erros')
                update_stats('processados')
                
        except requests.exceptions.Timeout:
            update_stats('erros')
            update_stats('processados')
        except Exception as e:
            update_stats('erros')
            update_stats('processados')
        
        finally:
            queue.task_done()
    
    # Armazenar resultados
    results[thread_id] = local_data
    session.close()

# ========================================
# COLETA PARALELA COM QUEUE
# ========================================

def executar_coleta_ultra_rapida(cookies_dict: Dict, alunos: List[Dict], num_threads: int):
    """Executa coleta ultra-rápida usando Queue"""
    
    print(f"\n🚀 Iniciando coleta com {num_threads} threads...")
    print(f"📦 {len(alunos)} alunos na fila")
    
    # Criar fila
    queue = Queue()
    for aluno in alunos:
        queue.put(aluno)
    
    # Dicionário para resultados
    results = {}
    
    # Criar threads
    threads = []
    for i in range(num_threads):
        t = threading.Thread(
            target=worker_coletar,
            args=(queue, cookies_dict, results, i)
        )
        t.daemon = True
        t.start()
        threads.append(t)
    
    # Monitor de progresso
    total_alunos = len(alunos)
    tempo_inicio = time.time()
    
    while queue.unfinished_tasks > 0:
        time.sleep(5)
        with stats_lock:
            processados = global_stats['processados']
            com_dados = global_stats['com_dados']
            sem_dados = global_stats['sem_dados']
            erros = global_stats['erros']
        
        tempo_decorrido = time.time() - tempo_inicio
        velocidade = processados / tempo_decorrido if tempo_decorrido > 0 else 0
        restantes = total_alunos - processados
        tempo_estimado = restantes / velocidade if velocidade > 0 else 0
        
        porcentagem = (processados / total_alunos) * 100
        
        print(f"\n📊 PROGRESSO: {processados}/{total_alunos} ({porcentagem:.1f}%)")
        print(f"   ✅ Com dados: {com_dados} | ⚪ Sem dados: {sem_dados} | ❌ Erros: {erros}")
        print(f"   ⚡ Velocidade: {velocidade:.2f} alunos/s")
        print(f"   ⏱️  Tempo restante estimado: {tempo_estimado/60:.1f} min")
    
    # Aguardar conclusão
    queue.join()
    for t in threads:
        t.join(timeout=1)
    
    # Consolidar resultados
    print(f"\n🔄 Consolidando dados...")
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
    
    for thread_id, data in results.items():
        for key in todos_dados.keys():
            todos_dados[key].extend(data[key])
    
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
        
        total_mts_ind = sum(1 for x in todos_dados['mts_individual'] if x[0] == id_aluno)
        total_mts_grp = sum(1 for x in todos_dados['mts_grupo'] if x[0] == id_aluno)
        total_msa_ind = sum(1 for x in todos_dados['msa_individual'] if x[0] == id_aluno)
        total_msa_grp = sum(1 for x in todos_dados['msa_grupo'] if x[0] == id_aluno)
        total_provas = sum(1 for x in todos_dados['provas'] if x[0] == id_aluno)
        total_hin_ind = sum(1 for x in todos_dados['hinario_individual'] if x[0] == id_aluno)
        total_hin_grp = sum(1 for x in todos_dados['hinario_grupo'] if x[0] == id_aluno)
        total_metodos = sum(1 for x in todos_dados['metodos'] if x[0] == id_aluno)
        total_esc_ind = sum(1 for x in todos_dados['escalas_individual'] if x[0] == id_aluno)
        total_esc_grp = sum(1 for x in todos_dados['escalas_grupo'] if x[0] == id_aluno)
        
        provas_aluno = [float(x[3]) for x in todos_dados['provas'] if x[0] == id_aluno and x[3].replace('.','').isdigit()]
        media_provas = round(sum(provas_aluno) / len(provas_aluno), 2) if provas_aluno else 0
        
        resumo.append([
            id_aluno, nome, id_igreja,
            total_mts_ind, total_mts_grp,
            total_msa_ind, total_msa_grp,
            total_provas, media_provas,
            total_hin_ind, total_hin_grp,
            total_metodos,
            total_esc_ind, total_esc_grp,
            "N/A",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ])
    
    return resumo

def enviar_dados_para_sheets(alunos: List[Dict], todos_dados: Dict, tempo_execucao: float):
    """Envia dados para Google Sheets"""
    print(f"\n📤 Enviando dados para Google Sheets...")
    
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
            "tempo_execucao_min": round(tempo_execucao/60, 2),
            "threads_utilizadas": NUM_THREADS,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "velocidade_alunos_por_segundo": round(len(alunos)/tempo_execucao, 2)
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=300)
        
        if response.status_code == 200:
            print("✅ Dados enviados com sucesso!")
            return True
        else:
            print(f"⚠️ Status HTTP: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
        return False

def salvar_backup_local(alunos: List[Dict], todos_dados: Dict):
    """Salva backup local"""
    try:
        nome = f"licoes_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        backup = {
            "alunos": alunos,
            "dados": todos_dados,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        with open(nome, 'w', encoding='utf-8') as f:
            json.dump(backup, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Backup: {nome}")
    except Exception as e:
        print(f"❌ Erro no backup: {e}")

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

# ========================================
# MAIN OTIMIZADO
# ========================================

def main():
    tempo_inicio = time.time()
    
    # Buscar alunos
    alunos = buscar_alunos_hortolandia()
    
    if not alunos:
        print("❌ Nenhum aluno encontrado")
        return
    
    print(f"\n🎓 {len(alunos)} alunos para processar")
    print(f"🎯 Meta: {len(alunos)/900:.1f} alunos/s para 15 minutos")
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
            print(f"❌ Erro no login: {e}")
            navegador.close()
            return
        
        cookies_dict = extrair_cookies_playwright(pagina)
        print(f"🍪 {len(cookies_dict)} cookies extraídos")
        navegador.close()
    
    print(f"\n{'='*70}")
    print(f"🚀 INICIANDO COLETA ULTRA-RÁPIDA")
    print(f"{'='*70}")
    
    # Executar coleta
    todos_dados = executar_coleta_ultra_rapida(cookies_dict, alunos, NUM_THREADS)
    
    tempo_total = time.time() - tempo_inicio
    
    # Estatísticas finais
    print(f"\n{'='*70}")
    print(f"🏁 COLETA FINALIZADA!")
    print(f"{'='*70}")
    print(f"🎓 Alunos processados: {len(alunos)}")
    print(f"⏱️  Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    print(f"⚡ Velocidade: {len(alunos)/tempo_total:.2f} alunos/segundo")
    
    total_registros = sum(len(v) for v in todos_dados.values())
    
    print(f"\n📦 TOTAL DE REGISTROS: {total_registros}")
    print(f"\n📊 DETALHAMENTO:")
    print(f"   📗 MTS Individual: {len(todos_dados['mts_individual'])}")
    print(f"   📗 MTS Grupo: {len(todos_dados['mts_grupo'])}")
    print(f"   📘 MSA Individual: {len(todos_dados['msa_individual'])}")
    print(f"   📘 MSA Grupo: {len(todos_dados['msa_grupo'])}")
    print(f"   📝 Provas: {len(todos_dados['provas'])}")
    print(f"   🎵 Hinário Individual: {len(todos_dados['hinario_individual'])}")
    print(f"   🎵 Hinário Grupo: {len(todos_dados['hinario_grupo'])}")
    print(f"   📖 Métodos: {len(todos_dados['metodos'])}")
    print(f"   🎼 Escalas Individual: {len(todos_dados['escalas_individual'])}")
    print(f"   🎼 Escalas Grupo: {len(todos_dados['escalas_grupo'])}")
    print(f"{'='*70}")
    
    # Salvar backup
    print(f"\n💾 Salvando backup local...")
    salvar_backup_local(alunos, todos_dados)
    
    # Enviar para Sheets
    if total_registros > 0:
        print(f"\n📤 Enviando para Google Sheets...")
        sucesso = enviar_dados_para_sheets(alunos, todos_dados, tempo_total)
        
        if sucesso:
            print(f"\n✅ PROCESSO CONCLUÍDO COM SUCESSO!")
            print(f"🎉 Todos os dados foram sincronizados!")
        else:
            print(f"\n⚠️ Dados coletados, mas houve problema no envio")
    else:
        print(f"\n⚠️ Nenhum registro coletado - verifique a sessão")
    
    print(f"\n{'='*70}")
    print(f"🏁 FIM DA EXECUÇÃO")
    print(f"⏰ Início: {datetime.fromtimestamp(tempo_inicio).strftime('%H:%M:%S')}")
    print(f"⏰ Término: {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
