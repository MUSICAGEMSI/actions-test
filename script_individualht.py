from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import os
import httpx
import time
import json
import concurrent.futures
from typing import List, Dict, Optional
import re
from datetime import datetime
import threading
from queue import Queue

# ========================================
# CONFIGURAÇÕES OTIMIZADAS E CONFIÁVEIS
# ========================================

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

# CONFIGURAÇÕES BALANCEADAS: Velocidade + Confiabilidade
NUM_THREADS = 40  # Otimizado para 2000 alunos em ~10min
TIMEOUT_REQUEST = 12  # Timeout razoável
DELAY_ENTRE_REQ = 0.02  # Micro delay para estabilidade
MAX_RETRIES = 2  # Retry em caso de falha

print(f"🚀 COLETOR DE LIÇÕES - ULTRA RÁPIDO E CONFIÁVEL")
print(f"🎯 META: 2000 alunos em ~10 minutos")
print(f"🧵 Threads: {NUM_THREADS}")
print(f"⏱️  Timeout: {TIMEOUT_REQUEST}s")
print(f"🔄 Retries: {MAX_RETRIES}")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas no .env")
    exit(1)

# ========================================
# ESTATÍSTICAS THREAD-SAFE
# ========================================

class ThreadSafeStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.processados = 0
        self.com_dados = 0
        self.sem_dados = 0
        self.erros = 0
        self.tempo_inicio = None
    
    def incrementar(self, campo: str):
        with self.lock:
            setattr(self, campo, getattr(self, campo) + 1)
    
    def obter_stats(self):
        with self.lock:
            return {
                'processados': self.processados,
                'com_dados': self.com_dados,
                'sem_dados': self.sem_dados,
                'erros': self.erros,
                'tempo_decorrido': time.time() - self.tempo_inicio if self.tempo_inicio else 0
            }

stats = ThreadSafeStats()
print_lock = threading.Lock()

def safe_print(msg):
    with print_lock:
        print(msg)

# ========================================
# BUSCAR ALUNOS
# ========================================

def buscar_alunos_hortolandia() -> List[Dict]:
    print("\n📥 Buscando lista de alunos...")
    
    try:
        params = {"acao": "listar_ids_alunos"}
        
        with httpx.Client(timeout=30) as client:
            response = client.get(URL_APPS_SCRIPT, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('sucesso'):
                alunos = data.get('alunos', [])
                print(f"✅ {len(alunos)} alunos carregados com sucesso")
                return alunos
            else:
                print(f"⚠️ Erro na resposta: {data.get('erro')}")
                return []
        else:
            print(f"⚠️ HTTP {response.status_code}")
            return []
            
    except Exception as e:
        print(f"❌ Erro ao buscar alunos: {e}")
        return []

# ========================================
# EXTRAÇÃO DE DADOS OTIMIZADA
# ========================================

def extrair_tabela_simples(tbody, id_aluno: int, nome: str, num_cols: int) -> List[List]:
    """Extrai tabela simples com validação mínima"""
    dados = []
    if not tbody:
        return dados
    
    for linha in tbody.find_all('tr'):
        cols = linha.find_all('td')
        if len(cols) >= num_cols:
            row = [id_aluno, nome]
            row.extend([c.get_text(strip=True) for c in cols[:num_cols]])
            dados.append(row)
    
    return dados

def extrair_dados_completos(soup, id_aluno: int, nome_aluno: str) -> Dict:
    """Extração completa e otimizada"""
    
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
        # MTS
        # ============================================
        aba_mts = soup.find('div', {'id': 'mts'})
        if aba_mts:
            # Individual
            tab = aba_mts.find('table', {'id': 'datatable1'})
            if tab:
                dados['mts_individual'] = extrair_tabela_simples(
                    tab.find('tbody'), id_aluno, nome_aluno, 7
                )
            
            # Grupo
            tab_g = aba_mts.find('table', {'id': 'datatable_mts_grupo'})
            if tab_g:
                dados['mts_grupo'] = extrair_tabela_simples(
                    tab_g.find('tbody'), id_aluno, nome_aluno, 3
                )
        
        # ============================================
        # MSA
        # ============================================
        aba_msa = soup.find('div', {'id': 'msa'})
        if aba_msa:
            # Individual
            tab = aba_msa.find('table', {'id': 'datatable1'})
            if tab:
                dados['msa_individual'] = extrair_tabela_simples(
                    tab.find('tbody'), id_aluno, nome_aluno, 7
                )
            
            # Grupo (com parsing de fases)
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
        
        # ============================================
        # PROVAS
        # ============================================
        aba_provas = soup.find('div', {'id': 'provas'})
        if aba_provas:
            tab = aba_provas.find('table', {'id': 'datatable2'})
            if tab:
                dados['provas'] = extrair_tabela_simples(
                    tab.find('tbody'), id_aluno, nome_aluno, 5
                )
        
        # ============================================
        # HINÁRIO
        # ============================================
        aba_hin = soup.find('div', {'id': 'hinario'})
        if aba_hin:
            # Individual
            tab = aba_hin.find('table', {'id': 'datatable4'})
            if tab:
                dados['hinario_individual'] = extrair_tabela_simples(
                    tab.find('tbody'), id_aluno, nome_aluno, 7
                )
            
            # Grupo (segunda tabela)
            todas_tabs = aba_hin.find_all('table')
            for tab in todas_tabs:
                if tab.get('id') != 'datatable4':
                    tbody = tab.find('tbody')
                    if tbody and tbody.find_all('tr'):
                        dados['hinario_grupo'] = extrair_tabela_simples(
                            tbody, id_aluno, nome_aluno, 3
                        )
                        break
        
        # ============================================
        # MÉTODOS
        # ============================================
        aba_met = soup.find('div', {'id': 'metodos'})
        if aba_met:
            tab = aba_met.find('table', {'id': 'datatable3'})
            if tab:
                dados['metodos'] = extrair_tabela_simples(
                    tab.find('tbody'), id_aluno, nome_aluno, 7
                )
        
        # ============================================
        # ESCALAS
        # ============================================
        aba_esc = soup.find('div', {'id': 'escalas'})
        if aba_esc:
            todas_tabs = aba_esc.find_all('table')
            
            # Individual (primeira)
            if len(todas_tabs) > 0:
                dados['escalas_individual'] = extrair_tabela_simples(
                    todas_tabs[0].find('tbody'), id_aluno, nome_aluno, 6
                )
            
            # Grupo (segunda)
            if len(todas_tabs) > 1:
                dados['escalas_grupo'] = extrair_tabela_simples(
                    todas_tabs[1].find('tbody'), id_aluno, nome_aluno, 3
                )
    
    except Exception as e:
        safe_print(f"⚠️ Erro ao extrair aluno {id_aluno}: {str(e)[:50]}")
    
    return dados

# ========================================
# WORKER COM RETRY INTELIGENTE
# ========================================

def worker_coletar_aluno(aluno: Dict, client: httpx.Client, tentativa: int = 1) -> Optional[Dict]:
    """Worker otimizado com retry automático"""
    
    id_aluno = aluno['id_aluno']
    nome_aluno = aluno['nome']
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'pt-BR,pt;q=0.9',
        'Connection': 'keep-alive'
    }
    
    try:
        url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
        
        resp = client.get(url, headers=headers)
        
        # Validações
        if resp.status_code != 200:
            if tentativa < MAX_RETRIES:
                time.sleep(0.5)
                return worker_coletar_aluno(aluno, client, tentativa + 1)
            stats.incrementar('erros')
            stats.incrementar('processados')
            return None
        
        if len(resp.text) < 1000 or "login" in resp.text.lower():
            stats.incrementar('erros')
            stats.incrementar('processados')
            return None
        
        # Extração
        soup = BeautifulSoup(resp.text, 'lxml')  # lxml é mais rápido
        dados = extrair_dados_completos(soup, id_aluno, nome_aluno)
        
        total = sum(len(v) for v in dados.values())
        
        if total > 0:
            stats.incrementar('com_dados')
        else:
            stats.incrementar('sem_dados')
        
        stats.incrementar('processados')
        
        # Micro delay para não sobrecarregar
        time.sleep(DELAY_ENTRE_REQ)
        
        return dados
        
    except httpx.TimeoutException:
        if tentativa < MAX_RETRIES:
            time.sleep(0.5)
            return worker_coletar_aluno(aluno, client, tentativa + 1)
        stats.incrementar('erros')
        stats.incrementar('processados')
        return None
    
    except Exception as e:
        if tentativa < MAX_RETRIES:
            time.sleep(0.5)
            return worker_coletar_aluno(aluno, client, tentativa + 1)
        stats.incrementar('erros')
        stats.incrementar('processados')
        return None

# ========================================
# COLETA PARALELA COM HTTPX
# ========================================

def executar_coleta_paralela(cookies_dict: Dict, alunos: List[Dict], num_threads: int):
    """Coleta paralela otimizada com httpx"""
    
    print(f"\n🚀 Iniciando coleta paralela...")
    print(f"🎯 Meta: {len(alunos)} alunos em ~{len(alunos)/200:.1f} minutos\n")
    
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
    
    stats.tempo_inicio = time.time()
    total_alunos = len(alunos)
    
    # Configurar httpx com limites otimizados
    limits = httpx.Limits(
        max_keepalive_connections=num_threads,
        max_connections=num_threads * 2,
        keepalive_expiry=30
    )
    
    cookies_httpx = httpx.Cookies()
    for k, v in cookies_dict.items():
        cookies_httpx.set(k, v)
    
    # Cliente httpx compartilhado (thread-safe)
    with httpx.Client(
        cookies=cookies_httpx,
        limits=limits,
        timeout=TIMEOUT_REQUEST,
        http2=True,  # HTTP/2 para melhor performance
        follow_redirects=True
    ) as client:
        
        # ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            
            # Submeter todas as tarefas
            futures = {
                executor.submit(worker_coletar_aluno, aluno, client): aluno 
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
                    
                    # Log de progresso (a cada 20 alunos)
                    if i % 20 == 0:
                        s = stats.obter_stats()
                        velocidade = s['processados'] / s['tempo_decorrido'] if s['tempo_decorrido'] > 0 else 0
                        restantes = total_alunos - s['processados']
                        tempo_est = restantes / velocidade if velocidade > 0 else 0
                        pct = (s['processados'] / total_alunos) * 100
                        
                        safe_print(
                            f"📊 {s['processados']}/{total_alunos} ({pct:.1f}%) | "
                            f"✅ {s['com_dados']} | ⚪ {s['sem_dados']} | ❌ {s['erros']} | "
                            f"⚡ {velocidade:.1f}/s | ⏱️  {tempo_est/60:.1f}min restantes"
                        )
                
                except concurrent.futures.TimeoutError:
                    safe_print(f"⚠️ Timeout ao processar aluno")
                except Exception as e:
                    safe_print(f"⚠️ Erro: {str(e)[:50]}")
    
    return todos_dados

# ========================================
# GERAR RESUMO OTIMIZADO
# ========================================

def gerar_resumo_alunos(alunos: List[Dict], todos_dados: Dict) -> List[List]:
    """Resumo com pré-computação de contagens"""
    
    # Pre-computar todas as contagens de uma vez
    contagens = {}
    for key in todos_dados.keys():
        contagens[key] = {}
        for registro in todos_dados[key]:
            id_a = int(registro[0])
            contagens[key][id_a] = contagens[key].get(id_a, 0) + 1
    
    # Calcular médias de provas
    medias = {}
    for registro in todos_dados['provas']:
        id_a = int(registro[0])
        try:
            nota = float(str(registro[3]).replace(',', '.'))
            if id_a not in medias:
                medias[id_a] = []
            medias[id_a].append(nota)
        except:
            pass
    
    # Gerar resumo
    resumo = []
    for aluno in alunos:
        id_aluno = int(aluno['id_aluno'])
        
        media = 0
        if id_aluno in medias and medias[id_aluno]:
            media = round(sum(medias[id_aluno]) / len(medias[id_aluno]), 2)
        
        resumo.append([
            id_aluno,
            aluno['nome'],
            aluno['id_igreja'],
            contagens['mts_individual'].get(id_aluno, 0),
            contagens['mts_grupo'].get(id_aluno, 0),
            contagens['msa_individual'].get(id_aluno, 0),
            contagens['msa_grupo'].get(id_aluno, 0),
            contagens['provas'].get(id_aluno, 0),
            media,
            contagens['hinario_individual'].get(id_aluno, 0),
            contagens['hinario_grupo'].get(id_aluno, 0),
            contagens['metodos'].get(id_aluno, 0),
            contagens['escalas_individual'].get(id_aluno, 0),
            contagens['escalas_grupo'].get(id_aluno, 0),
            "N/A",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ])
    
    return resumo

# ========================================
# ENVIAR PARA SHEETS
# ========================================

def enviar_dados_para_sheets(alunos: List[Dict], todos_dados: Dict, tempo: float) -> bool:
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
            "tempo_execucao_min": round(tempo/60, 2),
            "threads_utilizadas": NUM_THREADS,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "velocidade_media": round(len(alunos)/tempo, 2)
        }
    }
    
    try:
        with httpx.Client(timeout=300) as client:
            response = client.post(URL_APPS_SCRIPT, json=payload)
        
        if response.status_code == 200:
            print("✅ Dados enviados com sucesso!")
            return True
        else:
            print(f"⚠️ Status HTTP: {response.status_code}")
            print(f"Resposta: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
        return False

def salvar_backup_local(alunos: List[Dict], todos_dados: Dict):
    """Backup local em JSON"""
    try:
        nome = f"backup_licoes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(nome, 'w', encoding='utf-8') as f:
            json.dump({
                "alunos": alunos,
                "dados": todos_dados,
                "timestamp": datetime.now().isoformat(),
                "total_registros": sum(len(v) for v in todos_dados.values())
            }, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Backup salvo: {nome}")
        return True
    except Exception as e:
        print(f"⚠️ Erro ao salvar backup: {e}")
        return False

def extrair_cookies_playwright(pagina) -> Dict[str, str]:
    """Extrai cookies do Playwright"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

# ========================================
# MAIN
# ========================================

def main():
    print(f"\n{'='*70}")
    print(f"  COLETOR DE LIÇÕES - ULTRA RÁPIDO E CONFIÁVEL")
    print(f"{'='*70}\n")
    
    tempo_inicio = time.time()
    
    # Buscar alunos
    alunos = buscar_alunos_hortolandia()
    
    if not alunos:
        print("❌ Nenhum aluno encontrado. Verifique a conexão com Google Sheets.")
        return
    
    tempo_estimado = len(alunos) / 200  # ~200 alunos/min
    print(f"\n🎓 {len(alunos)} alunos identificados")
    print(f"⏱️  Tempo estimado: ~{tempo_estimado:.1f} minutos")
    
    # Login
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
                print("❌ Login falhou. Verifique as credenciais.")
                navegador.close()
                return
            
            print("✅ Login realizado com sucesso!")
            
        except Exception as e:
            print(f"❌ Erro no login: {e}")
            navegador.close()
            return
        
        cookies_dict = extrair_cookies_playwright(pagina)
        print(f"🍪 {len(cookies_dict)} cookies capturados")
        navegador.close()
    
    # Coleta
    print(f"\n{'='*70}")
    print(f"  INICIANDO COLETA")
    print(f"{'='*70}")
    
    todos_dados = executar_coleta_paralela(cookies_dict, alunos, NUM_THREADS)
    
    tempo_total = time.time() - tempo_inicio
    
    # Resultados
    print(f"\n{'='*70}")
    print(f"  COLETA CONCLUÍDA!")
    print(f"{'='*70}")
    print(f"⏱️  Tempo total: {tempo_total/60:.2f} minutos ({tempo_total:.1f}s)")
    print(f"⚡ Velocidade média: {len(alunos)/tempo_total:.1f} alunos/segundo")
    
    total_reg = sum(len(v) for v in todos_dados.values())
    
    print(f"\n📦 REGISTROS COLETADOS: {total_reg:,}")
    print(f"   • MTS Individual: {len(todos_dados['mts_individual']):,}")
    print(f"   • MTS Grupo: {len(todos_dados['mts_grupo']):,}")
    print(f"   • MSA Individual: {len(todos_dados['msa_individual']):,}")
    print(f"   • MSA Grupo: {len(todos_dados['msa_grupo']):,}")
    print(f"   • Provas: {len(todos_dados['provas']):,}")
    print(f"   • Hinário Individual: {len(todos_dados['hinario_individual']):,}")
    print(f"   • Hinário Grupo: {len(todos_dados['hinario_grupo']):,}")
    print(f"   • Métodos: {len(todos_dados['metodos']):,}")
    print(f"   • Escalas Individual: {len(todos_dados['escalas_individual']):,}")
    print(f"   • Escalas Grupo: {len(todos_dados['escalas_grupo']):,}")
    
    # Estatísticas finais
    s = stats.obter_stats()
    print(f"\n📊 ESTATÍSTICAS:")
    print(f"   • Processados: {s['processados']}")
    print(f"   • Com dados: {s['com_dados']}")
    print(f"   • Sem dados: {s['sem_dados']}")
    print(f"   • Erros: {s['erros']}")
    
    # Backup
    print(f"\n💾 Salvando backup local...")
    salvar_backup_local(alunos, todos_dados)
    
    # Enviar para Sheets
    if total_reg > 0:
        sucesso = enviar_dados_para_sheets(alunos, todos_dados, tempo_total)
        if sucesso:
            print(f"\n✅ SUCESSO TOTAL! Dados salvos no Google Sheets.")
        else:
            print(f"\n⚠️ Dados coletados mas houve erro no envio. Verifique o backup local.")
    else:
        print(f"\n⚠️ Nenhum dado foi coletado. Verifique a conexão e cookies.")
    
    print(f"\n{'='*70}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n\n❌ Erro fatal: {e}")
        import traceback
        traceback.print_exc()
