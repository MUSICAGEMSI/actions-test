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

# ========================================
# CONFIGURAÇÕES PARA 0% DE ERRO
# ========================================

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

# CONFIGURAÇÕES ULTRA CONFIÁVEIS
NUM_THREADS = 30  # Reduzido para maior confiabilidade
TIMEOUT_REQUEST = 20  # Timeout maior
DELAY_ENTRE_REQ = 0.05  # Delay maior entre requisições
MAX_RETRIES = 5  # MUITAS tentativas para garantir 0% erro
DELAY_ENTRE_RETRIES = 1  # 1 segundo entre retries

print(f"🎯 COLETOR 100% CONFIÁVEL - ZERO ERROS TOLERADOS")
print(f"🧵 Threads: {NUM_THREADS}")
print(f"⏱️  Timeout: {TIMEOUT_REQUEST}s")
print(f"🔄 Max Retries: {MAX_RETRIES}")
print(f"⏸️  Delay entre retries: {DELAY_ENTRE_RETRIES}s")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

# ========================================
# ESTATÍSTICAS
# ========================================

class ThreadSafeStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.processados = 0
        self.com_dados = 0
        self.sem_dados = 0
        self.erros = 0
        self.tentativas_extras = 0
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
                'tentativas_extras': self.tentativas_extras,
                'tempo_decorrido': time.time() - self.tempo_inicio if self.tempo_inicio else 0
            }

stats = ThreadSafeStats()
print_lock = threading.Lock()
alunos_falhados = []  # Lista de alunos que falharam para reprocessar
alunos_falhados_lock = threading.Lock()

def safe_print(msg):
    with print_lock:
        print(msg)

def adicionar_aluno_falhado(aluno: Dict):
    with alunos_falhados_lock:
        alunos_falhados.append(aluno)

# ========================================
# BUSCAR ALUNOS
# ========================================

def buscar_alunos_hortolandia() -> List[Dict]:
    print("\n📥 Buscando lista de alunos...")
    
    for tentativa in range(3):
        try:
            params = {"acao": "listar_ids_alunos"}
            
            with httpx.Client(timeout=30) as client:
                response = client.get(URL_APPS_SCRIPT, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('sucesso'):
                    alunos = data.get('alunos', [])
                    print(f"✅ {len(alunos)} alunos carregados")
                    return alunos
                else:
                    print(f"⚠️ Erro na resposta: {data.get('erro')}")
            else:
                print(f"⚠️ HTTP {response.status_code}")
                
        except Exception as e:
            print(f"⚠️ Tentativa {tentativa+1}/3 falhou: {e}")
            if tentativa < 2:
                time.sleep(2)
    
    print("❌ Falha ao buscar alunos após 3 tentativas")
    return []

# ========================================
# EXTRAÇÃO DE DADOS
# ========================================

def extrair_tabela_simples(tbody, id_aluno: int, nome: str, num_cols: int) -> List[List]:
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
            tab = aba_mts.find('table', {'id': 'datatable1'})
            if tab:
                dados['mts_individual'] = extrair_tabela_simples(
                    tab.find('tbody'), id_aluno, nome_aluno, 7
                )
            
            tab_g = aba_mts.find('table', {'id': 'datatable_mts_grupo'})
            if tab_g:
                dados['mts_grupo'] = extrair_tabela_simples(
                    tab_g.find('tbody'), id_aluno, nome_aluno, 3
                )
        
        # MSA
        aba_msa = soup.find('div', {'id': 'msa'})
        if aba_msa:
            tab = aba_msa.find('table', {'id': 'datatable1'})
            if tab:
                dados['msa_individual'] = extrair_tabela_simples(
                    tab.find('tbody'), id_aluno, nome_aluno, 7
                )
            
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
                dados['provas'] = extrair_tabela_simples(
                    tab.find('tbody'), id_aluno, nome_aluno, 5
                )
        
        # HINÁRIO
        aba_hin = soup.find('div', {'id': 'hinario'})
        if aba_hin:
            tab = aba_hin.find('table', {'id': 'datatable4'})
            if tab:
                dados['hinario_individual'] = extrair_tabela_simples(
                    tab.find('tbody'), id_aluno, nome_aluno, 7
                )
            
            todas_tabs = aba_hin.find_all('table')
            for tab in todas_tabs:
                if tab.get('id') != 'datatable4':
                    tbody = tab.find('tbody')
                    if tbody and tbody.find_all('tr'):
                        dados['hinario_grupo'] = extrair_tabela_simples(
                            tbody, id_aluno, nome_aluno, 3
                        )
                        break
        
        # MÉTODOS
        aba_met = soup.find('div', {'id': 'metodos'})
        if aba_met:
            tab = aba_met.find('table', {'id': 'datatable3'})
            if tab:
                dados['metodos'] = extrair_tabela_simples(
                    tab.find('tbody'), id_aluno, nome_aluno, 7
                )
        
        # ESCALAS
        aba_esc = soup.find('div', {'id': 'escalas'})
        if aba_esc:
            todas_tabs = aba_esc.find_all('table')
            
            if len(todas_tabs) > 0:
                dados['escalas_individual'] = extrair_tabela_simples(
                    todas_tabs[0].find('tbody'), id_aluno, nome_aluno, 6
                )
            
            if len(todas_tabs) > 1:
                dados['escalas_grupo'] = extrair_tabela_simples(
                    todas_tabs[1].find('tbody'), id_aluno, nome_aluno, 3
                )
    
    except Exception as e:
        safe_print(f"⚠️ Erro ao extrair aluno {id_aluno}: {str(e)[:50]}")
    
    return dados

# ========================================
# VALIDAÇÕES RIGOROSAS
# ========================================

def validar_resposta(resp: httpx.Response, id_aluno: int) -> tuple[bool, str]:
    """Valida rigorosamente a resposta HTTP"""
    
    if resp.status_code != 200:
        return False, f"Status {resp.status_code}"
    
    if len(resp.text) < 1000:
        return False, "HTML muito curto"
    
    if "login" in resp.text.lower():
        return False, "Redirecionado para login"
    
    if "erro" in resp.text.lower() and "404" in resp.text:
        return False, "Página não encontrada"
    
    # Verificar se tem estrutura HTML válida
    if "<html" not in resp.text.lower():
        return False, "HTML inválido"
    
    return True, "OK"

# ========================================
# WORKER COM RETRY AGRESSIVO
# ========================================

def worker_coletar_aluno(aluno: Dict, client: httpx.Client, tentativa: int = 1) -> Optional[Dict]:
    """Worker com retry agressivo para garantir 0% erro"""
    
    id_aluno = aluno['id_aluno']
    nome_aluno = aluno['nome']
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'pt-BR,pt;q=0.9',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache'
    }
    
    for tentativa_atual in range(1, MAX_RETRIES + 1):
        try:
            url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
            
            # Fazer requisição
            resp = client.get(url, headers=headers)
            
            # Validar resposta
            valido, motivo = validar_resposta(resp, id_aluno)
            
            if not valido:
                if tentativa_atual < MAX_RETRIES:
                    stats.incrementar('tentativas_extras')
                    safe_print(f"⚠️ Aluno {id_aluno} - Tentativa {tentativa_atual}/{MAX_RETRIES}: {motivo}")
                    time.sleep(DELAY_ENTRE_RETRIES * tentativa_atual)  # Delay progressivo
                    continue
                else:
                    safe_print(f"❌ Aluno {id_aluno} FALHOU após {MAX_RETRIES} tentativas: {motivo}")
                    adicionar_aluno_falhado(aluno)
                    stats.incrementar('erros')
                    stats.incrementar('processados')
                    return None
            
            # Extração
            soup = BeautifulSoup(resp.text, 'lxml')
            dados = extrair_dados_completos(soup, id_aluno, nome_aluno)
            
            total = sum(len(v) for v in dados.values())
            
            if total > 0:
                stats.incrementar('com_dados')
            else:
                stats.incrementar('sem_dados')
            
            stats.incrementar('processados')
            
            # Sucesso!
            if tentativa_atual > 1:
                safe_print(f"✅ Aluno {id_aluno} OK na tentativa {tentativa_atual}")
            
            time.sleep(DELAY_ENTRE_REQ)
            return dados
            
        except httpx.TimeoutException:
            if tentativa_atual < MAX_RETRIES:
                stats.incrementar('tentativas_extras')
                safe_print(f"⏱️ Aluno {id_aluno} - Timeout {tentativa_atual}/{MAX_RETRIES}")
                time.sleep(DELAY_ENTRE_RETRIES * tentativa_atual)
                continue
            else:
                safe_print(f"❌ Aluno {id_aluno} FALHOU: Timeout persistente")
                adicionar_aluno_falhado(aluno)
                stats.incrementar('erros')
                stats.incrementar('processados')
                return None
        
        except Exception as e:
            if tentativa_atual < MAX_RETRIES:
                stats.incrementar('tentativas_extras')
                safe_print(f"⚠️ Aluno {id_aluno} - Erro {tentativa_atual}/{MAX_RETRIES}: {str(e)[:30]}")
                time.sleep(DELAY_ENTRE_RETRIES * tentativa_atual)
                continue
            else:
                safe_print(f"❌ Aluno {id_aluno} FALHOU: {str(e)[:50]}")
                adicionar_aluno_falhado(aluno)
                stats.incrementar('erros')
                stats.incrementar('processados')
                return None
    
    # Não deveria chegar aqui
    adicionar_aluno_falhado(aluno)
    stats.incrementar('erros')
    stats.incrementar('processados')
    return None

# ========================================
# COLETA PARALELA
# ========================================

def executar_coleta_paralela(cookies_dict: Dict, alunos: List[Dict], num_threads: int):
    print(f"\n🚀 Iniciando coleta 100% confiável...")
    print(f"🎯 {len(alunos)} alunos para processar\n")
    
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
    
    # Configurar httpx
    limits = httpx.Limits(
        max_keepalive_connections=num_threads,
        max_connections=num_threads * 2,
        keepalive_expiry=60
    )
    
    cookies_httpx = httpx.Cookies()
    for k, v in cookies_dict.items():
        cookies_httpx.set(k, v)
    
    with httpx.Client(
        cookies=cookies_httpx,
        limits=limits,
        timeout=TIMEOUT_REQUEST,
        http2=True,
        follow_redirects=True
    ) as client:
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            
            futures = {
                executor.submit(worker_coletar_aluno, aluno, client): aluno 
                for aluno in alunos
            }
            
            for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
                try:
                    resultado = future.result(timeout=TIMEOUT_REQUEST + 10)
                    
                    if resultado:
                        for key in todos_dados.keys():
                            todos_dados[key].extend(resultado[key])
                    
                    # Log a cada 20 alunos
                    if i % 20 == 0:
                        s = stats.obter_stats()
                        velocidade = s['processados'] / s['tempo_decorrido'] if s['tempo_decorrido'] > 0 else 0
                        restantes = total_alunos - s['processados']
                        tempo_est = restantes / velocidade if velocidade > 0 else 0
                        pct = (s['processados'] / total_alunos) * 100
                        taxa_erro = (s['erros'] / s['processados'] * 100) if s['processados'] > 0 else 0
                        
                        safe_print(
                            f"📊 {s['processados']}/{total_alunos} ({pct:.1f}%) | "
                            f"✅ {s['com_dados']} | ⚪ {s['sem_dados']} | "
                            f"❌ {s['erros']} ({taxa_erro:.1f}%) | "
                            f"🔄 {s['tentativas_extras']} retries | "
                            f"⚡ {velocidade:.1f}/s | ⏱️ {tempo_est/60:.1f}min"
                        )
                
                except concurrent.futures.TimeoutError:
                    safe_print(f"⚠️ Timeout ao processar aluno")
                except Exception as e:
                    safe_print(f"⚠️ Erro: {str(e)[:50]}")
    
    return todos_dados

# ========================================
# REPROCESSAR FALHAS
# ========================================

def reprocessar_alunos_falhados(cookies_dict: Dict, todos_dados: Dict):
    """Tenta reprocessar alunos que falharam"""
    
    if not alunos_falhados:
        return
    
    print(f"\n🔄 Reprocessando {len(alunos_falhados)} alunos que falharam...")
    
    limits = httpx.Limits(
        max_keepalive_connections=5,
        max_connections=10,
        keepalive_expiry=60
    )
    
    cookies_httpx = httpx.Cookies()
    for k, v in cookies_dict.items():
        cookies_httpx.set(k, v)
    
    with httpx.Client(
        cookies=cookies_httpx,
        limits=limits,
        timeout=30,
        http2=True
    ) as client:
        
        recuperados = 0
        
        for aluno in alunos_falhados:
            safe_print(f"🔄 Reprocessando aluno {aluno['id_aluno']}...")
            time.sleep(2)  # Delay maior
            
            resultado = worker_coletar_aluno(aluno, client)
            
            if resultado:
                for key in todos_dados.keys():
                    todos_dados[key].extend(resultado[key])
                recuperados += 1
                safe_print(f"✅ Aluno {aluno['id_aluno']} recuperado!")
        
        print(f"\n✅ {recuperados}/{len(alunos_falhados)} alunos recuperados")

# ========================================
# GERAR RESUMO
# ========================================

def gerar_resumo_alunos(alunos: List[Dict], todos_dados: Dict) -> List[List]:
    contagens = {}
    for key in todos_dados.keys():
        contagens[key] = {}
        for registro in todos_dados[key]:
            id_a = int(registro[0])
            contagens[key][id_a] = contagens[key].get(id_a, 0) + 1
    
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
            "tentativas_extras": stats.tentativas_extras,
            "taxa_erro_final": round((stats.erros / len(alunos)) * 100, 2) if alunos else 0
        }
    }
    
    for tentativa in range(3):
        try:
            with httpx.Client(timeout=300) as client:
                response = client.post(URL_APPS_SCRIPT, json=payload)
            
            if response.status_code == 200:
                print("✅ Dados enviados com sucesso!")
                return True
            else:
                print(f"⚠️ Tentativa {tentativa+1}/3 - Status: {response.status_code}")
                if tentativa < 2:
                    time.sleep(5)
                    
        except Exception as e:
            print(f"⚠️ Tentativa {tentativa+1}/3 - Erro: {e}")
            if tentativa < 2:
                time.sleep(5)
    
    print("❌ Falha ao enviar após 3 tentativas")
    return False

def salvar_backup_local(alunos: List[Dict], todos_dados: Dict):
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
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

# ========================================
# MAIN
# ========================================

def main():
    print(f"\n{'='*70}")
    print(f"  COLETOR 100% CONFIÁVEL - ZERO ERROS TOLERADOS")
    print(f"{'='*70}\n")
    
    tempo_inicio = time.time()
    
    # Buscar alunos
    alunos = buscar_alunos_hortolandia()
    
    if not alunos:
        print("❌ Nenhum aluno encontrado")
        return
    
    tempo_estimado = len(alunos) / 120  # ~120 alunos/min com retry
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
                print("❌ Login falhou")
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
    
    # Reprocessar falhas
    if alunos_falhados:
        print(f"\n{'='*70}")
        print(f"  REPROCESSANDO FALHAS")
        print(f"{'='*70}")
        reprocessar_alunos_falhados(cookies_dict, todos_dados)
    
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
    taxa_erro = (s['erros'] / len(alunos) * 100) if alunos else 0
    taxa_sucesso = 100 - taxa_erro
    
    print(f"\n📊 ESTATÍSTICAS FINAIS:")
    print(f"   • Processados: {s['processados']}/{len(alunos)}")
    print(f"   • Com dados: {s['com_dados']}")
    print(f"   • Sem dados: {s['sem_dados']}")
    print(f"   • Erros: {s['erros']} ({taxa_erro:.2f}%)")
    print(f"   • Taxa de sucesso: {taxa_sucesso:.2f}%")
    print(f"   • Tentativas extras: {s['tentativas_extras']}")
    
    # Alerta se houver erros
    if s['erros'] > 0:
        print(f"\n⚠️  ATENÇÃO: {s['erros']} alunos não foram coletados!")
        print(f"   IDs dos alunos com erro:")
        for aluno in alunos_falhados[:10]:  # Mostrar primeiros 10
            print(f"   - ID: {aluno['id_aluno']} - {aluno['nome']}")
        if len(alunos_falhados) > 10:
            print(f"   ... e mais {len(alunos_falhados)-10} alunos")
    
    # Backup
    print(f"\n💾 Salvando backup local...")
    salvar_backup_local(alunos, todos_dados)
    
    # Enviar para Sheets
    if total_reg > 0:
        sucesso = enviar_dados_para_sheets(alunos, todos_dados, tempo_total)
        if sucesso:
            if s['erros'] == 0:
                print(f"\n✅ SUCESSO TOTAL! 100% dos alunos coletados e salvos!")
            else:
                print(f"\n⚠️  Dados enviados, mas {s['erros']} alunos falharam.")
        else:
            print(f"\n⚠️  Dados coletados mas erro ao enviar. Verifique o backup local.")
    else:
        print(f"\n⚠️  Nenhum dado foi coletado.")
    
    print(f"\n{'='*70}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n⚠️  Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n\n❌ Erro fatal: {e}")
        import traceback
        traceback.print_exc()
