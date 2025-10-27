from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
import json
import asyncio
import aiohttp
from typing import List, Dict, Optional
from collections import deque
import threading
import re

# ==================== CONFIGURAÇÕES GLOBAIS ====================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"

# ⚠️ ATUALIZE ESTAS URLs COM AS SUAS
URL_APPS_SCRIPT_LOCALIDADES = 'https://script.google.com/macros/s/AKfycbzJv9YlseCXdvXwi0OOpxh-Q61rmCly2kMUBEtcv5VSyPEKdcKg7MAVvIgDYSM1yWpV/exec'
URL_APPS_SCRIPT_HISTORICO = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

# MÓDULO 1: LOCALIDADES
LOCALIDADES_RANGE_INICIO = 1
LOCALIDADES_RANGE_FIM = 50000
LOCALIDADES_NUM_THREADS = 20

# MÓDULO 3: HISTÓRICO
HISTORICO_ASYNC_CONNECTIONS = 250
HISTORICO_ASYNC_TIMEOUT = 4
HISTORICO_ASYNC_MAX_RETRIES = 2
HISTORICO_FALLBACK_TIMEOUT = 12
HISTORICO_FALLBACK_RETRIES = 4
HISTORICO_CIRURGICO_TIMEOUT = 20
HISTORICO_CIRURGICO_RETRIES = 6
HISTORICO_CIRURGICO_DELAY = 2
HISTORICO_CHUNK_SIZE = 400
HISTORICO_BATCH_ENVIO = 200  # ✅ Alunos por lote (ajustar se houver timeout)

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

# Stats globais
historico_stats = {
    'fase1_sucesso': 0, 'fase1_falha': 0,
    'fase2_sucesso': 0, 'fase2_falha': 0,
    'fase3_sucesso': 0, 'fase3_falha': 0,
    'com_dados': 0, 'sem_dados': 0,
    'tempo_inicio': None,
    'tempos_resposta': deque(maxlen=200),
    'alunos_processados': set()
}
stats_lock = threading.Lock()
print_lock = threading.Lock()

# ==================== FUNÇÕES AUXILIARES ====================

def safe_print(msg):
    with print_lock:
        print(msg, flush=True)

def criar_sessao_robusta():
    session = requests.Session()
    retry_strategy = Retry(
        total=3, backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "HEAD"]
    )
    adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=retry_strategy)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def extrair_cookies_playwright(pagina):
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def gerar_timestamp():
    return datetime.now().strftime('%d_%m_%Y-%H:%M')

# ==================== LOGIN ====================

def fazer_login_unico():
    print("\n" + "=" * 80)
    print("🔐 REALIZANDO LOGIN ÚNICO")
    print("=" * 80)
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        print("   Acessando página de login...")
        pagina.goto(URL_INICIAL, timeout=20000)
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')
        
        try:
            pagina.wait_for_selector("nav", timeout=20000)
            print("   ✅ Login realizado com sucesso!")
        except PlaywrightTimeoutError:
            print("   ❌ Falha no login")
            navegador.close()
            return None, None
        
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
    
    session = criar_sessao_robusta()
    session.cookies.update(cookies_dict)
    print("   ✅ Sessão configurada\n")
    return session, cookies_dict

# ==================== MÓDULO 1: LOCALIDADES ====================

def verificar_hortolandia(texto: str) -> bool:
    if not texto:
        return False
    texto_upper = texto.upper()
    variacoes = ["HORTOL", "HORTOLANDIA", "HORTOLÃNDIA", "HORTOLÂNDIA"]
    tem_hortolandia = any(var in texto_upper for var in variacoes)
    if not tem_hortolandia:
        return False
    return "BR-SP-CAMPINAS" in texto_upper or "CAMPINAS-HORTOL" in texto_upper

def extrair_dados_localidade(texto_completo: str, igreja_id: int) -> Dict:
    try:
        partes = texto_completo.split(' - ')
        if len(partes) >= 2:
            nome_localidade = partes[0].strip()
            caminho_completo = partes[1].strip()
            caminho_partes = caminho_completo.split('-')
            
            if len(caminho_partes) >= 4:
                setor = f"{caminho_partes[0]}-{caminho_partes[1]}-{caminho_partes[2]}"
                cidade = caminho_partes[3].strip()
            elif len(caminho_partes) >= 3:
                setor = '-'.join(caminho_partes[:-1])
                cidade = caminho_partes[-1].strip()
            else:
                setor = ''
                cidade = 'HORTOLANDIA'
            
            return {
                'id_igreja': igreja_id,
                'nome_localidade': nome_localidade,
                'setor': setor,
                'cidade': cidade,
                'texto_completo': texto_completo
            }
        
        return {
            'id_igreja': igreja_id,
            'nome_localidade': texto_completo,
            'setor': '',
            'cidade': 'HORTOLANDIA',
            'texto_completo': texto_completo
        }
    except:
        return {
            'id_igreja': igreja_id,
            'nome_localidade': texto_completo,
            'setor': '',
            'cidade': 'HORTOLANDIA',
            'texto_completo': texto_completo
        }

def verificar_id_hortolandia(igreja_id: int, session: requests.Session) -> Optional[Dict]:
    try:
        url = f"https://musical.congregacao.org.br/igrejas/filtra_igreja_setor?id_igreja={igreja_id}"
        resp = session.get(url, headers={'X-Requested-With': 'XMLHttpRequest'}, timeout=15)
        
        if resp.status_code == 200:
            resp.encoding = 'utf-8'
            json_data = resp.json()
            if isinstance(json_data, list) and len(json_data) > 0:
                texto_completo = json_data[0].get('text', '')
                if verificar_hortolandia(texto_completo):
                    return extrair_dados_localidade(texto_completo, igreja_id)
        return None
    except:
        return None

def executar_localidades(session):
    tempo_inicio = time.time()
    timestamp_execucao = datetime.now()
    
    print("\n" + "=" * 80)
    print("📍 MÓDULO 1: LOCALIDADES DE HORTOLÂNDIA")
    print("=" * 80)
    print(f"📊 Range: {LOCALIDADES_RANGE_INICIO:,} até {LOCALIDADES_RANGE_FIM:,}")
    print(f"🧵 Threads: {LOCALIDADES_NUM_THREADS}\n")
    
    localidades = []
    total_ids = LOCALIDADES_RANGE_FIM - LOCALIDADES_RANGE_INICIO + 1
    
    print(f"🚀 Processando {total_ids:,} IDs...")
    
    with ThreadPoolExecutor(max_workers=LOCALIDADES_NUM_THREADS) as executor:
        futures = {
            executor.submit(verificar_id_hortolandia, id_igreja, session): id_igreja 
            for id_igreja in range(LOCALIDADES_RANGE_INICIO, LOCALIDADES_RANGE_FIM + 1)
        }
        
        processados = 0
        for future in as_completed(futures):
            processados += 1
            resultado = future.result()
            
            if resultado:
                localidades.append(resultado)
                print(f"✓ [{processados}/{total_ids}] ID {resultado['id_igreja']}: {resultado['nome_localidade'][:50]}")
            
            if processados % 1000 == 0:
                print(f"   Progresso: {processados:,}/{total_ids:,} | {len(localidades)} encontradas")
    
    tempo_total = time.time() - tempo_inicio
    print(f"\n✅ Coleta finalizada: {len(localidades)} localidades")
    print(f"⏱️ Tempo: {tempo_total/60:.2f} minutos")
    
    # Backup
    timestamp_backup = timestamp_execucao.strftime('%d_%m_%Y-%H_%M')
    backup_file = f'backup_localidades_{timestamp_backup}.json'
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump({'localidades': localidades, 'timestamp': timestamp_backup}, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup: {backup_file}")
    
    # Enviar para Sheets
    print("\n📤 Enviando para Google Sheets...")
    dados_formatados = [
        [loc['id_igreja'], loc['nome_localidade'], loc['setor'], loc['cidade'], loc['texto_completo']]
        for loc in localidades
    ]
    
    payload = {
        "tipo": "nova_planilha_localidades",
        "nome_planilha": timestamp_execucao.strftime("Localidades_%d_%m_%y-%H:%M"),
        "headers": ["ID_Igreja", "Nome_Localidade", "Setor", "Cidade", "Texto_Completo"],
        "dados": dados_formatados,
        "metadata": {
            "total_localidades": len(localidades),
            "range_inicio": LOCALIDADES_RANGE_INICIO,
            "range_fim": LOCALIDADES_RANGE_FIM,
            "tempo_execucao_min": round(tempo_total/60, 2),
            "timestamp": timestamp_backup
        }
    }
    
    try:
        resp = requests.post(URL_APPS_SCRIPT_LOCALIDADES, json=payload, timeout=120)
        if resp.status_code == 200:
            resposta = resp.json()
            if resposta.get('status') == 'sucesso':
                print(f"✅ Planilha criada: {resposta.get('planilha', {}).get('url', 'N/A')}")
    except Exception as e:
        print(f"⚠️ Erro: {e}")
    
    return [loc['id_igreja'] for loc in localidades]

# ==================== MÓDULO 2: BUSCAR ALUNOS ====================

def buscar_alunos_hortolandia() -> List[Dict]:
    print("\n" + "=" * 80)
    print("🎓 MÓDULO 2: BUSCAR ALUNOS DE HORTOLÂNDIA")
    print("=" * 80)
    print("🔍 Buscando do Google Sheets...")
    
    try:
        response = requests.get(URL_APPS_SCRIPT_HISTORICO, params={"acao": "listar_ids_alunos"}, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('sucesso'):
                alunos = data.get('alunos', [])
                print(f"✅ {len(alunos)} alunos encontrados")
                return alunos
        
        print("❌ Erro ao buscar alunos")
        return []
    except Exception as e:
        print(f"❌ Erro: {e}")
        return []

# ==================== MÓDULO 3: HISTÓRICO ====================

def validar_resposta_rigorosa(text: str, id_aluno: int) -> tuple:
    if len(text) < 1000:
        return False, False
    if 'name="login"' in text or 'name="password"' in text:
        return False, False
    if 'class="nav-tabs"' not in text and 'id="mts"' not in text:
        return False, False
    tem_dados = '<tr>' in text and '<td>' in text
    return True, tem_dados

def extrair_dados_completo(html: str, id_aluno: int, nome_aluno: str) -> Dict:
    dados = {
        'mts_individual': [], 'mts_grupo': [], 'msa_individual': [], 'msa_grupo': [],
        'provas': [], 'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [], 'escalas_individual': [], 'escalas_grupo': []
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
                            campos = [c.get_text(strip=True) for c in cols[:7]]
                            dados['mts_individual'].append([id_aluno, nome_aluno] + campos)
            
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            campos = [c.get_text(strip=True) for c in cols[:3]]
                            dados['mts_grupo'].append([id_aluno, nome_aluno] + campos)
        
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
                            campos = [c.get_text(strip=True) for c in cols[:7]]
                            dados['msa_individual'].append([id_aluno, nome_aluno] + campos)
            
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            paginas_html = cols[0].decode_contents()
                            
                            fases_m = re.search(r'<b>Fase\(s\):</b>\s*de\s+([\d.]+)\s+até\s+([\d.]+)', paginas_html)
                            pag_m = re.search(r'<b>Página\(s\):</b>\s*de\s+(\d+)\s+até\s+(\d+)', paginas_html)
                            clave_m = re.search(r'<b>Clave\(s\):</b>\s*([^<\n]+)', paginas_html)
                            
                            fases_de = fases_m.group(1) if fases_m else ""
                            fases_ate = fases_m.group(2) if fases_m else ""
                            pag_de = pag_m.group(1) if pag_m else ""
                            pag_ate = pag_m.group(2) if pag_m else ""
                            claves = clave_m.group(1).strip() if clave_m else ""
                            
                            observacoes = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                            data_licao = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                            
                            dados['msa_grupo'].append([
                                id_aluno, nome_aluno, fases_de, fases_ate,
                                pag_de, pag_ate, claves, observacoes, data_licao
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
                            campos = [c.get_text(strip=True) for c in cols[:5]]
                            dados['provas'].append([id_aluno, nome_aluno] + campos)
        
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
                            campos = [c.get_text(strip=True) for c in cols[:7]]
                            dados['hinario_individual'].append([id_aluno, nome_aluno] + campos)
            
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            campos = [c.get_text(strip=True) for c in cols[:3]]
                            dados['hinario_grupo'].append([id_aluno, nome_aluno] + campos)
        
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
                            campos = [c.get_text(strip=True) for c in cols[:7]]
                            dados['metodos'].append([id_aluno, nome_aluno] + campos)
        
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
                            campos = [c.get_text(strip=True) for c in cols[:6]]
                            dados['escalas_individual'].append([id_aluno, nome_aluno] + campos)
            
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            campos = [c.get_text(strip=True) for c in cols[:3]]
                            dados['escalas_grupo'].append([id_aluno, nome_aluno] + campos)
    
    except Exception as e:
        safe_print(f"⚠️ Erro ao extrair dados do aluno {id_aluno}: {e}")
    
    return dados

async def coletar_aluno_async(session: aiohttp.ClientSession, aluno: Dict, semaphore: asyncio.Semaphore) -> tuple:
    id_aluno = aluno['id_aluno']
    nome_aluno = aluno['nome']
    url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
    
    async with semaphore:
        for tentativa in range(HISTORICO_ASYNC_MAX_RETRIES):
            try:
                timeout = aiohttp.ClientTimeout(total=HISTORICO_ASYNC_TIMEOUT)
                async with session.get(url, timeout=timeout) as response:
                    if response.status != 200:
                        if tentativa < HISTORICO_ASYNC_MAX_RETRIES - 1:
                            await asyncio.sleep(0.2 * (tentativa + 1))
                            continue
                        return None, aluno
                    
                    html = await response.text()
                    valido, tem_dados = validar_resposta_rigorosa(html, id_aluno)
                    
                    if not valido:
                        if tentativa < HISTORICO_ASYNC_MAX_RETRIES - 1:
                            await asyncio.sleep(0.2 * (tentativa + 1))
                            continue
                        return None, aluno
                    
                    dados = extrair_dados_completo(html, id_aluno, nome_aluno)
                    total = sum(len(v) for v in dados.values())
                    
                    with stats_lock:
                        if total > 0:
                            historico_stats['com_dados'] += 1
                        else:
                            historico_stats['sem_dados'] += 1
                        historico_stats['fase1_sucesso'] += 1
                        historico_stats['alunos_processados'].add(id_aluno)
                    
                    return dados, None
                    
            except asyncio.TimeoutError:
                if tentativa < HISTORICO_ASYNC_MAX_RETRIES - 1:
                    await asyncio.sleep(0.2 * (tentativa + 1))
                    continue
            except Exception:
                if tentativa < HISTORICO_ASYNC_MAX_RETRIES - 1:
                    await asyncio.sleep(0.2 * (tentativa + 1))
                    continue
        
        with stats_lock:
            historico_stats['fase1_falha'] += 1
        return None, aluno

async def processar_chunk_async(alunos_chunk: List[Dict], cookies_dict: Dict) -> tuple:
    connector = aiohttp.TCPConnector(
        limit=HISTORICO_ASYNC_CONNECTIONS,
        limit_per_host=HISTORICO_ASYNC_CONNECTIONS,
        ttl_dns_cache=300
    )
    timeout = aiohttp.ClientTimeout(total=HISTORICO_ASYNC_TIMEOUT)
    
    cookie_str = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()])
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Cookie': cookie_str,
        'Connection': 'keep-alive'
    }
    
    todos_dados = {
        'mts_individual': [], 'mts_grupo': [], 'msa_individual': [], 'msa_grupo': [],
        'provas': [], 'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [], 'escalas_individual': [], 'escalas_grupo': []
    }
    
    falhas = []
    semaphore = asyncio.Semaphore(HISTORICO_ASYNC_CONNECTIONS)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:
        tasks = [coletar_aluno_async(session, aluno, semaphore) for aluno in alunos_chunk]
        resultados = await asyncio.gather(*tasks, return_exceptions=True)
        
        for resultado in resultados:
            if isinstance(resultado, Exception):
                continue
            
            dados, aluno_falha = resultado
            if dados:
                for key in todos_dados.keys():
                    todos_dados[key].extend(dados[key])
            elif aluno_falha:
                falhas.append(aluno_falha)
    
    return todos_dados, falhas

def coletar_fallback_robusto(alunos: List[Dict], cookies_dict: Dict) -> tuple:
    if not alunos:
        return {
            'mts_individual': [], 'mts_grupo': [], 'msa_individual': [], 'msa_grupo': [],
            'provas': [], 'hinario_individual': [], 'hinario_grupo': [],
            'metodos': [], 'escalas_individual': [], 'escalas_grupo': []
        }, []
    
    safe_print(f"\n🎯 FASE 2: Fallback robusto para {len(alunos)} alunos...")
    
    session = requests.Session()
    session.cookies.update(cookies_dict)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    })
    
    todos_dados = {
        'mts_individual': [], 'mts_grupo': [], 'msa_individual': [], 'msa_grupo': [],
        'provas': [], 'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [], 'escalas_individual': [], 'escalas_grupo': []
    }
    
    falhas_persistentes = []
    processados = 0
    
    for aluno in alunos:
        id_aluno = aluno['id_aluno']
        nome_aluno = aluno['nome']
        url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
        sucesso = False
        
        for tentativa in range(HISTORICO_FALLBACK_RETRIES):
            try:
                resp = session.get(url, timeout=HISTORICO_FALLBACK_TIMEOUT)
                
                if resp.status_code == 200:
                    valido, tem_dados = validar_resposta_rigorosa(resp.text, id_aluno)
                    
                    if valido:
                        dados = extrair_dados_completo(resp.text, id_aluno, nome_aluno)
                        for key in todos_dados.keys():
                            todos_dados[key].extend(dados[key])
                        
                        total = sum(len(v) for v in dados.values())
                        with stats_lock:
                            if total > 0:
                                historico_stats['com_dados'] += 1
                            else:
                                historico_stats['sem_dados'] += 1
                            historico_stats['fase2_sucesso'] += 1
                            historico_stats['alunos_processados'].add(id_aluno)
                        
                        sucesso = True
                        break
                
                if tentativa < HISTORICO_FALLBACK_RETRIES - 1:
                    time.sleep(0.5 * (tentativa + 1))
            
            except Exception:
                if tentativa < HISTORICO_FALLBACK_RETRIES - 1:
                    time.sleep(0.5 * (tentativa + 1))
                    continue
        
        if not sucesso:
            with stats_lock:
                historico_stats['fase2_falha'] += 1
            falhas_persistentes.append(aluno)
        
        processados += 1
        if processados % 10 == 0:
            safe_print(f"   Fallback: {processados}/{len(alunos)} processados")
    
    session.close()
    return todos_dados, falhas_persistentes

def coletar_cirurgico(alunos: List[Dict], cookies_dict: Dict) -> tuple:
    if not alunos:
        return {
            'mts_individual': [], 'mts_grupo': [], 'msa_individual': [], 'msa_grupo': [],
            'provas': [], 'hinario_individual': [], 'hinario_grupo': [],
            'metodos': [], 'escalas_individual': [], 'escalas_grupo': []
        }, []
    
    safe_print(f"\n🔬 FASE 3: Coleta cirúrgica para {len(alunos)} alunos...")
    
    todos_dados = {
        'mts_individual': [], 'mts_grupo': [], 'msa_individual': [], 'msa_grupo': [],
        'provas': [], 'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [], 'escalas_individual': [], 'escalas_grupo': []
    }
    
    falhas_finais = []
    
    for idx, aluno in enumerate(alunos, 1):
        id_aluno = aluno['id_aluno']
        nome_aluno = aluno['nome']
        
        safe_print(f"   [{idx}/{len(alunos)}] ID {id_aluno} - {nome_aluno[:30]}...")
        
        url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
        sucesso = False
        
        for tentativa in range(HISTORICO_CIRURGICO_RETRIES):
            try:
                session = requests.Session()
                session.cookies.update(cookies_dict)
                session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
                
                resp = session.get(url, timeout=HISTORICO_CIRURGICO_TIMEOUT)
                
                if resp.status_code == 200:
                    valido, tem_dados = validar_resposta_rigorosa(resp.text, id_aluno)
                    
                    if valido:
                        dados = extrair_dados_completo(resp.text, id_aluno, nome_aluno)
                        for key in todos_dados.keys():
                            todos_dados[key].extend(dados[key])
                        
                        total = sum(len(v) for v in dados.values())
                        with stats_lock:
                            if total > 0:
                                historico_stats['com_dados'] += 1
                            else:
                                historico_stats['sem_dados'] += 1
                            historico_stats['fase3_sucesso'] += 1
                            historico_stats['alunos_processados'].add(id_aluno)
                        
                        safe_print(f"      ✅ Sucesso na tentativa {tentativa + 1}")
                        sucesso = True
                        break
                
                session.close()
                
                if tentativa < HISTORICO_CIRURGICO_RETRIES - 1:
                    time.sleep(HISTORICO_CIRURGICO_DELAY)
            
            except Exception:
                if tentativa < HISTORICO_CIRURGICO_RETRIES - 1:
                    time.sleep(HISTORICO_CIRURGICO_DELAY)
                continue
        
        if not sucesso:
            with stats_lock:
                historico_stats['fase3_falha'] += 1
            falhas_finais.append(aluno)
            safe_print(f"      ❌ Falha após {HISTORICO_CIRURGICO_RETRIES} tentativas")
        
        if idx < len(alunos):
            time.sleep(0.5)
    
    return todos_dados, falhas_finais

def mesclar_dados(dados1: Dict, dados2: Dict) -> Dict:
    resultado = {}
    for key in dados1.keys():
        resultado[key] = dados1[key] + dados2[key]
    return resultado

def filtrar_dados_vazios(dados: Dict) -> Dict:
    dados_filtrados = {}
    for categoria, valores in dados.items():
        if valores and len(valores) > 0:
            valores_validos = [v for v in valores if v and len(v) > 0]
            dados_filtrados[categoria] = valores_validos
        else:
            dados_filtrados[categoria] = []
    return dados_filtrados

def gerar_resumo_alunos(alunos: List[Dict], todos_dados: Dict) -> List[List]:
    """Gera resumo com 14 colunas"""
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
        
        # Calcular média de provas
        provas_aluno = [x for x in todos_dados['provas'] if x[0] == id_aluno]
        if provas_aluno:
            try:
                notas = []
                for prova in provas_aluno:
                    if len(prova) > 3:
                        nota_str = str(prova[3]).replace(',', '.')
                        try:
                            nota = float(nota_str)
                            notas.append(nota)
                        except:
                            pass
                media = sum(notas) / len(notas) if notas else 0
            except:
                media = 0
        else:
            media = 0
        
        resumo.append([
            id_aluno, nome, id_igreja,
            t_mts_i, t_mts_g, t_msa_i, t_msa_g,
            t_prov, round(media, 2),
            t_hin_i, t_hin_g, t_met,
            t_esc_i, t_esc_g
        ])
    
    return resumo

def enviar_historico_em_lotes(todos_dados, alunos_modulo2, tempo_total):
    """✅ Envia histórico em múltiplos lotes para evitar timeout"""
    print("\n📤 Enviando histórico em lotes para Google Sheets...")
    
    total_alunos = len(alunos_modulo2)
    num_lotes = (total_alunos + HISTORICO_BATCH_ENVIO - 1) // HISTORICO_BATCH_ENVIO
    
    print(f"📦 Dividindo {total_alunos} alunos em {num_lotes} lotes de ~{HISTORICO_BATCH_ENVIO}")
    
    for lote_idx in range(num_lotes):
        inicio = lote_idx * HISTORICO_BATCH_ENVIO
        fim = min(inicio + HISTORICO_BATCH_ENVIO, total_alunos)
        
        print(f"\n📦 Lote {lote_idx + 1}/{num_lotes}: Alunos {inicio+1}-{fim}...")
        
        # Filtrar dados do lote
        ids_lote = set(alunos_modulo2[i]['id_aluno'] for i in range(inicio, fim))
        
        dados_lote = {
            'mts_individual': [x for x in todos_dados['mts_individual'] if x[0] in ids_lote],
            'mts_grupo': [x for x in todos_dados['mts_grupo'] if x[0] in ids_lote],
            'msa_individual': [x for x in todos_dados['msa_individual'] if x[0] in ids_lote],
            'msa_grupo': [x for x in todos_dados['msa_grupo'] if x[0] in ids_lote],
            'provas': [x for x in todos_dados['provas'] if x[0] in ids_lote],
            'hinario_individual': [x for x in todos_dados['hinario_individual'] if x[0] in ids_lote],
            'hinario_grupo': [x for x in todos_dados['hinario_grupo'] if x[0] in ids_lote],
            'metodos': [x for x in todos_dados['metodos'] if x[0] in ids_lote],
            'escalas_individual': [x for x in todos_dados['escalas_individual'] if x[0] in ids_lote],
            'escalas_grupo': [x for x in todos_dados['escalas_grupo'] if x[0] in ids_lote]
        }
        
        # Gerar resumo do lote
        resumo_lote = gerar_resumo_alunos(alunos_modulo2[inicio:fim], dados_lote)
        
        payload = {
            'tipo': 'licoes_alunos_lote',
            'lote_numero': lote_idx + 1,
            'total_lotes': num_lotes,
            'mts_individual': dados_lote['mts_individual'],
            'mts_grupo': dados_lote['mts_grupo'],
            'msa_individual': dados_lote['msa_individual'],
            'msa_grupo': dados_lote['msa_grupo'],
            'provas': dados_lote['provas'],
            'hinario_individual': dados_lote['hinario_individual'],
            'hinario_grupo': dados_lote['hinario_grupo'],
            'metodos': dados_lote['metodos'],
            'escalas_individual': dados_lote['escalas_individual'],
            'escalas_grupo': dados_lote['escalas_grupo'],
            'resumo': resumo_lote,
            'metadata': {
                'alunos_inicio': inicio,
                'alunos_fim': fim,
                'total_alunos_lote': fim - inicio,
                'tempo_coleta_segundos': tempo_total,
                'data_coleta': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        }
        
        try:
            response = requests.post(URL_APPS_SCRIPT_HISTORICO, json=payload, timeout=300)
            if response.status_code == 200:
                result = response.json()
                if result.get('sucesso'):
                    print(f"   ✅ Lote {lote_idx + 1} enviado!")
                    if lote_idx == num_lotes - 1 and result.get('planilha'):
                        print(f"   🎉 Planilha finalizada: {result['planilha']['url']}")
                else:
                    print(f"   ⚠️ Erro: {result.get('erro', 'Desconhecido')}")
            else:
                print(f"   ⚠️ HTTP {response.status_code}")
        except Exception as e:
            print(f"   ⚠️ Erro ao enviar: {e}")
        
        # Delay entre lotes
        if lote_idx < num_lotes - 1:
            time.sleep(2)
    
    print("\n✅ Todos os lotes enviados!")

def executar_historico(cookies_dict, alunos_modulo2):
    """Executa coleta de histórico individual com sistema de 3 fases"""
    tempo_inicio = time.time()
    historico_stats['tempo_inicio'] = tempo_inicio
    
    print("\n" + "=" * 80)
    print("📚 MÓDULO 3: HISTÓRICO INDIVIDUAL")
    print("=" * 80)
    
    if not alunos_modulo2:
        print("❌ Nenhum aluno recebido. Abortando.")
        return
    
    print(f"🎓 Total de alunos: {len(alunos_modulo2)}")
    print(f"⚡ Conexões simultâneas: {HISTORICO_ASYNC_CONNECTIONS}")
    print(f"📦 Tamanho do chunk: {HISTORICO_CHUNK_SIZE}")
    
    todos_dados = {
        'mts_individual': [], 'mts_grupo': [], 'msa_individual': [], 'msa_grupo': [],
        'provas': [], 'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [], 'escalas_individual': [], 'escalas_grupo': []
    }
    
    # FASE 1: COLETA ASSÍNCRONA
    print(f"\n⚡ FASE 1: Coleta assíncrona em alta velocidade...")
    
    falhas_fase1 = []
    total_chunks = (len(alunos_modulo2) + HISTORICO_CHUNK_SIZE - 1) // HISTORICO_CHUNK_SIZE
    
    for i in range(0, len(alunos_modulo2), HISTORICO_CHUNK_SIZE):
        chunk = alunos_modulo2[i:i+HISTORICO_CHUNK_SIZE]
        chunk_num = i // HISTORICO_CHUNK_SIZE + 1
        
        safe_print(f"📦 Chunk {chunk_num}/{total_chunks} ({len(chunk)} alunos)...")
        
        dados_chunk, falhas_chunk = asyncio.run(processar_chunk_async(chunk, cookies_dict))
        todos_dados = mesclar_dados(todos_dados, dados_chunk)
        falhas_fase1.extend(falhas_chunk)
        
        if i + HISTORICO_CHUNK_SIZE < len(alunos_modulo2):
            time.sleep(0.5)
    
    print(f"\n✅ FASE 1 CONCLUÍDA")
    print(f"   Sucesso: {historico_stats['fase1_sucesso']} | Falhas: {len(falhas_fase1)}")
    
    # FASE 2: FALLBACK ROBUSTO
    if falhas_fase1:
        dados_fase2, falhas_fase2 = coletar_fallback_robusto(falhas_fase1, cookies_dict)
        todos_dados = mesclar_dados(todos_dados, dados_fase2)
        
        print(f"✅ FASE 2 CONCLUÍDA")
        print(f"   Recuperados: {historico_stats['fase2_sucesso']} | Falhas: {len(falhas_fase2)}")
    else:
        falhas_fase2 = []
        print("\n🎉 FASE 2 não necessária - todos processados na Fase 1!")
    
    # FASE 3: COLETA CIRÚRGICA
    if falhas_fase2:
        dados_fase3, falhas_finais = coletar_cirurgico(falhas_fase2, cookies_dict)
        todos_dados = mesclar_dados(todos_dados, dados_fase3)
        
        print(f"✅ FASE 3 CONCLUÍDA")
        print(f"   Recuperados: {historico_stats['fase3_sucesso']} | Falhas: {len(falhas_finais)}")
        
        if falhas_finais:
            print("\n⚠️ ALUNOS NÃO COLETADOS:")
            for aluno in falhas_finais:
                print(f"   - ID {aluno['id_aluno']}: {aluno['nome']}")
    else:
        print("\n🎉 FASE 3 não necessária!")
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n✅ Coleta finalizada!")
    print(f"   Alunos processados: {len(historico_stats['alunos_processados'])}")
    print(f"   Com dados: {historico_stats['com_dados']}")
    print(f"   Sem dados: {historico_stats['sem_dados']}")
    print(f"⏱️ Tempo: {tempo_total/60:.2f} minutos")
    
    # Filtrar dados vazios
    todos_dados = filtrar_dados_vazios(todos_dados)
    
    # Backup local
    timestamp = gerar_timestamp()
    backup_file = f'backup_historico_{timestamp.replace(":", "-")}.json'
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump({'dados': todos_dados, 'timestamp': timestamp}, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup: {backup_file}")
    
    # ✅ ENVIAR EM LOTES
    enviar_historico_em_lotes(todos_dados, alunos_modulo2, tempo_total)

# ==================== MAIN ====================

def main():
    tempo_inicio_total = time.time()
    
    print("\n" + "=" * 80)
    print("🎼 SISTEMA MUSICAL - COLETOR UNIFICADO V2.1")
    print("=" * 80)
    print("📋 Ordem de execução:")
    print("   1️⃣ Localidades de Hortolândia")
    print("   2️⃣ Buscar Alunos (do Google Sheets)")
    print("   3️⃣ Histórico Individual (envio em lotes)")
    print("=" * 80)
    
    # LOGIN
    session, cookies = fazer_login_unico()
    if not session:
        print("\n❌ Falha no login. Encerrando.")
    return
    
    # MÓDULO 1: LOCALIDADES
    ids_localidades = executar_localidades(session)
    
    # MÓDULO 2: BUSCAR ALUNOS
    alunos_modulo2 = buscar_alunos_hortolandia()
    
    if not alunos_modulo2:
        print("\n⚠️ Nenhum aluno encontrado. Pulando Módulo 3.")
    else:
        # MÓDULO 3: HISTÓRICO
        executar_historico(cookies, alunos_modulo2)
    
    tempo_total = time.time() - tempo_inicio_total
    
    print("\n" + "=" * 80)
    print("🎉 EXECUÇÃO COMPLETA!")
    print("=" * 80)
    print(f"⏱️ Tempo total: {tempo_total/60:.2f} minutos")
    print(f"📊 Resumo:")
    print(f"   • Localidades encontradas: {len(ids_localidades)}")
    print(f"   • Alunos processados: {len(alunos_modulo2) if alunos_modulo2 else 0}")
    if alunos_modulo2:
        print(f"   • Histórico - Com dados: {historico_stats['com_dados']}")
        print(f"   • Histórico - Sem dados: {historico_stats['sem_dados']}")
        print(f"   • Alunos únicos coletados: {len(historico_stats['alunos_processados'])}")
    print("=" * 80)
    print("\n✅ Sistema finalizado com sucesso!\n")

if __name__ == "__main__":
    main()
