from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
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
from typing import List, Dict, Optional, Set
from collections import deque
import threading

# ==================== CONFIGURAÇÕES GLOBAIS ====================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"

# URLs dos Apps Scripts
URL_APPS_SCRIPT_LOCALIDADES = 'https://script.google.com/macros/s/AKfycbzJv9YlseCXdvXwi0OOpxh-Q61rmCly2kMUBEtcv5VSyPEKdcKg7MAVvIgDYSM1yWpV/exec'
URL_APPS_SCRIPT_ALUNOS = 'https://script.google.com/macros/s/AKfycbzl1l143sg2_S5a6bOQy6WqWATMDZpSglIyKUp3OVZtycuHXQmGjisOpzffHTW5TvyK/exec'
URL_APPS_SCRIPT_HISTORICO = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

# MÓDULO 1: LOCALIDADES
LOCALIDADES_RANGE_INICIO = 1
LOCALIDADES_RANGE_FIM = 50000
LOCALIDADES_NUM_THREADS = 20

# MÓDULO 2: ALUNOS
ALUNOS_RANGE_INICIO = 1
ALUNOS_RANGE_FIM = 850000
ALUNOS_NUM_THREADS = 25

# MÓDULO 3: HISTÓRICO - Configuração híbrida
HISTORICO_ASYNC_CONNECTIONS = 250
HISTORICO_ASYNC_TIMEOUT = 4
HISTORICO_ASYNC_MAX_RETRIES = 2
HISTORICO_FALLBACK_TIMEOUT = 12
HISTORICO_FALLBACK_RETRIES = 4
HISTORICO_CIRURGICO_TIMEOUT = 20
HISTORICO_CIRURGICO_RETRIES = 6
HISTORICO_CIRURGICO_DELAY = 2
HISTORICO_CHUNK_SIZE = 400
HISTORICO_TAMANHO_LOTE = 400

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

# Stats para histórico
historico_stats = {
    'fase1_sucesso': 0,
    'fase1_falha': 0,
    'fase2_sucesso': 0,
    'fase2_falha': 0,
    'fase3_sucesso': 0,
    'fase3_falha': 0,
    'com_dados': 0,
    'sem_dados': 0,
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
    """Cria sessão HTTP com retry automático"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "HEAD"]
    )
    adapter = HTTPAdapter(
        pool_connections=20,
        pool_maxsize=20,
        max_retries=retry_strategy
    )
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def gerar_timestamp():
    """Gera timestamp no formato DD_MM_YYYY-HH:MM"""
    return datetime.now().strftime('%d_%m_%Y-%H:%M')

# ==================== LOGIN ÚNICO ====================

def fazer_login_unico():
    """Realiza login único via Playwright e retorna sessão requests configurada"""
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
            print("   ❌ Falha no login. Verifique as credenciais.")
            navegador.close()
            return None, None
        
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
    
    session = criar_sessao_robusta()
    session.cookies.update(cookies_dict)
    
    print("   ✅ Sessão configurada e pronta para uso\n")
    return session, cookies_dict

# ==================== MÓDULO 1: LOCALIDADES ====================

def verificar_hortolandia(texto: str) -> bool:
    """Verifica se o texto contém referência a Hortolândia DO SETOR CAMPINAS"""
    if not texto:
        return False
    
    texto_upper = texto.upper()
    variacoes_hortolandia = ["HORTOL", "HORTOLANDIA", "HORTOLÃNDIA", "HORTOLÂNDIA"]
    tem_hortolandia = any(var in texto_upper for var in variacoes_hortolandia)
    
    if not tem_hortolandia:
        return False
    
    tem_setor_campinas = "BR-SP-CAMPINAS" in texto_upper or "CAMPINAS-HORTOL" in texto_upper
    return tem_setor_campinas

def extrair_dados_localidade(texto_completo: str, igreja_id: int) -> Dict:
    """Extrai dados estruturados da localidade"""
    try:
        partes = texto_completo.split(' - ')
        
        if len(partes) >= 2:
            nome_localidade = partes[0].strip()
            caminho_completo = partes[1].strip()
            caminho_partes = caminho_completo.split('-')
            
            if len(caminho_partes) >= 4:
                pais = caminho_partes[0].strip()
                estado = caminho_partes[1].strip()
                regiao = caminho_partes[2].strip()
                cidade = caminho_partes[3].strip()
                setor = f"{pais}-{estado}-{regiao}"
                
                return {
                    'id_igreja': igreja_id,
                    'nome_localidade': nome_localidade,
                    'setor': setor,
                    'cidade': cidade,
                    'texto_completo': texto_completo
                }
            elif len(caminho_partes) >= 3:
                setor = '-'.join(caminho_partes[:-1])
                cidade = caminho_partes[-1].strip()
                
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
        
    except Exception as e:
        print(f"⚠ Erro ao extrair dados do ID {igreja_id}: {e}")
        return {
            'id_igreja': igreja_id,
            'nome_localidade': texto_completo,
            'setor': '',
            'cidade': 'HORTOLANDIA',
            'texto_completo': texto_completo
        }

def verificar_id_hortolandia(igreja_id: int, session: requests.Session) -> Optional[Dict]:
    """Verifica um único ID de localidade"""
    try:
        url = f"https://musical.congregacao.org.br/igrejas/filtra_igreja_setor?id_igreja={igreja_id}"
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        resp = session.get(url, headers=headers, timeout=15)
        
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
    """Executa coleta de localidades e RETORNA OS IDs DAS IGREJAS"""
    tempo_inicio = time.time()
    timestamp_execucao = datetime.now()
    
    print("\n" + "=" * 80)
    print("📍 MÓDULO 1: LOCALIDADES DE HORTOLÂNDIA")
    print("=" * 80)
    print(f"📊 Range: {LOCALIDADES_RANGE_INICIO:,} até {LOCALIDADES_RANGE_FIM:,}")
    print(f"🧵 Threads: {LOCALIDADES_NUM_THREADS}")
    
    localidades = []
    total_ids = LOCALIDADES_RANGE_FIM - LOCALIDADES_RANGE_INICIO + 1
    
    print(f"\n🚀 Processando {total_ids:,} IDs...")
    
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
                print(f"   Progresso: {processados:,}/{total_ids:,} | {len(localidades)} localidades encontradas")
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n✅ Coleta finalizada: {len(localidades)} localidades encontradas")
    print(f"⏱️ Tempo: {tempo_total/60:.2f} minutos")
    
    # Backup local
    timestamp_backup = timestamp_execucao.strftime('%d_%m_%Y-%H_%M')
    backup_file = f'backup_localidades_{timestamp_backup}.json'
    
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump({'localidades': localidades, 'timestamp': timestamp_backup}, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo: {backup_file}")
    
    # Enviar para Google Sheets
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
        resp = requests.post(URL_APPS_SCRIPT_LOCALIDADES, json=payload, timeout=60)
        if resp.status_code == 200:
            resposta = resp.json()
            if resposta.get('status') == 'sucesso':
                print(f"✅ Planilha criada: {resposta.get('planilha', {}).get('url', 'N/A')}")
    except Exception as e:
        print(f"⚠️ Erro ao enviar: {e}")
    
    # RETORNA lista de IDs de igrejas
    ids_igrejas = [loc['id_igreja'] for loc in localidades]
    print(f"\n📦 Retornando {len(ids_igrejas)} IDs de igrejas para o próximo módulo")
    return ids_igrejas

# ==================== MÓDULO 2: COLETA DE ALUNOS ====================

def coletar_dados_membro(id_membro: int, session: requests.Session, ids_igrejas_set: Set[int]) -> Optional[Dict]:
    """Coleta dados completos de um único membro"""
    try:
        url = f"https://musical.congregacao.org.br/membros/view/{id_membro}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        
        resp = session.get(url, headers=headers, timeout=15)
        
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Verificar se é página válida de membro
        if 'login' in resp.text.lower() or len(resp.text) < 500:
            return None
        
        # Extrair dados
        nome_elem = soup.find('h2') or soup.find('h3')
        nome = nome_elem.get_text(strip=True) if nome_elem else ''
        
        if not nome:
            return None
        
        # Extrair ID da igreja
        id_igreja = None
        igreja_elem = soup.find('a', href=re.compile(r'/igrejas/view/\d+'))
        if igreja_elem:
            match = re.search(r'/igrejas/view/(\d+)', igreja_elem.get('href', ''))
            if match:
                id_igreja = int(match.group(1))
        
        # Filtrar apenas igrejas de Hortolândia
        if id_igreja not in ids_igrejas_set:
            return None
        
        # Extrair outros dados
        cargo_elem = soup.find('span', string=re.compile(r'Cargo', re.I))
        cargo_nome = ''
        id_cargo = 0
        if cargo_elem:
            cargo_parent = cargo_elem.find_parent()
            if cargo_parent:
                cargo_nome = cargo_parent.get_text(strip=True).replace('Cargo:', '').strip()
        
        nivel_elem = soup.find('span', string=re.compile(r'Nível', re.I))
        nivel_nome = ''
        id_nivel = 0
        if nivel_elem:
            nivel_parent = nivel_elem.find_parent()
            if nivel_parent:
                nivel_nome = nivel_parent.get_text(strip=True).replace('Nível:', '').strip()
        
        instrumento_elem = soup.find('span', string=re.compile(r'Instrumento', re.I))
        instrumento_nome = ''
        id_instrumento = 0
        if instrumento_elem:
            inst_parent = instrumento_elem.find_parent()
            if inst_parent:
                instrumento_nome = inst_parent.get_text(strip=True).replace('Instrumento:', '').strip()
        
        tonalidade_elem = soup.find('span', string=re.compile(r'Tonalidade', re.I))
        tonalidade_nome = ''
        id_tonalidade = 0
        if tonalidade_elem:
            ton_parent = tonalidade_elem.find_parent()
            if ton_parent:
                tonalidade_nome = ton_parent.get_text(strip=True).replace('Tonalidade:', '').strip()
        
        # Status (1 = ativo, 0 = inativo)
        status_elem = soup.find('span', class_='badge')
        status = 1
        if status_elem and 'inativo' in status_elem.get_text(strip=True).lower():
            status = 0
        
        fl_tipo = '1'
        
        return {
            'id_membro': id_membro,
            'nome': nome,
            'id_igreja': id_igreja,
            'id_cargo': id_cargo,
            'cargo_nome': cargo_nome,
            'id_nivel': id_nivel,
            'nivel_nome': nivel_nome,
            'id_instrumento': id_instrumento,
            'instrumento_nome': instrumento_nome,
            'id_tonalidade': id_tonalidade,
            'tonalidade_nome': tonalidade_nome,
            'fl_tipo': fl_tipo,
            'status': status
        }
        
    except Exception:
        return None

def executar_coleta_alunos(session, ids_igrejas):
    """Executa coleta de alunos das igrejas de Hortolândia"""
    tempo_inicio = time.time()
    timestamp_execucao = datetime.now()
    
    print("\n" + "=" * 80)
    print("🎓 MÓDULO 2: COLETA DE ALUNOS DE HORTOLÂNDIA")
    print("=" * 80)
    print(f"🏛️ Igrejas monitoradas: {len(ids_igrejas)}")
    print(f"🔢 Range de IDs: {ALUNOS_RANGE_INICIO:,} até {ALUNOS_RANGE_FIM:,}")
    print(f"🧵 Threads: {ALUNOS_NUM_THREADS}")
    
    ids_igrejas_set = set(ids_igrejas)
    
    membros = []
    total_ids = ALUNOS_RANGE_FIM - ALUNOS_RANGE_INICIO + 1
    
    print(f"\n🚀 Processando {total_ids:,} IDs de membros...")
    
    with ThreadPoolExecutor(max_workers=ALUNOS_NUM_THREADS) as executor:
        futures = {
            executor.submit(coletar_dados_membro, id_membro, session, ids_igrejas_set): id_membro 
            for id_membro in range(ALUNOS_RANGE_INICIO, ALUNOS_RANGE_FIM + 1)
        }
        
        processados = 0
        for future in as_completed(futures):
            processados += 1
            resultado = future.result()
            
            if resultado:
                membros.append(resultado)
                print(f"✓ [{processados}/{total_ids}] ID {resultado['id_membro']}: {resultado['nome'][:50]}")
            
            if processados % 1000 == 0:
                print(f"   Progresso: {processados:,}/{total_ids:,} | {len(membros)} membros encontrados")
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n✅ Coleta finalizada: {len(membros)} membros encontrados")
    print(f"⏱️ Tempo: {tempo_total/60:.2f} minutos")
    
    # Backup local
    timestamp_backup = timestamp_execucao.strftime('%d_%m_%Y-%H_%M')
    backup_file = f'backup_membros_{timestamp_backup}.json'
    
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump({'membros': membros, 'timestamp': timestamp_backup}, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo: {backup_file}")
    
    # Enviar para Google Sheets
    print("\n📤 Enviando para Google Sheets...")
    
    dados_formatados = [
        ['ID_MEMBRO', 'NOME', 'ID_IGREJA', 'ID_CARGO', 'CARGO_NOME', 'ID_NIVEL', 'NIVEL_NOME', 
         'ID_INSTRUMENTO', 'INSTRUMENTO_NOME', 'ID_TONALIDADE', 'TONALIDADE_NOME', 'FL_TIPO', 'STATUS']
    ]
    
    for membro in membros:
        dados_formatados.append([
            membro['id_membro'],
            membro['nome'],
            membro['id_igreja'],
            membro['id_cargo'],
            membro['cargo_nome'],
            membro['id_nivel'],
            membro['nivel_nome'],
            membro['id_instrumento'],
            membro['instrumento_nome'],
            membro['id_tonalidade'],
            membro['tonalidade_nome'],
            membro['fl_tipo'],
            membro['status']
        ])
    
    payload = {
        "tipo": "nova_planilha_membros_completo",
        "timestamp": timestamp_backup,
        "relatorio_formatado": dados_formatados,
        "metadata": {
            "total_membros": len(membros),
            "total_igrejas_monitoradas": len(ids_igrejas),
            "range_inicio": ALUNOS_RANGE_INICIO,
            "range_fim": ALUNOS_RANGE_FIM,
            "tempo_execucao_min": round(tempo_total/60, 2),
            "timestamp": timestamp_execucao.isoformat()
        }
    }
    
    try:
        resp = requests.post(URL_APPS_SCRIPT_ALUNOS, json=payload, timeout=60)
        if resp.status_code == 200:
            resposta = resp.json()
            if resposta.get('status') == 'sucesso':
                print(f"✅ Planilha criada: {resposta.get('planilha', {}).get('url', 'N/A')}")
            else:
                print(f"⚠️ Erro: {resposta.get('mensagem', 'Desconhecido')}")
        else:
            print(f"⚠️ Erro HTTP {resp.status_code}")
    except Exception as e:
        print(f"⚠️ Erro ao enviar: {e}")
    
    # Converter para formato esperado pelo Módulo 3
    alunos = [
        {
            'id_aluno': m['id_membro'],
            'nome': m['nome'],
            'id_igreja': m['id_igreja']
        }
        for m in membros
    ]
    
    print(f"\n📦 Retornando {len(alunos)} alunos para o Módulo 3")
    return alunos

# ==================== MÓDULO 3: HISTÓRICO INDIVIDUAL ====================

def validar_resposta_rigorosa(text: str, id_aluno: int) -> tuple:
    """Validação rigorosa da resposta - Retorna: (valido, tem_dados)"""
    if len(text) < 1000:
        return False, False
    
    if 'name="login"' in text or 'name="password"' in text:
        return False, False
    
    if 'class="nav-tabs"' not in text and 'id="mts"' not in text:
        return False, False
    
    tem_tabela = 'table' in text and 'tbody' in text
    tem_dados = '<tr>' in text and '<td>' in text
    
    return True, tem_dados

def extrair_dados_completo(html: str, id_aluno: int, nome_aluno: str) -> Dict:
    """Extração completa e robusta de todos os dados"""
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
                            campos = [c.get_text(strip=True) for c in cols[:7]]
                            dados['mts_individual'].append([id_aluno, nome_aluno] + campos)
            
            # MTS Grupo
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
            
            # MSA Grupo
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            paginas_html = cols[0].decode_contents()
                            
                            fases_de = ""
                            fases_ate = ""
                            pag_de = ""
                            pag_ate = ""
                            claves = ""
                            
                            fases_m = re.search(r'<b>Fase\(s\):</b>\s*de\s+([\d.]+)\s+até\s+([\d.]+)', paginas_html)
                            if fases_m:
                                fases_de = fases_m.group(1)
                                fases_ate = fases_m.group(2)
                            
                            pag_m = re.search(r'<b>Página\(s\):</b>\s*de\s+(\d+)\s+até\s+(\d+)', paginas_html)
                            if pag_m:
                                pag_de = pag_m.group(1)
                                pag_ate = pag_m.group(2)
                            
                            clave_m = re.search(r'<b>Clave\(s\):</b>\s*([^<\n]+)', paginas_html)
                            if clave_m:
                                claves = clave_m.group(1).strip()
                            
                            observacoes = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                            data_licao = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                            
                            dados['msa_grupo'].append([
                                id_aluno, nome_aluno,
                                fases_de, fases_ate,
                                pag_de, pag_ate,
                                claves, observacoes, data_licao
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
            
            # HINÁRIO Grupo
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
            
            # ESCALAS Grupo
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
    """Coleta assíncrona com validação rigorosa"""
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
    """Processa chunk com coleta assíncrona"""
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
        'mts_individual': [], 'mts_grupo': [],
        'msa_individual': [], 'msa_grupo': [],
        'provas': [],
        'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [], 'escalas_grupo': []
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
    """Fallback síncrono com múltiplas tentativas"""
    if not alunos:
        return {
            'mts_individual': [], 'mts_grupo': [],
            'msa_individual': [], 'msa_grupo': [],
            'provas': [],
            'hinario_individual': [], 'hinario_grupo': [],
            'metodos': [],
            'escalas_individual': [], 'escalas_grupo': []
        }, []
    
    safe_print(f"\n🎯 FASE 2: Fallback robusto para {len(alunos)} alunos...")
    
    session = requests.Session()
    session.cookies.update(cookies_dict)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    })
    
    todos_dados = {
        'mts_individual': [], 'mts_grupo': [],
        'msa_individual': [], 'msa_grupo': [],
        'provas': [],
        'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [], 'escalas_grupo': []
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
    """Coleta cirúrgica individual com máximo esforço"""
    if not alunos:
        return {
            'mts_individual': [], 'mts_grupo': [],
            'msa_individual': [], 'msa_grupo': [],
            'provas': [],
            'hinario_individual': [], 'hinario_grupo': [],
            'metodos': [],
            'escalas_individual': [], 'escalas_grupo': []
        }, []
    
    safe_print(f"\n🔬 FASE 3: Coleta cirúrgica para {len(alunos)} alunos...")
    
    todos_dados = {
        'mts_individual': [], 'mts_grupo': [],
        'msa_individual': [], 'msa_grupo': [],
        'provas': [],
        'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [], 'escalas_grupo': []
    }
    
    falhas_finais = []
    
    for idx, aluno in enumerate(alunos, 1):
        id_aluno = aluno['id_aluno']
        nome_aluno = aluno['nome']
        
        safe_print(f"   [{idx}/{len(alunos)}] Tentando ID {id_aluno} - {nome_aluno[:30]}...")
        
        url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
        sucesso = False
        
        for tentativa in range(HISTORICO_CIRURGICO_RETRIES):
            try:
                session = requests.Session()
                session.cookies.update(cookies_dict)
                session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                })
                
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
    """Mescla dois dicionários de dados"""
    resultado = {}
    for key in dados1.keys():
        resultado[key] = dados1[key] + dados2[key]
    return resultado

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

def filtrar_dados_vazios(dados: Dict) -> Dict:
    """Filtra arrays vazios"""
    dados_filtrados = {}
    
    for categoria, valores in dados.items():
        if valores and len(valores) > 0:
            valores_validos = [v for v in valores if v and len(v) > 0]
            dados_filtrados[categoria] = valores_validos
        else:
            dados_filtrados[categoria] = []
    
    return dados_filtrados

def executar_historico(cookies_dict, alunos_modulo2):
    """✅ CORRIGIDO: Executa coleta + ENVIO EM LOTES para Google Sheets"""
    tempo_inicio = time.time()
    historico_stats['tempo_inicio'] = tempo_inicio
    
    print("\n" + "=" * 80)
    print("📚 MÓDULO 3: HISTÓRICO INDIVIDUAL")
    print("=" * 80)
    
    if not alunos_modulo2:
        print("❌ Nenhum aluno recebido do Módulo 2. Abortando.")
        return
    
    print(f"🎓 Total de alunos a processar: {len(alunos_modulo2)}")
    print(f"⚡ Conexões simultâneas: {HISTORICO_ASYNC_CONNECTIONS}")
    print(f"📦 Tamanho do chunk: {HISTORICO_CHUNK_SIZE}")
    
    todos_dados = {
        'mts_individual': [], 'mts_grupo': [],
        'msa_individual': [], 'msa_grupo': [],
        'provas': [],
        'hinario_individual': [], 'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [], 'escalas_grupo': []
    }
    
    # ========== FASE 1: COLETA ASSÍNCRONA ==========
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
    
    # ========== FASE 2: FALLBACK ROBUSTO ==========
    if falhas_fase1:
        dados_fase2, falhas_fase2 = coletar_fallback_robusto(falhas_fase1, cookies_dict)
        todos_dados = mesclar_dados(todos_dados, dados_fase2)
        
        print(f"✅ FASE 2 CONCLUÍDA")
        print(f"   Recuperados: {historico_stats['fase2_sucesso']} | Falhas: {len(falhas_fase2)}")
    else:
        falhas_fase2 = []
        print("\n🎉 FASE 2 não necessária - todos processados na Fase 1!")
    
    # ========== FASE 3: COLETA CIRÚRGICA ==========
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
    
    # Gerar resumo
    resumo_alunos = gerar_resumo_alunos(alunos_modulo2, todos_dados)
    
    # Backup local
    timestamp = gerar_timestamp()
    backup_file = f'backup_historico_{timestamp.replace(":", "-")}.json'
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump({
            'dados': todos_dados,
            'resumo': resumo_alunos,
            'timestamp': timestamp
        }, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo: {backup_file}")
    
    # ========== ✅ ENVIAR EM LOTES PARA GOOGLE SHEETS ==========
    print("\n📤 Enviando para Google Sheets EM LOTES...")
    
    total_alunos = len(alunos_modulo2)
    total_lotes = (total_alunos + HISTORICO_TAMANHO_LOTE - 1) // HISTORICO_TAMANHO_LOTE
    
    print(f"📦 Total de lotes: {total_lotes} ({HISTORICO_TAMANHO_LOTE} alunos/lote)")
    
    # Criar conjuntos de IDs por lote
    alunos_ids_lotes = [
        set([aluno['id_aluno'] for aluno in alunos_modulo2[i:i+HISTORICO_TAMANHO_LOTE]])
        for i in range(0, total_alunos, HISTORICO_TAMANHO_LOTE)
    ]
    
    for lote_num in range(1, total_lotes + 1):
        ids_lote = alunos_ids_lotes[lote_num - 1]
        
        # Filtrar dados deste lote
        dados_lote = {
            'mts_individual': [d for d in todos_dados['mts_individual'] if d[0] in ids_lote],
            'mts_grupo': [d for d in todos_dados['mts_grupo'] if d[0] in ids_lote],
            'msa_individual': [d for d in todos_dados['msa_individual'] if d[0] in ids_lote],
            'msa_grupo': [d for d in todos_dados['msa_grupo'] if d[0] in ids_lote],
            'provas': [d for d in todos_dados['provas'] if d[0] in ids_lote],
            'hinario_individual': [d for d in todos_dados['hinario_individual'] if d[0] in ids_lote],
            'hinario_grupo': [d for d in todos_dados['hinario_grupo'] if d[0] in ids_lote],
            'metodos': [d for d in todos_dados['metodos'] if d[0] in ids_lote],
            'escalas_individual': [d for d in todos_dados['escalas_individual'] if d[0] in ids_lote],
            'escalas_grupo': [d for d in todos_dados['escalas_grupo'] if d[0] in ids_lote]
        }
        
        # Filtrar resumo deste lote
        resumo_lote = [r for r in resumo_alunos if r[0] in ids_lote]
        
        payload = {
            'tipo': 'licoes_alunos_lote',
            'lote_numero': lote_num,
            'total_lotes': total_lotes,
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
                'alunos_inicio': (lote_num - 1) * HISTORICO_TAMANHO_LOTE,
                'alunos_fim': min(lote_num * HISTORICO_TAMANHO_LOTE, total_alunos),
                'total_alunos_processados': total_alunos,
                'alunos_com_dados': historico_stats['com_dados'],
                'alunos_sem_dados': historico_stats['sem_dados'],
                'tempo_coleta_segundos': tempo_total,
                'data_coleta': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        }
        
        print(f"📤 Enviando lote {lote_num}/{total_lotes}...")
        
        try:
            response = requests.post(
                URL_APPS_SCRIPT_HISTORICO, 
                json=payload, 
                timeout=120
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('sucesso'):
                    print(f"   ✅ Lote {lote_num} enviado com sucesso")
                    
                    # Se é o último lote, mostrar URL final
                    if lote_num == total_lotes and 'planilha' in result:
                        print(f"\n🎉 PLANILHA COMPLETA CRIADA!")
                        print(f"📊 URL: {result['planilha']['url']}")
                else:
                    print(f"   ⚠️ Erro no lote {lote_num}: {result.get('erro', 'Desconhecido')}")
            else:
                print(f"   ⚠️ Erro HTTP {response.status_code} no lote {lote_num}")
        
        except requests.exceptions.Timeout:
            print(f"   ⚠️ Timeout no lote {lote_num} (continuando...)")
        except Exception as e:
            print(f"   ⚠️ Erro no lote {lote_num}: {e}")
        
        # Delay entre lotes
        if lote_num < total_lotes:
            time.sleep(2)
    
    print("\n✅ Envio em lotes concluído!")

# ==================== MAIN - ORQUESTRADOR ====================

def main():
    tempo_inicio_total = time.time()
    
    print("\n" + "=" * 80)
    print("🎼 SISTEMA MUSICAL - COLETOR UNIFICADO V2.0 CORRIGIDO")
    print("=" * 80)
    print("📋 Ordem de execução:")
    print("   1️⃣ Login Único")
    print("   2️⃣ Localidades de Hortolândia (salva IDs de igrejas)")
    print("   3️⃣ Buscar Alunos das Igrejas Coletadas")
    print("   4️⃣ Histórico Individual (coleta + envio em lotes)")
    print("=" * 80)
    
    # ==================== PASSO 1: LOGIN ÚNICO ====================
    session, cookies = fazer_login_unico()
    
    if not session:
        print("\n❌ Falha no login. Encerrando processo.")
        return
    
    # ==================== PASSO 2: EXECUTAR LOCALIDADES ====================
    print("\n🔄 Iniciando Módulo 1: Localidades...")
    ids_igrejas = executar_localidades(session)
    
    if not ids_igrejas:
        print("\n❌ Nenhuma localidade encontrada. Abortando processo.")
        return
    
    print(f"\n✅ Módulo 1 concluído: {len(ids_igrejas)} igrejas identificadas")
    
    # Salvar IDs em JSON para backup
    timestamp_ids = datetime.now().strftime('%d_%m_%Y-%H_%M')
    with open(f'ids_igrejas_{timestamp_ids}.json', 'w') as f:
        json.dump({'ids': ids_igrejas, 'timestamp': timestamp_ids}, f)
    print(f"💾 IDs salvos em: ids_igrejas_{timestamp_ids}.json")
    
    # ==================== PASSO 3: COLETAR ALUNOS ====================
    print("\n🔄 Iniciando Módulo 2: Coleta de Alunos...")
    alunos = executar_coleta_alunos(session, ids_igrejas)
    
    if not alunos:
        print("\n❌ Módulo 2 não encontrou alunos. Abortando processo.")
        return
    
    print(f"\n✅ Módulo 2 concluído: {len(alunos)} alunos coletados")
    
    # ==================== VALIDAÇÃO: IDs de igreja compatíveis ====================
    ids_igrejas_alunos = set([a['id_igreja'] for a in alunos])
    ids_igrejas_locais = set(ids_igrejas)
    
    print(f"\n🔍 Validação de Integridade:")
    print(f"   IDs de igrejas (Módulo 1): {len(ids_igrejas_locais)}")
    print(f"   IDs únicos nos alunos: {len(ids_igrejas_alunos)}")
    
    intersecao = ids_igrejas_alunos.intersection(ids_igrejas_locais)
    print(f"   IDs compatíveis: {len(intersecao)}")
    
    if len(intersecao) == len(ids_igrejas_alunos):
        print(f"   ✅ Todos os alunos pertencem às igrejas identificadas!")
    else:
        print(f"   ⚠️ {len(ids_igrejas_alunos) - len(intersecao)} IDs de alunos não encontrados nas localidades")
    
    # ==================== PASSO 4: EXECUTAR HISTÓRICO COM LOTES ====================
    print("\n🔄 Iniciando Módulo 3: Histórico Individual...")
    executar_historico(cookies, alunos)
    
    # ==================== RESUMO FINAL ====================
    tempo_total = time.time() - tempo_inicio_total
    
    print("\n" + "=" * 80)
    print("🎉 PROCESSO COMPLETO FINALIZADO!")
    print("=" * 80)
    print(f"⏱️ Tempo total: {tempo_total/60:.2f} minutos")
    print(f"📊 Módulos executados:")
    print(f"   ✅ Módulo 1: {len(ids_igrejas)} localidades coletadas")
    print(f"   ✅ Módulo 2: {len(alunos)} alunos coletados")
    print(f"   ✅ Módulo 3: {len(historico_stats['alunos_processados'])} históricos")
    print(f"\n💾 Backups locais:")
    print(f"   📄 backup_localidades_*.json")
    print(f"   📄 backup_membros_*.json")
    print(f"   📄 backup_historico_*.json")
    print(f"   📄 ids_igrejas_*.json")
    print(f"\n📊 Planilhas criadas no Google Sheets:")
    print(f"   📁 Localidades → Pasta: 1i53hnPKn0M5TG6489HbzkTd0p393z4xf")
    print(f"   📁 Membros → Pasta: 1cQVxXJBMxW62Hu1hq9RlpkRP2WBzM7YL")
    print(f"   📁 Histórico → Pasta: 1aplI0rCB-s9NXCrDNcvkcfQim_xfBZja")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    main()
