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

# URLs dos Apps Scripts (um para cada tipo de dados)
URL_APPS_SCRIPT_LOCALIDADES = 'https://script.google.com/macros/s/AKfycbzJv9YlseCXdvXwi0OOpxh-Q61rmCly2kMUBEtcv5VSyPEKdcKg7MAVvIgDYSM1yWpV/exec'
URL_APPS_SCRIPT_ALUNOS = 'https://script.google.com/macros/s/AKfycbzJv9YlseCXdvXwi0OOpxh-Q61rmCly2kMUBEtcv5VSyPEKdcKg7MAVvIgDYSM1yWpV/exec'
URL_APPS_SCRIPT_HISTORICO = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

# Configurações por módulo
# MÓDULO 1: LOCALIDADES
LOCALIDADES_RANGE_INICIO = 1
LOCALIDADES_RANGE_FIM = 50000
LOCALIDADES_NUM_THREADS = 20

# MÓDULO 2: ALUNOS
ALUNOS_RANGE_INICIO = 602300
ALUNOS_RANGE_FIM = 602400
ALUNOS_NUM_THREADS = 25

# MÓDULO 3: HISTÓRICO
HISTORICO_ASYNC_CONNECTIONS = 250
HISTORICO_CHUNK_SIZE = 400

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

# ==================== FUNÇÕES AUXILIARES COMPARTILHADAS ====================

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
    """
    Realiza login único via Playwright e retorna sessão requests configurada
    """
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
    """
    Executa coleta de localidades e RETORNA OS IDs DAS IGREJAS
    """
    tempo_inicio = time.time()
    timestamp_execucao = datetime.now()
    
    print("\n" + "=" * 80)
    print("📍 MÓDULO 1: LOCALIDADES DE HORTOLÂNDIA")
    print("=" * 80)
    print(f"📊 Range: {LOCALIDADES_RANGE_INICIO:,} até {LOCALIDADES_RANGE_FIM:,}")
    print(f"🧵 Threads: {LOCALIDADES_NUM_THREADS}")
    
    localidades = []
    total_ids = LOCALIDADES_RANGE_FIM - LOCALIDADES_RANGE_INICIO + 1
    batch_size = max(50, total_ids // LOCALIDADES_NUM_THREADS)
    
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

# ==================== MÓDULO 2: ALUNOS ====================

def extrair_dados_completos_membro(html_content: str, id_membro: int) -> Optional[Dict]:
    """Extrai TODOS os dados disponíveis do membro do HTML"""
    if not html_content or 'igreja_selecionada' not in html_content:
        return None
    
    soup = BeautifulSoup(html_content, 'html.parser')
    dados = {'id_membro': id_membro}
    
    # Nome
    nome_input = soup.find('input', {'name': 'nome'})
    dados['nome'] = nome_input.get('value', '').strip() if nome_input else ''
    
    # ID da Igreja
    match_igreja = re.search(r'igreja_selecionada\s*\((\d+)\)', html_content)
    dados['id_igreja'] = int(match_igreja.group(1)) if match_igreja else None
    
    # Cargo/Ministério
    cargo_select = soup.find('select', {'name': 'id_cargo'})
    if cargo_select:
        cargo_option = cargo_select.find('option', {'selected': True})
        if cargo_option:
            dados['id_cargo'] = cargo_option.get('value', '')
            dados['cargo_nome'] = cargo_option.text.strip()
        else:
            dados['id_cargo'] = ''
            dados['cargo_nome'] = ''
    else:
        dados['id_cargo'] = ''
        dados['cargo_nome'] = ''
    
    # Nível
    nivel_select = soup.find('select', {'name': 'id_nivel'})
    if nivel_select:
        nivel_option = nivel_select.find('option', {'selected': True})
        if nivel_option:
            dados['id_nivel'] = nivel_option.get('value', '')
            dados['nivel_nome'] = nivel_option.text.strip()
        else:
            dados['id_nivel'] = ''
            dados['nivel_nome'] = ''
    else:
        dados['id_nivel'] = ''
        dados['nivel_nome'] = ''
    
    # Instrumento
    instrumento_select = soup.find('select', {'name': 'id_instrumento'})
    if instrumento_select:
        instrumento_option = instrumento_select.find('option', {'selected': True})
        if instrumento_option:
            dados['id_instrumento'] = instrumento_option.get('value', '')
            dados['instrumento_nome'] = instrumento_option.text.strip()
        else:
            dados['id_instrumento'] = ''
            dados['instrumento_nome'] = ''
    else:
        dados['id_instrumento'] = ''
        dados['instrumento_nome'] = ''
    
    # Tonalidade
    tonalidade_select = soup.find('select', {'name': 'id_tonalidade'})
    if tonalidade_select:
        tonalidade_option = tonalidade_select.find('option', {'selected': True})
        if tonalidade_option:
            dados['id_tonalidade'] = tonalidade_option.get('value', '')
            dados['tonalidade_nome'] = tonalidade_option.text.strip()
        else:
            dados['id_tonalidade'] = ''
            dados['tonalidade_nome'] = ''
    else:
        dados['id_tonalidade'] = ''
        dados['tonalidade_nome'] = ''
    
    # Status
    fl_tipo_input = soup.find('input', {'name': 'fl_tipo'})
    dados['fl_tipo'] = fl_tipo_input.get('value', '') if fl_tipo_input else ''
    
    status_input = soup.find('input', {'name': 'status'})
    dados['status'] = status_input.get('value', '') if status_input else ''
    
    return dados

def coletar_membro(session: requests.Session, membro_id: int, ids_igrejas: Set[int]) -> Optional[Dict]:
    """Coleta dados de um único membro"""
    try:
        url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        resp = session.get(url, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            dados_membro = extrair_dados_completos_membro(resp.text, membro_id)
            
            if dados_membro and dados_membro['id_igreja'] in ids_igrejas:
                return dados_membro
        
        return None
        
    except:
        return None

def executar_alunos(session, ids_igrejas_modulo1):
    """
    Executa coleta de alunos usando IDs de igrejas do Módulo 1
    RETORNA lista de alunos com seus IDs
    """
    tempo_inicio = time.time()
    timestamp_execucao = gerar_timestamp()
    
    print("\n" + "=" * 80)
    print("🎓 MÓDULO 2: ALUNOS DE HORTOLÂNDIA")
    print("=" * 80)
    
    ids_igrejas = set(ids_igrejas_modulo1)
    
    if not ids_igrejas:
        print("❌ Nenhum ID de igreja recebido do Módulo 1. Abortando.")
        return []
    
    print(f"🏛️ Monitorando {len(ids_igrejas)} igrejas: {sorted(list(ids_igrejas))}")
    print(f"📊 Range: {ALUNOS_RANGE_INICIO:,} até {ALUNOS_RANGE_FIM:,}")
    print(f"🧵 Threads: {ALUNOS_NUM_THREADS}")
    
    membros_hortolandia = []
    total_ids = ALUNOS_RANGE_FIM - ALUNOS_RANGE_INICIO + 1
    
    print(f"\n🚀 Processando {total_ids:,} IDs...")
    
    with ThreadPoolExecutor(max_workers=ALUNOS_NUM_THREADS) as executor:
        futures = {
            executor.submit(coletar_membro, session, membro_id, ids_igrejas): membro_id 
            for membro_id in range(ALUNOS_RANGE_INICIO, ALUNOS_RANGE_FIM + 1)
        }
        
        processados = 0
        for future in as_completed(futures):
            processados += 1
            resultado = future.result()
            
            if resultado:
                membros_hortolandia.append(resultado)
                print(f"✓ [{processados}/{total_ids}] ID {resultado['id_membro']}: {resultado['nome'][:30]} | {resultado.get('instrumento_nome', 'N/A')}")
            
            if processados % 5000 == 0:
                print(f"   Progresso: {processados:,}/{total_ids:,} | {len(membros_hortolandia)} membros encontrados")
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n✅ Coleta finalizada: {len(membros_hortolandia)} membros encontrados")
    print(f"⏱️ Tempo: {tempo_total/60:.2f} minutos")
    
    # Backup local
    backup_file = f'backup_membros_{timestamp_execucao.replace(":", "-")}.json'
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump({'membros': membros_hortolandia, 'timestamp': timestamp_execucao}, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo: {backup_file}")
    
    # Enviar para Google Sheets
    print("\n📤 Enviando para Google Sheets...")
    
    headers = [
        "ID_MEMBRO", "NOME", "ID_IGREJA", 
        "ID_CARGO", "CARGO_NOME", 
        "ID_NIVEL", "NIVEL_NOME",
        "ID_INSTRUMENTO", "INSTRUMENTO_NOME",
        "ID_TONALIDADE", "TONALIDADE_NOME",
        "FL_TIPO", "STATUS"
    ]
    
    relatorio = [headers]
    for membro in membros_hortolandia:
        linha = [
            str(membro.get('id_membro', '')),
            membro.get('nome', ''),
            str(membro.get('id_igreja', '')),
            str(membro.get('id_cargo', '')),
            membro.get('cargo_nome', ''),
            str(membro.get('id_nivel', '')),
            membro.get('nivel_nome', ''),
            str(membro.get('id_instrumento', '')),
            membro.get('instrumento_nome', ''),
            str(membro.get('id_tonalidade', '')),
            membro.get('tonalidade_nome', ''),
            str(membro.get('fl_tipo', '')),
            str(membro.get('status', ''))
        ]
        relatorio.append(linha)
    
    payload = {
        "tipo": "nova_planilha_membros_completo",
        "timestamp": timestamp_execucao,
        "relatorio_formatado": relatorio,
        "metadata": {
            "total_membros": len(membros_hortolandia),
            "total_igrejas_monitoradas": len(ids_igrejas),
            "range_inicio": ALUNOS_RANGE_INICIO,
            "range_fim": ALUNOS_RANGE_FIM,
            "tempo_execucao_min": round(tempo_total/60, 2),
            "timestamp": timestamp_execucao
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT_ALUNOS, json=payload, timeout=180)
        if response.status_code == 200:
            resultado = response.json()
            if resultado.get('status') == 'sucesso':
                print(f"✅ Planilha criada: {resultado['planilha']['url']}")
    except Exception as e:
        print(f"⚠️ Erro ao enviar: {e}")
    
    # RETORNA lista de alunos (com IDs e nomes) para o próximo módulo
    alunos_para_historico = [
        {
            'id_aluno': m['id_membro'],
            'nome': m['nome'],
            'id_igreja': m['id_igreja']
        }
        for m in membros_hortolandia
    ]
    
    print(f"\n📦 Retornando {len(alunos_para_historico)} alunos para o próximo módulo")
    return alunos_para_historico

# ==================== MÓDULO 3: HISTÓRICO INDIVIDUAL ====================

# Stats para o módulo de histórico
historico_stats = {
    'fase1_sucesso': 0,
    'fase1_falha': 0,
    'com_dados': 0,
    'sem_dados': 0,
    'tempo_inicio': None,
    'tempos_resposta': deque(maxlen=200),
    'alunos_processados': set()
}
stats_lock = threading.Lock()

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
    """Extração completa de todos os dados de histórico"""
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
            
            # MTS Grupo
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
            
            # MSA Grupo
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            dados['msa_grupo'].append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:3]])
        
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
            
            # HINÁRIO Grupo
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
            
            # ESCALAS Grupo
            if len(tabelas) > 1:
                tbody_g = tabelas[1].find('tbody')
                if tbody_g:
                    for linha in tbody_g.find_all('tr'):
                        cols = linha.find_all('td')
                        if len(cols) >= 3:
                            dados['escalas_grupo'].append([id_aluno, nome_aluno] + [c.get_text(strip=True) for c in cols[:3]])
    
    except Exception as e:
        pass
    
    return dados

async def coletar_aluno_async(session: aiohttp.ClientSession, aluno: Dict, semaphore: asyncio.Semaphore) -> tuple:
    """Coleta assíncrona de histórico de um aluno"""
    id_aluno = aluno['id_aluno']
    nome_aluno = aluno['nome']
    
    url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
    
    async with semaphore:
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with session.get(url, timeout=timeout) as response:
                if response.status != 200:
                    return None, aluno
                
                html = await response.text()
                valido, tem_dados = validar_resposta_rigorosa(html, id_aluno)
                
                if not valido:
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
                
        except:
            with stats_lock:
                historico_stats['fase1_falha'] += 1
            return None, aluno

async def processar_chunk_async(alunos_chunk: List[Dict], cookies_dict: Dict) -> tuple:
    """Processa chunk de alunos com coleta assíncrona"""
    connector = aiohttp.TCPConnector(
        limit=HISTORICO_ASYNC_CONNECTIONS,
        limit_per_host=HISTORICO_ASYNC_CONNECTIONS,
        ttl_dns_cache=300
    )
    timeout = aiohttp.ClientTimeout(total=10)
    
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

def mesclar_dados(dados1: Dict, dados2: Dict) -> Dict:
    """Mescla dois dicionários de dados"""
    resultado = {}
    for key in dados1.keys():
        resultado[key] = dados1[key] + dados2[key]
    return resultado

def gerar_resumo_alunos(alunos: List[Dict], todos_dados: Dict) -> List[List]:
    """Gera resumo de registros por aluno"""
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

def executar_historico(cookies_dict, alunos_modulo2):
    """
    Executa coleta de histórico individual usando lista de alunos do Módulo 2
    """
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
    
    print(f"\n🚀 Processando em chunks assíncronos...")
    
    total_chunks = (len(alunos_modulo2) + HISTORICO_CHUNK_SIZE - 1) // HISTORICO_CHUNK_SIZE
    
    for i in range(0, len(alunos_modulo2), HISTORICO_CHUNK_SIZE):
        chunk = alunos_modulo2[i:i+HISTORICO_CHUNK_SIZE]
        chunk_num = i // HISTORICO_CHUNK_SIZE + 1
        
        print(f"📦 Chunk {chunk_num}/{total_chunks} ({len(chunk)} alunos)...")
        
        dados_chunk, falhas_chunk = asyncio.run(processar_chunk_async(chunk, cookies_dict))
        todos_dados = mesclar_dados(todos_dados, dados_chunk)
        
        if i + HISTORICO_CHUNK_SIZE < len(alunos_modulo2):
            time.sleep(0.5)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n✅ Coleta finalizada!")
    print(f"   Alunos processados: {len(historico_stats['alunos_processados'])}")
    print(f"   Com dados: {historico_stats['com_dados']}")
    print(f"   Sem dados: {historico_stats['sem_dados']}")
    print(f"⏱️ Tempo: {tempo_total/60:.2f} minutos")
    
    # Backup local
    timestamp = gerar_timestamp()
    backup_file = f'backup_historico_{timestamp.replace(":", "-")}.json'
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump({'dados': todos_dados, 'timestamp': timestamp}, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo: {backup_file}")
    
    # Enviar para Google Sheets
    print("\n📤 Enviando para Google Sheets...")
    
    resumo_alunos = gerar_resumo_alunos(alunos_modulo2, todos_dados)
    
    payload = {
        'tipo': 'licoes_alunos',
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
        'resumo': resumo_alunos,
        'metadata': {
            'total_alunos_processados': len(alunos_modulo2),
            'alunos_com_dados': historico_stats['com_dados'],
            'alunos_sem_dados': historico_stats['sem_dados'],
            'tempo_coleta_segundos': tempo_total,
            'data_coleta': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT_HISTORICO, json=payload, timeout=300)
        if response.status_code == 200:
            result = response.json()
            if result.get('sucesso'):
                print("✅ Dados enviados com sucesso!")
            else:
                print(f"⚠️ Erro do servidor: {result.get('erro', 'Desconhecido')}")
        else:
            print(f"⚠️ Erro HTTP {response.status_code}")
    except Exception as e:
        print(f"⚠️ Erro ao enviar: {e}")

# ==================== MAIN - ORQUESTRADOR ====================

def main():
    tempo_inicio_total = time.time()
    
    print("\n" + "=" * 80)
    print("🎼 SISTEMA MUSICAL - COLETOR UNIFICADO")
    print("=" * 80)
    print("📋 Ordem de execução:")
    print("   1️⃣ Localidades de Hortolândia")
    print("   2️⃣ Alunos das Localidades")
    print("   3️⃣ Histórico Individual dos Alunos")
    print("=" * 80)
    
    # PASSO 1: Login único
    session, cookies = fazer_login_unico()
    
    if not session:
        print("\n❌ Falha no login. Encerrando processo.")
        return
    
    # PASSO 2: Executar Localidades - RETORNA lista de IDs de igrejas
    ids_igrejas = executar_localidades(session)
    
    if not ids_igrejas:
        print("\n⚠️ Módulo 1 falhou. Interrompendo processo.")
        return
    
    # PASSO 3: Executar Alunos - USA IDs de igrejas, RETORNA lista de alunos
    alunos = executar_alunos(session, ids_igrejas)
    
    if not alunos:
        print("\n⚠️ Módulo 2 falhou. Interrompendo processo.")
        return
    
    # PASSO 4: Executar Histórico - USA lista de alunos
    executar_historico(cookies, alunos)
    
    # RESUMO FINAL
    tempo_total = time.time() - tempo_inicio_total
    
    print("\n" + "=" * 80)
    print("🎉 PROCESSO COMPLETO FINALIZADO!")
    print("=" * 80)
    print(f"⏱️ Tempo total: {tempo_total/60:.2f} minutos")
    print(f"📊 Módulos executados:")
    print(f"   ✅ Módulo 1: {len(ids_igrejas)} localidades")
    print(f"   ✅ Módulo 2: {len(alunos)} alunos")
    print(f"   ✅ Módulo 3: {len(historico_stats['alunos_processados'])} históricos")
    print(f"💾 Todos os backups salvos localmente")
    print(f"📊 Planilhas criadas no Google Sheets")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    main()
