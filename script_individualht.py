from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import re
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwByAvTIdpefgitKoSr0c3LepgfjsAyNbbEeV3krU1AkNEZca037RzpgHRhjmt-M8sesg/exec'

# Configurações do Google Sheets
SPREADSHEET_ID = '1lnzzToyBao-c5sptw4IcnXA0QCvS4bKFpyiQUcxbA3Q'
RANGE_NAME = 'alunos_hortolandia!A2:A'  # Assumindo que os IDs estão na coluna A

def obter_ids_alunos_google_sheets():
    """
    Busca os IDs dos alunos diretamente do Google Sheets
    Requer arquivo de credenciais JSON (service account)
    """
    print("\n📋 Obtendo lista de IDs dos alunos do Google Sheets...")
    try:
        # Carregar credenciais (você precisa criar um service account)
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        
        # Caminho para o arquivo de credenciais (você precisa baixar do Google Cloud Console)
        SERVICE_ACCOUNT_FILE = 'credenciais_google.json'
        
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            print(f"❌ Arquivo de credenciais não encontrado: {SERVICE_ACCOUNT_FILE}")
            print("📖 Consulte: https://developers.google.com/sheets/api/quickstart/python")
            return []
        
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        
        # Buscar dados
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        values = result.get('values', [])
        
        # Extrair IDs (primeira coluna)
        ids = [row[0] for row in values if row and row[0]]
        
        print(f"✅ {len(ids)} IDs obtidos da planilha!")
        return ids
        
    except Exception as e:
        print(f"❌ Erro ao obter IDs do Google Sheets: {e}")
        return []

def obter_ids_alunos():
    """
    Busca os IDs dos alunos do Google Apps Script
    """
    print("\n📋 Obtendo lista de IDs dos alunos via Apps Script...")
    try:
        response = requests.get(f"{URL_APPS_SCRIPT}?acao=obter_ids", timeout=30)
        if response.status_code == 200:
            dados = json.loads(response.text)
            ids = dados.get('ids', [])
            print(f"✅ {len(ids)} IDs obtidos da planilha!")
            return ids
        else:
            print(f"❌ Erro ao obter IDs: Status {response.status_code}")
            print(f"📝 Resposta: {response.text}")
            return []
    except Exception as e:
        print(f"❌ Erro ao obter IDs: {e}")
        return []

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def coletar_dados_aluno(session, id_aluno):
    """
    Coleta TODOS os dados de um aluno específico
    """
    try:
        url = f"https://musical.congregacao.org.br/alunos/editar/{id_aluno}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        resp = session.get(url, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        dados_aluno = {
            'id_aluno': id_aluno,
            'nome': '',
            'comum_congregacao': '',
            'cargo_ministerio': '',
            'nivel': '',
            'possui_instrumento': '',
            'instrumento_proprio': '',
            'instrumento': '',
            'tonalidade': '',
            'data_inicio_gem': '',
            'cadastrado_em': '',
            'cadastrado_por': '',
            'atualizado_em': '',
            'atualizado_por': '',
            'compartilhamentos': []
        }
        
        # Nome
        nome_input = soup.find('input', {'name': 'nome'})
        if nome_input:
            dados_aluno['nome'] = nome_input.get('value', '').strip()
        
        # Comum Congregação
        comum_select = soup.find('select', {'name': 'id_igreja'})
        if comum_select:
            selected = comum_select.find('option', {'selected': True})
            if selected:
                dados_aluno['comum_congregacao'] = selected.get_text(strip=True)
        
        # Cargo/Ministério
        cargo_select = soup.find('select', {'name': 'id_cargo'})
        if cargo_select:
            selected = cargo_select.find('option', {'selected': True})
            if selected:
                dados_aluno['cargo_ministerio'] = selected.get_text(strip=True)
        
        # Nível
        nivel_select = soup.find('select', {'name': 'id_nivel'})
        if nivel_select:
            selected = nivel_select.find('option', {'selected': True})
            if selected:
                dados_aluno['nivel'] = selected.get_text(strip=True)
        
        # Possui Instrumento
        possui_inst = soup.find('select', {'name': 'possui_instrumento'})
        if possui_inst:
            selected = possui_inst.find('option', {'selected': True})
            if selected:
                dados_aluno['possui_instrumento'] = selected.get_text(strip=True)
        
        # Instrumento Próprio
        inst_proprio = soup.find('select', {'name': 'instrumento_proprio'})
        if inst_proprio:
            selected = inst_proprio.find('option', {'selected': True})
            if selected:
                dados_aluno['instrumento_proprio'] = selected.get_text(strip=True)
        
        # Instrumento
        instrumento_select = soup.find('select', {'name': 'id_instrumento'})
        if instrumento_select:
            selected = instrumento_select.find('option', {'selected': True})
            if selected:
                dados_aluno['instrumento'] = selected.get_text(strip=True)
        
        # Tonalidade
        tonalidade_select = soup.find('select', {'name': 'id_tonalidade'})
        if tonalidade_select:
            selected = tonalidade_select.find('option', {'selected': True})
            if selected:
                dados_aluno['tonalidade'] = selected.get_text(strip=True)
        
        # Data Início GEM
        data_gem = soup.find('input', {'name': 'dt_inicio_gem'})
        if data_gem:
            dados_aluno['data_inicio_gem'] = data_gem.get('value', '').strip()
        
        # Histórico do registro
        panel_body = soup.find('div', {'id': 'collapseOne'})
        if panel_body:
            paragrafos = panel_body.find_all('p')
            for p in paragrafos:
                texto = p.get_text(strip=True)
                if 'Cadastrado em:' in texto:
                    match = re.search(r'Cadastrado em:\s*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})\s+por:\s*(.+)', texto)
                    if match:
                        dados_aluno['cadastrado_em'] = match.group(1)
                        dados_aluno['cadastrado_por'] = match.group(2)
                elif 'Atualizado em:' in texto:
                    match = re.search(r'Atualizado em:\s*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})\s+por:\s*(.+)', texto)
                    if match:
                        dados_aluno['atualizado_em'] = match.group(1)
                        dados_aluno['atualizado_por'] = match.group(2)
        
        # Compartilhamentos
        compartilhamentos_tbody = soup.find('table', class_='table table-striped')
        if compartilhamentos_tbody:
            tbody = compartilhamentos_tbody.find('tbody')
            if tbody:
                linhas = tbody.find_all('tr')
                for linha in linhas:
                    tds = linha.find_all('td')
                    if len(tds) >= 3:
                        comp = {
                            'congregacao': tds[0].get_text(strip=True),
                            'data_cadastro': tds[1].get_text(strip=True),
                            'usuario': tds[2].get_text(strip=True)
                        }
                        dados_aluno['compartilhamentos'].append(comp)
        
        return dados_aluno
        
    except Exception as e:
        print(f"❌ Erro ao coletar aluno {id_aluno}: {e}")
        return None

def coletar_historico_aluno(session, id_aluno):
    """
    Coleta o histórico completo de lições de um aluno
    """
    try:
        url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        resp = session.get(url, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        historico = {
            'id_aluno': id_aluno,
            'mts_individual': [],
            'mts_grupo': [],
            'msa_individual': [],
            'msa_grupo': [],
            'provas': [],
            'hinos_individual': [],
            'hinos_grupo': [],
            'metodos': [],
            'escalas_individual': [],
            'escalas_grupo': []
        }
        
        # MSA GRUPO
        msa_grupo_table = None
        h3_tags = soup.find_all('h3')
        for h3 in h3_tags:
            if 'MSA - Aulas em grupo' in h3.get_text():
                msa_grupo_table = h3.find_next('table', {'id': 'datatable_mts_grupo'})
                break
        
        if msa_grupo_table:
            tbody = msa_grupo_table.find('tbody')
            if tbody:
                linhas = tbody.find_all('tr')
                for linha in linhas:
                    tds = linha.find_all('td')
                    if len(tds) >= 3:
                        conteudo = tds[0].get_text(strip=True)
                        observacoes = tds[1].get_text(strip=True)
                        data_licao = tds[2].get_text(strip=True)
                        
                        fases = paginas = claves = ""
                        
                        if 'Fase(s):' in conteudo:
                            match_fase = re.search(r'Fase\(s\):\s*(.+?);', conteudo)
                            if match_fase:
                                fases = match_fase.group(1).strip()
                        
                        if 'Página(s):' in conteudo:
                            match_pag = re.search(r'Página\(s\):\s*(.+?);', conteudo)
                            if match_pag:
                                paginas = match_pag.group(1).strip()
                        
                        if 'Clave(s):' in conteudo:
                            match_clave = re.search(r'Clave\(s\):\s*(.+?)(?:$|<)', conteudo)
                            if match_clave:
                                claves = match_clave.group(1).strip()
                        
                        historico['msa_grupo'].append({
                            'fases': fases,
                            'paginas': paginas,
                            'claves': claves,
                            'observacoes': observacoes,
                            'data_licao': data_licao
                        })
        
        # MTS INDIVIDUAL
        mts_pane = soup.find('div', {'id': 'mts'})
        if mts_pane:
            mts_table = mts_pane.find('table', {'id': 'datatable1'})
            if mts_table:
                tbody = mts_table.find('tbody')
                if tbody:
                    linhas = tbody.find_all('tr')
                    for linha in linhas:
                        tds = linha.find_all('td')
                        if len(tds) >= 7:
                            historico['mts_individual'].append({
                                'modulo': tds[0].get_text(strip=True),
                                'licoes': tds[1].get_text(strip=True),
                                'data_licao': tds[2].get_text(strip=True),
                                'autorizante': tds[3].get_text(strip=True),
                                'data_cadastro': tds[4].get_text(strip=True),
                                'data_alteracao': tds[5].get_text(strip=True),
                                'observacoes': tds[6].get_text(strip=True)
                            })
        
        # MSA INDIVIDUAL
        msa_pane = soup.find('div', {'id': 'msa'})
        if msa_pane:
            msa_table = msa_pane.find('table', {'id': 'datatable1'})
            if msa_table:
                tbody = msa_table.find('tbody')
                if tbody:
                    linhas = tbody.find_all('tr')
                    for linha in linhas:
                        tds = linha.find_all('td')
                        if len(tds) >= 7:
                            historico['msa_individual'].append({
                                'data_licao': tds[0].get_text(strip=True),
                                'fases': tds[1].get_text(strip=True),
                                'paginas': tds[2].get_text(strip=True),
                                'licoes': tds[3].get_text(strip=True),
                                'claves': tds[4].get_text(strip=True),
                                'observacoes': tds[5].get_text(strip=True),
                                'autorizante': tds[6].get_text(strip=True)
                            })
        
        # PROVAS
        provas_pane = soup.find('div', {'id': 'provas'})
        if provas_pane:
            provas_table = provas_pane.find('table', {'id': 'datatable2'})
            if provas_table:
                tbody = provas_table.find('tbody')
                if tbody:
                    linhas = tbody.find_all('tr')
                    for linha in linhas:
                        tds = linha.find_all('td')
                        if len(tds) >= 5:
                            historico['provas'].append({
                                'modulo': tds[0].get_text(strip=True),
                                'nota': tds[1].get_text(strip=True),
                                'data_prova': tds[2].get_text(strip=True),
                                'autorizante': tds[3].get_text(strip=True),
                                'data_cadastro': tds[4].get_text(strip=True)
                            })
        
        # HINOS INDIVIDUAL
        hinario_pane = soup.find('div', {'id': 'hinario'})
        if hinario_pane:
            hinos_table = hinario_pane.find('table', {'id': 'datatable4'})
            if hinos_table:
                tbody = hinos_table.find('tbody')
                if tbody:
                    linhas = tbody.find_all('tr')
                    for linha in linhas:
                        tds = linha.find_all('td')
                        if len(tds) >= 7:
                            historico['hinos_individual'].append({
                                'hino': tds[0].get_text(strip=True),
                                'voz': tds[1].get_text(strip=True),
                                'data_aula': tds[2].get_text(strip=True),
                                'autorizante': tds[3].get_text(strip=True),
                                'data_cadastro': tds[4].get_text(strip=True),
                                'data_alteracao': tds[5].get_text(strip=True),
                                'observacoes': tds[6].get_text(strip=True)
                            })
        
        # MÉTODOS
        metodos_pane = soup.find('div', {'id': 'metodos'})
        if metodos_pane:
            metodos_table = metodos_pane.find('table', {'id': 'datatable3'})
            if metodos_table:
                tbody = metodos_table.find('tbody')
                if tbody:
                    linhas = tbody.find_all('tr')
                    for linha in linhas:
                        tds = linha.find_all('td')
                        if len(tds) >= 7:
                            historico['metodos'].append({
                                'paginas': tds[0].get_text(strip=True),
                                'licao': tds[1].get_text(strip=True),
                                'metodo': tds[2].get_text(strip=True),
                                'data_licao': tds[3].get_text(strip=True),
                                'autorizante': tds[4].get_text(strip=True),
                                'data_cadastro': tds[5].get_text(strip=True),
                                'observacoes': tds[6].get_text(strip=True)
                            })
        
        # ESCALAS INDIVIDUAL
        escalas_pane = soup.find('div', {'id': 'escalas'})
        if escalas_pane:
            escalas_table = escalas_pane.find('table', {'id': 'datatable4'})
            if escalas_table:
                tbody = escalas_table.find('tbody')
                if tbody:
                    linhas = tbody.find_all('tr')
                    for linha in linhas:
                        tds = linha.find_all('td')
                        if len(tds) >= 6:
                            historico['escalas_individual'].append({
                                'escala': tds[0].get_text(strip=True),
                                'data': tds[1].get_text(strip=True),
                                'autorizante': tds[2].get_text(strip=True),
                                'data_cadastro': tds[3].get_text(strip=True),
                                'data_alteracao': tds[4].get_text(strip=True),
                                'observacoes': tds[5].get_text(strip=True)
                            })
        
        return historico
        
    except Exception as e:
        print(f"❌ Erro ao coletar histórico do aluno {id_aluno}: {e}")
        return None

def processar_aluno_completo(session, id_aluno):
    """Coleta dados gerais + histórico de um aluno"""
    print(f"🔄 Processando aluno {id_aluno}...")
    
    dados_aluno = coletar_dados_aluno(session, id_aluno)
    if not dados_aluno:
        print(f"❌ Falha ao coletar dados do aluno {id_aluno}")
        return None
    
    historico = coletar_historico_aluno(session, id_aluno)
    if not historico:
        print(f"⚠️  Histórico vazio para aluno {id_aluno}")
        historico = {}
    
    dados_completos = {
        'dados_aluno': dados_aluno,
        'historico': historico
    }
    
    print(f"✅ Aluno {id_aluno} - {dados_aluno['nome']}")
    return dados_completos

def main():
    tempo_inicio = time.time()
    
    print("=" * 70)
    print("🎓 COLETOR DE DADOS DE ALUNOS - HORTOLÂNDIA")
    print("=" * 70)
    
    # Login com Playwright
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        pagina.goto(URL_INICIAL)
        
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')
        
        try:
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado!")
        except PlaywrightTimeoutError:
            print("❌ Falha no login.")
            navegador.close()
            return
        
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        navegador.close()
    
    # Tentar obter IDs via Apps Script primeiro
    ids_alunos = obter_ids_alunos()
    
    # Se falhar, tentar via Google Sheets API
    if not ids_alunos:
        print("\n⚠️  Tentando método alternativo (Google Sheets API)...")
        ids_alunos = obter_ids_alunos_google_sheets()
    
    if not ids_alunos:
        print("❌ Nenhum ID de aluno encontrado. Abortando.")
        return
    
    print(f"\n📊 Total de alunos para processar: {len(ids_alunos)}")
    print("=" * 70)
    
    # Processar alunos em paralelo
    resultado = []
    processados = 0
    
    MAX_WORKERS = 5
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(processar_aluno_completo, session, id_aluno): id_aluno 
            for id_aluno in ids_alunos
        }
        
        for future in as_completed(futures):
            processados += 1
            dados_completos = future.result()
            
            if dados_completos:
                resultado.append(dados_completos)
            
            if processados % 10 == 0:
                print(f"\n{'─' * 70}")
                print(f"⚡ {processados}/{len(ids_alunos)} alunos processados")
                print(f"{'─' * 70}\n")
    
    print(f"\n{'=' * 70}")
    print(f"🎉 COLETA FINALIZADA!")
    print(f"{'=' * 70}")
    print(f"✅ Alunos processados: {len(resultado)}/{len(ids_alunos)}")
    print(f"⏱️  Tempo total: {(time.time() - tempo_inicio)/60:.1f} minutos")
    print(f"{'=' * 70}\n")
    
    # Enviar para Google Sheets
    print("📤 Enviando dados para Google Sheets...")
    try:
        body = {
            "acao": "salvar_dados",
            "dados": resultado,
            "resumo": {
                "total_alunos": len(resultado),
                "tempo_minutos": round((time.time() - tempo_inicio)/60, 2)
            }
        }
        
        resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
        print(f"✅ Dados enviados! Status: {resposta_post.status_code}")
        print(f"📝 Resposta: {resposta_post.text}")
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
        with open('backup_alunos.json', 'w', encoding='utf-8') as f:
            json.dump(resultado, f, ensure_ascii=False, indent=2)
        print("💾 Dados salvos em backup_alunos.json")

if __name__ == "__main__":
    main()
