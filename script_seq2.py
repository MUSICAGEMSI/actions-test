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
import unicodedata

# ==================== CONFIGURAÇÕES GLOBAIS ====================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"

# URLs dos Apps Scripts (um para cada tipo de dados)
URL_APPS_SCRIPT_AULAS = 'https://script.google.com/macros/s/AKfycbyvEGIUPIvgbSuT_yikqg03nEjqXryd6RfI121A3pRt75v9oJoFNLTdvo3-onNdEsJd/exec'
URL_APPS_SCRIPT_TURMAS = 'https://script.google.com/macros/s/AKfycbyw2E0QH0ucHRdCMNOY_La7r4ElK6xcf0OWlnQGa9w7yCcg82mG_bJV_5fxbhuhbfuY/exec'
URL_APPS_SCRIPT_MATRICULAS = 'https://script.google.com/macros/s/AKfycbxnp24RMIG4zQEsot0KATnFjdeoEHP7nyrr4WXnp-LLLptQTT-Vc_UPYoy__VWipill/exec'

# Cache de instrutores de Hortolândia
INSTRUTORES_HORTOLANDIA = {}
NOMES_COMPLETOS_NORMALIZADOS = set()

# ==================== FUNÇÕES AUXILIARES ====================

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

def normalizar_nome(nome):
    """Normaliza nome para comparação consistente"""
    nome = unicodedata.normalize('NFD', nome)
    nome = ''.join(char for char in nome if unicodedata.category(char) != 'Mn')
    nome = nome.replace('/', ' ').replace('\\', ' ').replace('-', ' ')
    nome = ' '.join(nome.upper().split())
    return nome

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

# ==================== MÓDULO 1: HISTÓRICO DE AULAS ====================

def carregar_instrutores_hortolandia(session, max_tentativas=5):
    """Carrega lista completa de instrutores de Hortolândia"""
    print("\n📋 Carregando instrutores de Hortolândia...")
    
    for tentativa in range(1, max_tentativas + 1):
        try:
            url = "https://musical.congregacao.org.br/licoes/instrutores?q=a"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive'
            }
            
            timeout = 15 + (tentativa * 5)
            print(f"   Tentativa {tentativa}/{max_tentativas} (timeout: {timeout}s)...", end=" ")
            
            resp = session.get(url, headers=headers, timeout=timeout)
            
            if resp.status_code != 200:
                print(f"HTTP {resp.status_code}")
                continue
            
            instrutores = json.loads(resp.text)
            
            ids_dict = {}
            nomes_completos_normalizados = set()
            
            for instrutor in instrutores:
                id_instrutor = instrutor['id']
                texto_completo = instrutor['text']
                
                partes = texto_completo.split(' - ')
                
                if len(partes) >= 2:
                    nome_completo = f"{partes[0].strip()} - {partes[1].strip()}"
                    nome_normalizado = normalizar_nome(nome_completo)
                    
                    ids_dict[id_instrutor] = nome_completo
                    nomes_completos_normalizados.add(nome_normalizado)
            
            print(f"✅ {len(ids_dict)} instrutores carregados!")
            return ids_dict, nomes_completos_normalizados
            
        except requests.Timeout:
            print(f"Timeout")
            if tentativa < max_tentativas:
                time.sleep(tentativa * 2)
        except Exception as e:
            print(f"Erro: {e}")
            if tentativa < max_tentativas:
                time.sleep(tentativa * 2)
    
    print("\n❌ Falha ao carregar instrutores\n")
    return {}, set()

def extrair_data_hora_abertura_rapido(session, aula_id):
    """Extrai data e horário de abertura da aula"""
    try:
        url = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0'
        }
        
        resp = session.get(url, headers=headers, timeout=5)
        
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        tbody = soup.find('tbody')
        
        if not tbody:
            return None
        
        rows = tbody.find_all('tr')
        for row in rows:
            td_strong = row.find('strong')
            if not td_strong:
                continue
            
            label = td_strong.get_text(strip=True)
            
            if 'Data e Horário de abertura' in label:
                tds = row.find_all('td')
                if len(tds) >= 2:
                    valor = tds[1].get_text(strip=True)
                    try:
                        return datetime.strptime(valor, '%d/%m/%Y %H:%M:%S')
                    except:
                        try:
                            return datetime.strptime(valor, '%d/%m/%Y %H:%M')
                        except:
                            pass
        
        return None
        
    except:
        return None

def buscar_primeiro_id_a_partir_de(session, data_hora_alvo, id_min=1, id_max=1000000):
    """Busca binária para encontrar primeiro ID >= data_hora_alvo"""
    print(f"\n{'─' * 70}")
    print(f"🔍 BUSCA BINÁRIA: Primeiro ID >= {data_hora_alvo.strftime('%d/%m/%Y %H:%M')}")
    print(f"{'─' * 70}")
    
    melhor_id = None
    melhor_data = None
    tentativas = 0
    max_tentativas = 50
    
    esquerda = id_min
    direita = id_max
    
    while esquerda <= direita and tentativas < max_tentativas:
        tentativas += 1
        meio = (esquerda + direita) // 2
        
        print(f"   [{tentativas:2d}] ID {meio:,}...", end=" ")
        
        data_hora_abertura = extrair_data_hora_abertura_rapido(session, meio)
        
        if data_hora_abertura is None:
            print("INEXISTENTE")
            direita = meio - 1
            continue
        
        print(f"{data_hora_abertura.strftime('%d/%m/%Y %H:%M')}", end="")
        
        if data_hora_abertura >= data_hora_alvo:
            melhor_id = meio
            melhor_data = data_hora_abertura
            print(f" ✓")
            direita = meio - 1
        else:
            print(f" ✗")
            esquerda = meio + 1
    
    if melhor_id:
        print(f"\n✅ Primeiro ID encontrado: {melhor_id:,}")
        print(f"   Data: {melhor_data.strftime('%d/%m/%Y %H:%M:%S')}")
    
    return melhor_id

def buscar_ultimo_id_ate(session, data_hora_limite, id_min=1, id_max=1000000):
    """Busca binária para encontrar último ID <= data_hora_limite"""
    print(f"\n{'─' * 70}")
    print(f"🔍 BUSCA BINÁRIA: Último ID <= {data_hora_limite.strftime('%d/%m/%Y %H:%M')}")
    print(f"{'─' * 70}")
    
    ultimo_valido = None
    ultima_data = None
    tentativas = 0
    max_tentativas = 50
    
    esquerda = id_min
    direita = id_max
    
    while esquerda <= direita and tentativas < max_tentativas:
        tentativas += 1
        meio = (esquerda + direita) // 2
        
        print(f"   [{tentativas:2d}] ID {meio:,}...", end=" ")
        
        data_hora_abertura = extrair_data_hora_abertura_rapido(session, meio)
        
        if data_hora_abertura is None:
            print("INEXISTENTE")
            direita = meio - 1
            continue
        
        print(f"{data_hora_abertura.strftime('%d/%m/%Y %H:%M')}", end="")
        
        if data_hora_abertura <= data_hora_limite:
            ultimo_valido = meio
            ultima_data = data_hora_abertura
            print(f" ✓")
            esquerda = meio + 1
        else:
            print(f" ✗")
            direita = meio - 1
    
    if ultimo_valido:
        print(f"\n✅ Último ID encontrado: {ultimo_valido:,}")
        print(f"   Data: {ultima_data.strftime('%d/%m/%Y %H:%M:%S')}")
    
    return ultimo_valido

def coletar_tudo_de_uma_vez(session, aula_id):
    """Coleta todos os dados de uma aula (filtro: Hortolândia)"""
    global INSTRUTORES_HORTOLANDIA, NOMES_COMPLETOS_NORMALIZADOS
    
    try:
        url = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        resp = session.get(url, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        data_aula = ""
        modal_header = soup.find('div', class_='modal-header')
        if modal_header:
            date_span = modal_header.find('span', class_='pull-right')
            if date_span:
                texto = date_span.get_text(strip=True)
                data_aula = texto.strip()
        
        tbody = soup.find('tbody')
        if not tbody:
            return None
        
        descricao = ""
        comum = ""
        hora_inicio = ""
        hora_termino = ""
        data_hora_abertura = ""
        nome_instrutor_html = ""
        id_turma = ""
        
        rows = tbody.find_all('tr')
        for row in rows:
            td_strong = row.find('strong')
            if not td_strong:
                continue
            
            label = td_strong.get_text(strip=True)
            tds = row.find_all('td')
            if len(tds) < 2:
                continue
            
            valor = tds[1].get_text(strip=True)
            
            if 'Comum Congregação' in label:
                comum = valor.upper()
            elif 'Início' in label and 'Horário' not in label:
                hora_inicio = valor[:5]
            elif 'Término' in label:
                hora_termino = valor[:5]
            elif 'Data e Horário de abertura' in label:
                data_hora_abertura = valor
            elif 'Instrutor(a) que ministrou a aula' in label:
                nome_instrutor_html = valor.strip()
        
        table = soup.find('table', class_='table')
        if table:
            thead = table.find('thead')
            if thead:
                td_desc = thead.find('td', class_='bg-blue-gradient')
                if td_desc:
                    texto_completo = td_desc.get_text(strip=True)
                    descricao = re.sub(r'\s+', ' ', texto_completo).strip()
        
        if not descricao:
            td_colspan = soup.find('td', {'colspan': '2'})
            if td_colspan:
                descricao = td_colspan.get_text(strip=True)
        
        # FILTRO: Verificar se instrutor é de Hortolândia
        eh_hortolandia = False
        if nome_instrutor_html:
            nome_html_normalizado = normalizar_nome(nome_instrutor_html)
            if nome_html_normalizado in NOMES_COMPLETOS_NORMALIZADOS:
                eh_hortolandia = True
        
        if not eh_hortolandia:
            return None
        
        tem_ata = "Não"
        texto_ata = ""
        
        todas_tabelas = soup.find_all('table', class_='table')
        for tabela in todas_tabelas:
            thead = tabela.find('thead')
            if thead:
                tr_green = thead.find('tr', class_='bg-green-gradient')
                if tr_green:
                    td_ata = tr_green.find('td')
                    if td_ata and 'ATA DA AULA' in td_ata.get_text():
                        tem_ata = "Sim"
                        tbody_ata = tabela.find('tbody')
                        if tbody_ata:
                            td_texto = tbody_ata.find('td')
                            if td_texto:
                                texto_ata = td_texto.get_text(strip=True)
                        break
        
        dia_semana = ""
        if data_aula:
            try:
                data_obj = datetime.strptime(data_aula, '%d/%m/%Y')
                dias = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
                dia_semana = dias[data_obj.weekday()]
            except:
                dia_semana = ""
        
        url_editar = f"https://musical.congregacao.org.br/aulas_abertas/editar/{aula_id}"
        resp_editar = session.get(url_editar, headers=headers, timeout=5)
        
        if resp_editar.status_code == 200:
            soup_editar = BeautifulSoup(resp_editar.text, 'html.parser')
            turma_input = soup_editar.find('input', {'name': 'id_turma'})
            if turma_input:
                id_turma = turma_input.get('value', '').strip()
        
        total_alunos = 0
        presentes = 0
        lista_presentes = ""
        lista_ausentes = ""
        
        if id_turma:
            url_freq = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_frequencias/{aula_id}/{id_turma}"
            resp_freq = session.get(url_freq, headers=headers, timeout=10)
            
            if resp_freq.status_code == 200:
                soup_freq = BeautifulSoup(resp_freq.text, 'html.parser')
                tbody_freq = soup_freq.find('tbody')
                
                if tbody_freq:
                    linhas = tbody_freq.find_all('tr')
                    total_alunos = len(linhas)
                    
                    presentes_lista = []
                    ausentes_lista = []
                    
                    for linha in linhas:
                        td_nome = linha.find('td')
                        if not td_nome:
                            continue
                        
                        nome_completo = td_nome.get_text(strip=True)
                        nome_aluno = nome_completo.split(' - ')[0].strip()
                        
                        link = linha.find('a', {'data-id-membro': True})
                        id_membro = ""
                        if link:
                            id_membro = link.get('data-id-membro', '')
                        
                        icon_presente = linha.find('i', class_='fa-check')
                        
                        if icon_presente:
                            presentes_lista.append(f"{id_membro}-{nome_aluno}")
                        else:
                            ausentes_lista.append(f"{id_membro}-{nome_aluno}")
                    
                    lista_presentes = "; ".join(presentes_lista) if presentes_lista else ""
                    lista_ausentes = "; ".join(ausentes_lista) if ausentes_lista else ""
                    presentes = len(presentes_lista)
        
        return {
            'id_aula': aula_id,
            'id_turma': id_turma,
            'descricao': descricao,
            'comum': comum,
            'dia_semana': dia_semana,
            'hora_inicio': hora_inicio,
            'hora_termino': hora_termino,
            'data_aula': data_aula,
            'data_hora_abertura': data_hora_abertura,
            'tem_ata': tem_ata,
            'texto_ata': texto_ata,
            'instrutor': nome_instrutor_html,
            'total_alunos': total_alunos,
            'presentes': presentes,
            'lista_presentes': lista_presentes,
            'lista_ausentes': lista_ausentes
        }
        
    except:
        return None

def executar_historico_aulas(session):
    """Executa coleta de histórico de aulas e retorna ID da planilha criada"""
    global INSTRUTORES_HORTOLANDIA, NOMES_COMPLETOS_NORMALIZADOS
    
    tempo_inicio = time.time()
    
    print("\n" + "=" * 80)
    print("📚 MÓDULO 1: HISTÓRICO DE AULAS")
    print("=" * 80)
    
    INSTRUTORES_HORTOLANDIA, NOMES_COMPLETOS_NORMALIZADOS = carregar_instrutores_hortolandia(session)
    
    if not INSTRUTORES_HORTOLANDIA:
        print("❌ Não foi possível carregar instrutores. Abortando módulo.")
        return None
    
    data_hora_inicio = datetime(2024, 1, 1, 0, 0, 0)
    data_hora_fim = datetime.now()
    
    print(f"\n📅 Período: {data_hora_inicio.strftime('%d/%m/%Y')} até {data_hora_fim.strftime('%d/%m/%Y')}")
    
    primeiro_id = buscar_primeiro_id_a_partir_de(session, data_hora_alvo=data_hora_inicio, id_min=1, id_max=1000000)
    
    if primeiro_id is None:
        print("❌ Não foi possível encontrar primeiro ID. Abortando.")
        return None
    
    ultimo_id = buscar_ultimo_id_ate(session, data_hora_limite=data_hora_fim, id_min=primeiro_id, id_max=1000000)
    
    if ultimo_id is None:
        print("⚠️ Não foi possível encontrar último ID. Usando estimativa.")
        ultimo_id = primeiro_id + 50000
    
    resultado = []
    aulas_processadas = 0
    aulas_hortolandia = 0
    aulas_com_ata = 0
    
    ID_INICIAL = primeiro_id
    ID_FINAL = ultimo_id
    LOTE_SIZE = 200
    MAX_WORKERS = 15
    
    print(f"\n{'=' * 80}")
    print(f"🚀 Processando IDs {ID_INICIAL:,} até {ID_FINAL:,} ({ID_FINAL - ID_INICIAL + 1:,} IDs)")
    print(f"{'=' * 80}\n")
    
    for lote_inicio in range(ID_INICIAL, ID_FINAL + 1, LOTE_SIZE):
        lote_fim = min(lote_inicio + LOTE_SIZE - 1, ID_FINAL)
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(coletar_tudo_de_uma_vez, session, aula_id): aula_id 
                for aula_id in range(lote_inicio, lote_fim + 1)
            }
            
            for future in as_completed(futures):
                aulas_processadas += 1
                dados_completos = future.result()
                
                if dados_completos:
                    resultado.append([
                        dados_completos['id_aula'],
                        dados_completos['id_turma'],
                        dados_completos['descricao'],
                        dados_completos['comum'],
                        dados_completos['dia_semana'],
                        dados_completos['hora_inicio'],
                        dados_completos['hora_termino'],
                        dados_completos['data_aula'],
                        dados_completos['data_hora_abertura'],
                        dados_completos['tem_ata'],
                        dados_completos['texto_ata'],
                        dados_completos['instrutor'],
                        dados_completos['total_alunos'],
                        dados_completos['presentes'],
                        dados_completos['lista_presentes'],
                        dados_completos['lista_ausentes']
                    ])
                    
                    aulas_hortolandia += 1
                    
                    if dados_completos['tem_ata'] == "Sim":
                        aulas_com_ata += 1
                
                if aulas_processadas % 200 == 0:
                    print(f"   [{aulas_processadas:5d}] processadas | {aulas_hortolandia} HTL | {aulas_com_ata} ATA")
        
        time.sleep(0.5)
    
    print(f"\n✅ Coleta finalizada: {aulas_hortolandia:,} aulas de Hortolândia")
    
    # Enviar para Google Sheets
    body = {
        "tipo": "historico_aulas_hortolandia",
        "dados": resultado,
        "headers": [
            "ID_Aula", "ID_Turma", "Descrição", "Comum", "Dia_Semana",
            "Hora_Início", "Hora_Término", "Data_Aula", "Data_Hora_Abertura", 
            "Tem_Ata", "Texto_Ata", "Instrutor",
            "Total_Alunos", "Presentes", "IDs_Nomes_Presentes", "IDs_Nomes_Ausentes"
        ],
        "resumo": {
            "total_aulas": len(resultado),
            "aulas_processadas": aulas_processadas,
            "aulas_com_ata": aulas_com_ata,
            "tempo_minutos": round((time.time() - tempo_inicio)/60, 2)
        }
    }
    
    timestamp_backup = time.strftime("%Y%m%d_%H%M%S")
    backup_file = f'backup_aulas_{timestamp_backup}.json'
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(body, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo: {backup_file}")
    
    print("\n📤 Enviando para Google Sheets...")
    
    try:
        resposta_post = requests.post(URL_APPS_SCRIPT_AULAS, json=body, timeout=180)
        
        if resposta_post.status_code == 200:
            resposta_json = resposta_post.json()
            
            if 'body' in resposta_json:
                body_content = json.loads(resposta_json['body'])
                
                if 'detalhes' in body_content:
                    detalhes = body_content['detalhes']
                    planilha_id = detalhes.get('planilha_id')
                    
                    print(f"\n✅ PLANILHA DE AULAS CRIADA!")
                    print(f"   Nome: {detalhes.get('nome_planilha')}")
                    print(f"   ID: {planilha_id}")
                    print(f"   URL: {detalhes.get('url')}")
                    
                    return planilha_id
        
        print("⚠️ Não foi possível obter ID da planilha")
        return None
        
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
        return None

# ==================== MÓDULO 2: TURMAS ====================

def buscar_ids_da_planilha_aulas(planilha_id):
    """Busca IDs únicos da coluna ID_Turma da planilha de aulas"""
    print(f"\n📂 Buscando IDs de turmas da planilha {planilha_id}...")
    
    try:
        url = f"https://docs.google.com/spreadsheets/d/{planilha_id}/gviz/tq?tqx=out:csv&sheet=Dados"
        response = requests.get(url, timeout=30)
        response.encoding = 'utf-8'
        
        if response.status_code == 200:
            linhas = response.text.strip().split('\n')
            cabecalho = [col.strip('"') for col in linhas[0].split(',')]
            
            if 'ID_Turma' not in cabecalho:
                print("❌ Coluna 'ID_Turma' não encontrada!")
                return []
            
            idx_id_turma = cabecalho.index('ID_Turma')
            
            ids_turmas = set()
            for linha in linhas[1:]:
                try:
                    colunas = linha.split(',')
                    if len(colunas) > idx_id_turma:
                        id_turma = colunas[idx_id_turma].strip('"').strip()
                        if id_turma and id_turma.isdigit():
                            ids_turmas.add(int(id_turma))
                except:
                    continue
            
            ids_lista = sorted(list(ids_turmas))
            print(f"✅ {len(ids_lista)} IDs únicos encontrados")
            return ids_lista
        else:
            print(f"❌ Erro HTTP {response.status_code}")
            return []
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        return []

def coletar_dados_turma(session, turma_id):
    """Coleta todos os dados de uma turma"""
    try:
        url = f"https://musical.congregacao.org.br/turmas/editar/{turma_id}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        resp = session.get(url, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        form = soup.find('form', id='turmas')
        if not form:
            return None
        
        dados = {
            'id_turma': turma_id,
            'curso': '',
            'descricao': '',
            'comum': '',
            'dia_semana': '',
            'data_inicio': '',
            'data_encerramento': '',
            'hora_inicio': '',
            'hora_termino': '',
            'responsavel_1': '',
            'responsavel_2': '',
            'destinado_ao': '',
            'ativo': 'Não',
            'cadastrado_em': '',
            'cadastrado_por': '',
            'atualizado_em': '',
            'atualizado_por': ''
        }
        
        # Curso
        curso_select = soup.find('select', {'name': 'id_curso'})
        if curso_select:
            curso_option = curso_select.find('option', selected=True)
            if curso_option:
                dados['curso'] = curso_option.get_text(strip=True)
        
        # Descrição
        descricao_input = soup.find('input', {'name': 'descricao'})
        if descricao_input:
            dados['descricao'] = descricao_input.get('value', '').strip()
        
        # Comum
        comum_select = soup.find('select', {'name': 'id_igreja'})
        if comum_select:
            comum_option = comum_select.find('option', selected=True)
            if comum_option:
                texto_completo = comum_option.get_text(strip=True)
                dados['comum'] = texto_completo.split('|')[0].strip()
        
        # Dia da Semana
        dia_select = soup.find('select', {'name': 'dia_semana'})
        if dia_select:
            dia_option = dia_select.find('option', selected=True)
            if dia_option:
                dados['dia_semana'] = dia_option.get_text(strip=True)
        
        # Data de Início
        dt_inicio_input = soup.find('input', {'name': 'dt_inicio'})
        if dt_inicio_input:
            dados['data_inicio'] = dt_inicio_input.get('value', '').strip()
        
        # Data de Encerramento
        dt_fim_input = soup.find('input', {'name': 'dt_fim'})
        if dt_fim_input:
            dados['data_encerramento'] = dt_fim_input.get('value', '').strip()
        
        # Hora de Início
        hr_inicio_input = soup.find('input', {'name': 'hr_inicio'})
        if hr_inicio_input:
            hora_completa = hr_inicio_input.get('value', '').strip()
            dados['hora_inicio'] = hora_completa[:5] if hora_completa else ''
        
        # Hora de Término
        hr_fim_input = soup.find('input', {'name': 'hr_fim'})
        if hr_fim_input:
            hora_completa = hr_fim_input.get('value', '').strip()
            dados['hora_termino'] = hora_completa[:5] if hora_completa else ''
        
        # Responsável 1
        resp1_select = soup.find('select', {'id': 'id_responsavel'})
        if resp1_select:
            resp1_option = resp1_select.find('option', selected=True)
            if resp1_option:
                texto_completo = resp1_option.get_text(strip=True)
                dados['responsavel_1'] = texto_completo.split(' - ')[0].strip()
        
        # Responsável 2
        resp2_select = soup.find('select', {'id': 'id_responsavel2'})
        if resp2_select:
            resp2_option = resp2_select.find('option', selected=True)
            if resp2_option:
                texto_completo = resp2_option.get_text(strip=True)
                dados['responsavel_2'] = texto_completo.split(' - ')[0].strip()
        
        # Destinado ao
        genero_select = soup.find('select', {'name': 'id_turma_genero'})
        if genero_select:
            genero_option = genero_select.find('option', selected=True)
            if genero_option:
                dados['destinado_ao'] = genero_option.get_text(strip=True)
        
        # Ativo
        status_checkbox = soup.find('input', {'name': 'status'})
        if status_checkbox and status_checkbox.has_attr('checked'):
            dados['ativo'] = 'Sim'
        
        # Histórico
        historico_div = soup.find('div', id='collapseOne')
        if historico_div:
            paragrafos = historico_div.find_all('p')
            
            for p in paragrafos:
                texto = p.get_text(strip=True)
                
                if 'Cadastrado em:' in texto:
                    partes = texto.split('por:')
                    if len(partes) >= 2:
                        dados['cadastrado_em'] = partes[0].replace('Cadastrado em:', '').strip()
                        dados['cadastrado_por'] = partes[1].strip()
                
                elif 'Atualizado em:' in texto:
                    partes = texto.split('por:')
                    if len(partes) >= 2:
                        dados['atualizado_em'] = partes[0].replace('Atualizado em:', '').strip()
                        dados['atualizado_por'] = partes[1].strip()
        
        return dados
        
    except Exception as e:
        return None

def executar_turmas(session, planilha_aulas_id):
    """Executa coleta de dados de turmas e retorna ID da planilha criada"""
    tempo_inicio = time.time()
    timestamp_execucao = gerar_timestamp()
    
    print("\n" + "=" * 80)
    print("🎓 MÓDULO 2: DADOS DE TURMAS")
    print("=" * 80)
    
    if not planilha_aulas_id:
        print("❌ ID da planilha de aulas não fornecido. Abortando módulo.")
        return None
    
    ids_turmas = buscar_ids_da_planilha_aulas(planilha_aulas_id)
    
    if not ids_turmas:
        print("❌ Nenhum ID encontrado. Abortando módulo.")
        return None
    
    print(f"\n📊 Total de turmas a processar: {len(ids_turmas)}")
    
    resultado = []
    processadas = 0
    sucesso = 0
    erros = 0
    
    print(f"\n{'=' * 80}")
    print("🚀 Iniciando coleta de dados...")
    print(f"{'=' * 80}\n")
    
    for i, turma_id in enumerate(ids_turmas, 1):
        processadas += 1
        
        dados = coletar_dados_turma(session, turma_id)
        
        if dados:
            sucesso += 1
            resultado.append([
                dados['id_turma'],
                dados['curso'],
                dados['descricao'],
                dados['comum'],
                dados['dia_semana'],
                dados['data_inicio'],
                dados['data_encerramento'],
                dados['hora_inicio'],
                dados['hora_termino'],
                dados['responsavel_1'],
                dados['responsavel_2'],
                dados['destinado_ao'],
                dados['ativo'],
                dados['cadastrado_em'],
                dados['cadastrado_por'],
                dados['atualizado_em'],
                dados['atualizado_por'],
                'Coletado',
                time.strftime('%d/%m/%Y %H:%M:%S')
            ])
            
            print(f"[{i}/{len(ids_turmas)}] ID {turma_id}: {dados['curso']} | {dados['descricao']}")
        else:
            erros += 1
            resultado.append([
                turma_id, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                'Erro/Não encontrado', time.strftime('%d/%m/%Y %H:%M:%S')
            ])
            print(f"[{i}/{len(ids_turmas)}] ID {turma_id}: Não encontrado")
        
        if processadas % 50 == 0:
            print(f"\n   Progresso: {processadas}/{len(ids_turmas)} | Sucesso: {sucesso} | Erros: {erros}\n")
        
        time.sleep(0.1)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n✅ Coleta finalizada: {sucesso} turmas coletadas")
    
    # Enviar para Google Sheets
    body = {
        "tipo": "dados_turmas",
        "timestamp": timestamp_execucao,
        "dados": resultado,
        "headers": [
            "ID_Turma", "Curso", "Descricao", "Comum", "Dia_Semana",
            "Data_Inicio", "Data_Encerramento", "Hora_Inicio", "Hora_Termino",
            "Responsavel_1", "Responsavel_2", "Destinado_ao", "Ativo",
            "Cadastrado_em", "Cadastrado_por", "Atualizado_em", "Atualizado_por",
            "Status_Coleta", "Data_Coleta"
        ],
        "resumo": {
            "total_processadas": processadas,
            "sucesso": sucesso,
            "erros": erros,
            "tempo_minutos": round(tempo_total/60, 2)
        }
    }
    
    backup_file = f"backup_turmas_{timestamp_execucao.replace(':', '-')}.json"
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(body, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo: {backup_file}")
    
    print("\n📤 Enviando para Google Sheets...")
    
    try:
        resposta_post = requests.post(URL_APPS_SCRIPT_TURMAS, json=body, timeout=120)
        
        if resposta_post.status_code == 200:
            resposta_json = resposta_post.json()
            
            if resposta_json.get('status') == 'sucesso':
                planilha_info = resposta_json.get('planilha', {})
                planilha_id = planilha_info.get('id')
                
                print(f"\n✅ PLANILHA DE TURMAS CRIADA!")
                print(f"   Nome: {planilha_info.get('nome')}")
                print(f"   ID: {planilha_id}")
                print(f"   URL: {planilha_info.get('url')}")
                
                return planilha_id
        
        print("⚠️ Não foi possível obter ID da planilha")
        return None
        
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
        return None

# ==================== MÓDULO 3: MATRICULADOS ====================

def buscar_ids_da_planilha_turmas(planilha_id):
    """Busca IDs de turmas da planilha de turmas"""
    print(f"\n📂 Buscando IDs da planilha de turmas {planilha_id}...")
    
    try:
        url = f"https://docs.google.com/spreadsheets/d/{planilha_id}/gviz/tq?tqx=out:csv&sheet=Dados das Turmas"
        response = requests.get(url, timeout=30)
        response.encoding = 'utf-8'
        
        if response.status_code == 200:
            linhas = response.text.strip().split('\n')
            cabecalho = [col.strip('"') for col in linhas[0].split(',')]
            
            if 'ID_Turma' not in cabecalho:
                print("❌ Coluna 'ID_Turma' não encontrada!")
                return []
            
            idx_id_turma = cabecalho.index('ID_Turma')
            
            ids_turmas = []
            for i, linha in enumerate(linhas[1:], start=2):
                try:
                    colunas = linha.split(',')
                    if len(colunas) > idx_id_turma:
                        id_turma = colunas[idx_id_turma].strip('"').strip()
                        if id_turma and id_turma.isdigit():
                            ids_turmas.append(int(id_turma))
                except:
                    continue
            
            print(f"✅ {len(ids_turmas)} IDs encontrados")
            return ids_turmas
        else:
            print(f"❌ Erro HTTP {response.status_code}")
            return []
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        return []

def extrair_dados_alunos(session, turma_id):
    """Extrai dados detalhados de todos os alunos matriculados"""
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        url = f"https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/{turma_id}"
        resp = session.get(url, headers=headers, timeout=15)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            tbody = soup.find('tbody')
            if not tbody:
                return []
            
            alunos = []
            rows = tbody.find_all('tr')
            
            for row in rows:
                tds = row.find_all('td')
                
                if len(tds) >= 4:
                    primeiro_td = tds[0].get_text(strip=True)
                    
                    if not primeiro_td or 'Nenhum registro' in primeiro_td:
                        continue
                    
                    nome = tds[0].get_text(strip=True)
                    comum = tds[1].get_text(strip=True)
                    instrumento = tds[2].get_text(strip=True)
                    status = tds[3].get_text(strip=True)
                    
                    aluno = {
                        'ID_Turma': turma_id,
                        'Nome': nome,
                        'Comum': comum,
                        'Instrumento': instrumento,
                        'Status': status
                    }
                    
                    alunos.append(aluno)
            
            return alunos
        
        else:
            return None
        
    except Exception as e:
        return None

def executar_matriculados(session, planilha_turmas_id):
    """Executa coleta de matrículas"""
    tempo_inicio = time.time()
    
    print("\n" + "=" * 80)
    print("👥 MÓDULO 3: ALUNOS MATRICULADOS")
    print("=" * 80)
    
    if not planilha_turmas_id:
        print("❌ ID da planilha de turmas não fornecido. Abortando módulo.")
        return
    
    IDS_TURMAS = buscar_ids_da_planilha_turmas(planilha_turmas_id)
    
    if not IDS_TURMAS:
        print("❌ Nenhum ID encontrado. Abortando módulo.")
        return
    
    print(f"\n🎯 Total de turmas a processar: {len(IDS_TURMAS)}")
    
    resultados_resumo = []
    todos_alunos = []
    total = len(IDS_TURMAS)
    
    print(f"\n{'=' * 80}")
    print("🚀 Processando turmas...")
    print(f"{'=' * 80}\n")
    
    for idx, turma_id in enumerate(IDS_TURMAS, 1):
        print(f"[{idx}/{total}] Turma {turma_id}...", end=" ")
        
        alunos = extrair_dados_alunos(session, turma_id)
        
        if alunos is not None:
            quantidade = len(alunos)
            print(f"✅ {quantidade} alunos")
            status = "Sucesso"
            todos_alunos.extend(alunos)
        else:
            print(f"⚠️ Erro")
            quantidade = 0
            status = "Erro"
        
        resultados_resumo.append([turma_id, quantidade, status])
        time.sleep(0.3)
    
    print(f"\n✅ Coleta finalizada: {len(todos_alunos)} alunos coletados")
    
    # Preparar dados
    data_coleta = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    dados_resumo_com_cabecalho = [["ID_Turma", "Quantidade_Matriculados", "Status_Coleta"]] + resultados_resumo
    
    dados_alunos_para_envio = [["ID_Turma", "Nome", "Comum", "Instrumento", "Status"]]
    for aluno in todos_alunos:
        dados_alunos_para_envio.append([
            aluno['ID_Turma'],
            aluno['Nome'],
            aluno['Comum'],
            aluno['Instrumento'],
            aluno['Status']
        ])
    
    # Enviar resumo
    body_resumo = {
        "tipo": "contagem_matriculas",
        "dados": dados_resumo_com_cabecalho,
        "data_coleta": data_coleta
    }
    
    try:
        print("\n📤 Enviando dados para Google Sheets...")
        resposta_resumo = requests.post(URL_APPS_SCRIPT_MATRICULAS, json=body_resumo, timeout=60)
        
        if resposta_resumo.status_code == 200:
            resultado_resumo = resposta_resumo.json()
            
            if resultado_resumo.get('status') == 'sucesso':
                detalhes = resultado_resumo.get('detalhes', {})
                planilha_id = detalhes.get('planilha_id')
                
                print(f"\n✅ PLANILHA DE MATRÍCULAS CRIADA!")
                print(f"   Nome: {detalhes.get('nome_planilha')}")
                print(f"   ID: {planilha_id}")
                print(f"   URL: {detalhes.get('url')}")
                
                # Enviar dados detalhados
                body_detalhado = {
                    "tipo": "alunos_detalhados",
                    "dados": dados_alunos_para_envio,
                    "data_coleta": data_coleta,
                    "planilha_id": planilha_id
                }
                
                print("\n📋 Enviando dados detalhados...")
                resposta_detalhado = requests.post(URL_APPS_SCRIPT_MATRICULAS, json=body_detalhado, timeout=60)
                
                if resposta_detalhado.status_code == 200:
                    resultado_detalhado = resposta_detalhado.json()
                    if resultado_detalhado.get('status') == 'sucesso':
                        print(f"   ✅ {len(todos_alunos)} alunos enviados com sucesso")
                
        tempo_total = time.time() - tempo_inicio
        print(f"\n⏱️ Tempo do módulo: {tempo_total/60:.2f} minutos")
        
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
        # Backup local
        timestamp = datetime.now().strftime('%d_%m_%Y-%H_%M')
        with open(f'backup_matriculas_{timestamp}.json', 'w', encoding='utf-8') as f:
            json.dump({"resumo": resultados_resumo, "alunos": todos_alunos}, f, indent=2, ensure_ascii=False)

# ==================== MAIN - ORQUESTRADOR ====================

def main():
    tempo_inicio_total = time.time()
    
    print("\n" + "=" * 80)
    print("🎼 SISTEMA MUSICAL - COLETOR UNIFICADO")
    print("=" * 80)
    print("📋 Ordem de execução:")
    print("   1️⃣ Histórico de Aulas (Hortolândia)")
    print("   2️⃣ Dados de Turmas")
    print("   3️⃣ Alunos Matriculados")
    print("=" * 80)
    
    # PASSO 1: Login único
    session, cookies = fazer_login_unico()
    
    if not session:
        print("\n❌ Falha no login. Encerrando processo.")
        return
    
    # PASSO 2: Executar Histórico de Aulas
    planilha_aulas_id = executar_historico_aulas(session)
    
    if not planilha_aulas_id:
        print("\n⚠️ Módulo 1 falhou. Interrompendo processo.")
        return
    
    # PASSO 3: Executar Turmas
    planilha_turmas_id = executar_turmas(session, planilha_aulas_id)
    
    if not planilha_turmas_id:
        print("\n⚠️ Módulo 2 falhou. Interrompendo processo.")
        return
    
    # PASSO 4: Executar Matriculados
    executar_matriculados(session, planilha_turmas_id)
    
    # RESUMO FINAL
    tempo_total = time.time() - tempo_inicio_total
    
    print("\n" + "=" * 80)
    print("🎉 PROCESSO COMPLETO FINALIZADO!")
    print("=" * 80)
    print(f"⏱️ Tempo total: {tempo_total/60:.2f} minutos")
    print(f"📊 3 planilhas criadas com sucesso")
    print(f"💾 Backups salvos localmente")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    main()
