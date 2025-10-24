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
from collections import defaultdict
import random

# ==================== CONFIGURAÇÕES GLOBAIS ====================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"

# URLs dos Apps Scripts
URL_APPS_SCRIPT_AULAS = 'https://script.google.com/macros/s/AKfycbyvEGIUPIvgbSuT_yikqg03nEjqXryd6RfI121A3pRt75v9oJoFNLTdvo3-onNdEsJd/exec'
URL_APPS_SCRIPT_TURMAS = 'https://script.google.com/macros/s/AKfycbyw2E0QH0ucHRdCMNOY_La7r4ElK6xcf0OWlnQGa9w7yCcg82mG_bJV_5fxbhuhbfuY/exec'
URL_APPS_SCRIPT_MATRICULAS = 'https://script.google.com/macros/s/AKfycbxnp24RMIG4zQEsot0KATnFjdeoEHP7nyrr4WXnp-LLLptQTT-Vc_UPYoy__VWipill/exec'

# Cache de instrutores de Hortolândia
INSTRUTORES_HORTOLANDIA = {}
NOMES_COMPLETOS_NORMALIZADOS = set()

# ==================== CLASSES DE OTIMIZAÇÃO ====================

class DensityCache:
    """Cache para rastrear densidade de aulas por região"""
    def __init__(self, block_size=1000):
        self.block_size = block_size
        self.density = defaultdict(int)
        self.total_checks = defaultdict(int)
    
    def get_block(self, aula_id):
        return aula_id // self.block_size
    
    def register_hit(self, aula_id):
        block = self.get_block(aula_id)
        self.density[block] += 1
        self.total_checks[block] += 1
    
    def register_miss(self, aula_id):
        block = self.get_block(aula_id)
        self.total_checks[block] += 1
    
    def get_density(self, aula_id):
        block = self.get_block(aula_id)
        if self.total_checks[block] == 0:
            return 0.5
        return self.density[block] / self.total_checks[block]

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
    """Realiza login único via Playwright"""
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

# ==================== MÓDULO 1: HISTÓRICO DE AULAS (OTIMIZADO) ====================

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

def descobrir_regioes_densas(session, primeiro_id, ultimo_id, num_amostras=300):
    """Fase 1: Amostragem para identificar regiões com aulas"""
    print("\n" + "=" * 80)
    print("🔍 FASE 1: DESCOBERTA DE REGIÕES DENSAS")
    print("=" * 80)
    
    range_total = ultimo_id - primeiro_id
    passo_amostra = max(1, range_total // num_amostras)
    
    regioes_densas = []
    
    print(f"📊 Amostrando {num_amostras} pontos com passo de {passo_amostra:,}")
    print(f"   Verificando IDs: {primeiro_id:,} até {ultimo_id:,}")
    
    for i in range(num_amostras):
        id_teste = primeiro_id + (i * passo_amostra)
        if id_teste > ultimo_id:
            break
        
        data_hora = extrair_data_hora_abertura_rapido(session, id_teste)
        
        if data_hora is not None:
            regioes_densas.append(id_teste)
            
            if len(regioes_densas) % 10 == 0:
                print(f"   ✓ {len(regioes_densas)} regiões encontradas...")
    
    print(f"\n✅ {len(regioes_densas)} regiões densas identificadas")
    
    return regioes_densas

def buscar_vizinhos(session, id_central, raio=200):
    """Busca IDs próximos ao ID central (aulas tendem a estar agrupadas)"""
    ids_validos = [id_central]
    
    # Busca para trás
    for offset in range(1, raio + 1):
        id_teste = id_central - offset
        if id_teste < 1:
            break
        
        data_hora = extrair_data_hora_abertura_rapido(session, id_teste)
        if data_hora is None:
            # Tolera até 5 vazios consecutivos
            if offset > 5:
                vazios_consecutivos = 0
                for check in range(1, 6):
                    if extrair_data_hora_abertura_rapido(session, id_teste - check) is None:
                        vazios_consecutivos += 1
                if vazios_consecutivos >= 5:
                    break
        else:
            ids_validos.append(id_teste)
    
    # Busca para frente
    for offset in range(1, raio + 1):
        id_teste = id_central + offset
        data_hora = extrair_data_hora_abertura_rapido(session, id_teste)
        if data_hora is None:
            if offset > 5:
                vazios_consecutivos = 0
                for check in range(1, 6):
                    if extrair_data_hora_abertura_rapido(session, id_teste + check) is None:
                        vazios_consecutivos += 1
                if vazios_consecutivos >= 5:
                    break
        else:
            ids_validos.append(id_teste)
    
    return ids_validos

def expandir_regioes(session, regioes_densas, raio=200):
    """Fase 2: Expande cada região densa encontrada"""
    print("\n" + "=" * 80)
    print("🎯 FASE 2: EXPANSÃO DE REGIÕES DENSAS")
    print("=" * 80)
    
    todos_ids = set()
    
    for i, id_central in enumerate(regioes_densas, 1):
        print(f"[{i}/{len(regioes_densas)}] Expandindo região {id_central:,}...", end=" ")
        
        vizinhos = buscar_vizinhos(session, id_central, raio)
        todos_ids.update(vizinhos)
        
        print(f"{len(vizinhos)} IDs encontrados")
        
        # Pequeno delay para não sobrecarregar
        if i % 10 == 0:
            time.sleep(0.5)
    
    print(f"\n✅ Total de IDs candidatos: {len(todos_ids):,}")
    
    return sorted(list(todos_ids))

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

def validar_completude(session, ids_coletados, primeiro_id, ultimo_id, num_validacoes=500):
    """Valida que não perdemos aulas importantes"""
    print("\n" + "=" * 80)
    print("🔍 VALIDAÇÃO DE COMPLETUDE")
    print("=" * 80)
    
    ids_coletados_set = set(ids_coletados)
    falsos_negativos = []
    
    print(f"📊 Verificando {num_validacoes} amostras aleatórias...")
    
    for i in range(num_validacoes):
        id_aleatorio = random.randint(primeiro_id, ultimo_id)
        
        if id_aleatorio in ids_coletados_set:
            continue
        
        data_hora = extrair_data_hora_abertura_rapido(session, id_aleatorio)
        if data_hora is not None:
            falsos_negativos.append(id_aleatorio)
        
        if (i + 1) % 100 == 0:
            print(f"   {i+1}/{num_validacoes} verificadas...")
    
    taxa_erro = len(falsos_negativos) / num_validacoes
    
    print(f"\n📈 Resultado da validação:")
    print(f"   Amostras verificadas: {num_validacoes}")
    print(f"   IDs não capturados: {len(falsos_negativos)}")
    print(f"   Taxa de erro estimada: {taxa_erro * 100:.2f}%")
    
    if taxa_erro > 0.02:
        print(f"\n⚠️ ATENÇÃO: Taxa de erro acima de 2%")
        print(f"   Sugerimos executar coleta complementar nos IDs: {falsos_negativos[:10]}...")
        return False, falsos_negativos
    
    print(f"✅ Validação aprovada! Taxa de erro aceitável.")
    return True, []

def executar_historico_aulas(session):
    """Executa coleta de histórico de aulas OTIMIZADA"""
    global INSTRUTORES_HORTOLANDIA, NOMES_COMPLETOS_NORMALIZADOS
    
    tempo_inicio = time.time()
    
    print("\n" + "=" * 80)
    print("📚 MÓDULO 1: HISTÓRICO DE AULAS (OTIMIZADO)")
    print("=" * 80)
    
    INSTRUTORES_HORTOLANDIA, NOMES_COMPLETOS_NORMALIZADOS = carregar_instrutores_hortolandia(session)
    
    if not INSTRUTORES_HORTOLANDIA:
        print("❌ Não foi possível carregar instrutores. Abortando módulo.")
        return None
    
    data_hora_inicio = datetime(2025, 10, 21, 0, 0, 0)
    data_hora_fim = datetime.now()
    
    print(f"\n📅 Período: {data_hora_inicio.strftime('%d/%m/%Y')} até {data_hora_fim.strftime('%d/%m/%Y')}")
    
    # Busca binária para limites
    primeiro_id = buscar_primeiro_id_a_partir_de(session, data_hora_alvo=data_hora_inicio, id_min=1, id_max=1000000)
    
    if primeiro_id is None:
        print("❌ Não foi possível encontrar primeiro ID. Abortando.")
        return None
    
    ultimo_id = buscar_ultimo_id_ate(session, data_hora_limite=data_hora_fim, id_min=primeiro_id, id_max=1000000)
    
    if ultimo_id is None:
        print("⚠️ Não foi possível encontrar último ID. Usando estimativa.")
        ultimo_id = primeiro_id + 50000
    
    range_total = ultimo_id - primeiro_id
    print(f"\n📊 Range total: {range_total:,} IDs ({primeiro_id:,} até {ultimo_id:,})")
    
    # ================== ESTRATÉGIA OTIMIZADA ==================
    
    # FASE 1: Descobrir regiões densas (amostragem)
    regioes_densas = descobrir_regioes_densas(session, primeiro_id, ultimo_id, num_amostras=300)
    
    if not regioes_densas:
        print("⚠️ Nenhuma região densa encontrada. Usando método tradicional...")
        # Fallback para método tradicional se amostragem falhar
        ids_candidatos = list(range(primeiro_id, ultimo_id + 1))
    else:
        # FASE 2: Expandir regiões densas
        ids_candidatos = expandir_regioes(session, regioes_densas, raio=200)
    
    reducao_percentual = ((range_total - len(ids_candidatos)) / range_total * 100) if range_total > 0 else 0
    
    print(f"\n{'=' * 80}")
    print(f"🚀 FASE 3: COLETA DETALHADA")
    print(f"{'=' * 80}")
    print(f"📊 IDs originais: {range_total:,}")
    print(f"📊 IDs a verificar: {len(ids_candidatos):,}")
    print(f"⚡ Redução: {reducao_percentual:.1f}%")
    print(f"⏱️ Tempo estimado: {len(ids_candidatos) / 200:.1f} minutos")
    
    # FASE 3: Coleta paralela dos candidatos
    resultado = []
    aulas_processadas = 0
    aulas_hortolandia = 0
    aulas_com_ata = 0
    
    LOTE_SIZE = 500
    MAX_WORKERS = 20
    
    print(f"\n🔄 Iniciando coleta paralela...")
    
    for lote_inicio in range(0, len(ids_candidatos), LOTE_SIZE):
        lote_fim = min(lote_inicio + LOTE_SIZE, len(ids_candidatos))
        lote_ids = ids_candidatos[lote_inicio:lote_fim]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(coletar_tudo_de_uma_vez, session, aula_id): aula_id 
                for aula_id in lote_ids
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
                
                if aulas_processadas % 500 == 0:
                    percentual = (aulas_processadas / len(ids_candidatos)) * 100
                    tempo_decorrido = time.time() - tempo_inicio
                    velocidade = aulas_processadas / tempo_decorrido if tempo_decorrido > 0 else 0
                    tempo_restante = (len(ids_candidatos) - aulas_processadas) / velocidade if velocidade > 0 else 0
                    
                    print(f"   [{percentual:5.1f}%] {aulas_processadas:5d}/{len(ids_candidatos)} | "
                          f"{aulas_hortolandia} HTL | {aulas_com_ata} ATA | "
                          f"⏱️ Restam ~{tempo_restante/60:.1f}min")
        
        time.sleep(0.2)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'=' * 80}")
    print(f"✅ COLETA FINALIZADA")
    print(f"{'=' * 80}")
    print(f"📊 Aulas de Hortolândia: {aulas_hortolandia:,}")
    print(f"📋 Aulas com ATA: {aulas_com_ata}")
    print(f"⏱️ Tempo total: {tempo_total/60:.2f} minutos")
    print(f"⚡ Velocidade: {aulas_processadas/tempo_total:.1f} verificações/segundo")
    
    # VALIDAÇÃO DE COMPLETUDE
    if len(ids_candidatos) < range_total:
        ids_coletados_completos = [linha[0] for linha in resultado]
        validou, ids_faltantes = validar_completude(session, ids_candidatos, primeiro_id, ultimo_id, num_validacoes=500)
        
        if not validou and ids_faltantes:
            print(f"\n🔄 Coletando IDs faltantes identificados na validação...")
            for id_faltante in ids_faltantes[:20]:  # Limita a 20 para não demorar muito
                dados = coletar_tudo_de_uma_vez(session, id_faltante)
                if dados:
                    resultado.append([
                        dados['id_aula'], dados['id_turma'], dados['descricao'],
                        dados['comum'], dados['dia_semana'], dados['hora_inicio'],
                        dados['hora_termino'], dados['data_aula'], dados['data_hora_abertura'],
                        dados['tem_ata'], dados['texto_ata'], dados['instrutor'],
                        dados['total_alunos'], dados['presentes'], 
                        dados['lista_presentes'], dados['lista_ausentes']
                    ])
                    aulas_hortolandia += 1
            
            print(f"✅ Coleta complementar finalizada. Total: {aulas_hortolandia} aulas")
    
    # Backup local
    timestamp_backup = time.strftime("%Y%m%d_%H%M%S")
    backup_file = f'backup_aulas_{timestamp_backup}.json'
    
    velocidade = round(aulas_processadas / tempo_total, 2) if tempo_total > 0 else 0
    
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
            "periodo_inicio": data_hora_inicio.strftime('%d/%m/%Y %H:%M:%S'),
            "periodo_fim": data_hora_fim.strftime('%d/%m/%Y %H:%M:%S'),
            "total_aulas": len(resultado),
            "aulas_processadas": aulas_processadas,
            "aulas_com_ata": aulas_com_ata,
            "total_instrutores_htl": len(INSTRUTORES_HORTOLANDIA),
            "primeiro_id_2024": primeiro_id,
            "ultimo_id_2024": ultimo_id,
            "tempo_minutos": round(tempo_total/60, 2),
            "velocidade_aulas_por_segundo": velocidade,
            "otimizacao": {
                "ids_range_total": range_total,
                "ids_candidatos": len(ids_candidatos),
                "reducao_percentual": round(reducao_percentual, 1),
                "metodo": "amostragem_adaptativa_v2"
            }
        }
    }
    
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(body, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo: {backup_file}")
    
    # Envio para Google Sheets
    print("\n" + "=" * 80)
    print("📤 ENVIANDO PARA GOOGLE SHEETS")
    print("=" * 80)
    
    print(f"🌐 URL: {URL_APPS_SCRIPT_AULAS}")
    print(f"📊 Total de linhas: {len(resultado)}")
    
    try:
        print("\n🔄 Fazendo requisição POST...")
        resposta_post = requests.post(
            URL_APPS_SCRIPT_AULAS, 
            json=body, 
            timeout=300,
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        )
        
        print(f"📡 Status HTTP: {resposta_post.status_code}")
        
        if resposta_post.status_code == 200:
            try:
                resposta_json = resposta_post.json()
                
                if 'body' in resposta_json:
                    body_content = json.loads(resposta_json['body'])
                else:
                    body_content = resposta_json
                
                if body_content.get('status') == 'sucesso':
                    detalhes = body_content.get('detalhes', {})
                    
                    print(f"\n" + "=" * 80)
                    print("✅ PLANILHA DE AULAS CRIADA COM SUCESSO!")
                    print("=" * 80)
                    print(f"📛 Nome: {detalhes.get('nome_planilha')}")
                    print(f"🆔 ID: {detalhes.get('planilha_id')}")
                    print(f"🔗 URL: {detalhes.get('url')}")
                    print(f"📊 Linhas gravadas: {detalhes.get('linhas_gravadas')}")
                    print("=" * 80)
                else:
                    print(f"\n⚠️ Status: {body_content.get('status')}")
                    print(f"📝 Mensagem: {body_content.get('mensagem')}")
                    if body_content.get('status') == 'erro' and 'stack' in body_content:
                        print(f"Stack: {body_content.get('stack')}")
            
            except json.JSONDecodeError as e:
                print(f"\n❌ Erro ao decodificar resposta: {e}")
                print(f"Resposta: {resposta_post.text[:500]}")
        else:
            print(f"\n❌ Erro HTTP {resposta_post.status_code}")
            print(f"Resposta: {resposta_post.text[:500]}")
    
    except Exception as e:
        print(f"\n❌ Erro ao enviar: {e}")
    
    print(f"\n📦 Retornando {len(resultado)} linhas para o próximo módulo")
    return resultado

# ==================== MÓDULO 2: TURMAS ====================

def coletar_dados_turma(session, turma_id):
    """Coleta todos os dados de uma turma"""
    try:
        url = f"https://musical.congregacao.org.br/turmas/editar/{turma_id}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        resp = session.get(url, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        form = soup.find('form', id='turmas')
        if not form:
            return None
        
        dados = {
            'id_turma': turma_id, 'curso': '', 'descricao': '', 'comum': '',
            'dia_semana': '', 'data_inicio': '', 'data_encerramento': '',
            'hora_inicio': '', 'hora_termino': '', 'responsavel_1': '',
            'responsavel_2': '', 'destinado_ao': '', 'ativo': 'Não',
            'cadastrado_em': '', 'cadastrado_por': '',
            'atualizado_em': '', 'atualizado_por': ''
        }
        
        curso_select = soup.find('select', {'name': 'id_curso'})
        if curso_select:
            curso_option = curso_select.find('option', selected=True)
            if curso_option:
                dados['curso'] = curso_option.get_text(strip=True)
        
        descricao_input = soup.find('input', {'name': 'descricao'})
        if descricao_input:
            dados['descricao'] = descricao_input.get('value', '').strip()
        
        comum_select = soup.find('select', {'name': 'id_igreja'})
        if comum_select:
            comum_option = comum_select.find('option', selected=True)
            if comum_option:
                texto_completo = comum_option.get_text(strip=True)
                dados['comum'] = texto_completo.split('|')[0].strip()
        
        dia_select = soup.find('select', {'name': 'dia_semana'})
        if dia_select:
            dia_option = dia_select.find('option', selected=True)
            if dia_option:
                dados['dia_semana'] = dia_option.get_text(strip=True)
        
        dt_inicio_input = soup.find('input', {'name': 'dt_inicio'})
        if dt_inicio_input:
            dados['data_inicio'] = dt_inicio_input.get('value', '').strip()
        
        dt_fim_input = soup.find('input', {'name': 'dt_fim'})
        if dt_fim_input:
            dados['data_encerramento'] = dt_fim_input.get('value', '').strip()
        
        hr_inicio_input = soup.find('input', {'name': 'hr_inicio'})
        if hr_inicio_input:
            hora_completa = hr_inicio_input.get('value', '').strip()
            dados['hora_inicio'] = hora_completa[:5] if hora_completa else ''
        
        hr_fim_input = soup.find('input', {'name': 'hr_fim'})
        if hr_fim_input:
            hora_completa = hr_fim_input.get('value', '').strip()
            dados['hora_termino'] = hora_completa[:5] if hora_completa else ''
        
        resp1_select = soup.find('select', {'id': 'id_responsavel'})
        if resp1_select:
            resp1_option = resp1_select.find('option', selected=True)
            if resp1_option:
                texto_completo = resp1_option.get_text(strip=True)
                dados['responsavel_1'] = texto_completo.split(' - ')[0].strip()
        
        resp2_select = soup.find('select', {'id': 'id_responsavel2'})
        if resp2_select:
            resp2_option = resp2_select.find('option', selected=True)
            if resp2_option:
                texto_completo = resp2_option.get_text(strip=True)
                dados['responsavel_2'] = texto_completo.split(' - ')[0].strip()
        
        genero_select = soup.find('select', {'name': 'id_turma_genero'})
        if genero_select:
            genero_option = genero_select.find('option', selected=True)
            if genero_option:
                dados['destinado_ao'] = genero_option.get_text(strip=True)
        
        status_checkbox = soup.find('input', {'name': 'status'})
        if status_checkbox and status_checkbox.has_attr('checked'):
            dados['ativo'] = 'Sim'
        
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
        
    except:
        return None

def executar_turmas(session, resultado_modulo1):
    """Executa coleta de dados de turmas"""
    tempo_inicio = time.time()
    timestamp_execucao = gerar_timestamp()
    
    print("\n" + "=" * 80)
    print("🎓 MÓDULO 2: DADOS DE TURMAS")
    print("=" * 80)
    
    ids_turmas = set()
    for linha in resultado_modulo1:
        id_turma = str(linha[1]).strip()
        if id_turma and id_turma.isdigit():
            ids_turmas.add(int(id_turma))
    
    ids_turmas = sorted(list(ids_turmas))
    
    if not ids_turmas:
        print("❌ Nenhum ID de turma encontrado. Abortando módulo.")
        return None, []
    
    print(f"\n📊 Total de turmas a processar: {len(ids_turmas)}")
    
    resultado = []
    processadas = 0
    sucesso = 0
    erros = 0
    
    print(f"\n🚀 Iniciando coleta de dados...\n")
    
    for i, turma_id in enumerate(ids_turmas, 1):
        processadas += 1
        dados = coletar_dados_turma(session, turma_id)
        
        if dados:
            sucesso += 1
            resultado.append([
                dados['id_turma'], dados['curso'], dados['descricao'],
                dados['comum'], dados['dia_semana'], dados['data_inicio'],
                dados['data_encerramento'], dados['hora_inicio'], dados['hora_termino'],
                dados['responsavel_1'], dados['responsavel_2'], dados['destinado_ao'],
                dados['ativo'], dados['cadastrado_em'], dados['cadastrado_por'],
                dados['atualizado_em'], dados['atualizado_por'],
                'Coletado', time.strftime('%d/%m/%Y %H:%M:%S')
            ])
            print(f"[{i}/{len(ids_turmas)}] ID {turma_id}: ✅ {dados['curso']}")
        else:
            erros += 1
            resultado.append([
                turma_id, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                'Erro/Não encontrado', time.strftime('%d/%m/%Y %H:%M:%S')
            ])
            print(f"[{i}/{len(ids_turmas)}] ID {turma_id}: ❌ Não encontrado")
        
        time.sleep(0.1)
    
    tempo_total = time.time() - tempo_inicio
    print(f"\n✅ Coleta finalizada: {sucesso} turmas coletadas em {tempo_total/60:.2f} min")
    
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
                
                print(f"\n✅ PLANILHA DE TURMAS CRIADA!")
                print(f"   Nome: {planilha_info.get('nome')}")
                print(f"   URL: {planilha_info.get('url')}")
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
    
    return resultado, ids_turmas

# ==================== MÓDULO 3: MATRICULADOS ====================

def extrair_dados_alunos(session, turma_id):
    """Extrai dados de alunos matriculados"""
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0'
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
                    
                    alunos.append({
                        'ID_Turma': turma_id,
                        'Nome': tds[0].get_text(strip=True),
                        'Comum': tds[1].get_text(strip=True),
                        'Instrumento': tds[2].get_text(strip=True),
                        'Status': tds[3].get_text(strip=True)
                    })
            
            return alunos
        else:
            return None
        
    except:
        return None

def executar_matriculados(session, ids_turmas_modulo2):
    """Executa coleta de matrículas"""
    tempo_inicio = time.time()
    
    print("\n" + "=" * 80)
    print("👥 MÓDULO 3: ALUNOS MATRICULADOS")
    print("=" * 80)
    
    if not ids_turmas_modulo2:
        print("❌ Nenhum ID de turma recebido. Abortando módulo.")
        return
    
    print(f"\n🎯 Total de turmas: {len(ids_turmas_modulo2)}")
    
    resultados_resumo = []
    todos_alunos = []
    
    print(f"\n🚀 Processando turmas...\n")
    
    for idx, turma_id in enumerate(ids_turmas_modulo2, 1):
        print(f"[{idx}/{len(ids_turmas_modulo2)}] Turma {turma_id}...", end=" ")
        
        alunos = extrair_dados_alunos(session, turma_id)
        
        if alunos is not None:
            print(f"✅ {len(alunos)} alunos")
            todos_alunos.extend(alunos)
            resultados_resumo.append([turma_id, len(alunos), "Sucesso"])
        else:
            print(f"⚠️ Erro")
            resultados_resumo.append([turma_id, 0, "Erro"])
        
        time.sleep(0.3)
    
    print(f"\n✅ Coleta finalizada: {len(todos_alunos)} alunos")
    
    data_coleta = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    dados_resumo = [["ID_Turma", "Quantidade_Matriculados", "Status_Coleta"]] + resultados_resumo
    dados_alunos = [["ID_Turma", "Nome", "Comum", "Instrumento", "Status"]]
    for aluno in todos_alunos:
        dados_alunos.append([
            aluno['ID_Turma'], aluno['Nome'], aluno['Comum'],
            aluno['Instrumento'], aluno['Status']
        ])
    
    timestamp = datetime.now().strftime('%d_%m_%Y-%H_%M')
    backup_file = f'backup_matriculas_{timestamp}.json'
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump({"resumo": resultados_resumo, "alunos": todos_alunos}, f, indent=2, ensure_ascii=False)
    print(f"💾 Backup salvo: {backup_file}")
    
    print("\n📤 Enviando para Google Sheets...")
    
    try:
        body_resumo = {
            "tipo": "contagem_matriculas",
            "dados": dados_resumo,
            "data_coleta": data_coleta
        }
        
        resposta = requests.post(URL_APPS_SCRIPT_MATRICULAS, json=body_resumo, timeout=60)
        
        if resposta.status_code == 200:
            resultado = resposta.json()
            
            if resultado.get('status') == 'sucesso':
                detalhes = resultado.get('detalhes', {})
                planilha_id = detalhes.get('planilha_id')
                
                print(f"\n✅ PLANILHA DE MATRÍCULAS CRIADA!")
                print(f"   URL: {detalhes.get('url')}")
                
                body_detalhado = {
                    "tipo": "alunos_detalhados",
                    "dados": dados_alunos,
                    "data_coleta": data_coleta,
                    "planilha_id": planilha_id
                }
                
                resposta2 = requests.post(URL_APPS_SCRIPT_MATRICULAS, json=body_detalhado, timeout=60)
                
                if resposta2.status_code == 200:
                    print(f"   ✅ {len(todos_alunos)} alunos enviados")
        
        print(f"\n⏱️ Tempo do módulo: {(time.time() - tempo_inicio)/60:.2f} minutos")
        
    except Exception as e:
        print(f"❌ Erro: {e}")

# ==================== MAIN ====================

def main():
    tempo_inicio_total = time.time()
    
    print("\n" + "=" * 80)
    print("🎼 SISTEMA MUSICAL - COLETOR OTIMIZADO")
    print("=" * 80)
    print("📋 Módulos:")
    print("   1️⃣ Histórico de Aulas (Hortolândia) - OTIMIZADO")
    print("   2️⃣ Dados de Turmas")
    print("   3️⃣ Alunos Matriculados")
    print("=" * 80)
    
    session, cookies = fazer_login_unico()
    
    if not session:
        print("\n❌ Falha no login. Encerrando.")
        return
    
    resultado_aulas = executar_historico_aulas(session)
    
    if not resultado_aulas:
        print("\n⚠️ Módulo 1 falhou. Interrompendo.")
        return
    
    resultado_turmas, ids_turmas = executar_turmas(session, resultado_aulas)
    
    if not ids_turmas:
        print("\n⚠️ Módulo 2 falhou. Interrompendo.")
        return
    
    executar_matriculados(session, ids_turmas)
    
    tempo_total = time.time() - tempo_inicio_total
    
    print("\n" + "=" * 80)
    print("🎉 PROCESSO COMPLETO FINALIZADO!")
    print("=" * 80)
    print(f"⏱️ Tempo total: {tempo_total/60:.2f} minutos")
    print("=" * 80)

if __name__ == "__main__":
    main()
