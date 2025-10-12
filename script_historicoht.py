import os
import sys
import re
import asyncio
import httpx
import time
from playwright.sync_api import sync_playwright
from collections import deque
from tqdm import tqdm
from datetime import datetime
from bs4 import BeautifulSoup
import json

# ========================================
# CONFIGURAÇÕES ULTRA AGRESSIVAS
# ========================================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbyvEGIUPIvgbSuT_yikqg03nEjqXryd6RfI121A3pRt75v9oJoFNLTdvo3-onNdEsJd/exec'

# 🚀 MODO INSANO - META: 10 MINUTOS PARA 50K AULAS
CONCURRENT_REQUESTS = 400  # 400 requisições simultâneas
TIMEOUT_ULTRA_FAST = 3     # 3s primeira tentativa
TIMEOUT_FAST = 6           # 6s segunda tentativa
TIMEOUT_CAREFUL = 10       # 10s terceira tentativa

# Estratégia de Retry Adaptativo
SEMAPHORE_PHASE1 = 400     # Fase 1: Ultra agressivo
SEMAPHORE_PHASE2 = 250     # Fase 2: Moderado
SEMAPHORE_PHASE3 = 120     # Fase 3: Conservador

# Cache
INSTRUTORES_HORTOLANDIA = {}
NOMES_INSTRUTORES = set()
CACHE_IDS_NAO_HTL = set()

# ========================================
# REGEX PRÉ-COMPILADAS
# ========================================
REGEX_DATA_AULA = re.compile(r'<span[^>]*class="pull-right"[^>]*>([^<]+)</span>')
REGEX_COMUM = re.compile(r'<strong>Comum Congregação:</strong>.*?<td>([^<]+)</td>', re.DOTALL)
REGEX_HORA_INICIO = re.compile(r'<strong>Início:</strong>.*?<td>([^<]+)</td>', re.DOTALL)
REGEX_HORA_TERMINO = re.compile(r'<strong>Término:</strong>.*?<td>([^<]+)</td>', re.DOTALL)
REGEX_DATA_ABERTURA = re.compile(r'<strong>Data e Horário de abertura:</strong>.*?<td>([^<]+)</td>', re.DOTALL)
REGEX_INSTRUTOR = re.compile(r'<strong>Instrutor\(a\) que ministrou a aula:</strong>.*?<td>([^<\-]+)', re.DOTALL)
REGEX_ID_TURMA = re.compile(r'name="id_turma"[^>]*value="([^"]*)"')

# ========================================
# FUNÇÕES AUXILIARES
# ========================================
def normalizar_nome(nome):
    """Normaliza nome para comparação"""
    return ' '.join(nome.upper().split())

async def carregar_instrutores_async(client):
    """Carrega lista de instrutores de Hortolândia"""
    print("\n🔍 Carregando instrutores de Hortolândia...")
    
    try:
        url = "https://musical.congregacao.org.br/licoes/instrutores?q=a"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*'
        }
        
        response = await client.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            return {}, set()
        
        instrutores = response.json()
        
        ids_dict = {}
        nomes_set = set()
        
        for instrutor in instrutores:
            id_instrutor = instrutor['id']
            texto_completo = instrutor['text']
            nome = texto_completo.split(' - ')[0].strip()
            
            ids_dict[id_instrutor] = nome
            nomes_set.add(nome)
        
        print(f"✓ {len(ids_dict)} instrutores carregados!\n")
        return ids_dict, nomes_set
        
    except Exception as e:
        print(f"✗ Erro ao carregar instrutores: {e}")
        return {}, set()

def extrair_dados_aula(html_content, aula_id):
    """Extração ultra-rápida dos dados principais da aula"""
    try:
        if not html_content or len(html_content) < 300:
            return None
        
        # Verificar se é de Hortolândia PRIMEIRO (economia)
        instrutor_match = REGEX_INSTRUTOR.search(html_content)
        if not instrutor_match:
            return None
        
        nome_instrutor = instrutor_match.group(1).strip()
        nome_normalizado = normalizar_nome(nome_instrutor)
        
        eh_hortolandia = False
        for nome_htl in NOMES_INSTRUTORES:
            if normalizar_nome(nome_htl) == nome_normalizado:
                eh_hortolandia = True
                break
        
        if not eh_hortolandia:
            return None
        
        # Extrair dados básicos com regex
        data_aula_match = REGEX_DATA_AULA.search(html_content)
        data_aula = data_aula_match.group(1).strip() if data_aula_match else ""
        
        comum_match = REGEX_COMUM.search(html_content)
        comum = comum_match.group(1).strip().upper() if comum_match else ""
        
        hora_inicio_match = REGEX_HORA_INICIO.search(html_content)
        hora_inicio = hora_inicio_match.group(1).strip()[:5] if hora_inicio_match else ""
        
        hora_termino_match = REGEX_HORA_TERMINO.search(html_content)
        hora_termino = hora_termino_match.group(1).strip()[:5] if hora_termino_match else ""
        
        data_abertura_match = REGEX_DATA_ABERTURA.search(html_content)
        data_hora_abertura = data_abertura_match.group(1).strip() if data_abertura_match else ""
        
        # Dia da semana
        dia_semana = ""
        if data_aula:
            try:
                data_obj = datetime.strptime(data_aula, '%d/%m/%Y')
                dias = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
                dia_semana = dias[data_obj.weekday()]
            except:
                pass
        
        # Descrição (BeautifulSoup apenas aqui)
        soup = BeautifulSoup(html_content, 'html.parser')
        
        descricao = ""
        table = soup.find('table', class_='table')
        if table:
            thead = table.find('thead')
            if thead:
                td_desc = thead.find('td', class_='bg-blue-gradient')
                if td_desc:
                    descricao = re.sub(r'\s+', ' ', td_desc.get_text(strip=True)).strip()
        
        # ATA
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
        
        return {
            'id_aula': aula_id,
            'descricao': descricao,
            'comum': comum,
            'dia_semana': dia_semana,
            'hora_inicio': hora_inicio,
            'hora_termino': hora_termino,
            'data_aula': data_aula,
            'data_hora_abertura': data_hora_abertura,
            'tem_ata': tem_ata,
            'texto_ata': texto_ata,
            'instrutor': nome_instrutor
        }
        
    except:
        return None

def extrair_id_turma(html_content):
    """Extrai ID da turma"""
    try:
        match = REGEX_ID_TURMA.search(html_content)
        return match.group(1) if match else ""
    except:
        return ""

def extrair_frequencias(html_content):
    """Extrai dados de frequência"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        tbody_freq = soup.find('tbody')
        
        if not tbody_freq:
            return 0, 0, "", ""
        
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
            id_membro = link.get('data-id-membro', '') if link else ""
            
            icon_presente = linha.find('i', class_='fa-check')
            
            if icon_presente:
                presentes_lista.append(f"{id_membro}-{nome_aluno}")
            else:
                ausentes_lista.append(f"{id_membro}-{nome_aluno}")
        
        lista_presentes = "; ".join(presentes_lista) if presentes_lista else ""
        lista_ausentes = "; ".join(ausentes_lista) if ausentes_lista else ""
        presentes = len(presentes_lista)
        
        return total_alunos, presentes, lista_presentes, lista_ausentes
        
    except:
        return 0, 0, "", ""

# ========================================
# COLETOR INSANO - 3 FASES
# ========================================
class ColetorAulasInsano:
    def __init__(self, cookies, id_inicial, id_final):
        self.cookies = cookies
        self.id_inicial = id_inicial
        self.id_final = id_final
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9',
            'Connection': 'keep-alive',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel'
        }
        
        self.lock = asyncio.Lock()
        self.stats = {
            'processadas': 0,
            'hortolandia': 0,
            'com_ata': 0,
            'nao_htl': 0,
            'erros_fase1': 0,
            'erros_fase2': 0,
            'erros_fase3': 0
        }
        
        self.retry_fase2 = deque()
        self.retry_fase3 = deque()
        self.aulas = []
    
    async def coletar_aula_completa(self, client, aula_id, timeout, semaphore, fase=1):
        """Coleta uma aula completa (3 requests) - ultra otimizado"""
        
        # Cache negativo
        if aula_id in CACHE_IDS_NAO_HTL:
            async with self.lock:
                self.stats['nao_htl'] += 1
            return None
        
        async with semaphore:
            try:
                # REQUEST 1: Dados principais
                url_visualizar = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
                
                response1 = await client.get(url_visualizar, timeout=timeout)
                
                if response1.status_code != 200:
                    return ('retry', aula_id)
                
                html_visualizar = response1.text
                dados_aula = extrair_dados_aula(html_visualizar, aula_id)
                
                if not dados_aula:
                    # Não é de Hortolândia
                    CACHE_IDS_NAO_HTL.add(aula_id)
                    async with self.lock:
                        self.stats['nao_htl'] += 1
                    return None
                
                # REQUEST 2: ID da turma
                url_editar = f"https://musical.congregacao.org.br/aulas_abertas/editar/{aula_id}"
                response2 = await client.get(url_editar, timeout=timeout)
                
                id_turma = ""
                if response2.status_code == 200:
                    id_turma = extrair_id_turma(response2.text)
                
                dados_aula['id_turma'] = id_turma
                
                # REQUEST 3: Frequências (se tiver turma)
                if id_turma:
                    url_freq = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_frequencias/{aula_id}/{id_turma}"
                    response3 = await client.get(url_freq, timeout=timeout)
                    
                    if response3.status_code == 200:
                        total_alunos, presentes, lista_presentes, lista_ausentes = extrair_frequencias(response3.text)
                        dados_aula['total_alunos'] = total_alunos
                        dados_aula['presentes'] = presentes
                        dados_aula['lista_presentes'] = lista_presentes
                        dados_aula['lista_ausentes'] = lista_ausentes
                    else:
                        dados_aula['total_alunos'] = 0
                        dados_aula['presentes'] = 0
                        dados_aula['lista_presentes'] = ""
                        dados_aula['lista_ausentes'] = ""
                else:
                    dados_aula['total_alunos'] = 0
                    dados_aula['presentes'] = 0
                    dados_aula['lista_presentes'] = ""
                    dados_aula['lista_ausentes'] = ""
                
                async with self.lock:
                    self.stats['hortolandia'] += 1
                    if dados_aula['tem_ata'] == "Sim":
                        self.stats['com_ata'] += 1
                    self.aulas.append(dados_aula)
                
                return dados_aula
                
            except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError):
                return ('retry', aula_id)
            except Exception:
                return ('retry', aula_id)
        
        return None
    
    async def fase1_ultra_rapida(self, ids_chunk, pbar):
        """FASE 1: Ultra agressiva"""
        
        limits = httpx.Limits(
            max_keepalive_connections=200,
            max_connections=500,
            keepalive_expiry=60
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            limits=limits,
            http2=True,
            follow_redirects=True,
            timeout=None
        ) as client:
            
            # Carregar instrutores uma vez
            if not INSTRUTORES_HORTOLANDIA:
                global INSTRUTORES_HORTOLANDIA, NOMES_INSTRUTORES
                INSTRUTORES_HORTOLANDIA, NOMES_INSTRUTORES = await carregar_instrutores_async(client)
            
            semaphore = asyncio.Semaphore(SEMAPHORE_PHASE1)
            
            tasks = [self.coletar_aula_completa(client, aid, TIMEOUT_ULTRA_FAST, semaphore, fase=1) for aid in ids_chunk]
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            for resultado in resultados:
                if isinstance(resultado, tuple) and resultado[0] == 'retry':
                    self.retry_fase2.append(resultado[1])
                    async with self.lock:
                        self.stats['erros_fase1'] += 1
                
                async with self.lock:
                    self.stats['processadas'] += 1
                
                if pbar:
                    pbar.update(1)
                    pbar.set_postfix({
                        'HTL': self.stats['hortolandia'],
                        'ATA': self.stats['com_ata'],
                        'Retry': len(self.retry_fase2)
                    })
    
    async def fase2_moderada(self, pbar):
        """FASE 2: Moderada"""
        
        if not self.retry_fase2:
            return
        
        ids_retry = list(self.retry_fase2)
        self.retry_fase2.clear()
        
        limits = httpx.Limits(
            max_keepalive_connections=120,
            max_connections=300,
            keepalive_expiry=60
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            limits=limits,
            http2=True,
            follow_redirects=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(SEMAPHORE_PHASE2)
            
            tasks = [self.coletar_aula_completa(client, aid, TIMEOUT_FAST, semaphore, fase=2) for aid in ids_retry]
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, resultado in enumerate(resultados):
                if isinstance(resultado, tuple) and resultado[0] == 'retry':
                    self.retry_fase3.append(resultado[1])
                    async with self.lock:
                        self.stats['erros_fase2'] += 1
                elif isinstance(resultado, Exception):
                    self.retry_fase3.append(ids_retry[i])
                
                if pbar:
                    pbar.update(1)
                    pbar.set_postfix({
                        'HTL': self.stats['hortolandia'],
                        'ATA': self.stats['com_ata'],
                        'Retry': len(self.retry_fase3)
                    })
    
    async def fase3_garantia(self, pbar):
        """FASE 3: Conservadora"""
        
        if not self.retry_fase3:
            return
        
        ids_retry = list(self.retry_fase3)
        self.retry_fase3.clear()
        
        limits = httpx.Limits(
            max_keepalive_connections=60,
            max_connections=150,
            keepalive_expiry=60
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            limits=limits,
            http2=True,
            follow_redirects=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(SEMAPHORE_PHASE3)
            
            tasks = [self.coletar_aula_completa(client, aid, TIMEOUT_CAREFUL, semaphore, fase=3) for aid in ids_retry]
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            for resultado in resultados:
                if isinstance(resultado, tuple) and resultado[0] == 'retry':
                    async with self.lock:
                        self.stats['erros_fase3'] += 1
                elif isinstance(resultado, Exception):
                    async with self.lock:
                        self.stats['erros_fase3'] += 1
                
                if pbar:
                    pbar.update(1)
                    pbar.set_postfix({
                        'HTL': self.stats['hortolandia'],
                        'ATA': self.stats['com_ata']
                    })

# ========================================
# LOGIN
# ========================================
def login():
    """Login com Playwright"""
    print("🔐 Realizando login...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            context = browser.new_context(
                user_agent='Mozilla/5.0 (X11; Linux x64) AppleWebKit/537.36'
            )
            page = context.new_page()
            page.goto(URL_INICIAL, timeout=30000)
            page.fill('input[name="login"]', EMAIL)
            page.fill('input[name="password"]', SENHA)
            page.click('button[type="submit"]')
            page.wait_for_selector("nav", timeout=20000)
            cookies = {cookie['name']: cookie['value'] for cookie in context.cookies()}
            browser.close()
            print("✓ Login realizado\n")
            return cookies
    except Exception as e:
        print(f"✗ Erro no login: {e}")
        return None

# ========================================
# EXECUÇÃO INSANA
# ========================================
async def executar_coleta_insana(cookies, id_inicial, id_final):
    """Estratégia 3 fases"""
    
    coletor = ColetorAulasInsano(cookies, id_inicial, id_final)
    
    total_ids = id_final - id_inicial + 1
    todos_ids = list(range(id_inicial, id_final + 1))
    
    CHUNK_SIZE = 5000
    chunks = [todos_ids[i:i + CHUNK_SIZE] for i in range(0, len(todos_ids), CHUNK_SIZE)]
    
    print(f"\n{'='*80}")
    print(f"🚀 ESTRATÉGIA 3 FASES - COLETA DE AULAS")
    print(f"{'='*80}")
    print(f"📦 {total_ids:,} IDs → {len(chunks)} chunks de {CHUNK_SIZE:,}")
    print(f"⚡ FASE 1: {SEMAPHORE_PHASE1} concurrent | timeout {TIMEOUT_ULTRA_FAST}s")
    print(f"⚡ FASE 2: {SEMAPHORE_PHASE2} concurrent | timeout {TIMEOUT_FAST}s")
    print(f"⚡ FASE 3: {SEMAPHORE_PHASE3} concurrent | timeout {TIMEOUT_CAREFUL}s")
    print(f"{'='*80}\n")
    
    tempo_inicio = time.time()
    
    # FASE 1
    print("🔥 FASE 1: COLETA ULTRA RÁPIDA")
    with tqdm(total=total_ids, desc="Fase 1", unit="ID", ncols=100, colour='red') as pbar:
        for chunk in chunks:
            await coletor.fase1_ultra_rapida(chunk, pbar)
    
    tempo_fase1 = time.time() - tempo_inicio
    print(f"✓ Fase 1: {tempo_fase1:.1f}s | HTL: {coletor.stats['hortolandia']:,} | Retry: {len(coletor.retry_fase2):,}")
    
    # FASE 2
    if coletor.retry_fase2:
        print(f"\n🔄 FASE 2: RETRY MODERADO ({len(coletor.retry_fase2):,} IDs)")
        with tqdm(total=len(coletor.retry_fase2), desc="Fase 2", unit="ID", ncols=100, colour='yellow') as pbar:
            await coletor.fase2_moderada(pbar)
        
        tempo_fase2 = time.time() - tempo_inicio - tempo_fase1
        print(f"✓ Fase 2: {tempo_fase2:.1f}s | HTL: {coletor.stats['hortolandia']:,} | Retry: {len(coletor.retry_fase3):,}")
    
    # FASE 3
    if coletor.retry_fase3:
        print(f"\n🎯 FASE 3: GARANTIA FINAL ({len(coletor.retry_fase3):,} IDs)")
        with tqdm(total=len(coletor.retry_fase3), desc="Fase 3", unit="ID", ncols=100, colour='green') as pbar:
            await coletor.fase3_garantia(pbar)
        
        tempo_fase3 = time.time() - tempo_inicio - tempo_fase1 - (tempo_fase2 if coletor.retry_fase2 else 0)
        print(f"✓ Fase 3: {tempo_fase3:.1f}s | HTL: {coletor.stats['hortolandia']:,}")
    
    return coletor

# ========================================
# ENVIO DADOS
# ========================================
def enviar_dados(aulas, tempo_total, stats, id_inicial, id_final):
    """Envio para Google Sheets"""
    if not aulas:
        print("⚠️  Nenhuma aula para enviar")
        return False
    
    print(f"\n📤 Enviando {len(aulas):,} aulas para Google Sheets...")
    
    # Preparar dados
    resultado = []
    for aula in aulas:
        resultado.append([
            aula['id_aula'],
            aula.get('id_turma', ''),
            aula['descricao'],
            aula['comum'],
            aula['dia_semana'],
            aula['hora_inicio'],
            aula['hora_termino'],
            aula['data_aula'],
            aula['data_hora_abertura'],
            aula['tem_ata'],
            aula['texto_ata'],
            aula['instrutor'],
            aula.get('total_alunos', 0),
            aula.get('presentes', 0),
            aula.get('lista_presentes', ''),
            aula.get('lista_ausentes', '')
        ])
    
    payload = {
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
            "aulas_processadas": stats['processadas'],
            "aulas_com_ata": stats['com_ata'],
            "primeiro_id": id_inicial,
            "ultimo_id": id_final,
            "tempo_minutos": round(tempo_total/60, 2),
            "velocidade_ids_seg": round(stats['processadas'] / tempo_total, 2),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC")
        }
    }
    
    # Backup local
    backup_file = f'backup_aulas_{time.strftime("%Y%m%d_%H%M%S")}.json'
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"✓ Backup salvo: {backup_file}")
    
    try:
        import requests
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=180)
        
        if response.status_code == 200:
            print("✅ Dados enviados com sucesso!")
            return True
        else:
            print(f"✗ Erro HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Erro ao enviar: {e}")
        return False

# ========================================
# MAIN
# ========================================
def main():
    print("=" * 80)
    print("🔥 COLETOR DE AULAS INSANO - 3 FASES")
    print("=" * 80)
    
    if not EMAIL or not SENHA:
        print("✗ Credenciais não encontradas")
        sys.exit(1)
    
    # Definir range (você pode fazer busca binária antes se quiser)
    ID_INICIAL = 1
    ID_FINAL = 50000  # Ajuste conforme necessário
    
    print(f"📊 Range: {ID_INICIAL:,} → {ID_FINAL:,} ({ID_FINAL - ID_INICIAL + 1:,} IDs)")
    print("=" * 80)
    
    tempo_total_inicio = time.time()
    
    # Login
    cookies = login()
    if not cookies:
        sys.exit(1)
    
    # Coleta insana
    coletor = asyncio.run(executar_coleta_insana(cookies, ID_INICIAL, ID_FINAL))
    
    tempo_total = time.time() - tempo_total_inicio
    
    # Estatísticas
    print("\n" + "=" * 80)
    print("📊 RELATÓRIO FINAL")
    print("=" * 80)
    print(f"✅ Aulas processadas: {coletor.stats['processadas']:,}")
    print(f"🏫 Aulas de Hortolândia: {coletor.stats['hortolandia']:,}")
    print(f"📝 Aulas com ATA: {coletor.stats['com_ata']:,}")
    if coletor.stats['hortolandia'] > 0:
        print(f"📊 Taxa de ATA: {(coletor.stats['com_ata']/coletor.stats['hortolandia']*100):.1f}%")
    print(f"⚪ IDs não-HTL: {coletor.stats['nao_htl']:,}")
    print(f"❌ Erros irrecuperáveis: {coletor.stats['erros_fase3']:,}")
    print(f"⏱️  Tempo total: {tempo_total/60:.2f} min ({tempo_total:.0f}s)")
    print(f"⚡ Velocidade: {coletor.stats['processadas'] / tempo_total:.1f} IDs/s")
    
    if tempo_total < 600:  # < 10 min
        print(f"🏆 META ALCANÇADA! {tempo_total/60:.1f} min < 10 min")
    else:
        print(f"⚠️  Meta não alcançada: {tempo_total/60:.1f} min")
    
    print("=" * 80)
    
    # Enviar
    if coletor.aulas:
        enviar_dados(coletor.aulas, tempo_total, coletor.stats, ID_INICIAL, ID_FINAL)
        
        print("\n📋 AMOSTRAS (5 primeiras aulas):")
        for i, aula in enumerate(coletor.aulas[:5], 1):
            ata_tag = "[ATA]" if aula['tem_ata'] == "Sim" else "     "
            print(f"  {i}. {ata_tag} ID {aula['id_aula']:>6} | {aula['descricao'][:40]:40s} | {aula['instrutor'][:25]:25s} | {aula.get('presentes', 0)}/{aula.get('total_alunos', 0)}")
    
    print("\n" + "=" * 80)
    print("✅ COLETA FINALIZADA COM SUCESSO")
    print("=" * 80)

if __name__ == "__main__":
    main()
