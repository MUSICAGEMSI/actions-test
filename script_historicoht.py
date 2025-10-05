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

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbyvEGIUPIvgbSuT_yikqg03nEjqXryd6RfI121A3pRt75v9oJoFNLTdvo3-onNdEsJd/exec'

# Cache de instrutores
INSTRUTORES_HORTOLANDIA = {}
NOMES_INSTRUTORES = set()

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

def carregar_instrutores_hortolandia(session, max_tentativas=5):
    """Carrega a lista completa de instrutores de Hortolândia COM RETRY ROBUSTO"""
    print("\nCarregando lista de instrutores de Hortolândia...")
    
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
            print(f"   Tentativa {tentativa}/{max_tentativas} (timeout: {timeout}s)...")
            
            resp = session.get(url, headers=headers, timeout=timeout)
            
            if resp.status_code != 200:
                print(f"   HTTP {resp.status_code}")
                continue
            
            instrutores = json.loads(resp.text)
            
            ids_dict = {}
            nomes_set = set()
            
            for instrutor in instrutores:
                id_instrutor = instrutor['id']
                texto_completo = instrutor['text']
                nome = texto_completo.split(' - ')[0].strip()
                
                ids_dict[id_instrutor] = nome
                nomes_set.add(nome)
            
            print(f"   {len(ids_dict)} instrutores carregados!\n")
            return ids_dict, nomes_set
            
        except requests.Timeout:
            print(f"   Timeout na tentativa {tentativa}")
            if tentativa < max_tentativas:
                time.sleep(tentativa * 2)
        except Exception as e:
            print(f"   Erro: {e}")
            if tentativa < max_tentativas:
                time.sleep(tentativa * 2)
    
    print("\nFalha ao carregar instrutores após todas as tentativas\n")
    return {}, set()

def extrair_data_hora_abertura_rapido(session, aula_id):
    """
    Extrai APENAS a "Data e Horário de abertura" da aula
    Retorna datetime object ou None
    """
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
        
        # Procurar especificamente por "Data e Horário de abertura"
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
                    # Formato esperado: "01/07/2024 08:30:15" ou "01/07/2024 08:30"
                    try:
                        # Tentar com segundos
                        return datetime.strptime(valor, '%d/%m/%Y %H:%M:%S')
                    except:
                        try:
                            # Tentar sem segundos
                            return datetime.strptime(valor, '%d/%m/%Y %H:%M')
                        except:
                            pass
        
        return None
        
    except:
        return None

def buscar_primeiro_id_a_partir_de(session, data_hora_alvo, id_min=1, id_max=500000):
    """
    Busca binária para encontrar o primeiro ID cuja "Data e Horário de abertura"
    seja >= data_hora_alvo
    
    Args:
        data_hora_alvo: datetime object (ex: 01/07/2024 00:00:00)
        id_min: ID mínimo para busca
        id_max: ID máximo para busca
    
    Returns:
        ID da primeira aula aberta a partir da data/hora ou None
    """
    print(f"\nBuscando primeira aula aberta a partir de {data_hora_alvo.strftime('%d/%m/%Y %H:%M')}...")
    print(f"Range de busca: ID {id_min:,} ate {id_max:,}")
    
    melhor_id = None
    tentativas = 0
    max_tentativas = 100
    
    esquerda = id_min
    direita = id_max
    
    while esquerda <= direita and tentativas < max_tentativas:
        tentativas += 1
        meio = (esquerda + direita) // 2
        
        print(f"   [{tentativas:2d}] Testando ID {meio:,}...", end=" ")
        
        data_hora_abertura = extrair_data_hora_abertura_rapido(session, meio)
        
        if data_hora_abertura is None:
            print("(nao existe/erro)")
            direita = meio - 1
            continue
        
        print(f"Abertura: {data_hora_abertura.strftime('%d/%m/%Y %H:%M')}")
        
        # Se a abertura é >= data alvo
        if data_hora_abertura >= data_hora_alvo:
            melhor_id = meio
            # Procura à esquerda por um ID ainda mais antigo que atenda
            direita = meio - 1
        else:
            # Abertura é antes do alvo, procura à direita
            esquerda = meio + 1
    
    if melhor_id:
        print(f"\nID encontrado: {melhor_id}")
        print("Fazendo ajuste fino (voltando 100 IDs)...")
        
        id_ajustado = max(id_min, melhor_id - 100)
        
        # Verificar se existe um ID anterior que também atende
        for id_teste in range(id_ajustado, melhor_id):
            data_teste = extrair_data_hora_abertura_rapido(session, id_teste)
            if data_teste and data_teste >= data_hora_alvo:
                melhor_id = id_teste
                break
        
        print(f"Primeiro ID confirmado: {melhor_id}\n")
        return melhor_id
    
    print(f"\nNenhuma aula encontrada a partir de {data_hora_alvo.strftime('%d/%m/%Y %H:%M')}\n")
    return None

def buscar_ultimo_id_ate(session, data_hora_limite, id_min=1, id_max=999999):
    """
    Busca binária para encontrar o último ID cuja "Data e Horário de abertura"
    seja <= data_hora_limite (momento da execução do script)
    
    Returns:
        Último ID válido até a data/hora limite ou None
    """
    print(f"\nBuscando ultimo ID aberto ate {data_hora_limite.strftime('%d/%m/%Y %H:%M')}...")
    print(f"Range de busca: ID {id_min:,} ate {id_max:,}")
    
    ultimo_valido = None
    tentativas = 0
    max_tentativas = 100
    
    esquerda = id_min
    direita = id_max
    
    while esquerda <= direita and tentativas < max_tentativas:
        tentativas += 1
        meio = (esquerda + direita) // 2
        
        print(f"   [{tentativas:2d}] Testando ID {meio:,}...", end=" ")
        
        data_hora_abertura = extrair_data_hora_abertura_rapido(session, meio)
        
        if data_hora_abertura is None:
            print("Nao existe")
            direita = meio - 1
            continue
        
        print(f"Abertura: {data_hora_abertura.strftime('%d/%m/%Y %H:%M')}")
        
        # Se abertura <= limite
        if data_hora_abertura <= data_hora_limite:
            ultimo_valido = meio
            # Procura à direita por IDs maiores que ainda atendam
            esquerda = meio + 1
        else:
            # Abertura é depois do limite, procura à esquerda
            direita = meio - 1
    
    if ultimo_valido:
        print(f"\nID encontrado: {ultimo_valido}")
        print("Fazendo ajuste fino (avancando ate 100 IDs)...")
        
        for id_teste in range(ultimo_valido + 1, ultimo_valido + 101):
            data_teste = extrair_data_hora_abertura_rapido(session, id_teste)
            if data_teste and data_teste <= data_hora_limite:
                ultimo_valido = id_teste
        
        print(f"Ultimo ID confirmado: {ultimo_valido}\n")
        return ultimo_valido
    
    print(f"\nNenhum ID valido encontrado\n")
    return None

def normalizar_nome(nome):
    """Normaliza nome para comparação"""
    return ' '.join(nome.upper().split())

def coletar_tudo_de_uma_vez(session, aula_id):
    """Coleta TODOS os dados em uma única chamada (3 requests por aula)"""
    try:
        # REQUEST 1: visualizar_aula
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
        
        # Extrair data da aula
        data_aula = ""
        modal_header = soup.find('div', class_='modal-header')
        if modal_header:
            date_span = modal_header.find('span', class_='pull-right')
            if date_span:
                texto = date_span.get_text(strip=True)
                data_aula = texto.strip()
        
        # Extrair dados da tabela principal
        tbody = soup.find('tbody')
        if not tbody:
            return None
        
        descricao = ""
        comum = ""
        hora_inicio = ""
        hora_termino = ""
        data_hora_abertura = ""
        nome_instrutor = ""
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
                nome_instrutor = valor.split(' - ')[0].strip()
        
        # Extrair descrição
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
        
        # Verificar se o instrutor é de Hortolândia
        eh_hortolandia = False
        if nome_instrutor:
            nome_normalizado = normalizar_nome(nome_instrutor)
            for nome_htl in NOMES_INSTRUTORES:
                if normalizar_nome(nome_htl) == nome_normalizado:
                    eh_hortolandia = True
                    break
        
        if not eh_hortolandia:
            return None
        
        # Verificação de ATA
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
        
        # Dia da semana
        dia_semana = ""
        if data_aula:
            try:
                data_obj = datetime.strptime(data_aula, '%d/%m/%Y')
                dias = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
                dia_semana = dias[data_obj.weekday()]
            except:
                dia_semana = ""
        
        # REQUEST 2: ID da turma
        url_editar = f"https://musical.congregacao.org.br/aulas_abertas/editar/{aula_id}"
        resp_editar = session.get(url_editar, headers=headers, timeout=5)
        
        if resp_editar.status_code == 200:
            soup_editar = BeautifulSoup(resp_editar.text, 'html.parser')
            turma_input = soup_editar.find('input', {'name': 'id_turma'})
            if turma_input:
                id_turma = turma_input.get('value', '').strip()
        
        # REQUEST 3: Frequências
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
            'instrutor': nome_instrutor,
            'total_alunos': total_alunos,
            'presentes': presentes,
            'lista_presentes': lista_presentes,
            'lista_ausentes': lista_ausentes
        }
        
    except:
        return None

def main():
    global INSTRUTORES_HORTOLANDIA, NOMES_INSTRUTORES
    
    tempo_inicio = time.time()
    
    print("=" * 70)
    print("COLETOR ULTRA-RAPIDO - HORTOLANDIA (COM BUSCA INTELIGENTE)")
    print("=" * 70)
    
    # Login via Playwright
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        print("\nRealizando login...")
        pagina.goto(URL_INICIAL, timeout=20000)
        
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')
        
        try:
            pagina.wait_for_selector("nav", timeout=20000)
            print("Login realizado!")
        except PlaywrightTimeoutError:
            print("Falha no login.")
            navegador.close()
            return
        
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
    
    # Criar sessão robusta
    session = criar_sessao_robusta()
    session.cookies.update(cookies_dict)
    
    # Carregar instrutores
    INSTRUTORES_HORTOLANDIA, NOMES_INSTRUTORES = carregar_instrutores_hortolandia(session)
    
    if not INSTRUTORES_HORTOLANDIA:
        print("Nao foi possivel carregar a lista de instrutores. Abortando.")
        return
    
    # ========================================================================
    # BUSCA INTELIGENTE: Baseada em "Data e Horário de abertura"
    # ========================================================================
    
    # Definir período de coleta
    ano_atual = datetime.now().year
    mes_alvo = 7  # Julho
    
    # Data/hora de início: 01/07/2024 00:00:00
    data_hora_inicio = datetime(ano_atual, mes_alvo, 1, 0, 0, 0)
    
    # Data/hora de fim: momento atual da execução
    data_hora_fim = datetime.now()
    
    print(f"\n{'=' * 70}")
    print(f"PERIODO DE COLETA:")
    print(f"  Inicio: {data_hora_inicio.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"  Fim:    {data_hora_fim.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"{'=' * 70}")
    
    # Busca 1: Primeiro ID com abertura >= 01/07/2024 00:00
    primeiro_id = buscar_primeiro_id_a_partir_de(
        session, 
        data_hora_alvo=data_hora_inicio,
        id_min=327000,    # Estimativa conservadora
        id_max=500000     # Limite superior para busca
    )
    
    if primeiro_id is None:
        print(f"Nao foi possivel encontrar aulas abertas a partir de {data_hora_inicio.strftime('%d/%m/%Y')}. Abortando.")
        return
    
    # Busca 2: Último ID com abertura <= data/hora atual
    ultimo_id = buscar_ultimo_id_ate(
        session,
        data_hora_limite=data_hora_fim,
        id_min=primeiro_id,  # Começa do primeiro ID encontrado
        id_max=999999        # Limite teórico
    )
    
    if ultimo_id is None:
        print("Nao foi possivel encontrar o ultimo ID. Usando estimativa.")
        ultimo_id = primeiro_id + 50000  # Fallback: +50k IDs
    
    # ========================================================================
    # COLETA: Do primeiro ID até o último ID (baseado em data/hora abertura)
    # ========================================================================
    
    resultado = []
    aulas_processadas = 0
    aulas_hortolandia = 0
    aulas_com_ata = 0
    
    ID_INICIAL = primeiro_id
    ID_FINAL = ultimo_id
    
    LOTE_SIZE = 200
    MAX_WORKERS = 15
    
    print(f"\n{'=' * 70}")
    print(f"MODO TURBO ATIVADO!")
    print(f"Range: {ID_INICIAL:,} a {ID_FINAL:,} ({ID_FINAL - ID_INICIAL + 1:,} IDs)")
    print(f"{MAX_WORKERS} threads paralelas | Lotes de {LOTE_SIZE}")
    print(f"{'=' * 70}\n")
    
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
                        ata_status = "ATA"
                    else:
                        ata_status = "   "
                    
                    print(f"[{ata_status}] [{aulas_processadas:5d}] ID {dados_completos['id_aula']}: {dados_completos['descricao'][:20]:20s} | {dados_completos['instrutor'][:25]:25s} | {dados_completos['presentes']}/{dados_completos['total_alunos']}")
                
                if aulas_processadas % 200 == 0:
                    tempo_decorrido = time.time() - tempo_inicio
                    velocidade = aulas_processadas / tempo_decorrido
                    tempo_estimado = (ID_FINAL - ID_INICIAL + 1 - aulas_processadas) / velocidade / 60
                    print(f"\n{'─' * 70}")
                    print(f"{aulas_processadas} processadas | {aulas_hortolandia} HTL | {aulas_com_ata} com ATA | {velocidade:.1f} aulas/s | ETA: {tempo_estimado:.1f}min")
                    print(f"{'─' * 70}\n")
        
        time.sleep(0.5)
    
    print(f"\n{'=' * 70}")
    print(f"COLETA FINALIZADA!")
    print(f"{'=' * 70}")
    print(f"Total processado: {aulas_processadas:,}")
    print(f"Aulas de Hortolandia: {aulas_hortolandia:,}")
    if aulas_hortolandia > 0:
        print(f"Aulas com ATA: {aulas_com_ata} ({aulas_com_ata/aulas_hortolandia*100:.1f}%)")
    print(f"Tempo total: {(time.time() - tempo_inicio)/60:.1f} minutos")
    print(f"Velocidade media: {aulas_processadas/(time.time() - tempo_inicio):.1f} aulas/segundo")
    print(f"{'=' * 70}\n")
    
    # Preparar envio
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
            "total_instrutores_htl": len(INSTRUTORES_HORTOLANDIA),
            "primeiro_id_julho": ID_INICIAL,
            "tempo_minutos": round((time.time() - tempo_inicio)/60, 2),
            "velocidade_aulas_por_segundo": round(aulas_processadas/(time.time() - tempo_inicio), 2)
        }
    }
    
    # Salvar backup local
    backup_file = f'backup_aulas_{time.strftime("%Y%m%d_%H%M%S")}.json'
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(body, f, ensure_ascii=False, indent=2)
    print(f"Backup salvo em: {backup_file}")
    
    # Enviar para Apps Script
    print("\nEnviando dados para Google Sheets...")
    try:
        resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
        print(f"Dados enviados! Status: {resposta_post.status_code}")
        print(f"Resposta: {resposta_post.text[:200]}")
    except Exception as e:
        print(f"Erro ao enviar: {e}")
        print(f"Dados disponiveis no backup: {backup_file}")

if __name__ == "__main__":
    main()
