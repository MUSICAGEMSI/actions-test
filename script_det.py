# script_relatorio_consolidado.py
from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
import json
from bs4 import BeautifulSoup
from collections import defaultdict

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_LISTAGEM_ALUNOS = "https://musical.congregacao.org.br/alunos/listagem"
URL_LISTAGEM_GRUPOS = "https://musical.congregacao.org.br/grp_musical/listagem"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzRSGdID5WjLuukBUt-5TbQjCqSvCKjr0vOWHFfFr0rChW1vINwgQE5VJDQCKM5mc693Q/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def extrair_localidade_limpa(localidade_texto):
    """
    Extrai apenas o nome da localidade, removendo HTML e informações extras
    """
    import re
    
    # Remove todas as tags HTML e spans escapados
    localidade_texto = re.sub(r'<\\?/?span[^>]*>', '', localidade_texto)
    localidade_texto = re.sub(r'<[^>]+>', '', localidade_texto)
    
    # Remove classe CSS escapada
    localidade_texto = re.sub(r"class='[^']*'", '', localidade_texto)
    
    # Pega apenas a parte antes do " | "
    if ' | ' in localidade_texto:
        localidade = localidade_texto.split(' | ')[0].strip()
    else:
        localidade = localidade_texto.strip()
    
    # Remove espaços extras e caracteres especiais
    localidade = re.sub(r'\s+', ' ', localidade).strip()
    
    return localidade

def obter_candidatos_por_localidade_e_tipo(session, tipo_ministerio):
    """
    Obtém candidatos por localidade da listagem de alunos
    tipo_ministerio: 'MÚSICO' ou 'ORGANISTA'
    """
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd'
        }
        
        # Dados para o POST - estrutura DataTables completa
        form_data = {
            'draw': '1',
            'columns[0][data]': '0',
            'columns[0][name]': '',
            'columns[0][searchable]': 'true',
            'columns[0][orderable]': 'true',
            'columns[0][search][value]': '',
            'columns[0][search][regex]': 'false',
            'columns[1][data]': '1',
            'columns[1][name]': '',
            'columns[1][searchable]': 'true',
            'columns[1][orderable]': 'true',
            'columns[1][search][value]': '',
            'columns[1][search][regex]': 'false',
            'columns[2][data]': '2',
            'columns[2][name]': '',
            'columns[2][searchable]': 'true',
            'columns[2][orderable]': 'true',
            'columns[2][search][value]': '',
            'columns[2][search][regex]': 'false',
            'columns[3][data]': '3',
            'columns[3][name]': '',
            'columns[3][searchable]': 'true',
            'columns[3][orderable]': 'true',
            'columns[3][search][value]': '',
            'columns[3][search][regex]': 'false',
            'columns[4][data]': '4',
            'columns[4][name]': '',
            'columns[4][searchable]': 'true',
            'columns[4][orderable]': 'true',
            'columns[4][search][value]': '',
            'columns[4][search][regex]': 'false',
            'columns[5][data]': '5',
            'columns[5][name]': '',
            'columns[5][searchable]': 'true',
            'columns[5][orderable]': 'true',
            'columns[5][search][value]': '',
            'columns[5][search][regex]': 'false',
            'columns[6][data]': '6',
            'columns[6][name]': '',
            'columns[6][searchable]': 'false',
            'columns[6][orderable]': 'false',
            'columns[6][search][value]': '',
            'columns[6][search][regex]': 'false',
            'order[0][column]': '0',
            'order[0][dir]': 'asc',
            'start': '0',
            'length': '10000',
            'search[value]': '',
            'search[regex]': 'false'
        }
        
        print(f"📊 Obtendo {tipo_ministerio.lower()}s da listagem de alunos...")
        resp = session.post(URL_LISTAGEM_ALUNOS, headers=headers, data=form_data, timeout=60)
        
        print(f"📊 Status da requisição: {resp.status_code}")
        
        # Níveis válidos que devemos contar (colunas B-G)
        niveis_validos = {
            'CANDIDATO(A)': 0,
            'RJM / ENSAIO': 0,
            'ENSAIO': 0,
            'RJM': 0,
            'RJM / CULTO OFICIAL': 0,
            'CULTO OFICIAL': 0
        }
        
        dados_por_localidade = defaultdict(lambda: niveis_validos.copy())
        
        if resp.status_code == 200:
            try:
                data = resp.json()
                print(f"📊 JSON recebido com {len(data.get('data', []))} registros")
                
                if 'data' in data and isinstance(data['data'], list):
                    for record in data['data']:
                        if isinstance(record, list) and len(record) >= 6:
                            # Estrutura: [id, nome, localidade_completa, ministério, instrumento, nível, ...]
                            localidade_completa = record[2]
                            ministerio = record[3]
                            nivel = record[5]
                            
                            print(f"🔍 Processando: {ministerio} | {nivel} | {localidade_completa[:50]}...")
                            
                            # Filtrar por tipo de ministério
                            if ministerio != tipo_ministerio:
                                print(f"⏭️ Pulando: ministério {ministerio} != {tipo_ministerio}")
                                continue
                            
                            # Extrair localidade limpa
                            localidade = extrair_localidade_limpa(localidade_completa)
                            
                            # Ignorar compartilhados
                            if 'COMPARTILHADO' in nivel.upper() or 'COMPARTILHADA' in nivel.upper():
                                print(f"⏭️ Pulando: {nivel} contém COMPARTILHADO")
                                continue
                            
                            # Contar apenas os níveis válidos
                            if nivel in niveis_validos:
                                dados_por_localidade[localidade][nivel] += 1
                                print(f"✅ {localidade}: {nivel} (+1)")
                            else:
                                print(f"❌ Nível inválido: {nivel}")
                
                print(f"📊 Total de localidades processadas para {tipo_ministerio}: {len(dados_por_localidade)}")
                return dict(dados_por_localidade)
                
            except json.JSONDecodeError as e:
                print(f"❌ Erro ao decodificar JSON: {e}")
                print(f"📝 Resposta recebida: {resp.text[:500]}...")
                return {}
        
        else:
            print(f"❌ Erro na requisição: {resp.status_code}")
            print(f"📝 Resposta: {resp.text[:500]}...")
            return {}
        
    except Exception as e:
        print(f"⚠️ Erro ao obter candidatos: {e}")
        import traceback
        traceback.print_exc()
        return {}

def obter_grupos_musicais_por_localidade_e_tipo(session, tipo_ministerio):
    """
    Obtém dados de grupos musicais por localidade
    Captura apenas: RJM / OFICIALIZADO(A) e OFICIALIZADO(A)
    """
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd'
        }
        
        form_data = {
            'draw': '1',
            'columns[0][data]': '0',
            'columns[0][name]': '',
            'columns[0][searchable]': 'true',
            'columns[0][orderable]': 'true',
            'columns[0][search][value]': '',
            'columns[0][search][regex]': 'false',
            'columns[1][data]': '1',
            'columns[1][name]': '',
            'columns[1][searchable]': 'true',
            'columns[1][orderable]': 'true',
            'columns[1][search][value]': '',
            'columns[1][search][regex]': 'false',
            'columns[2][data]': '2',
            'columns[2][name]': '',
            'columns[2][searchable]': 'true',
            'columns[2][orderable]': 'true',
            'columns[2][search][value]': '',
            'columns[2][search][regex]': 'false',
            'columns[3][data]': '3',
            'columns[3][name]': '',
            'columns[3][searchable]': 'true',
            'columns[3][orderable]': 'true',
            'columns[3][search][value]': '',
            'columns[3][search][regex]': 'false',
            'columns[4][data]': '4',
            'columns[4][name]': '',
            'columns[4][searchable]': 'true',
            'columns[4][orderable]': 'true',
            'columns[4][search][value]': '',
            'columns[4][search][regex]': 'false',
            'columns[5][data]': '5',
            'columns[5][name]': '',
            'columns[5][searchable]': 'true',
            'columns[5][orderable]': 'true',
            'columns[5][search][value]': '',
            'columns[5][search][regex]': 'false',
            'order[0][column]': '0',
            'order[0][dir]': 'asc',
            'start': '0',
            'length': '10000',
            'search[value]': '',
            'search[regex]': 'false'
        }
        
        print(f"🎵 Obtendo grupos musicais para {tipo_ministerio.lower()}s...")
        resp = session.post(URL_LISTAGEM_GRUPOS, headers=headers, data=form_data, timeout=60)
        
        print(f"🎵 Status da requisição: {resp.status_code}")
        
        # Níveis válidos para grupos (colunas H-I)
        niveis_grupos = {
            'RJM / OFICIALIZADO(A)': 0,
            'OFICIALIZADO(A)': 0
        }
        
        dados_grupos_por_localidade = defaultdict(lambda: niveis_grupos.copy())
        
        if resp.status_code == 200:
            try:
                data = resp.json()
                print(f"🎵 JSON recebido com {len(data.get('data', []))} registros")
                
                if 'data' in data and isinstance(data['data'], list):
                    for record in data['data']:
                        if isinstance(record, list) and len(record) >= 5:
                            # Estrutura: [id, nome, localidade, ministério, nível, instrumento, tom]
                            localidade = record[2]
                            ministerio = record[3]
                            nivel = record[4]
                            
                            print(f"🎵 Processando grupo: {ministerio} | {nivel} | {localidade}")
                            
                            # Filtrar por tipo de ministério
                            if ministerio != tipo_ministerio:
                                print(f"⏭️ Pulando grupo: ministério {ministerio} != {tipo_ministerio}")
                                continue
                            
                            # Ignorar compartilhados
                            if 'COMPARTILHADO' in nivel.upper() or 'COMPARTILHADA' in nivel.upper():
                                print(f"⏭️ Pulando grupo: {nivel} contém COMPARTILHADO")
                                continue
                            
                            # Contar apenas os níveis válidos para grupos
                            if nivel in niveis_grupos:
                                dados_grupos_por_localidade[localidade][nivel] += 1
                                print(f"✅ Grupo {localidade}: {nivel} (+1)")
                            else:
                                print(f"❌ Nível de grupo inválido: {nivel}")
                
                print(f"🎵 Total de localidades processadas nos grupos para {tipo_ministerio}: {len(dados_grupos_por_localidade)}")
                return dict(dados_grupos_por_localidade)
                
            except json.JSONDecodeError as e:
                print(f"❌ Erro ao decodificar JSON dos grupos: {e}")
                return {}
        
        else:
            print(f"❌ Erro na requisição dos grupos: {resp.status_code}")
            return {}
        
    except Exception as e:
        print(f"⚠️ Erro ao obter grupos musicais: {e}")
        import traceback
        traceback.print_exc()
        return {}

def gerar_relatorio_por_tipo(dados_candidatos, dados_grupos, tipo_ministerio):
    """
    Gera o relatório para um tipo específico (MÚSICO ou ORGANISTA)
    """
    # Headers para o relatório
    headers = [
        "Localidade",
        "CANDIDATO(A)",
        "RJM / ENSAIO", 
        "ENSAIO",
        "RJM",
        "RJM / CULTO OFICIAL",
        "CULTO OFICIAL",
        "RJM / OFICIALIZADO(A)",
        "OFICIALIZADO(A)"
    ]
    
    # Combinar todas as localidades
    todas_localidades = set(dados_candidatos.keys()) | set(dados_grupos.keys())
    
    relatorio = []
    
    for localidade in sorted(todas_localidades):
        # Dados dos candidatos (colunas B-G)
        contadores_candidatos = dados_candidatos.get(localidade, {
            'CANDIDATO(A)': 0,
            'RJM / ENSAIO': 0,
            'ENSAIO': 0,
            'RJM': 0,
            'RJM / CULTO OFICIAL': 0,
            'CULTO OFICIAL': 0
        })
        
        # Dados dos grupos (colunas H-I)
        contadores_grupos = dados_grupos.get(localidade, {
            'RJM / OFICIALIZADO(A)': 0,
            'OFICIALIZADO(A)': 0
        })
        
        linha = [
            localidade,
            contadores_candidatos['CANDIDATO(A)'],
            contadores_candidatos['RJM / ENSAIO'],
            contadores_candidatos['ENSAIO'],
            contadores_candidatos['RJM'],
            contadores_candidatos['RJM / CULTO OFICIAL'],
            contadores_candidatos['CULTO OFICIAL'],
            contadores_grupos['RJM / OFICIALIZADO(A)'],
            contadores_grupos['OFICIALIZADO(A)']
        ]
        relatorio.append(linha)
    
    return headers, relatorio

def obter_matriculados_reais(session, turma_id):
    """
    Obtém o número real de matriculados contando as linhas da tabela
    """
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        }
        
        url = f"https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/{turma_id}"
        resp = session.get(url, headers=headers, timeout=15)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Primeiro: tentar encontrar o texto "de um total de X registros"
            info_div = soup.find('div', {'class': 'dataTables_info'})
            if info_div and info_div.text:
                match = re.search(r'de um total de (\d+) registros', info_div.text)
                if match:
                    return int(match.group(1))
                    
                match2 = re.search(r'Mostrando de \d+ até (\d+)', info_div.text)
                if match2:
                    return int(match2.group(1))
            
            # Segundo: contar linhas da tabela tbody
            tbody = soup.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                valid_rows = [row for row in rows if len(row.find_all('td')) >= 4]
                return len(valid_rows)
            
            # Terceiro: contar por padrão de linhas com dados de alunos
            aluno_pattern = re.findall(r'[A-Z\s]+ - [A-Z/]+/\d+', resp.text)
            if aluno_pattern:
                return len(aluno_pattern)
            
            # Quarto: contar botões "Desmatricular"
            desmatricular_count = resp.text.count('Desmatricular')
            if desmatricular_count > 0:
                return desmatricular_count
                
        return 0
        
    except Exception as e:
        print(f"⚠️ Erro ao obter matriculados para turma {turma_id}: {e}")
        return -1

def obter_alunos_unicos(session, turma_id):
    """
    Obtém lista de alunos únicos de uma turma para contagem sem repetição
    """
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        }
        
        url = f"https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/{turma_id}"
        resp = session.get(url, headers=headers, timeout=15)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            alunos = set()
            
            # Procurar por linhas da tabela com dados de alunos
            tbody = soup.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                for row in rows:
                    tds = row.find_all('td')
                    if len(tds) >= 2:
                        # Assumindo que o nome do aluno está na primeira coluna de dados
                        nome_aluno = tds[0].get_text(strip=True)
                        if nome_aluno and nome_aluno not in ['', 'Nenhum registro encontrado']:
                            alunos.add(nome_aluno)
            
            return list(alunos)
        
        return []
        
    except Exception as e:
        print(f"⚠️ Erro ao obter alunos únicos para turma {turma_id}: {e}")
        return []

def extrair_dias_da_semana(dia_hora_texto):
    """
    Extrai os dias da semana do texto de horário
    """
    dias_map = {
        'DOM': 'DOM', 'DOMINGO': 'DOM',
        'SEG': 'SEG', 'SEGUNDA': 'SEG',
        'TER': 'TER', 'TERÇA': 'TER', 'TERCA': 'TER',
        'QUA': 'QUA', 'QUARTA': 'QUA',
        'QUI': 'QUI', 'QUINTA': 'QUI',
        'SEX': 'SEX', 'SEXTA': 'SEX',
        'SÁB': 'SÁB', 'SÁBADO': 'SÁB', 'SÁBADO': 'SÁB'
    }
    
    dias_encontrados = set()
    texto_upper = dia_hora_texto.upper()
    
    for dia_key, dia_value in dias_map.items():
        if dia_key in texto_upper:
            dias_encontrados.add(dia_value)
    
    return sorted(list(dias_encontrados))

def processar_relatorio_por_localidade(dados_turmas, session):
    """
    Processa os dados das turmas e agrupa por localidade
    """
    localidades = defaultdict(lambda: {
        'turmas': [],
        'total_matriculados': 0,
        'alunos_unicos': set(),
        'dias_semana': set()
    })
    
    print("📊 Processando dados por localidade...")
    
    for turma in dados_turmas:
        try:
            localidade = turma[0]  # Igreja/Localidade
            turma_id = turma[9]    # ID da turma
            matriculados_badge = int(turma[3]) if turma[3].isdigit() else 0
            dia_hora = turma[6]    # Dia - Hora
            
            # Obter alunos únicos desta turma
            print(f"🔍 Obtendo alunos únicos da turma {turma_id} - {localidade}")
            alunos_turma = obter_alunos_unicos(session, turma_id)
            
            # Extrair dias da semana
            dias_turma = extrair_dias_da_semana(dia_hora)
            
            # Adicionar aos dados da localidade
            localidades[localidade]['turmas'].append(turma)
            localidades[localidade]['total_matriculados'] += matriculados_badge
            localidades[localidade]['alunos_unicos'].update(alunos_turma)
            localidades[localidade]['dias_semana'].update(dias_turma)
            
            print(f"   ✅ {localidade}: +{matriculados_badge} matriculados, +{len(alunos_turma)} alunos únicos")
            
            # Pausa para não sobrecarregar
            time.sleep(0.5)
            
        except Exception as e:
            print(f"⚠️ Erro ao processar turma: {e}")
            continue
    
    return localidades

def gerar_relatorio_formatado_gem(localidades):
    """
    Gera o relatório no formato solicitado para G.E.M
    """
    relatorio = []
    
    # Cabeçalho
    cabecalho = [
        "LOCALIDADE",
        "QUANTIDADE DE TURMAS",
        "SOMA DOS MATRICULADOS",
        "MATRICULADOS SEM REPETIÇÃO",
        "DIAS EM QUE HÁ GEM",
        "DOM", "SEG", "TER", "QUA", "QUI", "SEX", "SÁB"
    ]
    relatorio.append(cabecalho)
    
    # Dados por localidade
    for localidade, dados in localidades.items():
        quantidade_turmas = len(dados['turmas'])
        soma_matriculados = dados['total_matriculados']
        matriculados_unicos = len(dados['alunos_unicos'])
        
        # Montar string dos dias
        dias_ordenados = sorted(dados['dias_semana'])
        if len(dias_ordenados) > 1:
            dias_texto = f"{dias_ordenados[0]}/{dias_ordenados[-1]}"
        elif len(dias_ordenados) == 1:
            dias_texto = dias_ordenados[0]
        else:
            dias_texto = ""
        
        # Contar por dia da semana
        contadores_dias = {"DOM": 0, "SEG": 0, "TER": 0, "QUA": 0, "QUI": 0, "SEX": 0, "SÁB": 0}
        
        for turma in dados['turmas']:
            dias_turma = extrair_dias_da_semana(turma[6])
            for dia in dias_turma:
                if dia in contadores_dias:
                    contadores_dias[dia] += 1
        
        linha = [
            localidade,
            quantidade_turmas,
            soma_matriculados,
            matriculados_unicos,
            dias_texto,
            contadores_dias["DOM"] if contadores_dias["DOM"] > 0 else "",
            contadores_dias["SEG"] if contadores_dias["SEG"] > 0 else "",
            contadores_dias["TER"] if contadores_dias["TER"] > 0 else "",
            contadores_dias["QUA"] if contadores_dias["QUA"] > 0 else "",
            contadores_dias["QUI"] if contadores_dias["QUI"] > 0 else "",
            contadores_dias["SEX"] if contadores_dias["SEX"] > 0 else "",
            contadores_dias["SÁB"] if contadores_dias["SÁB"] > 0 else ""
        ]
        
        relatorio.append(linha)
    
    return relatorio

def extrair_cookies_playwright(pagina):
    """
    Extrai cookies do Playwright para usar em requests
    """
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def coletar_dados_gem(pagina, session, tempo_inicio):
    """
    Coleta dados das turmas G.E.M
    """
    print("📊 Iniciando coleta de dados G.E.M...")
    
    # Configurar exibição para mostrar mais itens
    try:
        select_length = pagina.query_selector('select[name="tabela-turmas_length"]')
        if select_length:
            pagina.select_option('select[name="tabela-turmas_length"]', '100')
            pagina.wait_for_timeout(2000)
            print("✅ Configurado para mostrar 100 itens por página.")
    except Exception:
        print("ℹ️ Seletor de quantidade não encontrado, continuando...")

    resultado = []
    parar = False
    pagina_atual = 1

    while not parar:
        if time.time() - tempo_inicio > 1800:  # 30 minutos
            print("⏹️ Tempo limite atingido. Encerrando a coleta.")
            break

        print(f"📄 Processando página {pagina_atual}...")

        linhas = pagina.query_selector_all('table#tabela-turmas tbody tr')
        
        for i, linha in enumerate(linhas):
            if time.time() - tempo_inicio > 1800:
                print("⏹️ Tempo limite atingido durante a iteração.")
                parar = True
                break

            try:
                colunas_td = linha.query_selector_all('td')
                
                dados_linha = []
                for j, td in enumerate(colunas_td[1:], 1):
                    if j == len(colunas_td) - 1:
                        continue
                    
                    badge = td.query_selector('span.badge')
                    if badge:
                        dados_linha.append(badge.inner_text().strip())
                    else:
                        texto = td.inner_text().strip().replace('\n', ' ').replace('\t', ' ')
                        texto = re.sub(r'\s+', ' ', texto).strip()
                        dados_linha.append(texto)

                radio_input = linha.query_selector('input[type="radio"][name="item[]"]')
                if not radio_input:
                    continue
                
                turma_id = radio_input.get_attribute('value')
                if not turma_id:
                    continue

                matriculados_badge = dados_linha[3] if len(dados_linha) > 3 else "0"

                linha_completa = [
                    dados_linha[0] if len(dados_linha) > 0 else "",
                    dados_linha[1] if len(dados_linha) > 1 else "",
                    dados_linha[2] if len(dados_linha) > 2 else "",
                    matriculados_badge,
                    dados_linha[4] if len(dados_linha) > 4 else "",
                    dados_linha[5] if len(dados_linha) > 5 else "",
                    dados_linha[6] if len(dados_linha) > 6 else "",
                    dados_linha[7] if len(dados_linha) > 7 else "",
                    "Ações",
                    turma_id,
                    matriculados_badge,
                    "0",  # Será calculado depois
                    "Pendente"
                ]

                resultado.append(linha_completa)

            except Exception as e:
                print(f"⚠️ Erro ao processar linha {i}: {e}")
                continue

        if parar:
            break

        # Verificar se há próxima página
        try:
            btn_next = pagina.query_selector('a.paginate_button.next:not(.disabled)')
            if btn_next and btn_next.is_enabled():
                print(f"➡️ Avançando para página {pagina_atual + 1}...")
                btn_next.click()
                
                pagina.wait_for_function(
                    """
                    () => {
                        const tbody = document.querySelector('table#tabela-turmas tbody');
                        return tbody && tbody.querySelectorAll('tr').length > 0;
                    }
                    """,
                    timeout=15000
                )
                pagina.wait_for_timeout(3000)
                pagina_atual += 1
            else:
                print("📄 Última página alcançada.")
                break
                
        except Exception as e:
            print(f"⚠️ Erro na paginação: {e}")
            break

    return resultado

def main():
    """
    Função principal que coordena toda a execução
    """
    tempo_inicio = time.time()
    print("🚀 Iniciando script consolidado...")
    print("=" * 80)

    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Login
        print("🔐 Fazendo login...")
        pagina.goto(URL_INICIAL)
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')

        try:
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado com sucesso")
        except PlaywrightTimeoutError:
            print("❌ Falha no login")
            navegador.close()
            return

        # Extrair cookies para usar com requests
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)

        # === PARTE 1: RELATÓRIOS DE MÚSICOS E ORGANISTAS ===
        print("\n" + "=" * 80)
        print("🎵 PROCESSANDO RELATÓRIOS MUSICAIS")
        print("=" * 80)

        # Navegar para as páginas para garantir contexto
        print("📄 Navegando para listagem de alunos...")
        pagina.goto("https://musical.congregacao.org.br/alunos")
        pagina.wait_for_timeout(1000)
        
        print("📄 Navegando para listagem de grupos...")
        pagina.goto("https://musical.congregacao.org.br/grp_musical")
        pagina.wait_for_timeout(1000)

        # Atualizar cookies após navegação
        cookies_dict = extrair_cookies_playwright(pagina)
        session.cookies.update(cookies_dict)

        # PROCESSAMENTO PARA MÚSICOS
        print("\n" + "="*60)
        print("🎸 PROCESSANDO DADOS PARA MÚSICOS")
        print("="*60)
        
        dados_candidatos_musicos = obter_candidatos_por_localidade_e_tipo(session, "MÚSICO")
        dados_grupos_musicos = obter_grupos_musicais_por_localidade_e_tipo(session, "MÚSICO")
        
        headers_musicos, relatorio_musicos = gerar_relatorio_por_tipo(
            dados_candidatos_musicos, dados_grupos_musicos, "MÚSICO"
        )

        # PROCESSAMENTO PARA ORGANISTAS
        print("\n" + "="*60)
        print("🎹 PROCESSANDO DADOS PARA ORGANISTAS")
        print("="*60)
        
        dados_candidatos_organistas = obter_candidatos_por_localidade_e_tipo(session, "ORGANISTA")
        dados_grupos_organistas = obter_grupos_musicais_por_localidade_e_tipo(session, "ORGANISTA")
        
        headers_organistas, relatorio_organistas = gerar_relatorio_por_tipo(
            dados_candidatos_organistas, dados_grupos_organistas, "ORGANISTA"
        )

        # === PARTE 2: RELATÓRIO G.E.M POR LOCALIDADE ===
        print("\n" + "=" * 80)
        print("📚 PROCESSANDO RELATÓRIO G.E.M POR LOCALIDADE")
        print("=" * 80)

        # Navegar para G.E.M
        try:
            gem_selector = 'span:has-text("G.E.M")'
            pagina.wait_for_selector(gem_selector, timeout=15000)
            gem_element = pagina.locator(gem_selector).first

            gem_element.hover()
            pagina.wait_for_timeout(1000)

            if gem_element.is_visible() and gem_element.is_enabled():
                gem_element.click()
                print("✅ Menu G.E.M acessado")
            else:
                print("❌ Elemento G.E.M não estava clicável.")
                navegador.close()
                return
        except PlaywrightTimeoutError:
            print("❌ Menu 'G.E.M' não apareceu a tempo.")
            navegador.close()
            return

        # Navegar para Turmas
        try:
            pagina.wait_for_selector('a[href="turmas"]', timeout=10000)
            pagina.click('a[href="turmas"]')
            print("✅ Navegando para Turmas...")
        except PlaywrightTimeoutError:
            print("❌ Link 'turmas' não encontrado.")
            navegador.close()
            return

        # Aguardar carregamento da tabela de turmas
        try:
            pagina.wait_for_selector('table#tabela-turmas', timeout=15000)
            print("✅ Tabela de turmas carregada.")
            
            pagina.wait_for_function(
                """
                () => {
                    const tbody = document.querySelector('table#tabela-turmas tbody');
                    return tbody && tbody.querySelectorAll('tr').length > 0;
                }
                """, timeout=15000
            )
            print("✅ Linhas da tabela de turmas carregadas.")
        except PlaywrightTimeoutError:
            print("❌ A tabela de turmas não carregou a tempo.")
            navegador.close()
            return

        # Atualizar cookies novamente
        cookies_dict = extrair_cookies_playwright(pagina)
        session.cookies.update(cookies_dict)

        # Coletar dados G.E.M
        dados_gem = coletar_dados_gem(pagina, session, tempo_inicio)
        print(f"📊 Total de turmas G.E.M coletadas: {len(dados_gem)}")

        # Processar dados por localidade
        print("\n🏢 Processando relatório G.E.M por localidade...")
        localidades_gem = processar_relatorio_por_localidade(dados_gem, session)
        
        # Gerar relatório formatado G.E.M
        relatorio_formatado_gem = gerar_relatorio_formatado_gem(localidades_gem)

        # === EXIBIÇÃO DOS RESULTADOS ===
        print(f"\n🎸 RELATÓRIO DE MÚSICOS POR LOCALIDADE:")
        print("="*150)
        print(f"{'Localidade':<25} {'CAND':<5} {'R/E':<5} {'ENS':<5} {'RJM':<5} {'R/C':<5} {'CULTO':<6} {'R/OF':<5} {'OFIC':<5}")
        print("-"*150)
        
        for linha in relatorio_musicos[:10]:  # Mostrar apenas as primeiras 10
            print(f"{linha[0]:<25} {linha[1]:<5} {linha[2]:<5} {linha[3]:<5} {linha[4]:<5} {linha[5]:<5} {linha[6]:<6} {linha[7]:<5} {linha[8]:<5}")
        
        if len(relatorio_musicos) > 10:
            print(f"... e mais {len(relatorio_musicos) - 10} localidades")

        print(f"\n🎹 RELATÓRIO DE ORGANISTAS POR LOCALIDADE:")
        print("="*150)
        print(f"{'Localidade':<25} {'CAND':<5} {'R/E':<5} {'ENS':<5} {'RJM':<5} {'R/C':<5} {'CULTO':<6} {'R/OF':<5} {'OFIC':<5}")
        print("-"*150)
        
        for linha in relatorio_organistas[:10]:  # Mostrar apenas as primeiras 10
            print(f"{linha[0]:<25} {linha[1]:<5} {linha[2]:<5} {linha[3]:<5} {linha[4]:<5} {linha[5]:<5} {linha[6]:<6} {linha[7]:<5} {linha[8]:<5}")
        
        if len(relatorio_organistas) > 10:
            print(f"... e mais {len(relatorio_organistas) - 10} localidades")

        # Mostrar relatório G.E.M na tela
        print("\n📊 RELATÓRIO G.E.M POR LOCALIDADE:")
        print("-" * 120)
        for i, linha in enumerate(relatorio_formatado_gem):
            if i == 0:  # Cabeçalho
                print(f"{'|'.join(f'{str(item):^15}' for item in linha)}")
                print("-" * 120)
            else:
                print(f"{'|'.join(f'{str(item):^15}' for item in linha)}")
                if i >= 10:  # Mostrar apenas as primeiras 10 linhas de dados
                    break
        
        if len(relatorio_formatado_gem) > 11:  # 10 + 1 cabeçalho
            print(f"... e mais {len(relatorio_formatado_gem) - 11} localidades")

        # === ENVIO PARA GOOGLE SHEETS ===
        print("\n" + "=" * 80)
        print("📤 ENVIANDO DADOS PARA GOOGLE SHEETS")
        print("=" * 80)

        # Preparar dados com headers
        dados_musicos_com_headers = [headers_musicos] + relatorio_musicos
        dados_organistas_com_headers = [headers_organistas] + relatorio_organistas
        
        # Enviar dados dos músicos
        try:
            print(f"\n📤 Enviando {len(relatorio_musicos)} localidades de MÚSICOS para Google Sheets...")
            body_musicos = {
                "tipo": "relatorio_musicos_localidade",
                "dados": dados_musicos_com_headers,
                "incluir_headers": True
            }
            resposta = requests.post(URL_APPS_SCRIPT, json=body_musicos, timeout=60)
            print(f"✅ Status do envio (músicos): {resposta.status_code}")
            if resposta.status_code == 200:
                print(f"📋 Resposta: {resposta.text}")
        except Exception as e:
            print(f"❌ Erro no envio (músicos): {e}")

        # Enviar dados dos organistas
        try:
            print(f"\n📤 Enviando {len(relatorio_organistas)} localidades de ORGANISTAS para Google Sheets...")
            body_organistas = {
                "tipo": "relatorio_organistas_localidade",
                "dados": dados_organistas_com_headers,
                "incluir_headers": True
            }
            resposta = requests.post(URL_APPS_SCRIPT, json=body_organistas, timeout=60)
            print(f"✅ Status do envio (organistas): {resposta.status_code}")
            if resposta.status_code == 200:
                print(f"📋 Resposta: {resposta.text}")
        except Exception as e:
            print(f"❌ Erro no envio (organistas): {e}")

        # Enviar dados do G.E.M
        try:
            print(f"\n📤 Enviando relatório G.E.M com {len(localidades_gem)} localidades para Google Sheets...")
            body_gem = {
                "tipo": "relatorio_gem_localidades",
                "relatorio_formatado": relatorio_formatado_gem,
                "dados_brutos": dados_gem,
                "resumo": {
                    "total_localidades": len(localidades_gem),
                    "total_turmas": len(dados_gem),
                    "total_matriculados": sum(loc['total_matriculados'] for loc in localidades_gem.values()),
                    "total_alunos_unicos": sum(len(loc['alunos_unicos']) for loc in localidades_gem.values())
                }
            }
            resposta = requests.post(URL_APPS_SCRIPT, json=body_gem, timeout=60)
            print(f"✅ Status do envio (G.E.M): {resposta.status_code}")
            if resposta.status_code == 200:
                print(f"📋 Resposta: {resposta.text}")
        except Exception as e:
            print(f"❌ Erro no envio (G.E.M): {e}")

        navegador.close()
        
        tempo_total = time.time() - tempo_inicio
        print(f"\n🎯 SCRIPT CONSOLIDADO CONCLUÍDO!")
        print("=" * 80)
        print(f"🎸 Músicos: {len(relatorio_musicos)} localidades processadas")
        print(f"🎹 Organistas: {len(relatorio_organistas)} localidades processadas")
        print(f"📚 G.E.M: {len(localidades_gem)} localidades processadas")
        print(f"⏱️ Tempo total: {tempo_total:.1f} segundos")
        print("=" * 80)

if __name__ == "__main__":
    main()
