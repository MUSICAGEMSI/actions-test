# script_unificado_otimizado.py
"""
Script Unificado - 3 Módulos Independentes
- Módulo 1: Relatório de Níveis (Músicos/Organistas)
- Módulo 2: Turmas G.E.M com Verificação
- Módulo 3: Relatório por Localidades

Características:
- Login único compartilhado
- Módulos independentes (falha de um não afeta os outros)
- Tratamento robusto de erros
- Logs detalhados de execução
"""

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

# ==================== CONFIGURAÇÕES GLOBAIS ====================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_LISTAGEM_ALUNOS = "https://musical.congregacao.org.br/alunos/listagem"
URL_LISTAGEM_GRUPOS = "https://musical.congregacao.org.br/grp_musical/listagem"

# URLs dos Apps Scripts
URL_APPS_SCRIPT_NIVEL = 'https://script.google.com/macros/s/AKfycbwck6h5TupkWvibkkcQjuQbN3ioROH594QuipKW_GUb8SC8Vii9O1e3rksjSWFL_nZP/exec'
URL_APPS_SCRIPT_EXPANDIDO = 'https://script.google.com/macros/s/AKfycbyDhrvHOn9afWBRxDPEMtmAcUcuUzLgfxUZRSjZRSaheUs52pOOb1N6sTDtTbBYCmvu/exec'
URL_APPS_SCRIPT_TURMA = 'https://script.google.com/macros/s/AKfycbxNpziYUDS2IL2L9bpfbtci8Mq1gDNWKL2XUhImPtgevyW_y7nVfRvFJjpHrozh9SiC/exec'

# Timeout máximo por módulo (em segundos)
TIMEOUT_MODULO = 1800  # 30 minutos

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

# ==================== FUNÇÕES AUXILIARES COMPARTILHADAS ====================

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para usar em requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def extrair_localidade_limpa(localidade_texto):
    """Extrai apenas o nome da localidade, removendo HTML e informações extras"""
    localidade_texto = re.sub(r'<\\?/?span[^>]*>', '', localidade_texto)
    localidade_texto = re.sub(r'<[^>]+>', '', localidade_texto)
    localidade_texto = re.sub(r"class='[^']*'", '', localidade_texto)
    
    if ' | ' in localidade_texto:
        localidade = localidade_texto.split(' | ')[0].strip()
    else:
        localidade = localidade_texto.strip()
    
    localidade = re.sub(r'\s+', ' ', localidade).strip()
    return localidade

def extrair_dias_da_semana(dia_hora_texto):
    """Extrai os dias da semana do texto de horário com melhor detecção"""
    dias_map = {
        'DOMINGO': 'DOM', 'DOM': 'DOM',
        'SEGUNDA': 'SEG', 'SEGUNDA-FEIRA': 'SEG', 'SEG': 'SEG',
        'TERÇA': 'TER', 'TERÇA-FEIRA': 'TER', 'TERCA': 'TER', 'TER': 'TER',
        'QUARTA': 'QUA', 'QUARTA-FEIRA': 'QUA', 'QUA': 'QUA',
        'QUINTA': 'QUI', 'QUINTA-FEIRA': 'QUI', 'QUI': 'QUI',
        'SEXTA': 'SEX', 'SEXTA-FEIRA': 'SEX', 'SEX': 'SEX',
        'SÁBADO': 'SÁB', 'SABADO': 'SÁB', 'SÁB': 'SÁB'
    }
    
    dias_encontrados = set()
    texto_upper = dia_hora_texto.upper()
    texto_normalizado = texto_upper.replace('Ç', 'C').replace('Ã', 'A')
    
    for dia_key, dia_value in dias_map.items():
        if dia_key in texto_normalizado or dia_key in texto_upper:
            dias_encontrados.add(dia_value)
    
    abreviacoes = ['DOM', 'SEG', 'TER', 'QUA', 'QUI', 'SEX', 'SAB', 'SÁB']
    for abrev in abreviacoes:
        if abrev in texto_upper:
            if abrev == 'SAB':
                dias_encontrados.add('SÁB')
            else:
                dias_encontrados.add(abrev)
    
    ordem_cronologica = ['DOM', 'SEG', 'TER', 'QUA', 'QUI', 'SEX', 'SÁB']
    dias_ordenados = [dia for dia in ordem_cronologica if dia in dias_encontrados]
    
    return dias_ordenados

def obter_matriculados_reais(session, turma_id):
    """Obtém o número real de matriculados contando as linhas da tabela"""
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
            
            info_div = soup.find('div', {'class': 'dataTables_info'})
            if info_div and info_div.text:
                match = re.search(r'de um total de (\d+) registros', info_div.text)
                if match:
                    return int(match.group(1))
                    
                match2 = re.search(r'Mostrando de \d+ até (\d+)', info_div.text)
                if match2:
                    return int(match2.group(1))
            
            tbody = soup.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                valid_rows = [row for row in rows if len(row.find_all('td')) >= 4]
                return len(valid_rows)
            
            aluno_pattern = re.findall(r'[A-Z\s]+ - [A-Z/]+/\d+', resp.text)
            if aluno_pattern:
                return len(aluno_pattern)
            
            desmatricular_count = resp.text.count('Desmatricular')
            if desmatricular_count > 0:
                return desmatricular_count
                
        return 0
        
    except Exception as e:
        return -1

def obter_alunos_unicos(session, turma_id):
    """Obtém lista de alunos únicos de uma turma para contagem sem repetição"""
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
            alunos = set()
            
            tbody = soup.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                for row in rows:
                    tds = row.find_all('td')
                    if len(tds) >= 2:
                        nome_aluno = tds[0].get_text(strip=True)
                        if nome_aluno and nome_aluno not in ['', 'Nenhum registro encontrado']:
                            alunos.add(nome_aluno)
            
            return list(alunos)
        
        return []
        
    except Exception as e:
        return []

# ==================== MÓDULO 1: RELATÓRIO DE NÍVEIS ====================

def obter_candidatos_por_localidade_e_tipo(session, tipo_ministerio):
    """Obtém candidatos por localidade da listagem de alunos"""
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
        }
        
        form_data = {
            'draw': '1',
            'start': '0',
            'length': '10000',
            'search[value]': '',
            'search[regex]': 'false',
            'order[0][column]': '0',
            'order[0][dir]': 'asc',
        }
        
        # Adicionar colunas
        for i in range(7):
            form_data[f'columns[{i}][data]'] = str(i)
            form_data[f'columns[{i}][searchable]'] = 'true'
            form_data[f'columns[{i}][orderable]'] = 'true' if i < 6 else 'false'
            form_data[f'columns[{i}][search][value]'] = ''
            form_data[f'columns[{i}][search][regex]'] = 'false'
        
        print(f"📊 Obtendo {tipo_ministerio.lower()}s da listagem de alunos...")
        resp = session.post(URL_LISTAGEM_ALUNOS, headers=headers, data=form_data, timeout=60)
        
        print(f"📊 Status da requisição: {resp.status_code}")
        
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
                            localidade_completa = record[2]
                            ministerio = record[3]
                            nivel = record[5]
                            
                            if ministerio != tipo_ministerio:
                                continue
                            
                            localidade = extrair_localidade_limpa(localidade_completa)
                            
                            if 'COMPARTILHADO' in nivel.upper() or 'COMPARTILHADA' in nivel.upper():
                                continue
                            
                            if nivel in niveis_validos:
                                dados_por_localidade[localidade][nivel] += 1
                
                print(f"📊 Total de localidades processadas para {tipo_ministerio}: {len(dados_por_localidade)}")
                return dict(dados_por_localidade)
                
            except json.JSONDecodeError as e:
                print(f"❌ Erro ao decodificar JSON: {e}")
                return {}
        else:
            print(f"❌ Erro na requisição: {resp.status_code}")
            return {}
        
    except Exception as e:
        print(f"⚠️ Erro ao obter candidatos: {e}")
        return {}

def obter_grupos_musicais_por_localidade_e_tipo(session, tipo_ministerio):
    """Obtém dados de grupos musicais por localidade"""
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
        }
        
        form_data = {
            'draw': '1',
            'start': '0',
            'length': '10000',
            'search[value]': '',
            'search[regex]': 'false',
            'order[0][column]': '0',
            'order[0][dir]': 'asc',
        }
        
        # Adicionar colunas
        for i in range(6):
            form_data[f'columns[{i}][data]'] = str(i)
            form_data[f'columns[{i}][searchable]'] = 'true'
            form_data[f'columns[{i}][orderable]'] = 'true'
            form_data[f'columns[{i}][search][value]'] = ''
            form_data[f'columns[{i}][search][regex]'] = 'false'
        
        print(f"🎵 Obtendo grupos musicais para {tipo_ministerio.lower()}s...")
        resp = session.post(URL_LISTAGEM_GRUPOS, headers=headers, data=form_data, timeout=60)
        
        print(f"🎵 Status da requisição: {resp.status_code}")
        
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
                            localidade = record[2]
                            ministerio = record[3]
                            nivel = record[4]
                            
                            if ministerio != tipo_ministerio:
                                continue
                            
                            if 'COMPARTILHADO' in nivel.upper() or 'COMPARTILHADA' in nivel.upper():
                                continue
                            
                            if nivel in niveis_grupos:
                                dados_grupos_por_localidade[localidade][nivel] += 1
                
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
        return {}

def gerar_relatorio_por_tipo(dados_candidatos, dados_grupos, tipo_ministerio):
    """Gera o relatório para um tipo específico"""
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
    
    todas_localidades = set(dados_candidatos.keys()) | set(dados_grupos.keys())
    
    relatorio = []
    
    for localidade in sorted(todas_localidades):
        contadores_candidatos = dados_candidatos.get(localidade, {
            'CANDIDATO(A)': 0,
            'RJM / ENSAIO': 0,
            'ENSAIO': 0,
            'RJM': 0,
            'RJM / CULTO OFICIAL': 0,
            'CULTO OFICIAL': 0
        })
        
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

def executar_modulo_nivel(session, pagina):
    """Executa o módulo de relatório de níveis"""
    tempo_inicio = time.time()
    
    print("\n" + "=" * 80)
    print("📊 MÓDULO 1: RELATÓRIO DE NÍVEIS POR LOCALIDADE")
    print("=" * 80)
    
    try:
        print("📄 Navegando para listagem de alunos...")
        pagina.goto("https://musical.congregacao.org.br/alunos", timeout=30000)
        pagina.wait_for_timeout(2000)
        
        print("📄 Navegando para listagem de grupos...")
        pagina.goto("https://musical.congregacao.org.br/grp_musical", timeout=30000)
        pagina.wait_for_timeout(2000)
        
        cookies_dict = extrair_cookies_playwright(pagina)
        session.cookies.update(cookies_dict)
        
        # MÚSICOS
        print("\n" + "="*60)
        print("🎸 PROCESSANDO DADOS PARA MÚSICOS")
        print("="*60)
        
        dados_candidatos_musicos = obter_candidatos_por_localidade_e_tipo(session, "MÚSICO")
        dados_grupos_musicos = obter_grupos_musicais_por_localidade_e_tipo(session, "MÚSICO")
        
        headers_musicos, relatorio_musicos = gerar_relatorio_por_tipo(
            dados_candidatos_musicos, dados_grupos_musicos, "MÚSICO"
        )
        
        # ORGANISTAS
        print("\n" + "="*60)
        print("🎹 PROCESSANDO DADOS PARA ORGANISTAS")
        print("="*60)
        
        dados_candidatos_organistas = obter_candidatos_por_localidade_e_tipo(session, "ORGANISTA")
        dados_grupos_organistas = obter_grupos_musicais_por_localidade_e_tipo(session, "ORGANISTA")
        
        headers_organistas, relatorio_organistas = gerar_relatorio_por_tipo(
            dados_candidatos_organistas, dados_grupos_organistas, "ORGANISTA"
        )
        
        # EXIBIÇÃO
        print(f"\n🎸 RELATÓRIO DE MÚSICOS POR LOCALIDADE:")
        print("="*150)
        print(f"{'Localidade':<25} {'CAND':<5} {'R/E':<5} {'ENS':<5} {'RJM':<5} {'R/C':<5} {'CULTO':<6} {'R/OF':<5} {'OFIC':<5}")
        print("-"*150)
        
        for linha in relatorio_musicos[:10]:
            print(f"{linha[0]:<25} {linha[1]:<5} {linha[2]:<5} {linha[3]:<5} {linha[4]:<5} {linha[5]:<5} {linha[6]:<6} {linha[7]:<5} {linha[8]:<5}")
        
        if len(relatorio_musicos) > 10:
            print(f"... e mais {len(relatorio_musicos) - 10} localidades")
        
        print(f"\n🎹 RELATÓRIO DE ORGANISTAS POR LOCALIDADE:")
        print("="*150)
        print(f"{'Localidade':<25} {'CAND':<5} {'R/E':<5} {'ENS':<5} {'RJM':<5} {'R/C':<5} {'CULTO':<6} {'R/OF':<5} {'OFIC':<5}")
        print("-"*150)
        
        for linha in relatorio_organistas[:10]:
            print(f"{linha[0]:<25} {linha[1]:<5} {linha[2]:<5} {linha[3]:<5} {linha[4]:<5} {linha[5]:<5} {linha[6]:<6} {linha[7]:<5} {linha[8]:<5}")
        
        if len(relatorio_organistas) > 10:
            print(f"... e mais {len(relatorio_organistas) - 10} localidades")
        
        # ENVIO
        dados_musicos_com_headers = [headers_musicos] + relatorio_musicos
        dados_organistas_com_headers = [headers_organistas] + relatorio_organistas
        
        try:
            print(f"\n📤 Enviando {len(relatorio_musicos)} localidades de MÚSICOS para Google Sheets...")
            body_musicos = {
                "tipo": "relatorio_musicos_localidade",
                "dados": dados_musicos_com_headers,
                "incluir_headers": True
            }
            resposta = requests.post(URL_APPS_SCRIPT_NIVEL, json=body_musicos, timeout=60)
            print(f"✅ Status do envio (músicos): {resposta.status_code}")
        except Exception as e:
            print(f"❌ Erro no envio (músicos): {e}")
        
        try:
            print(f"\n📤 Enviando {len(relatorio_organistas)} localidades de ORGANISTAS para Google Sheets...")
            body_organistas = {
                "tipo": "relatorio_organistas_localidade",
                "dados": dados_organistas_com_headers,
                "incluir_headers": True
            }
            resposta = requests.post(URL_APPS_SCRIPT_NIVEL, json=body_organistas, timeout=60)
            print(f"✅ Status do envio (organistas): {resposta.status_code}")
        except Exception as e:
            print(f"❌ Erro no envio (organistas): {e}")
        
        tempo_total = time.time() - tempo_inicio
        print(f"\n✅ Módulo 1 Concluído!")
        print(f"🎸 Músicos: {len(relatorio_musicos)} localidades processadas")
        print(f"🎹 Organistas: {len(relatorio_organistas)} localidades processadas")
        print(f"⏱️ Tempo: {tempo_total:.1f} segundos")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERRO NO MÓDULO 1: {e}")
        import traceback
        traceback.print_exc()
        return False

# ==================== MÓDULO 2: TURMAS G.E.M ====================

def coletar_turmas_gem(pagina, session):
    """Coleta dados das turmas G.E.M com verificação de matriculados"""
    tempo_inicio = time.time()
    
    print("\n" + "=" * 80)
    print("📚 MÓDULO 2: TURMAS G.E.M COM VERIFICAÇÃO")
    print("=" * 80)
    
    try:
        # Navegar para G.E.M com múltiplas tentativas
        print("🔄 Tentando acessar menu G.E.M...")
        
        # Aguardar carregamento completo da página
        pagina.wait_for_load_state("networkidle", timeout=30000)
        pagina.wait_for_timeout(3000)
        
        # Tentar diferentes estratégias para clicar no G.E.M
        gem_clicado = False
        
        # Estratégia 1: Hover + Click
        try:
            gem_selector = 'span:has-text("G.E.M")'
            pagina.wait_for_selector(gem_selector, timeout=15000)
            gem_element = pagina.locator(gem_selector).first
            gem_element.hover()
            pagina.wait_for_timeout(1500)
            
            if gem_element.is_visible() and gem_element.is_enabled():
                gem_element.click()
                gem_clicado = True
                print("✅ Menu G.E.M acessado (Estratégia 1)")
        except Exception as e:
            print(f"⚠️ Estratégia 1 falhou: {e}")
        
        # Estratégia 2: JavaScript click
        if not gem_clicado:
            try:
                pagina.evaluate("""
                    () => {
                        const gemElement = Array.from(document.querySelectorAll('span'))
                            .find(el => el.textContent.includes('G.E.M'));
                        if (gemElement) gemElement.click();
                    }
                """)
                pagina.wait_for_timeout(2000)
                gem_clicado = True
                print("✅ Menu G.E.M acessado (Estratégia 2)")
            except Exception as e:
                print(f"⚠️ Estratégia 2 falhou: {e}")
        
        if not gem_clicado:
            print("❌ Não foi possível acessar o menu G.E.M após múltiplas tentativas")
            return None
        
        # Navegar para Turmas
        try:
            pagina.wait_for_selector('a[href="turmas"]', timeout=15000)
            pagina.click('a[href="turmas"]')
            print("✅ Navegando para Turmas...")
        except PlaywrightTimeoutError:
            print("❌ Link 'turmas' não encontrado.")
            return None
        
        # Aguardar tabela
        try:
            pagina.wait_for_selector('table#tabela-turmas', timeout=20000)
            print("✅ Tabela de turmas carregada.")
            
            pagina.wait_for_function(
                """
                () => {
                    const tbody = document.querySelector('table#tabela-turmas tbody');
                    return tbody && tbody.querySelectorAll('tr').length > 0;
                }
                """, timeout=20000
            )
            print("✅ Linhas da tabela de turmas carregadas.")
        except PlaywrightTimeoutError:
            print("❌ A tabela de turmas não carregou a tempo.")
            return None
        
        # Configurar exibição
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
            if time.time() - tempo_inicio > TIMEOUT_MODULO:
                print("⏹️ Tempo limite atingido.")
                break
            
            print(f"📄 Processando página {pagina_atual}...")
            linhas = pagina.query_selector_all('table#tabela-turmas tbody tr')
            
            for i, linha in enumerate(linhas):
                if time.time() - tempo_inicio > TIMEOUT_MODULO:
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
                    
                    print(f"🔍 Verificando turma {turma_id} - Badge: {matriculados_badge}")
                    
                    matriculados_reais = obter_matriculados_reais(session, turma_id)
                    
                    if matriculados_reais >= 0:
                        if matriculados_reais == int(matriculados_badge):
                            status_verificacao = "✅ OK"
                        else:
                            status_verificacao = f"⚠️ Diferença (Badge: {matriculados_badge}, Real: {matriculados_reais})"
                    else:
                        status_verificacao = "❌ Erro ao verificar"
                    
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
                        str(matriculados_reais) if matriculados_reais >= 0 else "Erro",
                        status_verificacao
                    ]
                    
                    resultado.append(linha_completa)
                    print(f"   📊 {linha_completa[0]} | {linha_completa[1]} | {linha_completa[2][:50]}... | Badge: {matriculados_badge}, Real: {matriculados_reais}")
                    
                    time.sleep(0.5)
                    
                except Exception as e:
                    print(f"⚠️ Erro ao processar linha {i}: {e}")
                    continue
            
            if parar:
                break
            
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
        
        print(f"📊 Total de turmas processadas: {len(resultado)}")
        
        # Preparar dados para envio
        body = {
            "tipo": "turmas_matriculados",
            "dados": resultado,
            "headers": [
                "Igreja", "Curso", "Turma", "Matriculados_Badge", "Início", 
                "Término", "Dia_Hora", "Status", "Ações", "ID_Turma", 
                "Badge_Duplicado", "Real_Matriculados", "Status_Verificação"
            ],
            "resumo": {
                "total_turmas": len(resultado),
                "turmas_com_diferenca": len([r for r in resultado if "Diferença" in r[-1]]),
                "turmas_ok": len([r for r in resultado if "✅ OK" in r[-1]]),
                "turmas_erro": len([r for r in resultado if "❌ Erro" in r[-1]])
            }
        }
        
        # Enviar dados para Apps Script
        try:
            resposta_post = requests.post(URL_APPS_SCRIPT_EXPANDIDO, json=body, timeout=60)
            print("✅ Dados enviados!")
            print("Status code:", resposta_post.status_code)
            print("Resposta do Apps Script:", resposta_post.text)
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
        
        print("\n📈 RESUMO DA COLETA:")
        print(f"   🎯 Total de turmas: {len(resultado)}")
        print(f"   ✅ Turmas OK: {len([r for r in resultado if '✅ OK' in r[-1]])}")
        print(f"   ⚠️ Com diferenças: {len([r for r in resultado if 'Diferença' in r[-1]])}")
        print(f"   ❌ Com erro: {len([r for r in resultado if '❌ Erro' in r[-1]])}")
        
        tempo_total = time.time() - tempo_inicio
        print(f"✅ Módulo 2 Concluído!")
        print(f"⏱️ Tempo do módulo: {tempo_total:.1f} segundos")
        
        return resultado
        
    except Exception as e:
        print(f"\n❌ ERRO NO MÓDULO 2: {e}")
        import traceback
        traceback.print_exc()
        return None

# ==================== MÓDULO 3: RELATÓRIO DE LOCALIDADES ====================

def processar_relatorio_por_localidade(dados_turmas, session):
    """Processa os dados das turmas e agrupa por localidade"""
    localidades = defaultdict(lambda: {
        'turmas': [],
        'total_matriculados': 0,
        'alunos_unicos': set(),
        'dias_semana': set()
    })
    
    print("📊 Processando dados por localidade...")
    
    for turma in dados_turmas:
        try:
            localidade = turma[0]
            turma_id = turma[9]
            matriculados_badge = int(turma[3]) if turma[3].isdigit() else 0
            dia_hora = turma[6]
            
            print(f"🔍 Obtendo alunos únicos da turma {turma_id} - {localidade}")
            alunos_turma = obter_alunos_unicos(session, turma_id)
            
            dias_turma = extrair_dias_da_semana(dia_hora)
            print(f"   🗓️ Dias extraídos de '{dia_hora}': {dias_turma}")
            
            localidades[localidade]['turmas'].append(turma)
            localidades[localidade]['total_matriculados'] += matriculados_badge
            localidades[localidade]['alunos_unicos'].update(alunos_turma)
            localidades[localidade]['dias_semana'].update(dias_turma)
            
            print(f"   ✅ {localidade}: +{matriculados_badge} matriculados, +{len(alunos_turma)} alunos únicos")
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"⚠️ Erro ao processar turma: {e}")
            continue
    
    return localidades

def gerar_relatorio_formatado(localidades):
    """Gera o relatório no formato solicitado com ordenação cronológica dos dias"""
    relatorio = []
    
    cabecalho = [
        "LOCALIDADE",
        "QUANTIDADE DE TURMAS",
        "SOMA DOS MATRICULADOS",
        "MATRICULADOS SEM REPETIÇÃO",
        "DIAS EM QUE HÁ GEM",
        "DOM", "SEG", "TER", "QUA", "QUI", "SEX", "SÁB"
    ]
    relatorio.append(cabecalho)
    
    ordem_cronologica = ['DOM', 'SEG', 'TER', 'QUA', 'QUI', 'SEX', 'SÁB']
    
    for localidade, dados in localidades.items():
        quantidade_turmas = len(dados['turmas'])
        soma_matriculados = dados['total_matriculados']
        matriculados_unicos = len(dados['alunos_unicos'])
        
        dias_ordenados = [dia for dia in ordem_cronologica if dia in dados['dias_semana']]
        
        if len(dias_ordenados) > 1:
            dias_texto = f"{dias_ordenados[0]}/{dias_ordenados[-1]}"
        elif len(dias_ordenados) == 1:
            dias_texto = dias_ordenados[0]
        else:
            dias_texto = ""
        
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

def executar_relatorio_localidades(dados_turmas_modulo2, session):
    """Executa o módulo de relatório por localidades"""
    tempo_inicio = time.time()
    
    print("\n" + "=" * 80)
    print("🏢 MÓDULO 3: RELATÓRIO POR LOCALIDADES")
    print("=" * 80)
    
    try:
        if not dados_turmas_modulo2:
            print("❌ Nenhum dado recebido do Módulo 2. Abortando.")
            return False
        
        localidades = processar_relatorio_por_localidade(dados_turmas_modulo2, session)
        relatorio_formatado = gerar_relatorio_formatado(localidades)
        
        body = {
            "tipo": "relatorio_localidades",
            "relatorio_formatado": relatorio_formatado,
            "dados_brutos": dados_turmas_modulo2
        }
        
        try:
            resposta_post = requests.post(URL_APPS_SCRIPT_TURMA, json=body, timeout=60)
            print("✅ Dados enviados!")
            print("Status code:", resposta_post.status_code)
            print("Resposta do Apps Script:", resposta_post.text)
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
        
        tempo_total = time.time() - tempo_inicio
        print(f"✅ Módulo 3 Concluído!")
        print(f"⏱️ Tempo do módulo: {tempo_total:.1f} segundos")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERRO NO MÓDULO 3: {e}")
        import traceback
        traceback.print_exc()
        return False

# ==================== EXECUÇÃO PRINCIPAL ====================

def main():
    """Função principal que orquestra todos os módulos de forma independente"""
    tempo_inicio_total = time.time()
    
    print("\n" + "=" * 80)
    print("🚀 INICIANDO SCRIPT UNIFICADO OTIMIZADO - 3 MÓDULOS INDEPENDENTES")
    print("=" * 80)
    print("⏱️  Timeout por módulo: 30 minutos")
    print("💪 Cada módulo é independente - falha de um não afeta os outros")
    print("=" * 80)
    
    resultados = {
        'modulo1': False,
        'modulo2': False,
        'modulo3': False
    }
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # ============================================
        # LOGIN ÚNICO
        # ============================================
        print("\n🔐 Fazendo login...")
        try:
            pagina.goto(URL_INICIAL, timeout=30000)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            
            pagina.wait_for_selector("nav", timeout=20000)
            print("✅ Login realizado com sucesso")
        except Exception as e:
            print(f"❌ Falha no login: {e}")
            navegador.close()
            return
        
        # Criar sessão requests com cookies
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # ============================================
        # MÓDULO 1: Relatório de Níveis
        # ============================================
        print("\n" + "🔹" * 40)
        print("EXECUTANDO MÓDULO 1")
        print("🔹" * 40)
        
        try:
            resultados['modulo1'] = executar_modulo_nivel(session, pagina)
        except Exception as e:
            print(f"\n❌ Falha crítica no Módulo 1: {e}")
            resultados['modulo1'] = False
        
        # Aguardar entre módulos
        print("\n⏸️  Aguardando 5 segundos antes do próximo módulo...")
        time.sleep(5)
        
        # ============================================
        # MÓDULO 2: Turmas G.E.M Expandido
        # ============================================
        print("\n" + "🔹" * 40)
        print("EXECUTANDO MÓDULO 2")
        print("🔹" * 40)
        
        dados_turmas_modulo2 = None
        try:
            # Atualizar cookies antes do módulo 2
            cookies_dict = extrair_cookies_playwright(pagina)
            session.cookies.update(cookies_dict)
            
            dados_turmas_modulo2 = coletar_turmas_gem(pagina, session)
            resultados['modulo2'] = dados_turmas_modulo2 is not None
        except Exception as e:
            print(f"\n❌ Falha crítica no Módulo 2: {e}")
            resultados['modulo2'] = False
        
        # Aguardar entre módulos
        print("\n⏸️  Aguardando 5 segundos antes do próximo módulo...")
        time.sleep(5)
        
        # ============================================
        # MÓDULO 3: Relatório por Localidades
        # ============================================
        print("\n" + "🔹" * 40)
        print("EXECUTANDO MÓDULO 3")
        print("🔹" * 40)
        
        if dados_turmas_modulo2:
            try:
                # Atualizar cookies antes do módulo 3
                cookies_dict = extrair_cookies_playwright(pagina)
                session.cookies.update(cookies_dict)
                
                resultados['modulo3'] = executar_relatorio_localidades(dados_turmas_modulo2, session)
            except Exception as e:
                print(f"\n❌ Falha crítica no Módulo 3: {e}")
                resultados['modulo3'] = False
        else:
            print("⚠️ Módulo 2 não retornou dados. Módulo 3 será pulado.")
            resultados['modulo3'] = False
        
        navegador.close()
    
    # ============================================
    # RELATÓRIO FINAL
    # ============================================
    tempo_total = time.time() - tempo_inicio_total
    
    print("\n" + "=" * 80)
    print("📊 RELATÓRIO FINAL DE EXECUÇÃO")
    print("=" * 80)
    print(f"{'Módulo':<30} {'Status':<20}")
    print("-" * 80)
    print(f"{'1. Relatório de Níveis':<30} {'✅ Sucesso' if resultados['modulo1'] else '❌ Falha':<20}")
    print(f"{'2. Turmas G.E.M':<30} {'✅ Sucesso' if resultados['modulo2'] else '❌ Falha':<20}")
    print(f"{'3. Relatório Localidades':<30} {'✅ Sucesso' if resultados['modulo3'] else '❌ Falha':<20}")
    print("=" * 80)
    print(f"⏱️ Tempo total de execução: {tempo_total/60:.2f} minutos")
    print(f"📊 Módulos bem-sucedidos: {sum(resultados.values())}/3")
    print("=" * 80 + "\n")
    
    # Código de saída baseado nos resultados
    if all(resultados.values()):
        print("🎉 TODOS OS MÓDULOS EXECUTADOS COM SUCESSO!")
        exit(0)
    elif any(resultados.values()):
        print("⚠️ EXECUÇÃO PARCIAL - Alguns módulos falharam")
        exit(1)
    else:
        print("❌ FALHA TOTAL - Nenhum módulo foi executado com sucesso")
        exit(2)

if __name__ == "__main__":
    main()
