# script_det.py
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

# Configurações de login
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_LISTAGEM_ALUNOS = "https://musical.congregacao.org.br/alunos/listagem"

# URLs dos Google Apps Scripts
URL_APPS_SCRIPT_TURMAS = 'https://script.google.com/macros/s/AKfycbzw2TFfN-os4e3DwRUYXozQ2Uv5d978Xf0t85Mcwqcfq1oxqSCNyFMqmzj0Vowe4Juh/exec'
URL_APPS_SCRIPT_CANDIDATOS = 'https://script.google.com/macros/s/AKfycbwOqF3dKFIu2L52IIGfd9OeIN4Wj0tT1eXDV6G619cG7l1aSdGNISIzVa5aBaVJrFeO_w/exec'
URL_APPS_SCRIPT_LOCALIDADE = 'https://script.google.com/macros/s/AKfycbxs2_eUlbTh-a6cdFDOS7mu45Up1j5QovnGnvw2VpQwKnHCufqvHhZGRQPy-L5vGLnK/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para usar em requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def obter_matriculados_reais(session, turma_id):
    """Obtém o número real de matriculados contando as linhas da tabela"""
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
                    
                # Fallback: tentar "Mostrando de X até Y"
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

def extrair_alunos_matriculados(session, turma_id):
    """Extrai a lista de alunos matriculados na turma"""
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
            alunos = []
            
            tbody = soup.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        nome_completo = cells[0].get_text(strip=True)
                        if nome_completo and '-' in nome_completo:
                            nome_limpo = nome_completo.split('-')[0].strip()
                            if nome_limpo:
                                alunos.append(nome_limpo)
            
            # Fallback: usar regex
            if not alunos:
                aluno_patterns = re.findall(r'([A-ZÁÉÍÓÚÀÂÊÎÔÛÃÕÇ\s]+) - [A-Z/]+/\d+', resp.text)
                alunos = [nome.strip() for nome in aluno_patterns if nome.strip()]
            
            return len(alunos), alunos
            
        return 0, []
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair alunos da turma {turma_id}: {e}")
        return -1, []

def extrair_localidade_limpa(localidade_texto):
    """Extrai apenas o nome da localidade, removendo HTML e informações extras"""
    localidade_texto = localidade_texto.replace('<\\/span>', '').replace('<span>', '').replace('</span>', '')
    
    if ' | ' in localidade_texto:
        localidade = localidade_texto.split(' | ')[0].strip()
    else:
        localidade = localidade_texto.strip()
    
    return localidade

def obter_candidatos_por_localidade(session):
    """Obtém candidatos por localidade da listagem de alunos"""
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
        
        print("📊 Obtendo candidatos da listagem de alunos...")
        resp = session.post(URL_LISTAGEM_ALUNOS, headers=headers, data=form_data, timeout=60)
        
        print(f"📊 Status da requisição: {resp.status_code}")
        
        niveis_validos = {
            'CANDIDATO(A)': 0,
            'ENSAIO': 0,
            'RJM / ENSAIO': 0,
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
                            nivel = record[5]
                            
                            localidade = extrair_localidade_limpa(localidade_completa)
                            
                            termos_ignorados = [
                                'ORGANISTA',
                                'OFICIALIZADO(A)',
                                'RJM / OFICIALIZADO(A)', 
                                'RJM/OFICIALIZADO(A)',
                                'COMPARTILHADO',
                                'COMPARTILHADA'
                            ]
                            
                            if any(termo in nivel.upper() for termo in termos_ignorados):
                                continue
                            
                            if nivel in niveis_validos:
                                dados_por_localidade[localidade][nivel] += 1
                                print(f"📊 {localidade}: {nivel} (+1)")
                
                print(f"📊 Total de localidades processadas: {len(dados_por_localidade)}")
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

def executar_script_turmas(pagina, session):
    """Executa a coleta de dados das turmas"""
    print("\n" + "="*60)
    print("🎯 INICIANDO COLETA DE TURMAS")
    print("="*60)
    
    try:
        # Navegar para G.E.M
        gem_selector = 'span:has-text("G.E.M")'
        pagina.wait_for_selector(gem_selector, timeout=15000)
        gem_element = pagina.locator(gem_selector).first

        gem_element.hover()
        pagina.wait_for_timeout(1000)

        if gem_element.is_visible() and gem_element.is_enabled():
            gem_element.click()
        else:
            print("❌ Elemento G.E.M não estava clicável.")
            return False

        # Navegar para Turmas
        pagina.wait_for_selector('a[href="turmas"]', timeout=10000)
        pagina.click('a[href="turmas"]')
        print("✅ Navegando para Turmas...")

        # Aguardar carregamento da tabela de turmas
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
            print(f"📄 Processando página {pagina_atual}...")

            linhas = pagina.query_selector_all('table#tabela-turmas tbody tr')
            
            for i, linha in enumerate(linhas):
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

                    time.sleep(0.5)

                except Exception as e:
                    print(f"⚠️ Erro ao processar linha {i}: {e}")
                    continue

            # Paginação
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

        # Enviar dados
        try:
            resposta_post = requests.post(URL_APPS_SCRIPT_TURMAS, json=body, timeout=60)
            print("✅ Dados de turmas enviados!")
            print("Status code:", resposta_post.status_code)
            print("Resposta:", resposta_post.text)
        except Exception as e:
            print(f"❌ Erro ao enviar dados de turmas: {e}")

        # Mostrar resumo
        print("\n📈 RESUMO DA COLETA DE TURMAS:")
        print(f"   🎯 Total de turmas: {len(resultado)}")
        print(f"   ✅ Turmas OK: {len([r for r in resultado if '✅ OK' in r[-1]])}")
        print(f"   ⚠️ Com diferenças: {len([r for r in resultado if 'Diferença' in r[-1]])}")
        print(f"   ❌ Com erro: {len([r for r in resultado if '❌ Erro' in r[-1]])}")

        return True

    except Exception as e:
        print(f"❌ Erro na execução do script de turmas: {e}")
        return False

def executar_script_candidatos(pagina, session):
    """Executa a coleta de candidatos por localidade"""
    print("\n" + "="*60)
    print("🎯 INICIANDO COLETA DE CANDIDATOS POR LOCALIDADE")
    print("="*60)
    
    try:
        # Navegar para listagem de alunos
        print("📄 Navegando para listagem de alunos...")
        pagina.goto("https://musical.congregacao.org.br/alunos")
        pagina.wait_for_timeout(1000)

        # Atualizar cookies
        cookies_dict = extrair_cookies_playwright(pagina)
        session.cookies.update(cookies_dict)

        # Obter dados de candidatos
        print("📊 Coletando dados de candidatos...")
        dados_por_localidade = obter_candidatos_por_localidade(session)
        
        if not dados_por_localidade:
            print("❌ Nenhum dado foi coletado")
            return False

        relatorio_final = []
        
        headers = [
            "Localidade",
            "CANDIDATO(A)",
            "ENSAIO", 
            "RJM / ENSAIO",
            "RJM",
            "RJM / CULTO OFICIAL",
            "CULTO OFICIAL"
        ]
        
        for localidade in sorted(dados_por_localidade.keys()):
            contadores = dados_por_localidade[localidade]
            linha = [
                localidade,
                contadores['CANDIDATO(A)'],
                contadores['ENSAIO'],
                contadores['RJM / ENSAIO'],
                contadores['RJM'],
                contadores['RJM / CULTO OFICIAL'],
                contadores['CULTO OFICIAL']
            ]
            relatorio_final.append(linha)

        relatorio_final.sort(key=lambda x: x[1], reverse=True)

        print(f"\n📊 RELATÓRIO DE CANDIDATOS POR LOCALIDADE:")
        print("="*120)
        print(f"{'Localidade':<25} {'CAND':<5} {'ENS':<5} {'R/E':<5} {'RJM':<5} {'R/C':<5} {'CULTO':<5}")
        print("-"*120)
        
        for linha in relatorio_final:
            print(f"{linha[0]:<25} {linha[1]:<5} {linha[2]:<5} {linha[3]:<5} {linha[4]:<5} {linha[5]:<5} {linha[6]:<5}")

        totais = [sum(linha[i] for linha in relatorio_final) for i in range(1, len(headers))]
        print("-"*120)
        print(f"{'TOTAL':<25} {totais[0]:<5} {totais[1]:<5} {totais[2]:<5} {totais[3]:<5} {totais[4]:<5} {totais[5]:<5}")

        dados_com_headers = [headers] + relatorio_final
        
        body = {
            "tipo": "relatorio_candidatos_localidade_simplificado",
            "dados": dados_com_headers,
            "incluir_headers": True
        }

        # Enviar dados
        try:
            print(f"\n📤 Enviando {len(relatorio_final)} localidades para Google Sheets...")
            resposta = requests.post(URL_APPS_SCRIPT_CANDIDATOS, json=body, timeout=60)
            print(f"✅ Status do envio: {resposta.status_code}")
            print(f"📝 Resposta: {resposta.text}")
        except Exception as e:
            print(f"❌ Erro no envio: {e}")

        print(f"\n🎯 Candidatos processados! {len(relatorio_final)} localidades.")
        print(f"📊 Total geral de registros: {sum(totais)}")

        return True

    except Exception as e:
        print(f"❌ Erro na execução do script de candidatos: {e}")
        return False

def executar_script_localidade(pagina, session):
    """Executa a coleta de turmas por localidade"""
    print("\n" + "="*60)
    print("🎯 INICIANDO COLETA DE TURMAS POR LOCALIDADE")
    print("="*60)
    
    try:
        # Navegar para turmas novamente
        pagina.goto("https://musical.congregacao.org.br/gem/turmas")
        pagina.wait_for_timeout(2000)

        pagina.wait_for_selector('table#tabela-turmas', timeout=15000)
        print("✅ Tabela de turmas carregada para análise por localidade.")

        # Configurar exibição
        try:
            select_length = pagina.query_selector('select[name="tabela-turmas_length"]')
            if select_length:
                pagina.select_option('select[name="tabela-turmas_length"]', '100')
                pagina.wait_for_timeout(2000)
        except Exception:
            pass

        # Estruturas para análise por localidade
        dados_localidade = defaultdict(lambda: {
            'turmas': [],
            'total_matriculados': 0,
            'alunos_unicos': set(),
            'detalhes_turmas': []
        })

        resultado_detalhado = []
        parar = False
        pagina_atual = 1

        while not parar:
            print(f"📄 Processando página {pagina_atual} para análise por localidade...")

            linhas = pagina.query_selector_all('table#tabela-turmas tbody tr')
            
            for i, linha in enumerate(linhas):
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

                    igreja = dados_linha[0] if len(dados_linha) > 0 else ""
                    curso = dados_linha[1] if len(dados_linha) > 1 else ""
                    turma = dados_linha[2] if len(dados_linha) > 2 else ""
                    matriculados_badge = dados_linha[3] if len(dados_linha) > 3 else "0"

                    print(f"🔍 Processando {igreja} - {curso} - Turma {turma_id}")

                    matriculados_reais, lista_alunos = extrair_alunos_matriculados(session, turma_id)
                    
                    if matriculados_reais >= 0:
                        dados_localidade[igreja]['turmas'].append(turma_id)
                        dados_localidade[igreja]['total_matriculados'] += matriculados_reais
                        dados_localidade[igreja]['alunos_unicos'].update(lista_alunos)
                        
                        detalhes_turma = {
                            'turma_id': turma_id,
                            'curso': curso,
                            'turma': turma,
                            'matriculados_badge': int(matriculados_badge),
                            'matriculados_reais': matriculados_reais,
                            'alunos': lista_alunos
                        }
                        dados_localidade[igreja]['detalhes_turmas'].append(detalhes_turma)
                        
                        status = "✅ OK" if matriculados_reais == int(matriculados_badge) else f"⚠️ Diferença"
                    else:
                        status = "❌ Erro"

                    linha_completa = [
                        igreja, curso, turma, matriculados_badge,
                        dados_linha[4] if len(dados_linha) > 4 else "",
                        dados_linha[5] if len(dados_linha) > 5 else "",
                        dados_linha[6] if len(dados_linha) > 6 else "",
                        dados_linha[7] if len(dados_linha) > 7 else "",
                        turma_id,
                        str(matriculados_reais) if matriculados_reais >= 0 else "Erro",
                        status
                    ]

                    resultado_detalhado.append(linha_completa)
                    print(f"   📊 Badge: {matriculados_badge}, Real: {matriculados_reais}, Únicos acumulados: {len(dados_localidade[igreja]['alunos_unicos'])}")

                    time.sleep(0.5)

                except Exception as e:
                    print(f"⚠️ Erro ao processar linha {i}: {e}")
                    continue

            # Paginação
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

        # Processar dados por localidade - UMA LINHA POR LOCALIDADE
        relatorio_localidade = []
        for igreja, dados in dados_localidade.items():
            linha_localidade = [
                igreja,                                          
                len(dados['turmas']),                           
                dados['total_matriculados'],                    
                len(dados['alunos_unicos']),                   
                dados['total_matriculados'] - len(dados['alunos_unicos']),  
                f"{((dados['total_matriculados'] - len(dados['alunos_unicos'])) / dados['total_matriculados'] * 100):.1f}%" if dados['total_matriculados'] > 0 else "0%",  
                f"{dados['total_matriculados']/len(dados['turmas']):.1f}",  
                "; ".join(dados['turmas']),                    
                "; ".join(sorted(list(dados['alunos_unicos'])))  
            ]
            relatorio_localidade.append(linha_localidade)

        # Ordenar por quantidade de matriculados únicos (decrescente)
        relatorio_localidade.sort(key=lambda x: x[3], reverse=True)

        print(f"\n📊 RELATÓRIO POR LOCALIDADE:")
        print("="*120)
        headers = ["Localidade", "Turmas", "Total", "Únicos", "Sobrep.", "%Sobrep.", "Média", "IDs Turmas", "Alunos"]
        print(f"{headers[0]:<30} {headers[1]:<6} {headers[2]:<6} {headers[3]:<6} {headers[4]:<7} {headers[5]:<8} {headers[6]:<6}")
        print("-" * 120)
        for linha in relatorio_localidade:
            print(f"{linha[0]:<30} {linha[1]:<6} {linha[2]:<6} {linha[3]:<6} {linha[4]:<7} {linha[5]:<8} {linha[6]:<6}")

        # Preparar dados para envio
        body = {
            "tipo": "relatorio_localidade_tabular",
            "dados": relatorio_localidade,
            "headers": [
                "Localidade", 
                "Qty_Turmas", 
                "Total_Matriculados", 
                "Matriculados_Unicos", 
                "Sobreposicoes",
                "Percent_Sobreposicao",
                "Media_Por_Turma",
                "IDs_Turmas",
                "Lista_Alunos_Unicos"
            ],
            "resumo": {
                "total_localidades": len(relatorio_localidade),
                "total_turmas": sum(linha[1] for linha in relatorio_localidade),
                "total_matriculados": sum(linha[2] for linha in relatorio_localidade),
                "total_alunos_unicos": sum(linha[3] for linha in relatorio_localidade),
                "total_sobreposicoes": sum(linha[4] for linha in relatorio_localidade)
            },
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_registros": len(relatorio_localidade)
        }

        # Enviar dados com retry
        max_tentativas = 3
        for tentativa in range(max_tentativas):
            try:
                print(f"\n📤 Enviando dados de localidade (tentativa {tentativa + 1}/{max_tentativas})...")
                
                headers_envio = {
                    'Content-Type': 'application/json',
                    'User-Agent': 'Python-Script/1.0'
                }
                
                resposta_post = requests.post(
                    URL_APPS_SCRIPT_LOCALIDADE, 
                    json=body, 
                    headers=headers_envio,
                    timeout=180
                )
                
                print(f"✅ Resposta recebida!")
                print(f"   📊 Status code: {resposta_post.status_code}")
                print(f"   💬 Resposta: {resposta_post.text}")
                
                if resposta_post.status_code == 200:
                    print("✅ Dados de localidade enviados com sucesso!")
                    break
                    
            except Exception as e:
                print(f"❌ Erro na tentativa {tentativa + 1}: {e}")
            
            if tentativa < max_tentativas - 1:
                time.sleep(5)
        else:
            print("❌ Falha em todas as tentativas de envio de localidade!")

        # Resumo final
        resumo = body["resumo"]
        print(f"\n🎯 RESUMO FINAL LOCALIDADE:")
        print(f"   🏛️  Localidades: {resumo['total_localidades']}")
        print(f"   📚 Total de turmas: {resumo['total_turmas']}")
        print(f"   👥 Total matriculados: {resumo['total_matriculados']}")
        print(f"   🎯 Total alunos únicos: {resumo['total_alunos_unicos']}")
        print(f"   🔄 Total sobreposições: {resumo['total_sobreposicoes']}")

        return True

    except Exception as e:
        print(f"❌ Erro na execução do script de localidade: {e}")
        return False

def main():
    """Função principal que executa todos os scripts sequencialmente"""
    tempo_inicio = time.time()
    
    print("🚀 INICIANDO COLETA UNIFICADA DE DADOS")
    print("="*80)

    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        # Configurações do navegador
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
        # LOGIN ÚNICO
        print("🔐 Fazendo login...")
        pagina.goto(URL_INICIAL)
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')

        try:
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado com sucesso!")
        except PlaywrightTimeoutError:
            print("❌ Falha no login. Verifique suas credenciais.")
            navegador.close()
            return

        # Criar sessão requests com cookies do navegador
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)

        # Variáveis de controle para resultados
        resultados = {
            'turmas': False,
            'candidatos': False,
            'localidade': False
        }

        # SCRIPT 1: TURMAS
        print("\n🎯 EXECUTANDO SCRIPT 1/3...")
        try:
            resultados['turmas'] = executar_script_turmas(pagina, session)
            if resultados['turmas']:
                print("✅ Script de turmas concluído com sucesso!")
            else:
                print("⚠️ Script de turmas falhou!")
        except Exception as e:
            print(f"❌ Erro crítico no script de turmas: {e}")
            resultados['turmas'] = False

        # Atualizar cookies entre scripts
        cookies_dict = extrair_cookies_playwright(pagina)
        session.cookies.update(cookies_dict)

        # SCRIPT 2: CANDIDATOS
        print("\n🎯 EXECUTANDO SCRIPT 2/3...")
        try:
            resultados['candidatos'] = executar_script_candidatos(pagina, session)
            if resultados['candidatos']:
                print("✅ Script de candidatos concluído com sucesso!")
            else:
                print("⚠️ Script de candidatos falhou!")
        except Exception as e:
            print(f"❌ Erro crítico no script de candidatos: {e}")
            resultados['candidatos'] = False

        # Atualizar cookies entre scripts
        cookies_dict = extrair_cookies_playwright(pagina)
        session.cookies.update(cookies_dict)

        # SCRIPT 3: LOCALIDADE
        print("\n🎯 EXECUTANDO SCRIPT 3/3...")
        try:
            resultados['localidade'] = executar_script_localidade(pagina, session)
            if resultados['localidade']:
                print("✅ Script de localidade concluído com sucesso!")
            else:
                print("⚠️ Script de localidade falhou!")
        except Exception as e:
            print(f"❌ Erro crítico no script de localidade: {e}")
            resultados['localidade'] = False

        navegador.close()

        # RELATÓRIO FINAL
        tempo_total = time.time() - tempo_inicio
        print("\n" + "="*80)
        print("🎯 RELATÓRIO FINAL DA EXECUÇÃO UNIFICADA")
        print("="*80)
        print(f"⏱️  Tempo total de execução: {tempo_total:.1f} segundos ({tempo_total/60:.1f} minutos)")
        print(f"🎯 Scripts executados:")
        print(f"   📊 Turmas: {'✅ SUCESSO' if resultados['turmas'] else '❌ FALHOU'}")
        print(f"   👥 Candidatos: {'✅ SUCESSO' if resultados['candidatos'] else '❌ FALHOU'}")
        print(f"   🏛️  Localidade: {'✅ SUCESSO' if resultados['localidade'] else '❌ FALHOU'}")
        
        sucessos = sum(resultados.values())
        print(f"\n📈 Taxa de sucesso: {sucessos}/3 ({(sucessos/3*100):.1f}%)")
        
        if sucessos == 3:
            print("🎉 TODOS OS SCRIPTS EXECUTADOS COM SUCESSO!")
        elif sucessos > 0:
            print("⚠️ EXECUÇÃO PARCIAL - Verifique os logs acima")
        else:
            print("❌ TODOS OS SCRIPTS FALHARAM - Verifique configurações")

        print("="*80)
        print("🏁 EXECUÇÃO CONCLUÍDA")

if __name__ == "__main__":
    main()
