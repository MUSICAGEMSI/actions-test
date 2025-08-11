# script_relatorio_localidade.py
from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
from bs4 import BeautifulSoup
from collections import defaultdict

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzx5wJjPYSBEeoNQMc02fxi2j4JqROJ1HKbdM59tMHmb2TD2A2Y6IYDtTpHiZvmLFsGug/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def extrair_matriculados_reais(session, turma_id):
    """
    Obtém apenas a quantidade de matriculados reais
    """
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        url = f"https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/{turma_id}"
        resp = session.get(url, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Contar linhas válidas do tbody
            tbody = soup.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                valid_rows = [row for row in rows if len(row.find_all('td')) >= 4]
                return len(valid_rows)
            
            # Fallback: contar botões "Desmatricular"
            return resp.text.count('Desmatricular')
                
        return 0
        
    except Exception:
        return 0

def obter_candidatos_por_localidade(session):
    """
    Obtém contagem de candidatos por localidade através da listagem de alunos
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
        
        # Dados para o POST com estrutura correta para DataTables
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
        
        url = "https://musical.congregacao.org.br/alunos/listagem"
        resp = session.post(url, headers=headers, data=form_data, timeout=30)
        
        print(f"📊 Status da requisição candidatos: {resp.status_code}")
        
        candidatos_por_localidade = defaultdict(int)
        
        if resp.status_code == 200:
            try:
                # Primeiro tentar como JSON
                data = resp.json()
                print(f"📊 JSON recebido com {len(data.get('data', []))} registros")
                
                if 'data' in data and isinstance(data['data'], list):
                    for record in data['data']:
                        if isinstance(record, list) and len(record) >= 6:
                            # Estrutura: [checkbox, nome, comum, ministério, instrumento, nível, ações]
                            localidade_completa = record[2]  # Coluna "Comum Congregação"
                            nivel = record[5]  # Coluna "Nível"
                            
                            # Extrair apenas o nome da localidade (antes do primeiro <span>)
                            soup_local = BeautifulSoup(str(localidade_completa), 'html.parser')
                            localidade_texto = soup_local.get_text(strip=True)
                            
                            # Pegar apenas a parte antes do " | "
                            if ' | ' in localidade_texto:
                                localidade = localidade_texto.split(' | ')[0].strip()
                            else:
                                localidade = localidade_texto.split()[0] if localidade_texto else "Desconhecido"
                            
                            # Verificar se é candidato
                            if 'CANDIDATO' in str(nivel).upper():
                                candidatos_por_localidade[localidade] += 1
                                print(f"📊 Candidato encontrado: {localidade}")
                
                else:
                    print("⚠️ Resposta não é JSON válido, tentando HTML...")
                    # Fallback para HTML
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    rows = soup.find_all('tr', role='row')
                    
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) >= 6:
                            # Coluna 2: localidade, Coluna 5: nível
                            localidade_cell = cells[2]
                            nivel_cell = cells[5]
                            
                            localidade_texto = localidade_cell.get_text(strip=True)
                            nivel_texto = nivel_cell.get_text(strip=True)
                            
                            # Extrair nome da localidade
                            if ' | ' in localidade_texto:
                                localidade = localidade_texto.split(' | ')[0].strip()
                            else:
                                localidade = localidade_texto.split()[0] if localidade_texto else "Desconhecido"
                            
                            if 'CANDIDATO' in nivel_texto.upper():
                                candidatos_por_localidade[localidade] += 1
                                print(f"📊 Candidato HTML encontrado: {localidade}")
                                
            except ValueError as e:
                print(f"⚠️ Erro ao processar resposta: {e}")
                # Último recurso: processar como HTML puro
                soup = BeautifulSoup(resp.text, 'html.parser')
                tbody = soup.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr')
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) >= 6:
                            localidade_cell = cells[2]
                            nivel_cell = cells[5]
                            
                            localidade_texto = localidade_cell.get_text(strip=True)
                            nivel_texto = nivel_cell.get_text(strip=True)
                            
                            # Extrair nome da localidade
                            if ' | ' in localidade_texto:
                                localidade = localidade_texto.split(' | ')[0].strip()
                            else:
                                localidade = localidade_texto.split()[0] if localidade_texto else "Desconhecido"
                            
                            if 'CANDIDATO' in nivel_texto.upper():
                                candidatos_por_localidade[localidade] += 1
                                print(f"📊 Candidato final encontrado: {localidade}")
        
        print(f"📊 Total de candidatos encontrados: {sum(candidatos_por_localidade.values())}")
        for loc, count in candidatos_por_localidade.items():
            print(f"  - {loc}: {count} candidatos")
        
        return dict(candidatos_por_localidade)
        
    except Exception as e:
        print(f"⚠️ Erro ao obter candidatos: {e}")
        import traceback
        traceback.print_exc()
        return {}

def extrair_cookies_playwright(pagina):
    """
    Extrai cookies do Playwright para usar em requests
    """
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def main():
    tempo_inicio = time.time()

    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        pagina.goto(URL_INICIAL)

        # Login
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')

        try:
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado")
        except PlaywrightTimeoutError:
            print("❌ Falha no login")
            navegador.close()
            return

        # Criar sessão requests com cookies
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)

        # Obter candidatos por localidade primeiro
        print("📊 Obtendo candidatos por localidade...")
        candidatos_por_local = obter_candidatos_por_localidade(session)
        
        # Navegar para G.E.M > Turmas
        try:
            gem_selector = 'span:has-text("G.E.M")'
            pagina.wait_for_selector(gem_selector, timeout=15000)
            gem_element = pagina.locator(gem_selector).first
            gem_element.hover()
            pagina.wait_for_timeout(1000)
            gem_element.click()
            
            pagina.wait_for_selector('a[href="turmas"]', timeout=10000)
            pagina.click('a[href="turmas"]')
            print("✅ Navegando para Turmas")
        except PlaywrightTimeoutError:
            print("❌ Erro na navegação")
            navegador.close()
            return

        # Aguardar tabela de turmas
        try:
            pagina.wait_for_selector('table#tabela-turmas', timeout=15000)
            pagina.wait_for_function(
                """
                () => {
                    const tbody = document.querySelector('table#tabela-turmas tbody');
                    return tbody && tbody.querySelectorAll('tr').length > 0;
                }
                """, timeout=15000
            )
            print("✅ Tabela carregada")
        except PlaywrightTimeoutError:
            print("❌ Tabela não carregou")
            navegador.close()
            return

        # Configurar para mostrar mais itens
        try:
            select_length = pagina.query_selector('select[name="tabela-turmas_length"]')
            if select_length:
                pagina.select_option('select[name="tabela-turmas_length"]', '100')
                pagina.wait_for_timeout(2000)
        except Exception:
            pass

        # Estrutura para dados por localidade
        dados_localidade = defaultdict(lambda: {
            'quantidade_turmas': 0,
            'soma_matriculados_badge': 0,
            'soma_matriculados_reais': 0
        })
        
        pagina_atual = 1
        parar = False

        while not parar:
            if time.time() - tempo_inicio > 1200:  # 20 minutos
                print("⏹️ Tempo limite atingido")
                break

            print(f"📄 Página {pagina_atual}")
            linhas = pagina.query_selector_all('table#tabela-turmas tbody tr')
            
            for linha in linhas:
                if time.time() - tempo_inicio > 1200:
                    parar = True
                    break

                try:
                    # Extrair dados básicos
                    colunas_td = linha.query_selector_all('td')
                    if len(colunas_td) < 5:
                        continue
                    
                    igreja = colunas_td[1].inner_text().strip()
                    
                    # Badge de matriculados
                    badge = colunas_td[4].query_selector('span.badge')
                    matriculados_badge = int(badge.inner_text().strip()) if badge else 0
                    
                    # ID da turma
                    radio_input = linha.query_selector('input[type="radio"][name="item[]"]')
                    if not radio_input:
                        continue
                    
                    turma_id = radio_input.get_attribute('value')
                    if not turma_id:
                        continue

                    # Obter matriculados reais
                    matriculados_reais = extrair_matriculados_reais(session, turma_id)

                    # Acumular dados por localidade
                    dados_localidade[igreja]['quantidade_turmas'] += 1
                    dados_localidade[igreja]['soma_matriculados_badge'] += matriculados_badge
                    dados_localidade[igreja]['soma_matriculados_reais'] += matriculados_reais

                    print(f"📊 {igreja}: Turma {turma_id} - Badge: {matriculados_badge}, Real: {matriculados_reais}")
                    time.sleep(0.3)  # Pausa pequena

                except Exception as e:
                    print(f"⚠️ Erro na linha: {e}")
                    continue

            if parar:
                break

            # Próxima página
            try:
                btn_next = pagina.query_selector('a.paginate_button.next:not(.disabled)')
                if btn_next and btn_next.is_enabled():
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
                    pagina.wait_for_timeout(2000)
                    pagina_atual += 1
                else:
                    break
                    
            except Exception:
                break

        # Montar relatório final - APENAS 5 COLUNAS
        relatorio_final = []
        for localidade, dados in dados_localidade.items():
            candidatos = candidatos_por_local.get(localidade, 0)
            
            linha = [
                localidade,
                dados['quantidade_turmas'],
                dados['soma_matriculados_badge'],
                dados['soma_matriculados_reais'],
                candidatos
            ]
            relatorio_final.append(linha)

        # Ordenar por soma de matriculados reais (decrescente)
        relatorio_final.sort(key=lambda x: x[3], reverse=True)

        # Mostrar resultado
        print(f"\n📊 RELATÓRIO POR LOCALIDADE:")
        print("="*100)
        print(f"{'Localidade':<35} {'Turmas':<7} {'Badge':<7} {'Reais':<7} {'Candidatos':<10}")
        print("-"*100)
        for linha in relatorio_final:
            print(f"{linha[0]:<35} {linha[1]:<7} {linha[2]:<7} {linha[3]:<7} {linha[4]:<10}")

        # Preparar dados para envio
        body = {
            "tipo": "relatorio_localidade_simples",
            "dados": relatorio_final,
            "headers": [
                "Localidade",
                "Quantidade_Turmas", 
                "Soma_Matriculados_Badge",
                "Soma_Matriculados_Reais",
                "Quantidade_Candidatos"
            ]
        }

        # Enviar para Google Sheets
        try:
            print(f"\n📤 Enviando {len(relatorio_final)} localidades...")
            resposta = requests.post(URL_APPS_SCRIPT, json=body, timeout=60)
            print(f"✅ Status: {resposta.status_code}")
            print(f"📝 Resposta: {resposta.text}")
        except Exception as e:
            print(f"❌ Erro no envio: {e}")

        navegador.close()
        print(f"\n🎯 Concluído! {len(relatorio_final)} localidades processadas.")

if __name__ == "__main__":
    main()
