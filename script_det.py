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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }
        
        # Dados para o POST (expandir para 10000 registros)
        form_data = {
            'draw': '1',
            'start': '0',
            'length': '10000',
            'search[value]': '',
            'search[regex]': 'false',
            'order[0][column]': '0',
            'order[0][dir]': 'asc'
        }
        
        url = "https://musical.congregacao.org.br/grp_musical/listagem"
        resp = session.post(url, headers=headers, data=form_data, timeout=30)
        
        candidatos_por_localidade = defaultdict(int)
        
        if resp.status_code == 200:
            # Parse do JSON retornado
            try:
                data = resp.json()
                if 'data' in data:
                    for record in data['data']:
                        # record é uma lista: [checkbox, nome, comum, instrumento, tipo_membro, status, acoes]
                        if len(record) >= 5:
                            localidade = record[2]  # Coluna "Comum"
                            tipo_membro = record[4]  # Coluna "Tipo Membro"
                            
                            if 'CANDIDATO' in tipo_membro.upper():
                                candidatos_por_localidade[localidade] += 1
                                
            except (ValueError, KeyError):
                # Se não for JSON, tentar parsing HTML
                soup = BeautifulSoup(resp.text, 'html.parser')
                rows = soup.find_all('tr')
                
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 5:
                        localidade = cells[2].get_text(strip=True)
                        tipo_membro = cells[4].get_text(strip=True)
                        
                        if 'CANDIDATO' in tipo_membro.upper():
                            candidatos_por_localidade[localidade] += 1
        
        return dict(candidatos_por_localidade)
        
    except Exception as e:
        print(f"⚠️ Erro ao obter candidatos: {e}")
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
