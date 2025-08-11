# script_turmas.py
from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
import json

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzx5wJjPYSBEeoNQMc02fxi2j4JqROJ1HKbdM59tMHmb2TD2A2Y6IYDtTpHiZvmLFsGug/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def obter_matriculados_reais(session, turma_id):
    """
    Obtém o número real de matriculados fazendo requisição direta para a API
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
            # Procura pelo texto que indica o total de registros
            match = re.search(r'de um total de (\d+) registros', resp.text)
            if match:
                return int(match.group(1))
            else:
                # Fallback: contar linhas da tabela se não encontrar o padrão
                tbody_match = re.findall(r'<tr[^>]*class="[^"]*(?:odd|even)[^"]*"', resp.text)
                return len(tbody_match) if tbody_match else 0
        
        return -1  # Erro na requisição
        
    except Exception as e:
        print(f"⚠️ Erro ao obter matriculados para turma {turma_id}: {e}")
        return -1

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
        pagina.goto(URL_INICIAL)

        # Login
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

        # Navegar para G.E.M
        try:
            gem_selector = 'span:has-text("G.E.M")'
            pagina.wait_for_selector(gem_selector, timeout=15000)
            gem_element = pagina.locator(gem_selector).first

            gem_element.hover()
            pagina.wait_for_timeout(1000)

            if gem_element.is_visible() and gem_element.is_enabled():
                gem_element.click()
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
            pagina.wait_for_selector('table#tabela-turmas', timeout=10000)
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

        # Configurar exibição para mostrar mais itens (se disponível)
        try:
            select_length = pagina.query_selector('select[name="tabela-turmas_length"]')
            if select_length:
                pagina.select_option('select[name="tabela-turmas_length"]', '100')
                pagina.wait_for_timeout(2000)
                print("✅ Configurado para mostrar 100 itens por página.")
        except Exception:
            print("ℹ️ Seletor de quantidade não encontrado, continuando...")

        # Criar sessão requests com cookies do navegador
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)

        resultado = []
        parar = False
        pagina_atual = 1

        while not parar:
            if time.time() - tempo_inicio > 1800:  # 30 minutos
                print("⏹️ Tempo limite atingido. Encerrando a coleta.")
                break

            print(f"📄 Processando página {pagina_atual}...")

            # Extrair dados de todas as linhas da página atual
            linhas = pagina.query_selector_all('table#tabela-turmas tbody tr')
            
            for i, linha in enumerate(linhas):
                if time.time() - tempo_inicio > 1800:
                    print("⏹️ Tempo limite atingido durante a iteração.")
                    parar = True
                    break

                try:
                    # Extrair dados das colunas
                    colunas = linha.eval_on_selector_all(
                        'td',
                        'tds => tds.map(td => td.innerText.trim().replace(/\\n/g, " ").normalize("NFC"))'
                    )

                    # Extrair ID da turma do input radio
                    radio_input = linha.query_selector('input[type="radio"][name="item[]"]')
                    if not radio_input:
                        continue
                    
                    turma_id = radio_input.get_attribute('value')
                    if not turma_id:
                        continue

                    # Extrair número de matriculados exibido (badge)
                    badge_element = linha.query_selector('span.badge')
                    matriculados_badge = badge_element.inner_text().strip() if badge_element else "0"

                    print(f"🔍 Verificando turma {turma_id} - Badge: {matriculados_badge}")

                    # Obter número real de matriculados via API
                    matriculados_reais = obter_matriculados_reais(session, turma_id)
                    
                    if matriculados_reais >= 0:
                        status_verificacao = "✅ OK" if matriculados_reais == int(matriculados_badge) else f"⚠️ Diferença (Real: {matriculados_reais})"
                    else:
                        status_verificacao = "❌ Erro ao verificar"

                    # Adicionar dados extras à linha
                    linha_dados = colunas.copy()
                    linha_dados.extend([
                        turma_id,
                        matriculados_badge,
                        str(matriculados_reais) if matriculados_reais >= 0 else "Erro",
                        status_verificacao
                    ])

                    resultado.append(linha_dados)
                    print(f"   📊 {linha_dados[1]} - {linha_dados[2]} - {linha_dados[3]} - Badge: {matriculados_badge}, Real: {matriculados_reais}")

                except Exception as e:
                    print(f"⚠️ Erro ao processar linha {i}: {e}")
                    continue

            if parar:
                break

            # Verificar se há próxima página
            try:
                # Procurar pelo botão "Next" do DataTable
                btn_next = pagina.query_selector('a.paginate_button.next:not(.disabled)')
                if btn_next and btn_next.is_enabled():
                    print(f"➡️ Avançando para página {pagina_atual + 1}...")
                    btn_next.click()
                    
                    # Aguardar carregamento da nova página
                    pagina.wait_for_function(
                        """
                        () => {
                            const tbody = document.querySelector('table#tabela-turmas tbody');
                            return tbody && tbody.querySelectorAll('tr').length > 0;
                        }
                        """,
                        timeout=10000
                    )
                    pagina.wait_for_timeout(2000)  # Aguardar estabilização
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
            "tipo": "turmas",
            "dados": resultado,
            "headers": [
                "#", "Igreja", "Curso", "Turma", "Matriculados (Badge)", "Início", 
                "Término", "Dia - Hora", "Status", "Ações", "ID_Turma", 
                "Badge_Matriculados", "Real_Matriculados", "Status_Verificação"
            ]
        }

        # Enviar dados para Apps Script
        try:
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body)
            print("✅ Dados enviados!")
            print("Status code:", resposta_post.status_code)
            print("Resposta do Apps Script:", resposta_post.text)
        except Exception as e:
            print("Erro ao enviar para Apps Script:", e)

        navegador.close()

if __name__ == "__main__":
    main()
