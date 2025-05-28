# script_det.py
from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzx5wJjPYSBEeoNQMc02fxi2j4JqROJ1HKbdM59tMHmb2TD2A2Y6IYDtTpHiZvmLFsGug/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def buscar_detalhes(id_aula):
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/aulas_abertas',
            'User-Agent': 'Mozilla/5.0'
        }
        url = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{id_aula}"
        resp = requests.get(url, headers=headers, timeout=10)
        return '✅ OK' if 'ATA DA AULA' in resp.text else '❌ Fantasma'
    except Exception:
        return '⚠️ Erro'

def main():
    tempo_inicio = time.time()

    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
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

        try:
            pagina.wait_for_selector('a[href="aulas_abertas"]', timeout=10000)
            pagina.click('a[href="aulas_abertas"]')
        except PlaywrightTimeoutError:
            print("❌ Link 'aulas_abertas' não encontrado.")
            navegador.close()
            return

        try:
            pagina.wait_for_selector('table#listagem', timeout=10000)
            print("✅ Tabela de listagem carregada.")
            pagina.wait_for_function(
                """
                () => {
                    const tbody = document.querySelector('table#listagem tbody');
                    return tbody && tbody.querySelectorAll('tr').length > 0;
                }
                """, timeout=15000
            )
            print("✅ Linhas da tabela carregadas.")
        except PlaywrightTimeoutError:
            print("❌ A tabela ou suas linhas não carregaram a tempo.")
            navegador.close()
            return

        pagina.select_option('select[name="listagem_length"]', '2000')
        pagina.wait_for_timeout(1000)

        resultado = []
        parar = False

        while not parar:
            if time.time() - tempo_inicio > 900:
                print("⏹️ Tempo limite atingido. Encerrando a coleta.")
                break

            botoes = pagina.query_selector_all('button.btn-primary[onclick*="visualizarAula"]')

            for btn in botoes:
                if time.time() - tempo_inicio > 900:
                    print("⏹️ Tempo limite atingido durante a iteração.")
                    parar = True
                    break

                tr = btn.evaluate_handle("btn => btn.closest('tr')")
                colunas = tr.eval_on_selector_all(
                    'td',
                    'tds => tds.map(td => td.innerText.trim().replace(/\\n/g, " ").normalize("NFC"))'
                )

                data_coluna = next((c for c in colunas if re.match(r'\d{2}-\d{2}-\d{4}', c)), '')
                if '2025' not in data_coluna:
                    continue

                onclick = btn.get_attribute('onclick')
                id_match = re.search(r'\d+', onclick).group(0) if onclick else None
                if not id_match:
                    continue

                try:
                    resposta = pagina.evaluate(
                        """
                        async (id) => {
                            const r = await fetch(`https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/${id}`, {
                                headers: { 'X-Requested-With': 'XMLHttpRequest' }
                            });
                            return await r.text();
                        }
                        """,
                        id_match
                    )
                    colunas.append('✅ OK' if 'ATA DA AULA' in resposta else '❌ Fantasma')
                except Exception:
                    colunas.append('⚠️ Erro')

                resultado.append(colunas)

            if parar:
                break

            btn_next = pagina.query_selector('a:has(i.fa.fa-chevron-right):not(.disabled)')
            if btn_next:
                btn_next.click()
                pagina.wait_for_function(
                    """
                    () => {
                        const tbody = document.querySelector('table#listagem tbody');
                        return tbody && tbody.querySelectorAll('tr').length > 0;
                    }
                    """,
                    timeout=10000
                )
            else:
                break

        resultado = [list(linha) for linha in resultado]

        body = {
            "tipo": "detalhes",
            "dados": resultado
        }

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