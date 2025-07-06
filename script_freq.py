from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time

EMAIL = os.getenv("LOGIN_MUSICAL")
SENHA = os.getenv("SENHA_MUSICAL")
URL_BASE = "https://musical.congregacao.org.br"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzJozNuKoYBLe1IWCUtRGQ1q-sWcXV62NDXVpBMzYM_T0aUgJ1Vzq92l6n9sGJmhEnbwA/exec'

def avaliar_html(html):
    tem_check = "fa-check" in html
    tem_remove = "fa-remove" in html
    if tem_check:
        return "✅ OK"
    elif tem_remove:
        return "❌ Fantasma"
    else:
        return "⚠️ Indefinido"

def main():
    tempo_inicio = time.time()
    resultado = []

    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        pagina.goto(URL_BASE)

        # Login
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')
        pagina.wait_for_selector("nav", timeout=15000)
        print("✅ Login realizado com sucesso!")

        # Navegar até a tabela
        pagina.click('text=G.E.M')
        pagina.wait_for_timeout(1000)
        pagina.click('a[href="aulas_abertas"]')
        pagina.wait_for_selector('table#listagem', timeout=10000)
        print("✅ Tabela de listagem carregada.")

        pagina.select_option('select[name="listagem_length"]', '2000')
        pagina.wait_for_timeout(1000)

        linhas = pagina.query_selector_all('table#listagem tbody tr')

        # Coletar cookies da sessão para usar no requests
        cookies = pagina.context.cookies()
        sessao = requests.Session()
        for cookie in cookies:
            sessao.cookies.set(cookie['name'], cookie['value'])

        headers = {
            "Referer": f"{URL_BASE}/aulas_abertas",
            "User-Agent": "Mozilla/5.0",
        }

        for linha in linhas:
            colunas = linha.query_selector_all('td')
            textos = [c.inner_text().strip().replace('\n', ' ') for c in colunas]

            if not any("2025" in txt for txt in textos):
                continue

            btn = linha.query_selector('button.btn-primary[onclick*="visualizarAula"]')
            onclick = btn.get_attribute('onclick') if btn else ''
            ids = re.findall(r'\d+', onclick)
            if len(ids) < 2:
                continue

            id_aula, id_turma = ids[0], ids[1]
            url_frequencia = f"{URL_BASE}/aulas_abertas/frequencia/{id_aula}/{id_turma}"

            try:
                resposta = sessao.get(url_frequencia, headers=headers, timeout=15)
                status = avaliar_html(resposta.text)
                textos.append(status)
            except Exception as e:
                print(f"Erro ao acessar frequência {id_aula}/{id_turma}: {e}")
                textos.append("⚠️ Erro")

            resultado.append(textos)

    body = {
        "tipo": "frequencias",
        "dados": resultado
    }

    try:
        resposta_post = requests.post(URL_APPS_SCRIPT, json=body)
        print("✅ Dados enviados ao Apps Script!")
        print("Status code:", resposta_post.status_code)
        print("Resposta:", resposta_post.text)
    except Exception as e:
        print("❌ Falha ao enviar dados:", e)

if __name__ == "__main__":
    main()
