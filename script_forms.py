import re
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_TURMAS = "https://musical.congregacao.org.br/turmas"
URL_MATRICULADOS = "https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/"
URL_APPS_SCRIPT_TUR = "https://script.google.com/macros/s/AKfycbzZ07sISeDxyHWaRJZFAE2sJxe3L00gmVi2_YiU2puxTQ8HgYUJr27x8pxUiwjxGChjaA/exec"

def extrair_qtd_matriculados(sessao, id_turma):
    try:
        url = f"{URL_MATRICULADOS}{id_turma}"
        resposta = sessao.get(url)
        if resposta.status_code == 200:
            match = re.search(r"de um total de (\d+)", resposta.text)
            if match:
                return int(match.group(1))
    except Exception as e:
        print(f"⚠️ Erro ao extrair quantidade de matriculados para turma {id_turma}: {e}")
    return 0

def main():
    if not EMAIL or not SENHA:
        print("❌ Variáveis de ambiente LOGIN_MUSICAL e SENHA_MUSICAL não estão definidas!")
        return

    tempo_inicio = time.time()
    resultado = []
    json_recebido = None

    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()

        # Captura do JSON da listagem de turmas
        def captura_resposta(response):
            nonlocal json_recebido
            if "turmas/listagem" in response.url and response.request.method == "POST":
                try:
                    json_recebido = response.json()
                    print("✅ JSON de turmas capturado.")
                except Exception as e:
                    print("⚠️ Erro ao ler JSON de turmas:", e)

        pagina.on("response", captura_resposta)

        # Login
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

        # Ir direto para página de turmas
        pagina.goto(URL_TURMAS)

        # Esperar o JSON chegar
        for _ in range(20):
            if json_recebido:
                break
            pagina.wait_for_timeout(500)

        if not json_recebido:
            print("❌ Não foi possível capturar dados das turmas.")
            navegador.close()
            return

        data = json_recebido.get("data", [])
        if not data:
            print("⚠️ Nenhuma turma encontrada.")
            navegador.close()
            return

        # Criar sessão com cookies autenticados
        cookies = pagina.context.cookies()
        sessao = requests.Session()
        for cookie in cookies:
            sessao.cookies.set(cookie["name"], cookie["value"], domain=cookie.get("domain", ""))

        # Processar turmas
        for linha in data:
            try:
                id_turma = linha[0]
                igreja = linha[1]
                curso = linha[2]
                nome_turma = linha[3]

                matriculados = extrair_qtd_matriculados(sessao, id_turma)

                resultado.append([igreja, curso, nome_turma, matriculados])
                print(f"✅ {igreja} | {curso} | {nome_turma} -> {matriculados}")
            except Exception as e:
                print(f"⚠️ Erro ao processar turma {linha}: {e}")

        navegador.close()

    # Enviar resultado para o Google Apps Script
    if resultado:
        headers = ["IGREJA", "CURSO", "TURMA", "MATRICULADOS"]
        dados = [headers] + resultado
        payload = {"tipo": "forms", "dados": dados}

        try:
            resposta_post = requests.post(URL_APPS_SCRIPT_TUR, json=payload)
            print("✅ Dados enviados com sucesso!")
            print("Status code:", resposta_post.status_code)
            print("Resposta:", resposta_post.text)
        except Exception as e:
            print("❌ Erro ao enviar para Google Apps Script:", e)

    print(f"⏱️ Tempo total: {time.time() - tempo_inicio:.2f}s")

if __name__ == "__main__":
    main()
