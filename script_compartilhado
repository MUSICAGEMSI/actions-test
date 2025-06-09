# script_alunos.py

from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT_ALUNOS = 'https://script.google.com/macros/s/AKfycbxNypF1RVmUHthcwRom2WHvf-d4MBPTRkVB3H5tSbEhnah241G5bYAPhKN_2viRFxL6ng/exec'  

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def main():
    tempo_inicio = time.time()
    alunos_coletados = []

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
            print("❌ Falha no login.")
            navegador.close()
            return

        # Acessar menu G.E.M → Alunos
        try:
            pagina.hover('span:has-text("G.E.M")')
            pagina.wait_for_timeout(1000)
            pagina.click('a[href="alunos"]')
        except PlaywrightTimeoutError:
            print("❌ Não conseguiu acessar a página de alunos.")
            navegador.close()
            return

        # Interceptar resposta com JSON dos alunos
        json_recebido = None

        def captura_resposta(response):
            nonlocal json_recebido
            if "alunos/listagem" in response.url and response.request.method == "POST":
                try:
                    json_recebido = response.json()
                    print("✅ JSON da lista de alunos interceptado.")
                except Exception as e:
                    print("⚠️ Erro ao processar JSON:", e)

        pagina.on("response", captura_resposta)

        # Ajustar listagem para 10000 entradas
        try:
            pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
            pagina.select_option('select[name="listagem_length"]', '10000')
            pagina.wait_for_timeout(5000)
        except Exception as e:
            print("⚠️ Não foi possível ajustar para 10000 entradas:", e)

        # Aguardar JSON
        pagina.wait_for_timeout(5000)
        if not json_recebido:
            print("❌ Nenhum JSON de alunos capturado.")
            navegador.close()
            return

        dados = json_recebido.get("data", [])
        if not dados:
            print("⚠️ Lista de alunos vazia.")
            navegador.close()
            return

        print(f"🔍 {len(dados)} alunos encontrados.")

        for linha in dados:
            try:
                id_aluno = linha[0]
                nome = linha[1]
                url_editar = f"https://musical.congregacao.org.br/alunos/editar/{id_aluno}"

                # Acessar página de edição do aluno
                pagina_edicao = navegador.new_page()
                pagina_edicao.goto(url_editar)
                pagina_edicao.wait_for_selector("form", timeout=10000)

                # Verificar se há checkbox ou texto indicando compartilhamento
                compartilhado = False
                try:
                    # exemplo: <input type="checkbox" name="compartilhado" checked>
                    checkbox = pagina_edicao.query_selector('input[name="compartilhado"]')
                    if checkbox and checkbox.is_checked():
                        compartilhado = True
                except:
                    pass

                alunos_coletados.append([id_aluno, nome, "SIM" if compartilhado else "NÃO"])
                pagina_edicao.close()
            except Exception as e:
                print(f"⚠️ Falha ao processar aluno {linha}: {e}")

        # Montar payload
        headers = ["id_aluno", "nome", "compartilhado"]
        dados_envio = [headers] + alunos_coletados

        payload = {
            "tipo": "compartilhado",
            "dados": dados
        }

        # Enviar
        try:
            resposta = requests.post(URL_APPS_SCRIPT_ALUNOS, json=payload)
            print("✅ Dados enviados!")
            print("Status code:", resposta.status_code)
            print("Resposta:", resposta.text)
        except Exception as e:
            print("❌ Erro ao enviar dados:", e)

        navegador.close()

if __name__ == "__main__":
    main()
