from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT_TUR = 'https://script.google.com/macros/s/AKfycbxkOzQseqk6Y2jeITGl0RnWoPtsiJ6xvXs7zXhN_1D7JBiYRthSUcksuME9H39rELZg/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def main():
    tempo_inicio = time.time()
    resultado = []
    ids_coletados = set()
    json_recebido = None  # vai guardar o JSON da resposta

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

        # Interceptar a resposta JSON da requisição que carrega os dados
        def captura_resposta(response):
            nonlocal json_recebido
            if "listagem" in response.url and response.request.method == "POST":
                try:
                    json_recebido = response.json()
                    print("✅ JSON da resposta interceptado com sucesso.")
                except Exception as e:
                    print("⚠️ Erro ao tentar ler JSON da resposta:", e)

        pagina.on("response", captura_resposta)

        # Abrir menu G.E.M
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

        # Acessar página "Turmas"
        try:
            pagina.wait_for_selector('a[href="turmas"]', timeout=10000)
            pagina.click('a[href="turmas"]')
        except PlaywrightTimeoutError:
            print("❌ Link 'turmas' não encontrado.")
            navegador.close()
            return

        # Esperar a tabela aparecer
        try:
            pagina.wait_for_selector('table#listagem', timeout=15000)
            pagina.wait_for_timeout(1000)
        except PlaywrightTimeoutError:
            print("❌ Tabela de listagem não carregada a tempo.")
            navegador.close()
            return

        # Tentar ajustar para mostrar 2000 entradas
        try:
            seletor_paginacao = pagina.locator('select[name="listagem_length"]')
            seletor_paginacao.wait_for(timeout=10000)
            if seletor_paginacao.is_visible():
                seletor_paginacao.select_option('2000')
                print("✅ Ajustado para mostrar 2000 entradas.")
                pagina.wait_for_timeout(3000)
            else:
                print("⚠️ Seletor de paginação visível mas não interativo.")
        except Exception as e:
            print("⚠️ Não foi possível ajustar paginação para 2000:", e)

        # Esperar o JSON ser interceptado
        tentativas = 20
        for _ in range(tentativas):
            if json_recebido:
                break
            pagina.wait_for_timeout(500)  # aguarda meio segundo por tentativa

        # Verificar se conseguimos o JSON com os dados
        if not json_recebido:
            print("❌ Não foi possível capturar os dados JSON da tabela.")
            navegador.close()
            return

        # Processar os dados do JSON
        data = json_recebido.get("data", [])
        if not data:
            print("⚠️ Dados JSON da tabela estão vazios.")
            navegador.close()
            return

        for linha in data:
            id_val = linha[0]
            if id_val not in ids_coletados:
                ids_coletados.add(id_val)
                dados_linha = linha[:9]
                resultado.append(dados_linha)

        if not resultado:
            print("⚠️ Nenhum dado coletado após processar JSON.")
            navegador.close()
            return

        # Montar payload
        headers = ["id", "bairro", "curso", "nome_turma", "vagas", "inicio", "termino", "horario", "status"]
        dados = [headers] + resultado

        payload = {
            "tipo": "turmas",
            "dados": dados
        }

        try:
            resposta_post = requests.post(URL_APPS_SCRIPT_TUR, json=payload)
            print("✅ Dados enviados!")
            print("Status code:", resposta_post.status_code)
            print("Resposta:", resposta_post.text)
        except Exception as e:
            print("❌ Erro ao enviar dados:", e)

        navegador.close()

if __name__ == "__main__":
    main()
