import re
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time

# Não usar load_dotenv porque vamos usar variáveis do ambiente diretamente
# from dotenv import load_dotenv
# load_dotenv(dotenv_path="credencial.env")

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_MATRICULADOS = "https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/"
URL_APPS_SCRIPT_TUR = 'https://script.google.com/macros/s/AKfycbzZ07sISeDxyHWaRJZFAE2sJxe3L00gmVi2_YiU2puxTQ8HgYUJr27x8pxUiwjxGChjaA/exec'

def extrair_qtd_matriculados(sessao, id_turma):
    try:
        url = f"{URL_MATRICULADOS}{id_turma}"
        resposta = sessao.get(url)
        if resposta.status_code == 200:
            match = re.search(r"de um total de (\d+) registro", resposta.text)
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
    ids_coletados = set()
    json_recebido = None

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

        def captura_resposta(response):
            nonlocal json_recebido
            if "listagem" in response.url and response.request.method == "POST":
                try:
                    json_recebido = response.json()
                    print("✅ JSON da resposta interceptado com sucesso.")
                except Exception as e:
                    print("⚠️ Erro ao tentar ler JSON da resposta:", e)

        pagina.on("response", captura_resposta)

        # Acessar "G.E.M" > Turmas
        try:
            gem_selector = 'span:has-text("G.E.M")'
            pagina.wait_for_selector(gem_selector, timeout=15000)
            gem_element = pagina.locator(gem_selector).first
            gem_element.hover()
            pagina.wait_for_timeout(1000)
            gem_element.click()
        except:
            print("❌ Menu G.E.M não clicável.")
            navegador.close()
            return

        try:
            pagina.wait_for_selector('a[href="turmas"]', timeout=10000)
            pagina.click('a[href="turmas"]')
        except:
            print("❌ Link 'turmas' não encontrado.")
            navegador.close()
            return

        try:
            pagina.wait_for_selector('table#listagem', timeout=15000)
            pagina.wait_for_timeout(1000)
        except:
            print("❌ Tabela de listagem não carregada.")
            navegador.close()
            return

        try:
            seletor_paginacao = pagina.locator('select[name="listagem_length"]')
            seletor_paginacao.select_option('2000')
            print("✅ Paginação ajustada para 2000.")
            pagina.wait_for_timeout(3000)
        except:
            print("⚠️ Falha ao ajustar paginação.")

        for _ in range(20):
            if json_recebido:
                break
            pagina.wait_for_timeout(500)

        if not json_recebido:
            print("❌ JSON não capturado.")
            navegador.close()
            return

        data = json_recebido.get("data", [])
        if not data:
            print("⚠️ Nenhum dado nas turmas.")
            navegador.close()
            return

        # Criar sessão para requisições GET com cookies
        cookies = pagina.context.cookies()
        sessao = requests.Session()
        for cookie in cookies:
            sessao.cookies.set(cookie['name'], cookie['value'], domain=cookie.get('domain', ''))

        for linha in data:
            id_val = linha[0]
            if id_val not in ids_coletados:
                ids_coletados.add(id_val)
                dados_linha = linha[:9]
                qtd_matriculados = extrair_qtd_matriculados(sessao, id_val)
                dados_linha.append(str(qtd_matriculados))
                resultado.append(dados_linha)

        if not resultado:
            print("⚠️ Nenhum dado coletado.")
            navegador.close()
            return

        headers = ["id", "bairro", "curso", "nome_turma", "vagas", "inicio", "termino", "horario", "status", "matriculados"]
        dados = [headers] + resultado

        payload = {
            "tipo": "forms",
            "dados": dados
        }

        try:
            resposta_post = requests.post(URL_APPS_SCRIPT_TUR, json=payload)
            print("✅ Dados enviados com sucesso!")
            print("Status code:", resposta_post.status_code)
            print("Resposta:", resposta_post.text)
        except Exception as e:
            print("❌ Erro ao enviar para Google Apps Script:", e)

        navegador.close()

if __name__ == "__main__":
    main()
