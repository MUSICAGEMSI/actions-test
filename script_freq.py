from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")


from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time
from datetime import datetime
import re


EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT_P_AULA = 'https://script.google.com/macros/s/AKfycbxjuEpLALSZcJQTgsL8TNzDu3JHWXLGDnOViP1K_hjO3PPXORctieJI2XScF4C3ZQIK-A/exec'


if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)


def extrair_data_da_linha(linha):
    try:
        # Coluna 4 é a data (índice 4)
        data_str = linha[4]


        if not re.match(r'\d{2}[-/]\d{2}[-/]\d{4}', data_str):
            return None


        # Ajusta formato
        if "-" in data_str:
            data_aula = datetime.strptime(data_str, "%d-%m-%Y")
        else:
            data_aula = datetime.strptime(data_str, "%d/%m/%Y")


        return data_aula
    except:
        return None


def main():
    tempo_inicio = time.time()
    turmas_primeira_aula = {}


    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)  # troquei False por True
        pagina = navegador.new_page()


        json_recebido = None


        def captura_resposta(response):
            nonlocal json_recebido
            if "listagem" in response.url and response.request.method == "POST":
                try:
                    json_recebido = response.json()
                    print(f"✅ JSON da resposta interceptado ({len(json_recebido.get('data', []))} linhas)")
                except Exception as e:
                    print("⚠️ Erro ao ler JSON da resposta:", e)


        pagina.on("response", captura_resposta)


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


        # Abrir menu G.E.M
        try:
            gem_selector = 'span:has-text("G.E.M")'
            pagina.wait_for_selector(gem_selector, timeout=15000)
            gem_element = pagina.locator(gem_selector).first
            gem_element.hover()
            pagina.wait_for_timeout(1000)
            if True and True:  # troquei gem_element.is_visible() and gem_element.is_enabled() por True and True
                gem_element.click()
            else:
                print("❌ Elemento G.E.M não clicável.")
                navegador.close()
                return
        except PlaywrightTimeoutError:
            print("❌ Menu 'G.E.M' não apareceu a tempo.")
            navegador.close()
            return

        # Acessar página turmas
        try:
            pagina.wait_for_selector('a[href="aulas_abertas"]', timeout=10000)
            pagina.click('a[href="aulas_abertas"]')
        except PlaywrightTimeoutError:
            print("❌ Link 'turmas' não encontrado.")
            navegador.close()
            return

        # Ajustar para mostrar 2000 entradas
        try:
            pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
            pagina.select_option('select[name="listagem_length"]', '2000')
            print("✅ Ajustado para mostrar 2000 entradas.")

            # Esperar carregar linhas
            timeout_ms = 20000
            intervalo_espera = 500
            tempo_espera = 0

            while True:
                linhas = pagina.locator("table#listagem tbody tr").count()
                print(f"⏳ Linhas carregadas: {linhas}")
                # Se tiver linhas >= 2000 ou timeout, quebra
                if linhas >= 2000 or tempo_espera >= timeout_ms:
                    break
                pagina.wait_for_timeout(intervalo_espera)
                tempo_espera += intervalo_espera

            if linhas < 2000:
                print(f"⚠️ Apenas {linhas} linhas carregadas, percorrendo paginação manualmente.")

            # Coleta paginada
            def coletar_dados_da_pagina():
                nonlocal json_recebido
                # Espera JSON chegar para esta página
                espera_json = 5000
                pagina.wait_for_timeout(espera_json)
                if not json_recebido:
                    print("⚠️ Dados da página não carregaram.")
                    return []

                dados = json_recebido.get("data", [])
                json_recebido = None  # limpa para próxima página
                return dados

            dados_completos = []

            # Página inicial já carregada, coletar dados dela
            dados_pagina = coletar_dados_da_pagina()
            dados_completos.extend(dados_pagina)

            def pagina_tem_data_2025(dados_pagina):
                for linha in dados_pagina:
                    data_aula = extrair_data_da_linha(linha)
                    if data_aula and data_aula.year == 2025:
                        return True
                return False

            pagina_atual = 1

            while True:
                if not pagina_tem_data_2025(dados_pagina):
                    print(f"⏹️ Nenhuma data 2025 encontrada na página {pagina_atual}, encerrando coleta.")
                    break

                # tenta ir para próxima página
                btn_proximo = pagina.locator('a.paginate_button.next:not(.disabled)')
                if btn_proximo.count() == 0:
                    print(f"⏹️ Botão Próximo desabilitado ou não encontrado na página {pagina_atual}, fim da paginação.")
                    break

                btn_proximo.first.click()
                pagina_atual += 1

                # Espera carregar dados da nova página
                dados_pagina = coletar_dados_da_pagina()
                dados_completos.extend(dados_pagina)

            # Filtrar primeira aula por turma somente 2025
            for linha in dados_completos:
                try:
                    linha_relevante = linha[:5]
                    nome_turma = linha_relevante[3]
                    data_aula = extrair_data_da_linha(linha_relevante)

                    if not data_aula or data_aula.year != 2025:
                        continue

                    if nome_turma not in turmas_primeira_aula:
                        turmas_primeira_aula[nome_turma] = linha_relevante
                    else:
                        data_existente = extrair_data_da_linha(turmas_primeira_aula[nome_turma])
                        if data_aula < data_existente:
                            turmas_primeira_aula[nome_turma] = linha_relevante
                except Exception as e:
                    print(f"⚠️ Erro processando linha: {linha} - {e}")

            if not turmas_primeira_aula:
                print("⚠️ Nenhum dado coletado após filtragem.")
                navegador.close()
                return

            headers = ["id", "bairro", "curso", "nome_turma", "data"]
            dados_envio = [headers] + list(turmas_primeira_aula.values())

            payload = {
                "tipo": "p_aula",
                "dados": dados_envio
            }

            try:
                resposta_post = requests.post(URL_APPS_SCRIPT_P_AULA, json=payload)
                print("✅ Dados enviados!")
                print("Status code:", resposta_post.status_code)
                print("Resposta:", resposta_post.text)
            except Exception as e:
                print("❌ Erro ao enviar dados:", e)

        except Exception as e:
            print("❌ Erro no processo:", e)

        navegador.close()

if __name__ == "__main__":
    main()