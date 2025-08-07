import re
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT_TUR = "https://script.google.com/macros/s/AKfycbzZ07sISeDxyHWaRJZFAE2sJxe3L00gmVi2_YiU2puxTQ8HgYUJr27x8pxUiwjxGChjaA/exec"

def main():
    if not EMAIL or not SENHA:
        print("❌ Variáveis de ambiente LOGIN_MUSICAL e SENHA_MUSICAL não estão definidas!")
        return

    tempo_inicio = time.time()
    resultado = []

    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=False)
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

        # Acessar "G.E.M" > Turmas
        try:
            gem_selector = 'span:has-text("G.E.M")'
            pagina.wait_for_selector(gem_selector, timeout=15000)
            gem_element = pagina.locator(gem_selector).first
            gem_element.hover()
            pagina.wait_for_timeout(500)
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

        # Esperar tabela principal
        try:
            pagina.wait_for_selector('table#listagem tbody tr', timeout=15000)
            print("✅ Tabela de listagem carregada.")
        except:
            print("❌ Tabela de listagem não carregada.")
            navegador.close()
            return

        # Ajustar paginação (opcional)
        try:
            seletor_paginacao = pagina.locator('select[name="listagem_length"]')
            seletor_paginacao.select_option('2000')
            print("✅ Paginação ajustada para 2000.")
            pagina.wait_for_timeout(2000)
        except:
            print("⚠️ Falha ao ajustar paginação.")

        radios = pagina.locator('input[name="item[]"]')
        total_turmas = radios.count()

        for i in range(total_turmas):
            try:
                linha = pagina.locator('table#listagem tbody tr').nth(i)
                igreja = linha.locator('td').nth(1).inner_text().strip()
                curso = linha.locator('td').nth(2).inner_text().strip()
                turma_nome = linha.locator('td').nth(3).inner_text().strip()

                radio = radios.nth(i)
                radio.click()
                pagina.click('#adicionarLicoes')

                # Esperar tabela de matriculados
                pagina.wait_for_selector('#listagem_info', timeout=10000)
                info_text = pagina.inner_text('#listagem_info')
                match = re.search(r"de um total de (\d+)", info_text)
                matriculados = int(match.group(1)) if match else 0

                # Fechar modal
                try:
                    pagina.click('button:has-text("Fechar")')
                except:
                    print(f"⚠️ Botão Fechar não encontrado para turma {turma_nome}")

                resultado.append([igreja, curso, turma_nome, matriculados])
                print(f"✅ {igreja} | {curso} | {turma_nome} -> {matriculados}")

                pagina.wait_for_timeout(500)

            except Exception as e:
                print(f"⚠️ Erro ao processar turma {i+1}: {e}")

        if not resultado:
            print("⚠️ Nenhum dado coletado.")
            navegador.close()
            return

        headers = ["IGREJA", "CURSO", "TURMA", "MATRICULADOS"]
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
        print(f"⏱️ Tempo total: {time.time() - tempo_inicio:.2f}s")

if __name__ == "__main__":
    main()
