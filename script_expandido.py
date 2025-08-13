# script_turmas.py
from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
import json
from bs4 import BeautifulSoup

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbyDhrvHOn9afWBRxDPEMtmAcUcuUzLgfxUZRSjZRSaheUs52pOOb1N6sTDtTbBYCmvu/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def obter_matriculados_reais(session, turma_id):
    """
    Obtém o número real de matriculados contando as linhas da tabela
    """
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        }
        
        url = f"https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/{turma_id}"
        resp = session.get(url, headers=headers, timeout=15)
        
        if resp.status_code == 200:
            # Usar BeautifulSoup para parsing mais confiável
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Primeiro: tentar encontrar o texto "de um total de X registros"
            info_div = soup.find('div', {'class': 'dataTables_info'})
            if info_div and info_div.text:
                match = re.search(r'de um total de (\d+) registros', info_div.text)
                if match:
                    return int(match.group(1))
                    
                # Fallback: tentar "Mostrando de X até Y"
                match2 = re.search(r'Mostrando de \d+ até (\d+)', info_div.text)
                if match2:
                    return int(match2.group(1))
            
            # Segundo: contar linhas da tabela tbody
            tbody = soup.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                # Filtrar linhas vazias ou inválidas
                valid_rows = [row for row in rows if len(row.find_all('td')) >= 4]
                return len(valid_rows)
            
            # Terceiro: contar por padrão de linhas com dados de alunos
            # Procurar por padrões de nome (contém hífen e barra)
            aluno_pattern = re.findall(r'[A-Z\s]+ - [A-Z/]+/\d+', resp.text)
            if aluno_pattern:
                return len(aluno_pattern)
            
            # Quarto: contar botões "Desmatricular"
            desmatricular_count = resp.text.count('Desmatricular')
            if desmatricular_count > 0:
                return desmatricular_count
                
        return 0  # Se não conseguir encontrar, retorna 0
        
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
        
        # Configurações adicionais do navegador
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
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
            pagina.wait_for_selector('table#tabela-turmas', timeout=15000)
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

        # Configurar exibição para mostrar mais itens
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
                    # Extrair dados das colunas (exceto a primeira coluna do radio e última de ações)
                    colunas_td = linha.query_selector_all('td')
                    
                    # Pular primeira coluna (radio button) e capturar dados das outras
                    dados_linha = []
                    for j, td in enumerate(colunas_td[1:], 1):  # Skip first column
                        if j == len(colunas_td) - 1:  # Última coluna (ações)
                            continue
                        
                        # Tratamento especial para coluna de matriculados (badge)
                        badge = td.query_selector('span.badge')
                        if badge:
                            dados_linha.append(badge.inner_text().strip())
                        else:
                            texto = td.inner_text().strip().replace('\n', ' ').replace('\t', ' ')
                            # Limpar texto de ícones e espaços extras
                            texto = re.sub(r'\s+', ' ', texto).strip()
                            dados_linha.append(texto)

                    # Extrair ID da turma do input radio
                    radio_input = linha.query_selector('input[type="radio"][name="item[]"]')
                    if not radio_input:
                        continue
                    
                    turma_id = radio_input.get_attribute('value')
                    if not turma_id:
                        continue

                    # Matriculados mostrado no badge (coluna 4, índice 3)
                    matriculados_badge = dados_linha[3] if len(dados_linha) > 3 else "0"

                    print(f"🔍 Verificando turma {turma_id} - Badge: {matriculados_badge}")

                    # Obter número real de matriculados via API
                    matriculados_reais = obter_matriculados_reais(session, turma_id)
                    
                    # Determinar status
                    if matriculados_reais >= 0:
                        if matriculados_reais == int(matriculados_badge):
                            status_verificacao = "✅ OK"
                        else:
                            status_verificacao = f"⚠️ Diferença (Badge: {matriculados_badge}, Real: {matriculados_reais})"
                    else:
                        status_verificacao = "❌ Erro ao verificar"

                    # Montar linha completa
                    linha_completa = [
                        dados_linha[0] if len(dados_linha) > 0 else "",  # Igreja
                        dados_linha[1] if len(dados_linha) > 1 else "",  # Curso
                        dados_linha[2] if len(dados_linha) > 2 else "",  # Turma
                        matriculados_badge,                              # Matriculados Badge
                        dados_linha[4] if len(dados_linha) > 4 else "",  # Início
                        dados_linha[5] if len(dados_linha) > 5 else "",  # Término
                        dados_linha[6] if len(dados_linha) > 6 else "",  # Dia - Hora
                        dados_linha[7] if len(dados_linha) > 7 else "",  # Status
                        "Ações",                                         # Ações
                        turma_id,                                        # ID Turma
                        matriculados_badge,                              # Badge (duplicado para análise)
                        str(matriculados_reais) if matriculados_reais >= 0 else "Erro",  # Real
                        status_verificacao                               # Status Verificação
                    ]

                    resultado.append(linha_completa)
                    print(f"   📊 {linha_completa[0]} | {linha_completa[1]} | {linha_completa[2][:50]}... | Badge: {matriculados_badge}, Real: {matriculados_reais}")

                    # Pequena pausa para não sobrecarregar
                    time.sleep(0.5)

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
                        timeout=15000
                    )
                    pagina.wait_for_timeout(3000)  # Aguardar estabilização
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
            "tipo": "turmas_matriculados",
            "dados": resultado,
            "headers": [
                "Igreja", "Curso", "Turma", "Matriculados_Badge", "Início", 
                "Término", "Dia_Hora", "Status", "Ações", "ID_Turma", 
                "Badge_Duplicado", "Real_Matriculados", "Status_Verificação"
            ],
            "resumo": {
                "total_turmas": len(resultado),
                "turmas_com_diferenca": len([r for r in resultado if "Diferença" in r[-1]]),
                "turmas_ok": len([r for r in resultado if "✅ OK" in r[-1]]),
                "turmas_erro": len([r for r in resultado if "❌ Erro" in r[-1]])
            }
        }

        # Enviar dados para Apps Script
        try:
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=60)
            print("✅ Dados enviados!")
            print("Status code:", resposta_post.status_code)
            print("Resposta do Apps Script:", resposta_post.text)
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")

        # Mostrar resumo
        print("\n📈 RESUMO DA COLETA:")
        print(f"   🎯 Total de turmas: {len(resultado)}")
        print(f"   ✅ Turmas OK: {len([r for r in resultado if '✅ OK' in r[-1]])}")
        print(f"   ⚠️ Com diferenças: {len([r for r in resultado if 'Diferença' in r[-1]])}")
        print(f"   ❌ Com erro: {len([r for r in resultado if '❌ Erro' in r[-1]])}")

        navegador.close()

if __name__ == "__main__":
    main()
