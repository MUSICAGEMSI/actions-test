# script_turmas_localidade.py
from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
import json
from bs4 import BeautifulSoup
from collections import defaultdict

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzx5wJjPYSBEeoNQMc02fxi2j4JqROJ1HKbdM59tMHmb2TD2A2Y6IYDtTpHiZvmLFsGug/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def extrair_alunos_matriculados(session, turma_id):
    """
    Extrai a lista de alunos matriculados na turma
    Retorna: (quantidade, lista_de_nomes)
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
            soup = BeautifulSoup(resp.text, 'html.parser')
            alunos = []
            
            # Procurar linhas da tabela de alunos
            tbody = soup.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:  # Nome deve estar na primeira célula
                        nome_completo = cells[0].get_text(strip=True)
                        if nome_completo and '-' in nome_completo:
                            # Extrair apenas o nome (antes do hífen)
                            nome_limpo = nome_completo.split('-')[0].strip()
                            if nome_limpo:
                                alunos.append(nome_limpo)
            
            # Fallback: usar regex para encontrar padrões de nomes
            if not alunos:
                aluno_patterns = re.findall(r'([A-ZÁÉÍÓÚÀÂÊÎÔÛÃÕÇ\s]+) - [A-Z/]+/\d+', resp.text)
                alunos = [nome.strip() for nome in aluno_patterns if nome.strip()]
            
            return len(alunos), alunos
            
        return 0, []
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair alunos da turma {turma_id}: {e}")
        return -1, []

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

        # Estruturas para análise por localidade
        dados_localidade = defaultdict(lambda: {
            'turmas': [],
            'total_matriculados': 0,
            'alunos_unicos': set(),
            'detalhes_turmas': []
        })
        
        resultado_detalhado = []
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
                    colunas_td = linha.query_selector_all('td')
                    
                    # Capturar dados das colunas principais
                    dados_linha = []
                    for j, td in enumerate(colunas_td[1:], 1):  # Skip first column (radio)
                        if j == len(colunas_td) - 1:  # Skip last column (actions)
                            continue
                        
                        badge = td.query_selector('span.badge')
                        if badge:
                            dados_linha.append(badge.inner_text().strip())
                        else:
                            texto = td.inner_text().strip().replace('\n', ' ').replace('\t', ' ')
                            texto = re.sub(r'\s+', ' ', texto).strip()
                            dados_linha.append(texto)

                    # Extrair ID da turma
                    radio_input = linha.query_selector('input[type="radio"][name="item[]"]')
                    if not radio_input:
                        continue
                    
                    turma_id = radio_input.get_attribute('value')
                    if not turma_id:
                        continue

                    # Dados principais
                    igreja = dados_linha[0] if len(dados_linha) > 0 else ""
                    curso = dados_linha[1] if len(dados_linha) > 1 else ""
                    turma = dados_linha[2] if len(dados_linha) > 2 else ""
                    matriculados_badge = dados_linha[3] if len(dados_linha) > 3 else "0"

                    print(f"🔍 Processando {igreja} - {curso} - Turma {turma_id}")

                    # Obter lista de alunos matriculados
                    matriculados_reais, lista_alunos = extrair_alunos_matriculados(session, turma_id)
                    
                    if matriculados_reais >= 0:
                        # Adicionar dados à estrutura de localidade
                        dados_localidade[igreja]['turmas'].append(turma_id)
                        dados_localidade[igreja]['total_matriculados'] += matriculados_reais
                        dados_localidade[igreja]['alunos_unicos'].update(lista_alunos)
                        
                        # Detalhes da turma
                        detalhes_turma = {
                            'turma_id': turma_id,
                            'curso': curso,
                            'turma': turma,
                            'matriculados_badge': int(matriculados_badge),
                            'matriculados_reais': matriculados_reais,
                            'alunos': lista_alunos
                        }
                        dados_localidade[igreja]['detalhes_turmas'].append(detalhes_turma)
                        
                        status = "✅ OK" if matriculados_reais == int(matriculados_badge) else f"⚠️ Diferença"
                    else:
                        status = "❌ Erro"

                    # Linha detalhada para relatório completo
                    linha_completa = [
                        igreja, curso, turma, matriculados_badge,
                        dados_linha[4] if len(dados_linha) > 4 else "",  # Início
                        dados_linha[5] if len(dados_linha) > 5 else "",  # Término
                        dados_linha[6] if len(dados_linha) > 6 else "",  # Dia - Hora
                        dados_linha[7] if len(dados_linha) > 7 else "",  # Status
                        turma_id,
                        str(matriculados_reais) if matriculados_reais >= 0 else "Erro",
                        status
                    ]

                    resultado_detalhado.append(linha_completa)
                    print(f"   📊 Badge: {matriculados_badge}, Real: {matriculados_reais}, Únicos acumulados: {len(dados_localidade[igreja]['alunos_unicos'])}")

                    # Pausa para não sobrecarregar
                    time.sleep(0.5)

                except Exception as e:
                    print(f"⚠️ Erro ao processar linha {i}: {e}")
                    continue

            if parar:
                break

            # Paginação
            try:
                btn_next = pagina.query_selector('a.paginate_button.next:not(.disabled)')
                if btn_next and btn_next.is_enabled():
                    print(f"➡️ Avançando para página {pagina_atual + 1}...")
                    btn_next.click()
                    
                    pagina.wait_for_function(
                        """
                        () => {
                            const tbody = document.querySelector('table#tabela-turmas tbody');
                            return tbody && tbody.querySelectorAll('tr').length > 0;
                        }
                        """,
                        timeout=15000
                    )
                    pagina.wait_for_timeout(3000)
                    pagina_atual += 1
                else:
                    print("📄 Última página alcançada.")
                    break
                    
            except Exception as e:
                print(f"⚠️ Erro na paginação: {e}")
                break

        # Processar dados por localidade
        relatorio_localidade = []
        for igreja, dados in dados_localidade.items():
            relatorio_localidade.append({
                'localidade': igreja,
                'quantidade_turmas': len(dados['turmas']),
                'total_matriculados_reais': dados['total_matriculados'],
                'matriculados_unicos': len(dados['alunos_unicos']),
                'lista_alunos_unicos': sorted(list(dados['alunos_unicos'])),
                'turmas_ids': dados['turmas'],
                'detalhes_turmas': dados['detalhes_turmas']
            })

        # Ordenar por quantidade de matriculados únicos (decrescente)
        relatorio_localidade.sort(key=lambda x: x['matriculados_unicos'], reverse=True)

        print(f"\n📊 RELATÓRIO POR LOCALIDADE:")
        print("="*80)
        for loc in relatorio_localidade:
            print(f"🏛️  {loc['localidade']}")
            print(f"   📚 Turmas: {loc['quantidade_turmas']}")
            print(f"   👥 Total matriculados: {loc['total_matriculados_reais']}")
            print(f"   🎯 Alunos únicos: {loc['matriculados_unicos']}")
            print(f"   📊 Média por turma: {loc['total_matriculados_reais']/loc['quantidade_turmas']:.1f}")
            print("-" * 60)

        # Preparar dados para envio
        body = {
            "tipo": "turmas_por_localidade",
            "relatorio_localidade": relatorio_localidade,
            "dados_detalhados": resultado_detalhado,
            "headers_detalhados": [
                "Igreja", "Curso", "Turma", "Matriculados_Badge", "Início", 
                "Término", "Dia_Hora", "Status", "ID_Turma", 
                "Real_Matriculados", "Status_Verificação"
            ],
            "resumo_geral": {
                "total_localidades": len(relatorio_localidade),
                "total_turmas": sum(loc['quantidade_turmas'] for loc in relatorio_localidade),
                "total_matriculados": sum(loc['total_matriculados_reais'] for loc in relatorio_localidade),
                "total_alunos_unicos": sum(loc['matriculados_unicos'] for loc in relatorio_localidade)
            }
        }

        # Enviar dados para Apps Script
        try:
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
            print("✅ Dados enviados!")
            print("Status code:", resposta_post.status_code)
            print("Resposta do Apps Script:", resposta_post.text)
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")

        # Resumo final
        resumo = body["resumo_geral"]
        print(f"\n🎯 RESUMO FINAL:")
        print(f"   🏛️  Localidades: {resumo['total_localidades']}")
        print(f"   📚 Total de turmas: {resumo['total_turmas']}")
        print(f"   👥 Total matriculados: {resumo['total_matriculados']}")
        print(f"   🎯 Total alunos únicos: {resumo['total_alunos_unicos']}")
        print(f"   📊 Taxa de sobreposição: {((resumo['total_matriculados'] - resumo['total_alunos_unicos']) / resumo['total_matriculados'] * 100):.1f}%")

        navegador.close()

if __name__ == "__main__":
    main()
