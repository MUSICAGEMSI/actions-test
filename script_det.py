# script_relatorio_localidade.py
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
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzRSGdID5WjLuukBUt-5TbQjCqSvCKjr0vOWHFfFr0rChW1vINwgQE5VJDQCKM5mc693Q/exec'

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
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Primeiro: tentar encontrar o texto "de um total de X registros"
            info_div = soup.find('div', {'class': 'dataTables_info'})
            if info_div and info_div.text:
                match = re.search(r'de um total de (\d+) registros', info_div.text)
                if match:
                    return int(match.group(1))
                    
                match2 = re.search(r'Mostrando de \d+ até (\d+)', info_div.text)
                if match2:
                    return int(match2.group(1))
            
            # Segundo: contar linhas da tabela tbody
            tbody = soup.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                valid_rows = [row for row in rows if len(row.find_all('td')) >= 4]
                return len(valid_rows)
            
            # Terceiro: contar por padrão de linhas com dados de alunos
            aluno_pattern = re.findall(r'[A-Z\s]+ - [A-Z/]+/\d+', resp.text)
            if aluno_pattern:
                return len(aluno_pattern)
            
            # Quarto: contar botões "Desmatricular"
            desmatricular_count = resp.text.count('Desmatricular')
            if desmatricular_count > 0:
                return desmatricular_count
                
        return 0
        
    except Exception as e:
        print(f"⚠️ Erro ao obter matriculados para turma {turma_id}: {e}")
        return -1

def obter_alunos_unicos(session, turma_id):
    """
    Obtém lista de alunos únicos de uma turma para contagem sem repetição
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
            alunos = set()
            
            # Procurar por linhas da tabela com dados de alunos
            tbody = soup.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                for row in rows:
                    tds = row.find_all('td')
                    if len(tds) >= 2:
                        # Assumindo que o nome do aluno está na primeira coluna de dados
                        nome_aluno = tds[0].get_text(strip=True)
                        if nome_aluno and nome_aluno not in ['', 'Nenhum registro encontrado']:
                            alunos.add(nome_aluno)
            
            return list(alunos)
        
        return []
        
    except Exception as e:
        print(f"⚠️ Erro ao obter alunos únicos para turma {turma_id}: {e}")
        return []

def extrair_dias_da_semana(dia_hora_texto):
    """
    Extrai os dias da semana do texto de horário
    """
    dias_map = {
        'DOM': 'DOM', 'DOMINGO': 'DOM',
        'SEG': 'SEG', 'SEGUNDA': 'SEG',
        'TER': 'TER', 'TERÇA': 'TER', 'TERCA': 'TER',
        'QUA': 'QUA', 'QUARTA': 'QUA',
        'QUI': 'QUI', 'QUINTA': 'QUI',
        'SEX': 'SEX', 'SEXTA': 'SEX',
        'SAB': 'SÁB', 'SÁBADO': 'SÁB', 'SABADO': 'SÁB'
    }
    
    dias_encontrados = set()
    texto_upper = dia_hora_texto.upper()
    
    for dia_key, dia_value in dias_map.items():
        if dia_key in texto_upper:
            dias_encontrados.add(dia_value)
    
    return sorted(list(dias_encontrados))

def processar_relatorio_por_localidade(dados_turmas, session):
    """
    Processa os dados das turmas e agrupa por localidade
    """
    localidades = defaultdict(lambda: {
        'turmas': [],
        'total_matriculados': 0,
        'alunos_unicos': set(),
        'dias_semana': set()
    })
    
    print("📊 Processando dados por localidade...")
    
    for turma in dados_turmas:
        try:
            localidade = turma[0]  # Igreja/Localidade
            turma_id = turma[9]    # ID da turma
            matriculados_badge = int(turma[3]) if turma[3].isdigit() else 0
            dia_hora = turma[6]    # Dia - Hora
            
            # Obter alunos únicos desta turma
            print(f"🔍 Obtendo alunos únicos da turma {turma_id} - {localidade}")
            alunos_turma = obter_alunos_unicos(session, turma_id)
            
            # Extrair dias da semana
            dias_turma = extrair_dias_da_semana(dia_hora)
            
            # Adicionar aos dados da localidade
            localidades[localidade]['turmas'].append(turma)
            localidades[localidade]['total_matriculados'] += matriculados_badge
            localidades[localidade]['alunos_unicos'].update(alunos_turma)
            localidades[localidade]['dias_semana'].update(dias_turma)
            
            print(f"   ✅ {localidade}: +{matriculados_badge} matriculados, +{len(alunos_turma)} alunos únicos")
            
            # Pausa para não sobrecarregar
            time.sleep(0.5)
            
        except Exception as e:
            print(f"⚠️ Erro ao processar turma: {e}")
            continue
    
    return localidades

def gerar_relatorio_formatado(localidades):
    """
    Gera o relatório no formato solicitado
    """
    relatorio = []
    
    # Cabeçalho
    cabecalho = [
        "LOCALIDADE",
        "QUANTIDADE DE TURMAS",
        "SOMA DOS MATRICULADOS",
        "MATRICULADOS SEM REPETIÇÃO",
        "DIAS EM QUE HÁ GEM",
        "DOM", "SEG", "TER", "QUA", "QUI", "SEX", "SÁB"
    ]
    relatorio.append(cabecalho)
    
    # Dados por localidade
    for localidade, dados in localidades.items():
        quantidade_turmas = len(dados['turmas'])
        soma_matriculados = dados['total_matriculados']
        matriculados_unicos = len(dados['alunos_unicos'])
        
        # Montar string dos dias
        dias_ordenados = sorted(dados['dias_semana'])
        if len(dias_ordenados) > 1:
            dias_texto = f"{dias_ordenados[0]}/{dias_ordenados[-1]}"
        elif len(dias_ordenados) == 1:
            dias_texto = dias_ordenados[0]
        else:
            dias_texto = ""
        
        # Contar por dia da semana
        contadores_dias = {"DOM": 0, "SEG": 0, "TER": 0, "QUA": 0, "QUI": 0, "SEX": 0, "SÁB": 0}
        
        for turma in dados['turmas']:
            dias_turma = extrair_dias_da_semana(turma[6])
            for dia in dias_turma:
                if dia in contadores_dias:
                    contadores_dias[dia] += 1
        
        linha = [
            localidade,
            quantidade_turmas,
            soma_matriculados,
            matriculados_unicos,
            dias_texto,
            contadores_dias["DOM"] if contadores_dias["DOM"] > 0 else "",
            contadores_dias["SEG"] if contadores_dias["SEG"] > 0 else "",
            contadores_dias["TER"] if contadores_dias["TER"] > 0 else "",
            contadores_dias["QUA"] if contadores_dias["QUA"] > 0 else "",
            contadores_dias["QUI"] if contadores_dias["QUI"] > 0 else "",
            contadores_dias["SEX"] if contadores_dias["SEX"] > 0 else "",
            contadores_dias["SÁB"] if contadores_dias["SÁB"] > 0 else ""
        ]
        
        relatorio.append(linha)
    
    return relatorio

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

        # Coletar dados das turmas (mesmo processo do código original)
        resultado = []
        parar = False
        pagina_atual = 1

        while not parar:
            if time.time() - tempo_inicio > 1800:  # 30 minutos
                print("⏹️ Tempo limite atingido. Encerrando a coleta.")
                break

            print(f"📄 Processando página {pagina_atual}...")

            linhas = pagina.query_selector_all('table#tabela-turmas tbody tr')
            
            for i, linha in enumerate(linhas):
                if time.time() - tempo_inicio > 1800:
                    print("⏹️ Tempo limite atingido durante a iteração.")
                    parar = True
                    break

                try:
                    colunas_td = linha.query_selector_all('td')
                    
                    dados_linha = []
                    for j, td in enumerate(colunas_td[1:], 1):
                        if j == len(colunas_td) - 1:
                            continue
                        
                        badge = td.query_selector('span.badge')
                        if badge:
                            dados_linha.append(badge.inner_text().strip())
                        else:
                            texto = td.inner_text().strip().replace('\n', ' ').replace('\t', ' ')
                            texto = re.sub(r'\s+', ' ', texto).strip()
                            dados_linha.append(texto)

                    radio_input = linha.query_selector('input[type="radio"][name="item[]"]')
                    if not radio_input:
                        continue
                    
                    turma_id = radio_input.get_attribute('value')
                    if not turma_id:
                        continue

                    matriculados_badge = dados_linha[3] if len(dados_linha) > 3 else "0"

                    linha_completa = [
                        dados_linha[0] if len(dados_linha) > 0 else "",
                        dados_linha[1] if len(dados_linha) > 1 else "",
                        dados_linha[2] if len(dados_linha) > 2 else "",
                        matriculados_badge,
                        dados_linha[4] if len(dados_linha) > 4 else "",
                        dados_linha[5] if len(dados_linha) > 5 else "",
                        dados_linha[6] if len(dados_linha) > 6 else "",
                        dados_linha[7] if len(dados_linha) > 7 else "",
                        "Ações",
                        turma_id,
                        matriculados_badge,
                        "0",  # Será calculado depois
                        "Pendente"
                    ]

                    resultado.append(linha_completa)

                except Exception as e:
                    print(f"⚠️ Erro ao processar linha {i}: {e}")
                    continue

            if parar:
                break

            # Verificar se há próxima página
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

        print(f"📊 Total de turmas coletadas: {len(resultado)}")

        # Processar dados por localidade
        print("\n🏢 Processando relatório por localidade...")
        localidades = processar_relatorio_por_localidade(resultado, session)
        
        # Gerar relatório formatado
        relatorio_formatado = gerar_relatorio_formatado(localidades)
        
        # Preparar dados para envio
        body = {
            "tipo": "relatorio_localidades",
            "relatorio_formatado": relatorio_formatado,
            "dados_brutos": resultado,
            "resumo": {
                "total_localidades": len(localidades),
                "total_turmas": len(resultado),
                "total_matriculados": sum(loc['total_matriculados'] for loc in localidades.values()),
                "total_alunos_unicos": sum(len(loc['alunos_unicos']) for loc in localidades.values())
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

        # Mostrar relatório na tela
        print("\n📊 RELATÓRIO POR LOCALIDADE:")
        print("-" * 120)
        for i, linha in enumerate(relatorio_formatado):
            if i == 0:  # Cabeçalho
                print(f"{'|'.join(f'{str(item):^15}' for item in linha)}")
                print("-" * 120)
            else:
                print(f"{'|'.join(f'{str(item):^15}' for item in linha)}")
        
        print(f"\n📈 RESUMO GERAL:")
        print(f"   🏢 Total de localidades: {len(localidades)}")
        print(f"   📚 Total de turmas: {len(resultado)}")
        print(f"   👥 Total de matriculados: {sum(loc['total_matriculados'] for loc in localidades.values())}")
        print(f"   👤 Total de alunos únicos: {sum(len(loc['alunos_unicos']) for loc in localidades.values())}")

        navegador.close()

if __name__ == "__main__":
    main()
