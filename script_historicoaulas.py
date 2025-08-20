from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
import json
from bs4 import BeautifulSoup
from datetime import datetime
import multiprocessing as mp
from multiprocessing import Queue, Process, Manager
import queue
import pickle
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

# Configurações otimizadas - REDUZIDAS para evitar overload
MAX_PROCESSOS_SIMULTANEOS = 5  # Reduzido drasticamente
TIMEOUT_POR_PAGINA = 1800  # Aumentado
TIMEOUT_SELECTOR = 30000  # Aumentado
DELAY_ENTRE_INICIALIZACOES = 3  # Aumentado para dar tempo entre inicializações
DELAY_ENTRE_ACOES = 2  # Delay entre ações na página

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

class CookieManager:
    """Gerenciador centralizado de cookies para compartilhar sessão entre processos"""
    
    def __init__(self):
        self.cookies = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        }
        self.session_data = {}
    
    def fazer_login_global(self):
        """Faz login uma única vez e extrai cookies + dados de sessão para reutilização"""
        print("🔐 Realizando login global único...")
        
        with sync_playwright() as p:
            navegador = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-web-security',  # Adicionado
                    '--disable-features=VizDisplayCompositor',
                    '--disable-ipc-flooding-protection'  # Adicionado
                ]
            )
            
            context = navegador.new_context(
                viewport={'width': 1920, 'height': 1080},
                extra_http_headers=self.headers
            )
            pagina = context.new_page()
            
            try:
                # Login com timeouts maiores
                print("🌐 Acessando página inicial...")
                pagina.goto(URL_INICIAL, timeout=60000)
                
                print("📝 Preenchendo credenciais...")
                pagina.wait_for_selector('input[name="login"]', timeout=TIMEOUT_SELECTOR)
                pagina.fill('input[name="login"]', EMAIL)
                pagina.fill('input[name="password"]', SENHA)
                
                print("🔑 Fazendo login...")
                pagina.click('button[type="submit"]')
                
                # Aguardar login com timeout maior
                print("⏳ Aguardando confirmação do login...")
                pagina.wait_for_selector("nav", timeout=30000)
                
                # Extrair cookies e dados de sessão
                cookies_playwright = pagina.context.cookies()
                self.cookies = {cookie['name']: cookie['value'] for cookie in cookies_playwright}
                
                # Capturar dados adicionais da sessão
                try:
                    local_storage = pagina.evaluate("() => Object.assign({}, localStorage)")
                    session_storage = pagina.evaluate("() => Object.assign({}, sessionStorage)")
                    self.session_data = {
                        'localStorage': local_storage,
                        'sessionStorage': session_storage
                    }
                except:
                    print("⚠️ Não foi possível capturar storage data")
                
                print("🔍 Navegando para histórico para descobrir páginas...")
                if self.navegar_para_historico(pagina):
                    total_paginas = self.descobrir_total_paginas(pagina)
                else:
                    print("⚠️ Falha ao navegar para histórico, usando valor padrão")
                    total_paginas = 30
                
                navegador.close()
                
                print(f"✅ Login global realizado!")
                print(f"🍪 {len(self.cookies)} cookies extraídos")
                print(f"📊 Total de páginas descoberto: {total_paginas}")
                
                return total_paginas
                
            except Exception as e:
                navegador.close()
                raise Exception(f"Falha no login global: {e}")
    
    def navegar_para_historico(self, pagina):
        """Navega para o histórico de aulas com melhor tratamento de erros"""
        try:
            print("🔍 Navegando para G.E.M...")
            
            # Aguardar página carregar completamente
            pagina.wait_for_load_state("networkidle", timeout=30000)
            pagina.wait_for_selector("nav", timeout=TIMEOUT_SELECTOR)
            
            # Tentar diferentes seletores para o menu G.E.M
            seletores_gem = [
                'a:has-text("G.E.M")',
                'a:has(.fa-graduation-cap)',
                'a[href="#"]:has(span:text-is("G.E.M"))',
                'a:has(span):has-text("G.E.M")',
                'li:has-text("G.E.M") a',
                '.nav-link:has-text("G.E.M")'
            ]
            
            menu_gem_clicado = False
            for i, seletor in enumerate(seletores_gem):
                try:
                    print(f"🔍 Tentando seletor {i+1}: {seletor}")
                    pagina.wait_for_selector(seletor, timeout=10000)
                    pagina.click(seletor)
                    menu_gem_clicado = True
                    print("✅ Menu G.E.M clicado com sucesso")
                    break
                except Exception as e:
                    print(f"⚠️ Seletor {i+1} falhou: {e}")
                    continue
            
            if not menu_gem_clicado:
                print("❌ Não foi possível encontrar o menu G.E.M")
                return False
            
            time.sleep(DELAY_ENTRE_ACOES)
            
            # Clicar em "Histórico de Aulas" com múltiplas tentativas
            historico_clicado = False
            seletores_historico = [
                'a:has-text("Histórico de Aulas")',
                'a[href*="aulas_abertas"]',
                'li:has-text("Histórico") a'
            ]
            
            for i, seletor in enumerate(seletores_historico):
                try:
                    print(f"🔍 Tentando acessar histórico - método {i+1}")
                    pagina.wait_for_selector(seletor, state="visible", timeout=15000)
                    pagina.click(seletor)
                    historico_clicado = True
                    print("✅ Link do histórico clicado")
                    break
                except Exception as e:
                    print(f"⚠️ Método {i+1} falhou: {e}")
                    continue
            
            # Se nenhum método funcionou, tentar navegação direta
            if not historico_clicado:
                try:
                    print("🔍 Tentando navegação direta para histórico...")
                    pagina.goto("https://musical.congregacao.org.br/aulas_abertas", timeout=30000)
                    historico_clicado = True
                    print("✅ Navegação direta bem-sucedida")
                except Exception as e:
                    print(f"❌ Navegação direta falhou: {e}")
                    return False
            
            # Aguardar página do histórico carregar
            print("⏳ Aguardando página do histórico carregar...")
            try:
                # Aguardar elementos da tabela aparecerem
                pagina.wait_for_load_state("networkidle", timeout=30000)
                
                # Tentar diferentes seletores para confirmar que estamos na página certa
                elementos_confirmacao = [
                    'input[type="checkbox"][name="item[]"]',
                    "table tbody tr",
                    'select[name="listagem_length"]',
                    ".dataTables_wrapper"
                ]
                
                pagina_carregada = False
                for seletor in elementos_confirmacao:
                    try:
                        pagina.wait_for_selector(seletor, timeout=10000)
                        print(f"✅ Elemento encontrado: {seletor}")
                        pagina_carregada = True
                        break
                    except:
                        print(f"⚠️ Elemento não encontrado: {seletor}")
                        continue
                
                return pagina_carregada
                        
            except PlaywrightTimeoutError as e:
                print(f"❌ Timeout aguardando página do histórico: {e}")
                return False
                    
        except Exception as e:
            print(f"❌ Erro durante navegação: {e}")
            return False
    
    def descobrir_total_paginas(self, pagina):
        """Descobre o total de páginas com melhor tratamento de erros"""
        try:
            print("🔍 Descobrindo total de páginas...")
            
            # Aguardar página estabilizar
            time.sleep(3)
            
            # Configurar para mostrar 100 registros se possível
            try:
                print("⚙️ Configurando para mostrar 100 registros...")
                select_elem = pagina.query_selector('select[name="listagem_length"]')
                if select_elem:
                    pagina.select_option('select[name="listagem_length"]', "100")
                    time.sleep(5)  # Aguardar recarregamento da tabela
                    pagina.wait_for_load_state("networkidle", timeout=15000)
                    print("✅ Configurado para 100 registros")
                else:
                    print("⚠️ Seletor de quantidade não encontrado")
            except Exception as e:
                print(f"⚠️ Erro ao configurar registros por página: {e}")
            
            # Método 1: Extrair do elemento de informação da paginação
            try:
                info_selectors = [
                    'div.dataTables_info',
                    '.dataTables_info',
                    'div:has-text("Mostrando")',
                    'div:has-text("registros")'
                ]
                
                for seletor in info_selectors:
                    try:
                        info_elem = pagina.query_selector(seletor)
                        if info_elem:
                            texto_info = info_elem.inner_text()
                            print(f"📊 Info de paginação: {texto_info}")
                            
                            # Diferentes padrões de regex para extrair total
                            padroes = [
                                r'de\s+(\d+)\s+registros',
                                r'of\s+(\d+)\s+entries',
                                r'total\s+de\s+(\d+)',
                                r'(\d+)\s+total'
                            ]
                            
                            for padrao in padroes:
                                match = re.search(padrao, texto_info.lower())
                                if match:
                                    total_registros = int(match.group(1))
                                    registros_por_pagina = 100
                                    total_paginas = (total_registros + registros_por_pagina - 1) // registros_por_pagina
                                    print(f"✅ Total de páginas calculado: {total_paginas}")
                                    return min(total_paginas, 50)  # Limite máximo de segurança
                            break
                    except:
                        continue
                        
            except Exception as e:
                print(f"⚠️ Método 1 (info) falhou: {e}")
            
            # Método 2: Contar links de paginação
            try:
                print("🔍 Tentando método 2: links de paginação...")
                pagination_selectors = [
                    'div.dataTables_paginate a',
                    '.pagination a',
                    '.dataTables_paginate .paginate_button',
                    'ul.pagination li a'
                ]
                
                for seletor in pagination_selectors:
                    try:
                        links_paginacao = pagina.query_selector_all(seletor)
                        if links_paginacao:
                            numeros = []
                            for link in links_paginacao:
                                texto = link.inner_text().strip()
                                if texto.isdigit():
                                    numeros.append(int(texto))
                            
                            if numeros:
                                total_paginas = max(numeros)
                                print(f"✅ Total de páginas encontrado via paginação: {total_paginas}")
                                return min(total_paginas, 50)  # Limite máximo de segurança
                    except:
                        continue
                        
            except Exception as e:
                print(f"⚠️ Método 2 (paginação) falhou: {e}")
            
            # Método 3: Contar linhas e estimar
            try:
                print("🔍 Tentando método 3: estimativa por linhas...")
                linhas = pagina.query_selector_all("table tbody tr")
                if linhas and len(linhas) >= 90:  # Se tem quase 100 linhas, provavelmente tem mais páginas
                    print("⚠️ Página cheia, assumindo múltiplas páginas")
                    return 30  # Valor conservador
                elif linhas:
                    print(f"📊 Encontradas {len(linhas)} linhas - assumindo página única ou poucas páginas")
                    return max(5, len(linhas) // 20)  # Estimativa conservadora
                    
            except Exception as e:
                print(f"⚠️ Método 3 (estimativa) falhou: {e}")
            
            # Fallback final
            print("⚠️ Todos os métodos falharam, assumindo 20 páginas como padrão conservador")
            return 20
            
        except Exception as e:
            print(f"❌ Erro crítico ao descobrir total de páginas: {e}")
            return 20
    
    def get_cookies(self):
        """Retorna os cookies para uso em outros processos"""
        return self.cookies
    
    def get_headers(self):
        """Retorna os headers para uso em outros processos"""
        return self.headers
    
    def get_session_data(self):
        """Retorna dados de sessão adicionais"""
        return self.session_data

def extrair_detalhes_aula(session, aula_id):
    """Extrai detalhes da aula via requests para verificar ATA"""
    try:
        url_detalhes = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
        resp = session.get(url_detalhes, timeout=15)
        
        if resp.status_code == 200:
            return "OK" if "ATA DA AULA" in resp.text else "FANTASMA"
        return "ERRO"
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair detalhes da aula {aula_id}: {e}")
        return "ERRO"

def processar_frequencia_modal(pagina, aula_id, professor_id):
    """Processa a frequência após abrir o modal com melhor tratamento de erros"""
    try:
        # Aguardar modal aparecer e carregar
        pagina.wait_for_selector("#modalFrequencia", state="visible", timeout=15000)
        pagina.wait_for_selector("table.table-bordered tbody tr", timeout=15000)
        
        presentes_ids = []
        presentes_nomes = []
        ausentes_ids = []
        ausentes_nomes = []
        
        # Aguardar conteúdo da tabela carregar
        time.sleep(2)
        
        linhas = pagina.query_selector_all("table.table-bordered tbody tr")
        print(f"📊 Processando {len(linhas)} linhas de frequência")
        
        for i, linha in enumerate(linhas):
            try:
                nome_cell = linha.query_selector("td:first-child")
                nome_completo = nome_cell.inner_text().strip() if nome_cell else ""
                
                if not nome_completo:
                    continue
                
                link_presenca = linha.query_selector("td:last-child a")
                
                if link_presenca:
                    id_membro = link_presenca.get_attribute("data-id-membro")
                    
                    if not id_membro:
                        continue
                    
                    icone = link_presenca.query_selector("i")
                    if icone:
                        classes = icone.get_attribute("class") or ""
                        
                        if "fa-check" in classes and "text-success" in classes:
                            presentes_ids.append(id_membro)
                            presentes_nomes.append(nome_completo)
                        elif "fa-remove" in classes and "text-danger" in classes:
                            ausentes_ids.append(id_membro)
                            ausentes_nomes.append(nome_completo)
                        
            except Exception as e:
                print(f"⚠️ Erro ao processar linha {i}: {e}")
                continue
        
        print(f"✅ Frequência processada: {len(presentes_ids)} presentes, {len(ausentes_ids)} ausentes")
        
        return {
            'presentes_ids': presentes_ids,
            'presentes_nomes': presentes_nomes,
            'ausentes_ids': ausentes_ids,
            'ausentes_nomes': ausentes_nomes,
            'tem_presenca': "OK" if presentes_ids else "FANTASMA"
        }
        
    except Exception as e:
        print(f"⚠️ Erro ao processar frequência: {e}")
        return {
            'presentes_ids': [],
            'presentes_nomes': [],
            'ausentes_ids': [],
            'ausentes_nomes': [],
            'tem_presenca': "ERRO"
        }

def extrair_dados_de_linha_por_indice(pagina, indice_linha):
    """Extrai dados de uma linha específica pelo índice com melhor tratamento"""
    try:
        # Aguardar tabela estar presente
        pagina.wait_for_selector("table tbody tr", timeout=10000)
        linhas = pagina.query_selector_all("table tbody tr")
        
        if indice_linha >= len(linhas):
            return None, False
        
        linha = linhas[indice_linha]
        colunas = linha.query_selector_all("td")
        
        if len(colunas) >= 6:
            data_aula = colunas[1].inner_text().strip()
            
            # Parar se encontrou 2024
            if "2024" in data_aula:
                return None, True
            
            congregacao = colunas[2].inner_text().strip()
            curso = colunas[3].inner_text().strip()
            turma = colunas[4].inner_text().strip()
            
            btn_freq = linha.query_selector("button[onclick*='visualizarFrequencias']")
            if btn_freq:
                onclick = btn_freq.get_attribute("onclick")
                match = re.search(r'visualizarFrequencias\((\d+),\s*(\d+)\)', onclick)
                if match:
                    aula_id = match.group(1)
                    professor_id = match.group(2)
                    
                    return {
                        'aula_id': aula_id,
                        'professor_id': professor_id,
                        'data': data_aula,
                        'congregacao': congregacao,
                        'curso': curso,
                        'turma': turma
                    }, False
        
        return None, False
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair dados da linha {indice_linha}: {e}")
        return None, False

def clicar_botao_frequencia_por_indice(pagina, indice_linha):
    """Clica no botão de frequência com melhor tratamento de erros"""
    try:
        linhas = pagina.query_selector_all("table tbody tr")
        
        if indice_linha >= len(linhas):
            return False
        
        linha = linhas[indice_linha]
        btn_freq = linha.query_selector("button[onclick*='visualizarFrequencias']")
        
        if btn_freq:
            # Tentar scroll para o elemento se necessário
            try:
                pagina.evaluate("element => element.scrollIntoView()", btn_freq)
            except:
                pass
            
            # Clicar no botão
            btn_freq.click()
            return True
        
        return False
        
    except Exception as e:
        print(f"⚠️ Erro ao clicar no botão da linha {indice_linha}: {e}")
        return False

def contar_linhas_na_pagina(pagina):
    """Conta quantas linhas existem na página atual"""
    try:
        pagina.wait_for_selector("table tbody", timeout=10000)
        linhas = pagina.query_selector_all("table tbody tr")
        return len(linhas)
    except:
        return 0

def processar_pagina_com_cookies_compartilhados(numero_pagina, cookies_compartilhados, headers_compartilhados, session_data, resultado_queue, erro_queue):
    """Processa uma página específica usando cookies compartilhados - VERSÃO MELHORADA"""
    try:
        print(f"🚀 [Página {numero_pagina}] Iniciando processamento com cookies compartilhados")
        
        with sync_playwright() as p:
            navegador = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-web-security',
                    '--disable-ipc-flooding-protection'
                ]
            )
            
            context = navegador.new_context(
                viewport={'width': 1920, 'height': 1080},
                extra_http_headers=headers_compartilhados
            )
            
            # APLICAR COOKIES COMPARTILHADOS
            cookies_playwright = [
                {
                    'name': name,
                    'value': value,
                    'domain': 'musical.congregacao.org.br',
                    'path': '/',
                    'httpOnly': False,
                    'secure': True
                }
                for name, value in cookies_compartilhados.items()
            ]
            context.add_cookies(cookies_playwright)
            
            pagina = context.new_page()
            
            # IR DIRETAMENTE PARA HISTÓRICO
            print(f"🔄 [Página {numero_pagina}] Navegando diretamente para histórico...")
            pagina.goto("https://musical.congregacao.org.br/aulas_abertas", timeout=60000)
            
            # Aguardar página carregar completamente
            pagina.wait_for_load_state("networkidle", timeout=30000)
            
            # Configurar para 100 registros com retry
            configuracao_ok = False
            for tentativa in range(3):
                try:
                    print(f"⚙️ [Página {numero_pagina}] Tentativa {tentativa + 1} de configurar registros...")
                    pagina.wait_for_selector('select[name="listagem_length"]', timeout=TIMEOUT_SELECTOR)
                    pagina.select_option('select[name="listagem_length"]', "100")
                    time.sleep(3)
                    pagina.wait_for_load_state("networkidle", timeout=20000)
                    
                    # Verificar se a configuração funcionou
                    pagina.wait_for_selector("table tbody tr", timeout=15000)
                    configuracao_ok = True
                    print(f"✅ [Página {numero_pagina}] Registros configurados com sucesso")
                    break
                    
                except Exception as e:
                    print(f"⚠️ [Página {numero_pagina}] Tentativa {tentativa + 1} falhou: {e}")
                    if tentativa < 2:
                        time.sleep(5)
                    else:
                        print(f"⚠️ [Página {numero_pagina}] Prosseguindo sem configurar registros")
            
            # Navegar para a página específica se não for a primeira
            if numero_pagina > 1:
                print(f"🔄 [Página {numero_pagina}] Navegando para página {numero_pagina}...")
                navegacao_ok = False
                
                for tentativa in range(3):
                    try:
                        # Método 1: Campo de input de página
                        input_pagina = pagina.query_selector('input[type="number"][aria-controls="listagem"]')
                        if input_pagina:
                            input_pagina.fill("")  # Limpar primeiro
                            time.sleep(0.5)
                            input_pagina.fill(str(numero_pagina))
                            pagina.keyboard.press("Enter")
                            time.sleep(3)
                            pagina.wait_for_load_state("networkidle", timeout=20000)
                            navegacao_ok = True
                            break
                        
                        # Método 2: Link direto da página
                        link_pagina = pagina.query_selector(f'a:has-text("{numero_pagina}")')
                        if link_pagina and not navegacao_ok:
                            link_pagina.click()
                            time.sleep(3)
                            pagina.wait_for_load_state("networkidle", timeout=20000)
                            navegacao_ok = True
                            break
                        
                        # Método 3: Botões next (para páginas próximas)
                        if numero_pagina <= 5 and not navegacao_ok:
                            for i in range(numero_pagina - 1):
                                btn_proximo = pagina.query_selector("a:has(i.fa-chevron-right)")
                                if btn_proximo:
                                    parent = btn_proximo.query_selector("..")
                                    parent_class = parent.get_attribute("class") if parent else ""
                                    if "disabled" not in parent_class:
                                        btn_proximo.click()
                                        time.sleep(3)
                                        pagina.wait_for_load_state("networkidle", timeout=15000)
                                    else:
                                        break
                                else:
                                    break
                            navegacao_ok = True
                            break
                            
                    except Exception as nav_error:
                        print(f"⚠️ [Página {numero_pagina}] Tentativa {tentativa + 1} de navegação falhou: {nav_error}")
                        if tentativa < 2:
                            time.sleep(5)
                        else:
                            raise Exception(f"Falha em todas as tentativas de navegação para página {numero_pagina}")
                
                if not navegacao_ok:
                    raise Exception(f"Não foi possível navegar para a página {numero_pagina}")
            
            # Aguardar página da paginação carregar
            time.sleep(5)
            
            # Verificar se chegamos na página correta
            try:
                pagina.wait_for_selector("table tbody tr", timeout=15000)
            except:
                print(f"⚠️ [Página {numero_pagina}] Timeout aguardando linhas")
            
            # Criar sessão requests com cookies compartilhados
            session = requests.Session()
            session.cookies.update(cookies_compartilhados)
            session.headers.update(headers_compartilhados)
            
            # Processar aulas da página
            resultado_pagina = []
            deve_parar = False
            
            total_linhas = contar_linhas_na_pagina(pagina)
            
            if total_linhas == 0:
                print(f"🏁 [Página {numero_pagina}] Página {numero_pagina} não tem linhas")
                navegador.close()
                resultado_queue.put((numero_pagina, []))
                return
            
            print(f"📊 [Página {numero_pagina}] {total_linhas} aulas encontradas")
            
            # Processar cada linha da página
            for i in range(total_linhas):
                dados_aula, deve_parar_ano = extrair_dados_de_linha_por_indice(pagina, i)
                
                if deve_parar_ano:
                    print(f"🛑 [Página {numero_pagina}] Encontrado ano 2024 - parando processo")
                    deve_parar = True
                    break
                
                if not dados_aula:
                    continue
                
                print(f"🎯 [Página {numero_pagina}] Aula {i+1}/{total_linhas}: {dados_aula['data']}")
                
                try:
                    # Aguardar que não haja modal aberto
                    try:
                        pagina.wait_for_selector("#modalFrequencia", state="hidden", timeout=5000)
                    except:
                        try:
                            pagina.keyboard.press("Escape")
                            time.sleep(1)
                        except:
                            pass
                    
                    # Clicar no botão de frequência
                    if clicar_botao_frequencia_por_indice(pagina, i):
                        time.sleep(DELAY_ENTRE_ACOES)
                        
                        # Processar dados de frequência
                        freq_data = processar_frequencia_modal(pagina, dados_aula['aula_id'], dados_aula['professor_id'])
                        
                        # Fechar modal
                        try:
                            btn_fechar = pagina.query_selector('button.btn-warning[data-dismiss="modal"]:has-text("Fechar")')
                            if btn_fechar:
                                btn_fechar.click()
                            else:
                                # Tentar outras formas de fechar
                                try:
                                    pagina.evaluate("$('#modalFrequencia').modal('hide')")
                                except:
                                    pagina.keyboard.press("Escape")
                            
                            pagina.wait_for_selector("#modalFrequencia", state="hidden", timeout=10000)
                        except:
                            try:
                                pagina.keyboard.press("Escape")
                                time.sleep(1)
                            except:
                                pass
                        
                        time.sleep(DELAY_ENTRE_ACOES)
                        
                        # Obter detalhes da ATA
                        ata_status = extrair_detalhes_aula(session, dados_aula['aula_id'])
                        
                        # Montar resultado
                        linha_resultado = [
                            dados_aula['congregacao'],
                            dados_aula['curso'],
                            dados_aula['turma'],
                            dados_aula['data'],
                            "; ".join(freq_data['presentes_ids']),
                            "; ".join(freq_data['presentes_nomes']),
                            "; ".join(freq_data['ausentes_ids']),
                            "; ".join(freq_data['ausentes_nomes']),
                            freq_data['tem_presenca'],
                            ata_status
                        ]
                        
                        resultado_pagina.append(linha_resultado)
                        
                        print(f"✓ [Página {numero_pagina}] {len(freq_data['presentes_ids'])} presentes, {len(freq_data['ausentes_ids'])} ausentes - ATA: {ata_status}")
                    
                    else:
                        print(f"❌ [Página {numero_pagina}] Falha ao clicar no botão de frequência")
                        
                except Exception as e:
                    print(f"⚠️ [Página {numero_pagina}] Erro ao processar aula: {e}")
                    continue
                
                time.sleep(1)  # Delay entre aulas
                
                if deve_parar:
                    break
            
            print(f"✅ [Página {numero_pagina}] Concluída: {len(resultado_pagina)} aulas processadas")
            
            navegador.close()
            resultado_queue.put((numero_pagina, resultado_pagina))
            
    except Exception as e:
        print(f"❌ [Página {numero_pagina}] Erro crítico: {e}")
        erro_queue.put((numero_pagina, str(e)))

def processar_em_lotes(total_paginas, cookies_compartilhados, headers_compartilhados, session_data):
    """Processa páginas em lotes menores para evitar sobrecarga"""
    print(f"\n📦 Processamento em lotes de {MAX_PROCESSOS_SIMULTANEOS} processos")
    
    resultado_queue = Queue()
    erro_queue = Queue()
    
    todos_resultados = []
    todos_erros = []
    
    # Dividir páginas em lotes
    for lote_inicio in range(1, total_paginas + 1, MAX_PROCESSOS_SIMULTANEOS):
        lote_fim = min(lote_inicio + MAX_PROCESSOS_SIMULTANEOS - 1, total_paginas)
        
        print(f"\n🎯 Processando lote: páginas {lote_inicio} a {lote_fim}")
        
        # Criar processos para este lote
        processos = []
        
        for numero_pagina in range(lote_inicio, lote_fim + 1):
            processo = Process(
                target=processar_pagina_com_cookies_compartilhados,
                args=(numero_pagina, cookies_compartilhados, headers_compartilhados, session_data, resultado_queue, erro_queue)
            )
            processo.start()
            processos.append(processo)
            print(f"   ✅ Processo {numero_pagina} iniciado (PID: {processo.pid})")
            time.sleep(DELAY_ENTRE_INICIALIZACOES)
        
        # Aguardar todos os processos do lote terminarem
        print(f"⏳ Aguardando {len(processos)} processos do lote...")
        
        for i, processo in enumerate(processos, lote_inicio):
            processo.join(timeout=TIMEOUT_POR_PAGINA)
            
            if processo.is_alive():
                print(f"⚠️ Processo {i} excedeu timeout, terminando...")
                processo.terminate()
                processo.join()
            else:
                print(f"✅ Processo {i} finalizado")
        
        # Coletar resultados do lote
        while True:
            try:
                numero_pagina, resultado_pagina = resultado_queue.get_nowait()
                todos_resultados.extend(resultado_pagina)
                print(f"📋 Lote: Página {numero_pagina} coletada - {len(resultado_pagina)} aulas")
            except queue.Empty:
                break
        
        # Coletar erros do lote
        while True:
            try:
                numero_pagina, erro = erro_queue.get_nowait()
                todos_erros.append(f"Página {numero_pagina}: {erro}")
            except queue.Empty:
                break
        
        print(f"✅ Lote {lote_inicio}-{lote_fim} concluído")
        
        # Delay entre lotes para não sobrecarregar o servidor
        if lote_fim < total_paginas:
            print("⏳ Aguardando entre lotes...")
            time.sleep(10)
    
    return todos_resultados, todos_erros

def main():
    tempo_inicio = time.time()
    
    # FASE 1: Login Global Único
    print("=" * 60)
    print("🚀 INICIANDO SCRAPER OTIMIZADO E ROBUSTO")
    print("=" * 60)
    
    cookie_manager = CookieManager()
    
    try:
        total_paginas = cookie_manager.fazer_login_global()
        cookies_compartilhados = cookie_manager.get_cookies()
        headers_compartilhados = cookie_manager.get_headers()
        session_data = cookie_manager.get_session_data()
    except Exception as e:
        print(f"❌ Falha no login global: {e}")
        return
    
    if not cookies_compartilhados:
        print("❌ Não foi possível obter cookies de autenticação")
        return
    
    # Limitar páginas para teste/segurança
    total_paginas = min(total_paginas, 30)
    
    print(f"🎯 Total de páginas a processar: {total_paginas}")
    print(f"🍪 Cookies compartilhados: {len(cookies_compartilhados)}")
    print(f"⚙️ Máximo de processos simultâneos: {MAX_PROCESSOS_SIMULTANEOS}")
    
    # FASE 2: Processamento em Lotes
    print("\n" + "=" * 60)
    print("⚡ INICIANDO PROCESSAMENTO EM LOTES")
    print("=" * 60)
    
    # Configurar multiprocessing
    mp.set_start_method('spawn', force=True)
    
    try:
        todos_resultados, todos_erros = processar_em_lotes(
            total_paginas, cookies_compartilhados, headers_compartilhados, session_data
        )
    except Exception as e:
        print(f"❌ Erro durante processamento em lotes: {e}")
        todos_resultados, todos_erros = [], [str(e)]
    
    # FASE 3: Organizar Resultados
    print("\n" + "=" * 60)
    print("📊 ORGANIZANDO RESULTADOS")
    print("=" * 60)
    
    print(f"✅ Total de aulas coletadas: {len(todos_resultados)}")
    
    # FASE 4: Enviar para Google Sheets
    if todos_resultados:
        print("\n📤 Enviando dados para Google Sheets...")
        
        headers = [
            "CONGREGAÇÃO", "CURSO", "TURMA", "DATA", "PRESENTES IDs", 
            "PRESENTES Nomes", "AUSENTES IDs", "AUSENTES Nomes", "TEM PRESENÇA", "ATA DA AULA"
        ]
        
        # Ordenar por data
        try:
            todos_resultados.sort(key=lambda x: datetime.strptime(x[3], "%d/%m/%Y"), reverse=True)
            print("✅ Resultados ordenados por data")
        except Exception as e:
            print(f"⚠️ Não foi possível ordenar por data: {e}")
        
        body = {
            "tipo": "historico_aulas_robusto",
            "dados": todos_resultados,
            "headers": headers,
            "resumo": {
                "total_aulas": len(todos_resultados),
                "tempo_processamento": f"{(time.time() - tempo_inicio) / 60:.1f} minutos",
                "paginas_processadas": total_paginas,
                "modo": "lotes_robustos",
                "processos_por_lote": MAX_PROCESSOS_SIMULTANEOS,
                "login_unico": True,
                "erros_encontrados": len(todos_erros)
            }
        }
        
        try:
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=180)
            print("✅ Dados enviados para Google Sheets!")
            print(f"Status: {resposta_post.status_code}")
            if resposta_post.text:
                print(f"Resposta: {resposta_post.text[:200]}...")
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
    
    # FASE 5: Resumo Final
    print("\n" + "=" * 60)
    print("📈 RESUMO DA EXECUÇÃO ROBUSTA")
    print("=" * 60)
    
    tempo_total = (time.time() - tempo_inicio) / 60
    
    print(f"🎯 Total de aulas processadas: {len(todos_resultados)}")
    print(f"📄 Páginas processadas: {total_paginas}")
    print(f"⚡ Modo: Lotes Robustos (Login Compartilhado)")
    print(f"🚀 Processos por lote: {MAX_PROCESSOS_SIMULTANEOS}")
    print(f"⏱️ Tempo total: {tempo_total:.1f} minutos")
    
    # Estatísticas detalhadas
    if todos_resultados:
        aulas_com_presenca = sum(1 for linha in todos_resultados if linha[8] == "OK")
        aulas_com_ata = sum(1 for linha in todos_resultados if linha[9] == "OK")
        
        print(f"👥 Aulas com presença: {aulas_com_presenca}/{len(todos_resultados)} ({(aulas_com_presenca/len(todos_resultados)*100):.1f}%)")
        print(f"📝 Aulas com ATA: {aulas_com_ata}/{len(todos_resultados)} ({(aulas_com_ata/len(todos_resultados)*100):.1f}%)")
        
        # Análise temporal
        try:
            datas_processadas = []
            for linha in todos_resultados:
                try:
                    data = datetime.strptime(linha[3], "%d/%m/%Y")
                    datas_processadas.append(data)
                except:
                    continue
            
            if datas_processadas:
                data_mais_antiga = min(datas_processadas)
                data_mais_recente = max(datas_processadas)
                print(f"📅 Período processado: {data_mais_antiga.strftime('%d/%m/%Y')} a {data_mais_recente.strftime('%d/%m/%Y')}")
        except Exception as e:
            print(f"⚠️ Erro na análise temporal: {e}")
    
    # Mostrar erros se houver
    if todos_erros:
        print("\n⚠️ ERROS ENCONTRADOS:")
        for erro in todos_erros[:5]:  # Mostrar apenas os primeiros 5 erros
            print(f"   • {erro}")
        if len(todos_erros) > 5:
            print(f"   • ... e mais {len(todos_erros) - 5} erros")
    
    # Análise de eficiência
    print(f"\n🔬 MELHORIAS IMPLEMENTADAS:")
    print(f"   • Login único: ✅ (vs múltiplos logins)")
    print(f"   • Processamento em lotes: ✅ (vs todos simultâneos)")
    print(f"   • Timeouts aumentados: ✅ (30s vs 10s)")
    print(f"   • Retry em operações críticas: ✅")
    print(f"   • Delays entre ações: ✅")
    print(f"   • Melhor tratamento de erros: ✅")
    
    if tempo_total > 0 and todos_resultados:
        aulas_por_minuto = len(todos_resultados) / tempo_total
        print(f"   • Velocidade alcançada: {aulas_por_minuto:.1f} aulas/minuto")

if __name__ == "__main__":
    main()
