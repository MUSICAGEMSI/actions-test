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

# Configurações otimizadas
MAX_PROCESSOS_SIMULTANEOS = 30
TIMEOUT_POR_PAGINA = 900
DELAY_ENTRE_INICIALIZACOES = 1  # Reduzido para não criar gargalo

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
    
    def fazer_login_global(self):
        """Faz login uma única vez e extrai cookies para reutilização"""
        print("🔐 Realizando login global único...")
        
        with sync_playwright() as p:
            navegador = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-features=VizDisplayCompositor'
                ]
            )
            
            context = navegador.new_context()
            pagina = context.new_page()
            pagina.set_extra_http_headers(self.headers)
            
            try:
                # Login
                pagina.goto(URL_INICIAL)
                pagina.fill('input[name="login"]', EMAIL)
                pagina.fill('input[name="password"]', SENHA)
                pagina.click('button[type="submit"]')
                
                # Aguardar login
                pagina.wait_for_selector("nav", timeout=15000)
                
                # Extrair cookies
                cookies_playwright = pagina.context.cookies()
                self.cookies = {cookie['name']: cookie['value'] for cookie in cookies_playwright}
                
                # Navegar para histórico para obter total de páginas
                if self.navegar_para_historico(pagina):
                    total_paginas = self.descobrir_total_paginas(pagina)
                else:
                    total_paginas = 30
                
                navegador.close()
                
                print(f"✅ Login global realizado! {len(self.cookies)} cookies extraídos")
                print(f"📊 Total de páginas descoberto: {total_paginas}")
                
                return total_paginas
                
            except Exception as e:
                navegador.close()
                raise Exception(f"Falha no login global: {e}")
    
    def navegar_para_historico(self, pagina):
        """Navega para o histórico de aulas"""
        try:
            print("🔍 Navegando para G.E.M...")
            
            pagina.wait_for_selector("nav", timeout=15000)
            
            # Tentar diferentes seletores para o menu G.E.M
            seletores_gem = [
                'a:has-text("G.E.M")',
                'a:has(.fa-graduation-cap)',
                'a[href="#"]:has(span:text-is("G.E.M"))',
                'a:has(span):has-text("G.E.M")'
            ]
            
            menu_gem_clicado = False
            for seletor in seletores_gem:
                try:
                    elemento_gem = pagina.query_selector(seletor)
                    if elemento_gem:
                        elemento_gem.click()
                        menu_gem_clicado = True
                        break
                except:
                    continue
            
            if not menu_gem_clicado:
                print("❌ Não foi possível encontrar o menu G.E.M")
                return False
            
            time.sleep(3)
            
            # Clicar em "Histórico de Aulas"
            historico_clicado = False
            try:
                historico_link = pagina.wait_for_selector('a:has-text("Histórico de Aulas")', 
                                                         state="visible", timeout=10000)
                if historico_link:
                    historico_link.click()
                    historico_clicado = True
            except:
                try:
                    elemento = pagina.query_selector('a:has-text("Histórico de Aulas")')
                    if elemento:
                        pagina.evaluate("element => element.click()", elemento)
                        historico_clicado = True
                except:
                    try:
                        pagina.goto("https://musical.congregacao.org.br/aulas_abertas")
                        historico_clicado = True
                    except:
                        pass
            
            if not historico_clicado:
                return False
            
            try:
                pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=20000)
                return True
            except PlaywrightTimeoutError:
                try:
                    pagina.wait_for_selector("table", timeout=5000)
                    return True
                except:
                    return False
                    
        except Exception as e:
            print(f"❌ Erro durante navegação: {e}")
            return False
    
    def descobrir_total_paginas(self, pagina):
        """Descobre o total de páginas disponíveis"""
        try:
            print("🔍 Descobrindo total de páginas...")
            
            # Configurar para mostrar 100 registros
            try:
                pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
                pagina.select_option('select[name="listagem_length"]', "100")
                time.sleep(3)
                pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=15000)
            except Exception as e:
                print(f"⚠️ Erro ao configurar registros: {e}")
            
            # Método 1: Extrair do elemento de paginação
            try:
                info_elem = pagina.query_selector('div.dataTables_info')
                if info_elem:
                    texto_info = info_elem.inner_text()
                    print(f"📊 Info de paginação: {texto_info}")
                    
                    match = re.search(r'de\s+(\d+)\s+entradas', texto_info)
                    if match:
                        total_registros = int(match.group(1))
                        registros_por_pagina = 100
                        total_paginas = (total_registros + registros_por_pagina - 1) // registros_por_pagina
                        print(f"✅ Total de páginas calculado: {total_paginas}")
                        return total_paginas
            except Exception as e:
                print(f"⚠️ Método 1 falhou: {e}")
            
            # Método 2: Links de paginação
            try:
                links_paginacao = pagina.query_selector_all('div.dataTables_paginate a')
                if links_paginacao:
                    numeros = []
                    for link in links_paginacao:
                        texto = link.inner_text().strip()
                        if texto.isdigit():
                            numeros.append(int(texto))
                    
                    if numeros:
                        total_paginas = max(numeros)
                        print(f"✅ Total de páginas encontrado: {total_paginas}")
                        return total_paginas
            except Exception as e:
                print(f"⚠️ Método 2 falhou: {e}")
            
            # Fallback
            print("⚠️ Assumindo 30 páginas como padrão")
            return 30
            
        except Exception as e:
            print(f"❌ Erro ao descobrir total de páginas: {e}")
            return 30
    
    def get_cookies(self):
        """Retorna os cookies para uso em outros processos"""
        return self.cookies
    
    def get_headers(self):
        """Retorna os headers para uso em outros processos"""
        return self.headers

def extrair_detalhes_aula(session, aula_id):
    """Extrai detalhes da aula via requests para verificar ATA"""
    try:
        url_detalhes = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
        resp = session.get(url_detalhes, timeout=10)
        
        if resp.status_code == 200:
            return "OK" if "ATA DA AULA" in resp.text else "FANTASMA"
        return "ERRO"
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair detalhes da aula {aula_id}: {e}")
        return "ERRO"

def processar_frequencia_modal(pagina, aula_id, professor_id):
    """Processa a frequência após abrir o modal"""
    try:
        pagina.wait_for_selector("table.table-bordered tbody tr", timeout=10000)
        
        presentes_ids = []
        presentes_nomes = []
        ausentes_ids = []
        ausentes_nomes = []
        
        linhas = pagina.query_selector_all("table.table-bordered tbody tr")
        
        for linha in linhas:
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
                    classes = icone.get_attribute("class")
                    
                    if "fa-check text-success" in classes:
                        presentes_ids.append(id_membro)
                        presentes_nomes.append(nome_completo)
                    elif "fa-remove text-danger" in classes:
                        ausentes_ids.append(id_membro)
                        ausentes_nomes.append(nome_completo)
        
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
    """Extrai dados de uma linha específica pelo índice"""
    try:
        linhas = pagina.query_selector_all("table tbody tr")
        
        if indice_linha >= len(linhas):
            return None, False
        
        linha = linhas[indice_linha]
        colunas = linha.query_selector_all("td")
        
        if len(colunas) >= 6:
            data_aula = colunas[1].inner_text().strip()
            
            if "2024" in data_aula:
                return None, True  # Sinal para parar
            
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
    """Clica no botão de frequência de uma linha específica pelo índice"""
    try:
        linhas = pagina.query_selector_all("table tbody tr")
        
        if indice_linha >= len(linhas):
            return False
        
        linha = linhas[indice_linha]
        btn_freq = linha.query_selector("button[onclick*='visualizarFrequencias']")
        
        if btn_freq:
            btn_freq.click()
            return True
        
        return False
        
    except Exception as e:
        print(f"⚠️ Erro ao clicar no botão da linha {indice_linha}: {e}")
        return False

def contar_linhas_na_pagina(pagina):
    """Conta quantas linhas existem na página atual"""
    try:
        linhas = pagina.query_selector_all("table tbody tr")
        return len(linhas)
    except:
        return 0

def processar_pagina_com_cookies_compartilhados(numero_pagina, cookies_compartilhados, headers_compartilhados, resultado_queue, erro_queue):
    """Processa uma página específica usando cookies compartilhados - SEM LOGIN"""
    try:
        print(f"🚀 [Página {numero_pagina}] Iniciando processamento com cookies compartilhados")
        
        with sync_playwright() as p:
            navegador = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-features=VizDisplayCompositor'
                ]
            )
            
            context = navegador.new_context()
            
            # APLICAR COOKIES COMPARTILHADOS (sem login)
            cookies_playwright = [
                {
                    'name': name,
                    'value': value,
                    'domain': 'musical.congregacao.org.br',
                    'path': '/'
                }
                for name, value in cookies_compartilhados.items()
            ]
            context.add_cookies(cookies_playwright)
            
            pagina = context.new_page()
            pagina.set_extra_http_headers(headers_compartilhados)
            
            # IR DIRETAMENTE PARA HISTÓRICO (sem login)
            print(f"🔄 [Página {numero_pagina}] Navegando diretamente para histórico...")
            pagina.goto("https://musical.congregacao.org.br/aulas_abertas")
            
            # Configurar para 100 registros
            try:
                pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
                pagina.select_option('select[name="listagem_length"]', "100")
                time.sleep(3)
                pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=15000)
            except Exception as e:
                print(f"⚠️ [Página {numero_pagina}] Erro ao configurar registros: {e}")
            
            # Navegar para a página específica
            if numero_pagina > 1:
                print(f"🔄 [Página {numero_pagina}] Navegando para página {numero_pagina}...")
                try:
                    for tentativa in range(3):
                        try:
                            # Método 1: Campo de input de página
                            input_pagina = pagina.query_selector('input[type="number"][aria-controls="listagem"]')
                            if input_pagina:
                                input_pagina.fill(str(numero_pagina))
                                pagina.keyboard.press("Enter")
                                break
                            
                            # Método 2: Botões de paginação
                            for i in range(numero_pagina - 1):
                                btn_proximo = pagina.query_selector("a:has(i.fa-chevron-right)")
                                if btn_proximo:
                                    parent = btn_proximo.query_selector("..")
                                    parent_class = parent.get_attribute("class") if parent else ""
                                    if "disabled" not in parent_class:
                                        btn_proximo.click()
                                        time.sleep(2)
                                    else:
                                        break
                                else:
                                    break
                            break
                            
                        except Exception as nav_error:
                            print(f"⚠️ [Página {numero_pagina}] Tentativa {tentativa + 1} falhou: {nav_error}")
                            if tentativa < 2:
                                time.sleep(3)
                            else:
                                raise nav_error
                    
                    time.sleep(3)
                    pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=10000)
                    
                except Exception as e:
                    raise Exception(f"Erro ao navegar para página {numero_pagina}: {e}")
            
            # Criar sessão requests com cookies compartilhados
            session = requests.Session()
            session.cookies.update(cookies_compartilhados)
            session.headers.update(headers_compartilhados)
            
            # Processar aulas da página
            resultado_pagina = []
            deve_parar = False
            
            try:
                pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=10000)
                time.sleep(2)
            except:
                print(f"⚠️ [Página {numero_pagina}] Timeout aguardando linhas")
            
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
                        pagina.wait_for_selector("#modalFrequencia", state="hidden", timeout=3000)
                    except:
                        pagina.keyboard.press("Escape")
                        time.sleep(1)
                    
                    # Clicar no botão de frequência
                    if clicar_botao_frequencia_por_indice(pagina, i):
                        time.sleep(1)
                        
                        # Processar dados de frequência
                        freq_data = processar_frequencia_modal(pagina, dados_aula['aula_id'], dados_aula['professor_id'])
                        
                        # Fechar modal
                        try:
                            btn_fechar = pagina.query_selector('button.btn-warning[data-dismiss="modal"]:has-text("Fechar")')
                            if btn_fechar:
                                btn_fechar.click()
                            else:
                                pagina.evaluate("$('#modalFrequencia').modal('hide')")
                            
                            pagina.wait_for_selector("#modalFrequencia", state="hidden", timeout=5000)
                        except:
                            pagina.keyboard.press("Escape")
                        
                        time.sleep(1)
                        
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
                
                time.sleep(0.5)
                
                if deve_parar:
                    break
            
            print(f"✅ [Página {numero_pagina}] Concluída: {len(resultado_pagina)} aulas processadas")
            
            navegador.close()
            resultado_queue.put((numero_pagina, resultado_pagina))
            
    except Exception as e:
        print(f"❌ [Página {numero_pagina}] Erro crítico: {e}")
        erro_queue.put((numero_pagina, str(e)))

def main():
    tempo_inicio = time.time()
    
    # FASE 1: Login Global Único
    print("=" * 60)
    print("🚀 INICIANDO SCRAPER OTIMIZADO COM LOGIN COMPARTILHADO")
    print("=" * 60)
    
    cookie_manager = CookieManager()
    
    try:
        total_paginas = cookie_manager.fazer_login_global()
        cookies_compartilhados = cookie_manager.get_cookies()
        headers_compartilhados = cookie_manager.get_headers()
    except Exception as e:
        print(f"❌ Falha no login global: {e}")
        return
    
    if not cookies_compartilhados:
        print("❌ Não foi possível obter cookies de autenticação")
        return
    
    print(f"🎯 Total de páginas: {total_paginas}")
    print(f"🍪 Cookies compartilhados: {len(cookies_compartilhados)}")
    
    # FASE 2: Processamento Verdadeiramente Paralelo
    print("\n" + "=" * 60)
    print("⚡ INICIANDO PROCESSAMENTO PARALELO SIMULTÂNEO")
    print("=" * 60)
    
    # Configurar multiprocessing
    mp.set_start_method('spawn', force=True)
    
    # Queues para comunicação
    resultado_queue = Queue()
    erro_queue = Queue()
    
    # CRIAR E INICIAR TODOS OS PROCESSOS SIMULTANEAMENTE
    processos = []
    
    print(f"🚀 Iniciando {total_paginas} processos simultâneos...")
    
    for numero_pagina in range(1, total_paginas + 1):
        processo = Process(
            target=processar_pagina_com_cookies_compartilhados,
            args=(numero_pagina, cookies_compartilhados, headers_compartilhados, resultado_queue, erro_queue)
        )
        processo.start()
        processos.append(processo)
        print(f"   ✅ Processo {numero_pagina} iniciado (PID: {processo.pid})")
        time.sleep(DELAY_ENTRE_INICIALIZACOES)  # Pequeno delay para não sobrecarregar
    
    print(f"🔥 {len(processos)} processos executando simultaneamente!")
    
    # AGUARDAR TODOS OS PROCESSOS TERMINAREM
    print("\n📊 Aguardando conclusão dos processos...")
    
    processos_finalizados = 0
    for i, processo in enumerate(processos, 1):
        print(f"⏳ Aguardando processo {i}/{total_paginas}...")
        processo.join(timeout=TIMEOUT_POR_PAGINA)
        
        if processo.is_alive():
            print(f"⚠️ Processo {i} excedeu timeout, terminando...")
            processo.terminate()
            processo.join()
        else:
            processos_finalizados += 1
            print(f"✅ Processo {i} finalizado")
    
    print(f"🏁 {processos_finalizados}/{total_paginas} processos finalizados com sucesso")
    
    # FASE 3: Coletar Resultados
    print("\n" + "=" * 60)
    print("📊 COLETANDO E ORGANIZANDO RESULTADOS")
    print("=" * 60)
    
    todos_resultados = []
    resultados_por_pagina = {}
    
    # Coletar todos os resultados
    try:
        while True:
            numero_pagina, resultado_pagina = resultado_queue.get_nowait()
            resultados_por_pagina[numero_pagina] = resultado_pagina
            print(f"📋 Página {numero_pagina}: {len(resultado_pagina)} aulas coletadas")
    except queue.Empty:
        pass
    
    # Adicionar resultados na ordem das páginas
    for numero_pagina in range(1, total_paginas + 1):
        if numero_pagina in resultados_por_pagina:
            resultado_pagina = resultados_por_pagina[numero_pagina]
            todos_resultados.extend(resultado_pagina)
    
    print(f"✅ Total de aulas coletadas: {len(todos_resultados)}")
    
    # Coletar erros
    erros_encontrados = []
    try:
        while True:
            numero_pagina, erro = erro_queue.get_nowait()
            erros_encontrados.append(f"Página {numero_pagina}: {erro}")
    except queue.Empty:
        pass
    
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
        except:
            print("⚠️ Não foi possível ordenar por data")
        
        body = {
            "tipo": "historico_aulas_otimizado",
            "dados": todos_resultados,
            "headers": headers,
            "resumo": {
                "total_aulas": len(todos_resultados),
                "tempo_processamento": f"{(time.time() - tempo_inicio) / 60:.1f} minutos",
                "paginas_processadas": total_paginas,
                "modo": "paralelo_cookies_compartilhados",
                "processos_simultaneos": total_paginas,
                "login_unico": True
            }
        }
        
        try:
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
            print("✅ Dados enviados para Google Sheets!")
            print(f"Status: {resposta_post.status_code}")
            print(f"Resposta: {resposta_post.text}")
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
    
    # FASE 5: Resumo Final
    print("\n" + "=" * 60)
    print("📈 RESUMO DA EXECUÇÃO OTIMIZADA")
    print("=" * 60)
    
    tempo_total = (time.time() - tempo_inicio) / 60
    
    print(f"🎯 Total de aulas processadas: {len(todos_resultados)}")
    print(f"📄 Páginas processadas: {total_paginas}")
    print(f"⚡ Modo: Paralelo Verdadeiro (Login Compartilhado)")
    print(f"🚀 Processos simultâneos: {total_paginas}")
    print(f"⏱️ Tempo total: {tempo_total:.1f} minutos")
    
    # Calcular economia vs versão original
    tempo_sequencial_estimado = tempo_total * total_paginas
    economia_tempo = tempo_sequencial_estimado - tempo_total
    print(f"💡 Economia vs sequencial: {economia_tempo:.1f} minutos")
    
    # Estatísticas detalhadas
    if todos_resultados:
        total_presentes = sum(len(linha[4].split('; ')) if linha[4] else 0 for linha in todos_resultados)
        total_ausentes = sum(len(linha[6].split('; ')) if linha[6] else 0 for linha in todos_resultados)
        aulas_com_ata = sum(1 for linha in todos_resultados if linha[9] == "OK")
        
        print(f"👥 Total de presenças: {total_presentes}")
        print(f"❌ Total de ausências: {total_ausentes}")
        print(f"📝 Aulas com ATA: {aulas_com_ata}/{len(todos_resultados)} ({(aulas_com_ata/len(todos_resultados)*100):.1f}%)")
        
        # Análise por mês/ano
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
            print(f"📅 Período: {data_mais_antiga.strftime('%d/%m/%Y')} a {data_mais_recente.strftime('%d/%m/%Y')}")
    
    # Mostrar erros se houver
    if erros_encontrados:
        print("\n⚠️ ERROS ENCONTRADOS:")
        for erro in erros_encontrados[:10]:  # Mostrar apenas os primeiros 10 erros
            print(f"   • {erro}")
        if len(erros_encontrados) > 10:
            print(f"   • ... e mais {len(erros_encontrados) - 10} erros")
    
    # Análise de eficiência
    print(f"\n🔬 ANÁLISE DE EFICIÊNCIA:")
    print(f"   • Login único: ✅ (vs {total_paginas} logins na versão original)")
    print(f"   • Paralelização: ✅ (vs processamento sequencial)")
    print(f"   • Reutilização de sessão: ✅ (cookies compartilhados)")
    print(f"   • Processos simultâneos: {total_paginas} (máximo possível)")
    
    if tempo_total > 0:
        aulas_por_minuto = len(todos_resultados) / tempo_total
        print(f"   • Velocidade: {aulas_por_minuto:.1f} aulas/minuto")
        
        if total_paginas > 0:
            tempo_medio_por_pagina = tempo_total / total_paginas
            print(f"   • Tempo médio por página: {tempo_medio_por_pagina:.2f} minutos")

if __name__ == "__main__":
    main()
