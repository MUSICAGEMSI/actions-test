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
from multiprocessing import Queue, Process
import queue

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

# Configurações de paralelização
MAX_PROCESSOS_SIMULTANEOS = 4  # Número máximo de processos ao mesmo tempo
TIMEOUT_POR_PAGINA = 300  # Timeout máximo por página (5 minutos)

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para usar em requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def extrair_detalhes_aula(session, aula_id):
    """Extrai detalhes da aula via requests para verificar ATA"""
    try:
        url_detalhes = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/aulas_abertas/listagem',
        }
        
        resp = session.get(url_detalhes, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            if "ATA DA AULA" in resp.text:
                return "OK"
            else:
                return "FANTASMA"
        
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

def navegar_para_historico_aulas(pagina):
    """Navega pelos menus para chegar ao histórico de aulas"""
    try:
        print("🔍 Navegando para G.E.M...")
        
        pagina.wait_for_selector("nav", timeout=15000)
        
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
            except Exception as e:
                continue
        
        if not menu_gem_clicado:
            print("❌ Não foi possível encontrar o menu G.E.M")
            return False
        
        time.sleep(3)
        
        historico_clicado = False
        try:
            historico_link = pagina.wait_for_selector('a:has-text("Histórico de Aulas")', 
                                                     state="visible", timeout=10000)
            if historico_link:
                historico_link.click()
                historico_clicado = True
        except Exception as e:
            pass
        
        if not historico_clicado:
            try:
                elemento = pagina.query_selector('a:has-text("Histórico de Aulas")')
                if elemento:
                    pagina.evaluate("element => element.click()", elemento)
                    historico_clicado = True
            except Exception as e:
                pass
        
        if not historico_clicado:
            try:
                pagina.goto("https://musical.congregacao.org.br/aulas_abertas")
                historico_clicado = True
            except Exception as e:
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

def descobrir_total_paginas(pagina):
    """Descobre o total de páginas disponíveis"""
    try:
        print("🔍 Descobrindo total de páginas...")
        
        # Configurar para mostrar 2000 registros primeiro
        try:
            pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
            pagina.select_option('select[name="listagem_length"]', "2000")
            time.sleep(3)
            pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=15000)
        except Exception as e:
            print(f"⚠️ Erro ao configurar registros: {e}")
        
        # Método 1: Tentar extrair do elemento de paginação
        try:
            # Buscar elemento que mostra "Exibindo X até Y de Z entradas"
            info_elem = pagina.query_selector('div.dataTables_info')
            if info_elem:
                texto_info = info_elem.inner_text()
                print(f"📊 Info de paginação: {texto_info}")
                
                # Extrair total de registros
                match = re.search(r'de\s+(\d+)\s+entradas', texto_info)
                if match:
                    total_registros = int(match.group(1))
                    registros_por_pagina = 2000  # Configuramos para 2000
                    total_paginas = (total_registros + registros_por_pagina - 1) // registros_por_pagina
                    print(f"✅ Total de páginas calculado: {total_paginas}")
                    return total_paginas
        except Exception as e:
            print(f"⚠️ Método 1 falhou: {e}")
        
        # Método 2: Navegar até a última página
        try:
            # Buscar links de paginação
            links_paginacao = pagina.query_selector_all('div.dataTables_paginate a')
            if links_paginacao:
                # Tentar encontrar o último número
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
        
        # Método 3: Assumir valor padrão baseado na experiência
        print("⚠️ Não foi possível determinar total de páginas automaticamente")
        print("📝 Assumindo 30 páginas como estimativa conservadora")
        return 30
        
    except Exception as e:
        print(f"❌ Erro ao descobrir total de páginas: {e}")
        return 30

def processar_pagina_especifica(numero_pagina, resultado_queue, erro_queue):
    """Processa uma página específica em um processo dedicado"""
    try:
        print(f"🚀 [Processo {numero_pagina}] Iniciando processamento da página {numero_pagina}")
        
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
            pagina.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
            })
            
            # Login
            pagina.goto(URL_INICIAL)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            
            try:
                pagina.wait_for_selector("nav", timeout=15000)
            except PlaywrightTimeoutError:
                raise Exception(f"Falha no login no processo {numero_pagina}")
            
            # Navegar para histórico
            if not navegar_para_historico_aulas(pagina):
                raise Exception(f"Falha na navegação para histórico no processo {numero_pagina}")
            
            # Configurar para 2000 registros
            try:
                pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
                pagina.select_option('select[name="listagem_length"]', "2000")
                time.sleep(3)
                pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=15000)
            except Exception as e:
                print(f"⚠️ [Processo {numero_pagina}] Erro ao configurar registros: {e}")
            
            # Navegar para a página específica
            if numero_pagina > 1:
                print(f"🔄 [Processo {numero_pagina}] Navegando para página {numero_pagina}...")
                try:
                    for tentativa in range(3):  # 3 tentativas
                        try:
                            # Método 1: Usar campo de input de página (se existir)
                            input_pagina = pagina.query_selector('input[type="number"][aria-controls="listagem"]')
                            if input_pagina:
                                input_pagina.fill(str(numero_pagina))
                                pagina.keyboard.press("Enter")
                                break
                            
                            # Método 2: Clicar nos botões de paginação
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
                            print(f"⚠️ [Processo {numero_pagina}] Tentativa {tentativa + 1} falhou: {nav_error}")
                            if tentativa < 2:
                                time.sleep(3)
                            else:
                                raise nav_error
                    
                    # Aguardar nova página carregar
                    time.sleep(3)
                    pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=10000)
                    
                except Exception as e:
                    raise Exception(f"Erro ao navegar para página {numero_pagina}: {e}")
            
            # Criar sessão requests com cookies
            cookies_dict = extrair_cookies_playwright(pagina)
            session = requests.Session()
            session.cookies.update(cookies_dict)
            
            # Processar aulas da página
            resultado_pagina = []
            deve_parar = False
            
            # Aguardar linhas carregarem
            try:
                pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=10000)
                time.sleep(2)
            except:
                print(f"⚠️ [Processo {numero_pagina}] Timeout aguardando linhas")
            
            total_linhas = contar_linhas_na_pagina(pagina)
            
            if total_linhas == 0:
                print(f"🏁 [Processo {numero_pagina}] Página {numero_pagina} não tem linhas")
                navegador.close()
                resultado_queue.put((numero_pagina, []))
                return
            
            print(f"📊 [Processo {numero_pagina}] Página {numero_pagina}: {total_linhas} aulas encontradas")
            
            # Processar cada linha da página
            for i in range(total_linhas):
                dados_aula, deve_parar_ano = extrair_dados_de_linha_por_indice(pagina, i)
                
                if deve_parar_ano:
                    print(f"🛑 [Processo {numero_pagina}] Encontrado ano 2024 - parando processo")
                    deve_parar = True
                    break
                
                if not dados_aula:
                    continue
                
                print(f"🎯 [Processo {numero_pagina}] Página {numero_pagina}, Aula {i+1}/{total_linhas}: {dados_aula['data']}")
                
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
                        
                        total_alunos = len(freq_data['presentes_ids']) + len(freq_data['ausentes_ids'])
                        print(f"✓ [Processo {numero_pagina}] {len(freq_data['presentes_ids'])} presentes, {len(freq_data['ausentes_ids'])} ausentes - ATA: {ata_status}")
                    
                    else:
                        print(f"❌ [Processo {numero_pagina}] Falha ao clicar no botão de frequência")
                        
                except Exception as e:
                    print(f"⚠️ [Processo {numero_pagina}] Erro ao processar aula: {e}")
                    continue
                
                time.sleep(0.5)
                
                if deve_parar:
                    break
            
            print(f"✅ [Processo {numero_pagina}] Página {numero_pagina} concluída: {len(resultado_pagina)} aulas processadas")
            
            navegador.close()
            resultado_queue.put((numero_pagina, resultado_pagina))
            
    except Exception as e:
        print(f"❌ [Processo {numero_pagina}] Erro crítico na página {numero_pagina}: {e}")
        erro_queue.put((numero_pagina, str(e)))

def main():
    tempo_inicio = time.time()
    
    # Descobrir total de páginas usando processo principal
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
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
        print("🔐 Fazendo login inicial para descobrir total de páginas...")
        pagina.goto(URL_INICIAL)
        
        # Login inicial
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
        
        # Navegar para histórico e descobrir total de páginas
        if not navegar_para_historico_aulas(pagina):
            print("❌ Falha na navegação inicial.")
            navegador.close()
            return
        
        total_paginas = descobrir_total_paginas(pagina)
        navegador.close()
    
    print(f"🎯 Total de páginas a processar: {total_paginas}")
    print(f"🚀 Máximo de {MAX_PROCESSOS_SIMULTANEOS} processos simultâneos")
    
    # Configurar multiprocessing
    mp.set_start_method('spawn', force=True)
    
    # Queues para comunicação entre processos
    resultado_queue = Queue()
    erro_queue = Queue()
    
    # Criar e iniciar processos
    processos = []
    todos_resultados = []
    
    # Processar páginas em lotes
    for lote_inicio in range(1, total_paginas + 1, MAX_PROCESSOS_SIMULTANEOS):
        lote_fim = min(lote_inicio + MAX_PROCESSOS_SIMULTANEOS - 1, total_paginas)
        paginas_do_lote = list(range(lote_inicio, lote_fim + 1))
        
        print(f"\n🔄 Processando lote: páginas {lote_inicio} a {lote_fim}")
        
        # Iniciar processos para o lote
        processos_lote = []
        for numero_pagina in paginas_do_lote:
            processo = Process(
                target=processar_pagina_especifica,
                args=(numero_pagina, resultado_queue, erro_queue)
            )
            processo.start()
            processos_lote.append(processo)
            time.sleep(2)  # Delay entre inicialização dos processos
        
        # Aguardar conclusão dos processos do lote
        for processo in processos_lote:
            processo.join(timeout=TIMEOUT_POR_PAGINA)
            if processo.is_alive():
                print(f"⚠️ Processo {processo.pid} excedeu timeout, terminando...")
                processo.terminate()
                processo.join()
        
        # Coletar resultados do lote
        resultados_lote = {}
        try:
            while True:
                numero_pagina, resultado_pagina = resultado_queue.get_nowait()
                resultados_lote[numero_pagina] = resultado_pagina
        except queue.Empty:
            pass
        
        # Adicionar resultados na ordem das páginas
        for numero_pagina in paginas_do_lote:
            if numero_pagina in resultados_lote:
                resultado_pagina = resultados_lote[numero_pagina]
                todos_resultados.extend(resultado_pagina)
                print(f"✅ Página {numero_pagina} finalizada: {len(resultado_pagina)} aulas")
        
        print(f"✅ Lote concluído. Total acumulado: {len(todos_resultados)} aulas")
    
    print(f"\n📊 Coleta paralela finalizada! Total de aulas processadas: {len(todos_resultados)}")
    
    # Preparar dados para envio
    headers = [
        "CONGREGAÇÃO", "CURSO", "TURMA", "DATA", "PRESENTES IDs", 
        "PRESENTES Nomes", "AUSENTES IDs", "AUSENTES Nomes", "TEM PRESENÇA", "ATA DA AULA"
    ]
    
    # Ordenar resultados por data (opcional)
    try:
        todos_resultados.sort(key=lambda x: datetime.strptime(x[3], "%d/%m/%Y"), reverse=True)
    except:
        print("⚠️ Não foi possível ordenar por data")
    
    body = {
        "tipo": "historico_aulas_paralelo",
        "dados": todos_resultados,
        "headers": headers,
        "resumo": {
            "total_aulas": len(todos_resultados),
            "tempo_processamento": f"{(time.time() - tempo_inicio) / 60:.1f} minutos",
            "paginas_processadas": total_paginas,
            "modo": "paralelo_multiprocessing"
        }
    }
    
    # Enviar dados para Apps Script
    if todos_resultados:
        try:
            print("📤 Enviando dados para Google Sheets...")
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
            print("✅ Dados enviados!")
            print("Status code:", resposta_post.status_code)
            print("Resposta do Apps Script:", resposta_post.text)
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
    
    # Resumo final
    print("\n📈 RESUMO DA COLETA PARALELA MULTIPROCESSING:")
    print(f"   🎯 Total de aulas: {len(todos_resultados)}")
    print(f"   📄 Páginas processadas: {total_paginas}")
    print(f"   🚀 Modo: Paralelo Multiprocessing ({MAX_PROCESSOS_SIMULTANEOS} processos simultâneos)")
    print(f"   ⏱️ Tempo total: {(time.time() - tempo_inicio) / 60:.1f} minutos")
    
    tempo_sequencial_estimado = (time.time() - tempo_inicio) * MAX_PROCESSOS_SIMULTANEOS
    economia_tempo = ((tempo_sequencial_estimado - (time.time() - tempo_inicio)) / 60)
    print(f"   💡 Economia estimada: {economia_tempo:.1f} minutos vs processamento sequencial")
    
    if todos_resultados:
        total_presentes = sum(len(linha[4].split('; ')) if linha[4] else 0 for linha in todos_resultados)
        total_ausentes = sum(len(linha[6].split('; ')) if linha[6] else 0 for linha in todos_resultados)
        aulas_com_ata = sum(1 for linha in todos_resultados if linha[9] == "OK")
        
        print(f"   👥 Total de presenças registradas: {total_presentes}")
        print(f"   ❌ Total de ausências registradas: {total_ausentes}")
        print(f"   📝 Aulas com ATA: {aulas_com_ata}/{len(todos_resultados)}")
    
    # Mostrar erros se houver
    erros_encontrados = []
    try:
        while True:
            numero_pagina, erro = erro_queue.get_nowait()
            erros_encontrados.append(f"Página {numero_pagina}: {erro}")
    except queue.Empty:
        pass
    
    if erros_encontrados:
        print("\n⚠️ ERROS ENCONTRADOS:")
        for erro in erros_encontrados:
            print(f"   • {erro}")

if __name__ == "__main__":
    main()
