from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import aiohttp
import asyncio
import json
from bs4 import BeautifulSoup
from datetime import datetime
import time

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

# Configurações de paralelização
MAX_GUIAS_SIMULTANEAS = 8  # Número máximo de guias abertas ao mesmo tempo
INTERVALO_ABERTURA_GUIAS = 2  # Segundos entre abertura de cada guia (reduzido para async)
TIMEOUT_POR_PAGINA = 300  # Timeout máximo por página (5 minutos)

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def extrair_cookies_playwright(cookies):
    """Converte cookies do Playwright para formato do aiohttp"""
    return {cookie['name']: cookie['value'] for cookie in cookies}

async def extrair_detalhes_aula(session, aula_id):
    """Extrai detalhes da aula via aiohttp para verificar ATA"""
    try:
        url_detalhes = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/aulas_abertas/listagem',
        }
        
        async with session.get(url_detalhes, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                content = await resp.text()
                if "ATA DA AULA" in content:
                    return "OK"
                else:
                    return "FANTASMA"
            
            return "ERRO"
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair detalhes da aula {aula_id}: {e}")
        return "ERRO"

async def processar_frequencia_modal(pagina, aula_id, professor_id):
    """Processa a frequência após abrir o modal"""
    try:
        await pagina.wait_for_selector("table.table-bordered tbody tr", timeout=10000)
        
        presentes_ids = []
        presentes_nomes = []
        ausentes_ids = []
        ausentes_nomes = []
        
        linhas = await pagina.query_selector_all("table.table-bordered tbody tr")
        
        for linha in linhas:
            nome_cell = await linha.query_selector("td:first-child")
            nome_completo = (await nome_cell.inner_text()).strip() if nome_cell else ""
            
            if not nome_completo:
                continue
            
            link_presenca = await linha.query_selector("td:last-child a")
            
            if link_presenca:
                id_membro = await link_presenca.get_attribute("data-id-membro")
                
                if not id_membro:
                    continue
                
                icone = await link_presenca.query_selector("i")
                if icone:
                    classes = await icone.get_attribute("class")
                    
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

async def extrair_dados_de_linha_por_indice(pagina, indice_linha):
    """Extrai dados de uma linha específica pelo índice"""
    try:
        linhas = await pagina.query_selector_all("table tbody tr")
        
        if indice_linha >= len(linhas):
            return None, False
        
        linha = linhas[indice_linha]
        colunas = await linha.query_selector_all("td")
        
        if len(colunas) >= 6:
            data_aula = (await colunas[1].inner_text()).strip()
            
            if "2024" in data_aula:
                return None, True  # Sinal para parar
            
            congregacao = (await colunas[2].inner_text()).strip()
            curso = (await colunas[3].inner_text()).strip()
            turma = (await colunas[4].inner_text()).strip()
            
            btn_freq = await linha.query_selector("button[onclick*='visualizarFrequencias']")
            if btn_freq:
                onclick = await btn_freq.get_attribute("onclick")
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

async def clicar_botao_frequencia_por_indice(pagina, indice_linha):
    """Clica no botão de frequência de uma linha específica pelo índice"""
    try:
        linhas = await pagina.query_selector_all("table tbody tr")
        
        if indice_linha >= len(linhas):
            return False
        
        linha = linhas[indice_linha]
        btn_freq = await linha.query_selector("button[onclick*='visualizarFrequencias']")
        
        if btn_freq:
            await btn_freq.click()
            return True
        
        return False
        
    except Exception as e:
        print(f"⚠️ Erro ao clicar no botão da linha {indice_linha}: {e}")
        return False

async def contar_linhas_na_pagina(pagina):
    """Conta quantas linhas existem na página atual"""
    try:
        linhas = await pagina.query_selector_all("table tbody tr")
        return len(linhas)
    except:
        return 0

async def navegar_para_historico_aulas(pagina):
    """Navega pelos menus para chegar ao histórico de aulas"""
    try:
        print("🔍 Navegando para G.E.M...")
        
        await pagina.wait_for_selector("nav", timeout=15000)
        
        seletores_gem = [
            'a:has-text("G.E.M")',
            'a:has(.fa-graduation-cap)',
            'a[href="#"]:has(span:text-is("G.E.M"))',
            'a:has(span):has-text("G.E.M")'
        ]
        
        menu_gem_clicado = False
        for seletor in seletores_gem:
            try:
                elemento_gem = await pagina.query_selector(seletor)
                if elemento_gem:
                    await elemento_gem.click()
                    menu_gem_clicado = True
                    break
            except Exception as e:
                continue
        
        if not menu_gem_clicado:
            print("❌ Não foi possível encontrar o menu G.E.M")
            return False
        
        await asyncio.sleep(3)
        
        historico_clicado = False
        try:
            historico_link = await pagina.wait_for_selector('a:has-text("Histórico de Aulas")', 
                                                           state="visible", timeout=10000)
            if historico_link:
                await historico_link.click()
                historico_clicado = True
        except Exception as e:
            pass
        
        if not historico_clicado:
            try:
                elemento = await pagina.query_selector('a:has-text("Histórico de Aulas")')
                if elemento:
                    await pagina.evaluate("element => element.click()", elemento)
                    historico_clicado = True
            except Exception as e:
                pass
        
        if not historico_clicado:
            try:
                await pagina.goto("https://musical.congregacao.org.br/aulas_abertas")
                historico_clicado = True
            except Exception as e:
                pass
        
        if not historico_clicado:
            return False
        
        try:
            await pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=20000)
            return True
        except PlaywrightTimeoutError:
            try:
                await pagina.wait_for_selector("table", timeout=5000)
                return True
            except:
                return False
                
    except Exception as e:
        print(f"❌ Erro durante navegação: {e}")
        return False

async def descobrir_total_paginas(pagina):
    """Descobre o total de páginas disponíveis"""
    try:
        print("🔍 Descobrindo total de páginas...")
        
        # Configurar para mostrar 2000 registros primeiro
        try:
            await pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
            await pagina.select_option('select[name="listagem_length"]', "2000")
            await asyncio.sleep(3)
            await pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=15000)
        except Exception as e:
            print(f"⚠️ Erro ao configurar registros: {e}")
        
        # Método 1: Tentar extrair do elemento de paginação
        try:
            info_elem = await pagina.query_selector('div.dataTables_info')
            if info_elem:
                texto_info = await info_elem.inner_text()
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
            links_paginacao = await pagina.query_selector_all('div.dataTables_paginate a')
            if links_paginacao:
                numeros = []
                for link in links_paginacao:
                    texto = (await link.inner_text()).strip()
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

async def processar_pagina_especifica(navegador, numero_pagina, thread_id, semaforo):
    """Processa uma página específica em uma guia dedicada"""
    async with semaforo:  # Limita o número de guias simultâneas
        try:
            print(f"🚀 [Thread {thread_id}] Iniciando processamento da página {numero_pagina}")
            
            # Criar novo contexto e página
            context = await navegador.new_context()
            pagina = await context.new_page()
            await pagina.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
            })
            
            # Login
            await pagina.goto(URL_INICIAL)
            await pagina.fill('input[name="login"]', EMAIL)
            await pagina.fill('input[name="password"]', SENHA)
            await pagina.click('button[type="submit"]')
            
            try:
                await pagina.wait_for_selector("nav", timeout=15000)
            except PlaywrightTimeoutError:
                raise Exception(f"Falha no login na thread {thread_id}")
            
            # Navegar para histórico
            if not await navegar_para_historico_aulas(pagina):
                raise Exception(f"Falha na navegação para histórico na thread {thread_id}")
            
            # Configurar para 2000 registros
            try:
                await pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
                await pagina.select_option('select[name="listagem_length"]', "2000")
                await asyncio.sleep(3)
                await pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=15000)
            except Exception as e:
                print(f"⚠️ [Thread {thread_id}] Erro ao configurar registros: {e}")
            
            # Navegar para a página específica
            if numero_pagina > 1:
                print(f"🔄 [Thread {thread_id}] Navegando para página {numero_pagina}...")
                try:
                    for tentativa in range(3):  # 3 tentativas
                        try:
                            # Método 1: Usar campo de input de página (se existir)
                            input_pagina = await pagina.query_selector('input[type="number"][aria-controls="listagem"]')
                            if input_pagina:
                                await input_pagina.fill(str(numero_pagina))
                                await pagina.keyboard.press("Enter")
                                break
                            
                            # Método 2: Clicar nos botões de paginação
                            for i in range(numero_pagina - 1):
                                btn_proximo = await pagina.query_selector("a:has(i.fa-chevron-right)")
                                if btn_proximo:
                                    parent = await btn_proximo.query_selector("..")
                                    parent_class = await parent.get_attribute("class") if parent else ""
                                    if "disabled" not in parent_class:
                                        await btn_proximo.click()
                                        await asyncio.sleep(2)
                                    else:
                                        break
                                else:
                                    break
                            break
                            
                        except Exception as nav_error:
                            print(f"⚠️ [Thread {thread_id}] Tentativa {tentativa + 1} falhou: {nav_error}")
                            if tentativa < 2:
                                await asyncio.sleep(3)
                            else:
                                raise nav_error
                    
                    # Aguardar nova página carregar
                    await asyncio.sleep(3)
                    await pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=10000)
                    
                except Exception as e:
                    raise Exception(f"Erro ao navegar para página {numero_pagina}: {e}")
            
            # Criar sessão aiohttp com cookies
            cookies = await context.cookies()
            cookies_dict = extrair_cookies_playwright(cookies)
            
            async with aiohttp.ClientSession(cookies=cookies_dict) as session:
                # Processar aulas da página
                resultado_pagina = []
                deve_parar = False
                
                # Aguardar linhas carregarem
                try:
                    await pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=10000)
                    await asyncio.sleep(2)
                except:
                    print(f"⚠️ [Thread {thread_id}] Timeout aguardando linhas")
                
                total_linhas = await contar_linhas_na_pagina(pagina)
                
                if total_linhas == 0:
                    print(f"🏁 [Thread {thread_id}] Página {numero_pagina} não tem linhas")
                    await context.close()
                    return []
                
                print(f"📊 [Thread {thread_id}] Página {numero_pagina}: {total_linhas} aulas encontradas")
                
                # Processar cada linha da página
                for i in range(total_linhas):
                    dados_aula, deve_parar_ano = await extrair_dados_de_linha_por_indice(pagina, i)
                    
                    if deve_parar_ano:
                        print(f"🛑 [Thread {thread_id}] Encontrado ano 2024 - parando thread")
                        deve_parar = True
                        break
                    
                    if not dados_aula:
                        continue
                    
                    print(f"🎯 [Thread {thread_id}] Página {numero_pagina}, Aula {i+1}/{total_linhas}: {dados_aula['data']}")
                    
                    try:
                        # Aguardar que não haja modal aberto
                        try:
                            await pagina.wait_for_selector("#modalFrequencia", state="hidden", timeout=3000)
                        except:
                            await pagina.keyboard.press("Escape")
                            await asyncio.sleep(1)
                        
                        # Clicar no botão de frequência
                        if await clicar_botao_frequencia_por_indice(pagina, i):
                            await asyncio.sleep(1)
                            
                            # Processar dados de frequência
                            freq_data = await processar_frequencia_modal(pagina, dados_aula['aula_id'], dados_aula['professor_id'])
                            
                            # Fechar modal
                            try:
                                btn_fechar = await pagina.query_selector('button.btn-warning[data-dismiss="modal"]:has-text("Fechar")')
                                if btn_fechar:
                                    await btn_fechar.click()
                                else:
                                    await pagina.evaluate("$('#modalFrequencia').modal('hide')")
                                
                                await pagina.wait_for_selector("#modalFrequencia", state="hidden", timeout=5000)
                            except:
                                await pagina.keyboard.press("Escape")
                            
                            await asyncio.sleep(1)
                            
                            # Obter detalhes da ATA
                            ata_status = await extrair_detalhes_aula(session, dados_aula['aula_id'])
                            
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
                            print(f"✓ [Thread {thread_id}] {len(freq_data['presentes_ids'])} presentes, {len(freq_data['ausentes_ids'])} ausentes - ATA: {ata_status}")
                        
                        else:
                            print(f"❌ [Thread {thread_id}] Falha ao clicar no botão de frequência")
                            
                    except Exception as e:
                        print(f"⚠️ [Thread {thread_id}] Erro ao processar aula: {e}")
                        continue
                    
                    await asyncio.sleep(0.5)
                    
                    if deve_parar:
                        break
            
            print(f"✅ [Thread {thread_id}] Página {numero_pagina} concluída: {len(resultado_pagina)} aulas processadas")
            
            # Fechar contexto
            await context.close()
            
            return resultado_pagina
            
        except Exception as e:
            print(f"❌ [Thread {thread_id}] Erro crítico na página {numero_pagina}: {e}")
            try:
                await context.close()
            except:
                pass
            return []

async def main():
    tempo_inicio = time.time()
    
    async with async_playwright() as p:
        navegador = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        
        # Criar contexto principal para descobrir total de páginas
        context_principal = await navegador.new_context()
        pagina_principal = await context_principal.new_page()
        await pagina_principal.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
        print("🔐 Fazendo login inicial para descobrir total de páginas...")
        await pagina_principal.goto(URL_INICIAL)
        
        # Login inicial
        await pagina_principal.fill('input[name="login"]', EMAIL)
        await pagina_principal.fill('input[name="password"]', SENHA)
        await pagina_principal.click('button[type="submit"]')
        
        try:
            await pagina_principal.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado com sucesso!")
        except PlaywrightTimeoutError:
            print("❌ Falha no login. Verifique suas credenciais.")
            await navegador.close()
            return
        
        # Navegar para histórico e descobrir total de páginas
        if not await navegar_para_historico_aulas(pagina_principal):
            print("❌ Falha na navegação inicial.")
            await navegador.close()
            return
        
        total_paginas = await descobrir_total_paginas(pagina_principal)
        
        # Fechar contexto principal
        await context_principal.close()
        
        print(f"🎯 Total de páginas a processar: {total_paginas}")
        print(f"🚀 Máximo de {MAX_GUIAS_SIMULTANEAS} guias simultâneas")
        print(f"⏱️ Intervalo entre abertura de guias: {INTERVALO_ABERTURA_GUIAS}s")
        
        # Criar semáforo para limitar guias simultâneas
        semaforo = asyncio.Semaphore(MAX_GUIAS_SIMULTANEAS)
        
        # Criar tasks para processar todas as páginas
        tasks = []
        for numero_pagina in range(1, total_paginas + 1):
            if numero_pagina > 1:  # Delay escalonado
                await asyncio.sleep(INTERVALO_ABERTURA_GUIAS)
            
            task = asyncio.create_task(
                processar_pagina_especifica(navegador, numero_pagina, numero_pagina, semaforo)
            )
            tasks.append(task)
        
        # Executar todas as tasks e coletar resultados
        print(f"\n🔄 Processando todas as {total_paginas} páginas simultaneamente...")
        todos_resultados = []
        
        # Aguardar conclusão de todas as tasks
        resultados_tasks = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, resultado in enumerate(resultados_tasks):
            numero_pagina = i + 1
            if isinstance(resultado, Exception):
                print(f"❌ Erro na página {numero_pagina}: {resultado}")
            else:
                todos_resultados.extend(resultado)
                print(f"✅ Página {numero_pagina} finalizada: {len(resultado)} aulas")
        
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
                "modo": "paralelo_async"
            }
        }
        
        # Enviar dados para Apps Script
        if todos_resultados:
            try:
                print("📤 Enviando dados para Google Sheets...")
                async with aiohttp.ClientSession() as session:
                    async with session.post(URL_APPS_SCRIPT, json=body, timeout=aiohttp.ClientTimeout(total=120)) as resposta:
                        resposta_texto = await resposta.text()
                        print("✅ Dados enviados!")
                        print("Status code:", resposta.status)
                        print("Resposta do Apps Script:", resposta_texto)
            except Exception as e:
                print(f"❌ Erro ao enviar para Apps Script: {e}")
        
        # Resumo final
        print("\n📈 RESUMO DA COLETA PARALELA ASYNC:")
        print(f"   🎯 Total de aulas: {len(todos_resultados)}")
        print(f"   📄 Páginas processadas: {total_paginas}")
        print(f"   🚀 Modo: Paralelo Assíncrono ({MAX_GUIAS_SIMULTANEAS} guias simultâneas)")
        print(f"   ⏱️ Tempo total: {(time.time() - tempo_inicio) / 60:.1f} minutos")
        
        if todos_resultados:
            total_presentes = sum(len(linha[4].split('; ')) if linha[4] else 0 for linha in todos_resultados)
            total_ausentes = sum(len(linha[6].split('; ')) if linha[6] else 0 for linha in todos_resultados)
            aulas_com_ata = sum(1 for linha in todos_resultados if linha[9] == "OK")
            
            print(f"   👥 Total de presenças registradas: {total_presentes}")
            print(f"   ❌ Total de ausências registradas: {total_ausentes}")
            print(f"   📝 Aulas com ATA: {aulas_com_ata}/{len(todos_resultados)}")
        
        await navegador.close()

if __name__ == "__main__":
    asyncio.run(main())
