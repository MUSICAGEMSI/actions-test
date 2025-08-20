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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_PAINEL = "https://musical.congregacao.org.br/painel"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

# Queue thread-safe para coletar resultados
resultado_global = queue.Queue()
lock = threading.Lock()

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
        return "ERRO"

def processar_frequencia_modal(pagina, aula_id, professor_id):
    """Processa a frequência após abrir o modal"""
    try:
        # Aguardar o modal carregar completamente
        pagina.wait_for_selector("table.table-bordered tbody tr", timeout=10000)
        
        presentes_ids = []
        presentes_nomes = []
        ausentes_ids = []
        ausentes_nomes = []
        
        # Extrair todas as linhas da tabela de frequência
        linhas = pagina.query_selector_all("table.table-bordered tbody tr")
        
        for linha in linhas:
            # Extrair nome do aluno
            nome_cell = linha.query_selector("td:first-child")
            nome_completo = nome_cell.inner_text().strip() if nome_cell else ""
            
            # IGNORAR linhas sem nome (vazias)
            if not nome_completo:
                continue
            
            # Extrair status de presença
            link_presenca = linha.query_selector("td:last-child a")
            
            if link_presenca:
                # Extrair ID do membro do data-id-membro
                id_membro = link_presenca.get_attribute("data-id-membro")
                
                # IGNORAR se não tem ID válido
                if not id_membro:
                    continue
                
                # Verificar se está presente ou ausente pelo ícone
                icone = link_presenca.query_selector("i")
                if icone:
                    classes = icone.get_attribute("class")
                    
                    if "fa-check text-success" in classes:
                        # Presente
                        presentes_ids.append(id_membro)
                        presentes_nomes.append(nome_completo)
                    elif "fa-remove text-danger" in classes:
                        # Ausente
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
            # Extrair data da aula
            data_aula = colunas[1].inner_text().strip()
            
            # Verificar se é 2024 - parar processamento
            if "2024" in data_aula:
                return None, True  # Sinal para parar
            
            # Extrair outros dados
            congregacao = colunas[2].inner_text().strip()
            curso = colunas[3].inner_text().strip()
            turma = colunas[4].inner_text().strip()
            
            # Extrair IDs do botão de frequência
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
        return False

def contar_linhas_na_pagina(pagina):
    """Conta quantas linhas existem na página atual"""
    try:
        linhas = pagina.query_selector_all("table tbody tr")
        return len(linhas)
    except:
        return 0

def navegar_para_historico_aulas(pagina):
    """Navega para o histórico de aulas"""
    try:
        print(f"🔍 [Thread] Navegando para G.E.M...")
        
        # Aguardar o menu carregar
        pagina.wait_for_selector("nav", timeout=15000)
        
        # Estratégias para encontrar menu G.E.M
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
            return False
        
        time.sleep(2)
        
        # Tentar clicar em Histórico de Aulas
        historico_clicado = False
        try:
            historico_link = pagina.wait_for_selector('a:has-text("Histórico de Aulas")', 
                                                     state="visible", timeout=10000)
            if historico_link:
                historico_link.click()
                historico_clicado = True
        except:
            try:
                # Forçar com JavaScript
                elemento = pagina.query_selector('a:has-text("Histórico de Aulas")')
                if elemento:
                    pagina.evaluate("element => element.click()", elemento)
                    historico_clicado = True
            except:
                # Navegação direta
                pagina.goto("https://musical.congregacao.org.br/aulas_abertas")
                historico_clicado = True
        
        if not historico_clicado:
            return False
        
        # Aguardar tabela carregar
        try:
            pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=20000)
            return True
        except:
            try:
                pagina.wait_for_selector("table", timeout=5000)
                return True
            except:
                return False
                
    except Exception as e:
        return False

def processar_pagina_especifica(navegador, numero_pagina, cookies_compartilhados):
    """Processa uma página específica do histórico"""
    print(f"🚀 [Página {numero_pagina}] Iniciando processamento...")
    
    try:
        # Criar nova aba
        contexto = navegador.new_context()
        
        # Aplicar cookies compartilhados se disponíveis
        if cookies_compartilhados:
            contexto.add_cookies(cookies_compartilhados)
        
        pagina = contexto.new_page()
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
        # Ir direto para o painel (já logado via cookies)
        print(f"🌐 [Página {numero_pagina}] Navegando para painel...")
        pagina.goto(URL_PAINEL)
        
        # Aguardar um pouco para carregar
        time.sleep(2)
        
        # Navegar para histórico
        if not navegar_para_historico_aulas(pagina):
            print(f"❌ [Página {numero_pagina}] Falha na navegação")
            contexto.close()
            return [], False
        
        # Configurar para 100 registros
        print(f"⚙️ [Página {numero_pagina}] Configurando para 100 registros...")
        try:
            pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
            pagina.select_option('select[name="listagem_length"]', "100")
            time.sleep(3)
        except Exception as e:
            print(f"⚠️ [Página {numero_pagina}] Erro ao configurar registros: {e}")
        
        # Aguardar recarregamento
        try:
            pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=15000)
        except:
            pass
        
        # Ir para página específica (se não for a primeira)
        if numero_pagina > 1:
            print(f"➡️ [Página {numero_pagina}] Navegando para página {numero_pagina}...")
            try:
                # Buscar link da página específica
                link_pagina = pagina.query_selector(f'a:has-text("{numero_pagina}")')
                if link_pagina:
                    link_pagina.click()
                    time.sleep(3)
                    
                    # Aguardar nova página carregar
                    try:
                        pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=10000)
                    except:
                        pass
                else:
                    print(f"⚠️ [Página {numero_pagina}] Link da página não encontrado")
                    contexto.close()
                    return [], False
            except Exception as e:
                print(f"⚠️ [Página {numero_pagina}] Erro ao navegar: {e}")
                contexto.close()
                return [], False
        
        # Criar sessão requests com cookies para ATA
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # Processar aulas desta página
        resultado_pagina = []
        total_linhas = contar_linhas_na_pagina(pagina)
        
        if total_linhas == 0:
            print(f"📭 [Página {numero_pagina}] Não há linhas para processar")
            contexto.close()
            return [], False
        
        print(f"📊 [Página {numero_pagina}] Encontradas {total_linhas} aulas")
        
        # Verificar se chegamos em 2024 logo de cara
        dados_primeira_aula, deve_parar_ano = extrair_dados_de_linha_por_indice(pagina, 0)
        if deve_parar_ano:
            print(f"🛑 [Página {numero_pagina}] Encontrado 2024 - finalizando!")
            contexto.close()
            return [], True
        
        # Processar cada linha
        for i in range(total_linhas):
            dados_aula, deve_parar_ano = extrair_dados_de_linha_por_indice(pagina, i)
            
            if deve_parar_ano:
                print(f"🛑 [Página {numero_pagina}] Encontrado 2024 - finalizando coleta!")
                contexto.close()
                return resultado_pagina, True
            
            if not dados_aula:
                continue
            
            print(f"   🎯 [Página {numero_pagina}] Aula {i+1}/{total_linhas}: {dados_aula['data']} - {dados_aula['curso']}")
            
            try:
                # Fechar modal anterior se existir
                try:
                    pagina.wait_for_selector("#modalFrequencia", state="hidden", timeout=2000)
                except:
                    try:
                        btn_fechar = pagina.query_selector('button[data-dismiss="modal"]')
                        if btn_fechar:
                            btn_fechar.click()
                        else:
                            pagina.evaluate("$('#modalFrequencia').modal('hide')")
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
                        fechou = False
                        
                        btn_fechar = pagina.query_selector('button.btn-warning[data-dismiss="modal"]:has-text("Fechar")')
                        if btn_fechar:
                            btn_fechar.click()
                            fechou = True
                        
                        if not fechou:
                            btn_fechar = pagina.query_selector('button[data-dismiss="modal"]')
                            if btn_fechar:
                                btn_fechar.click()
                                fechou = True
                        
                        if not fechou:
                            pagina.evaluate("$('#modalFrequencia').modal('hide')")
                        
                        try:
                            pagina.wait_for_selector("#modalFrequencia", state="hidden", timeout=3000)
                        except:
                            pass
                            
                    except:
                        pagina.keyboard.press("Escape")
                    
                    time.sleep(0.5)
                    
                    # Obter detalhes da ATA
                    ata_status = extrair_detalhes_aula(session, dados_aula['aula_id'])
                    
                    # Montar linha de resultado
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
                    
                    # Resumo da aula
                    total_alunos = len(freq_data['presentes_ids']) + len(freq_data['ausentes_ids'])
                    print(f"      ✓ [Página {numero_pagina}] {len(freq_data['presentes_ids'])} presentes, {len(freq_data['ausentes_ids'])} ausentes (Total: {total_alunos}) - ATA: {ata_status}")
                
            except Exception as e:
                print(f"⚠️ [Página {numero_pagina}] Erro ao processar aula: {e}")
                continue
            
            # Pequena pausa entre aulas
            time.sleep(0.3)
        
        print(f"✅ [Página {numero_pagina}] Concluída! {len(resultado_pagina)} aulas processadas")
        contexto.close()
        return resultado_pagina, False
        
    except Exception as e:
        print(f"❌ [Página {numero_pagina}] Erro geral: {e}")
        if 'contexto' in locals():
            contexto.close()
        return [], False

def main():
    tempo_inicio = time.time()
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        
        # PRIMEIRA FASE: Login e descobrir quantas páginas existem
        print("🔐 Fase 1: Fazendo login e descobrindo estrutura...")
        pagina_principal = navegador.new_page()
        pagina_principal.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
        # Login
        pagina_principal.goto(URL_INICIAL)
        pagina_principal.fill('input[name="login"]', EMAIL)
        pagina_principal.fill('input[name="password"]', SENHA)
        pagina_principal.click('button[type="submit"]')
        
        try:
            pagina_principal.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado com sucesso!")
        except PlaywrightTimeoutError:
            print("❌ Falha no login. Verifique suas credenciais.")
            navegador.close()
            return
        
        # Navegar para histórico de aulas
        if not navegar_para_historico_aulas(pagina_principal):
            print("❌ Falha na navegação para histórico de aulas.")
            navegador.close()
            return
        
        # Configurar para 100 registros na página principal
        print("⚙️ Configurando para 100 registros...")
        try:
            pagina_principal.wait_for_selector('select[name="listagem_length"]', timeout=10000)
            pagina_principal.select_option('select[name="listagem_length"]', "100")
            time.sleep(3)
            
            # Aguardar recarregamento
            pagina_principal.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=15000)
        except Exception as e:
            print(f"⚠️ Erro ao configurar registros: {e}")
        
        # Descobrir quantas páginas existem
        print("🔍 Descobrindo quantas páginas existem...")
        numero_paginas = 1
        try:
            # Procurar por links de paginação
            links_paginacao = pagina_principal.query_selector_all("ul.pagination li a")
            numeros = []
            
            for link in links_paginacao:
                texto = link.inner_text().strip()
                if texto.isdigit():
                    numeros.append(int(texto))
            
            if numeros:
                numero_paginas = max(numeros)
            
            print(f"📊 Encontradas {numero_paginas} páginas para processar")
        except Exception as e:
            print(f"⚠️ Erro ao descobrir páginas: {e} - assumindo 10 páginas")
            numero_paginas = 10
        
        # Extrair cookies para compartilhar com outras abas
        cookies_compartilhados = pagina_principal.context.cookies()
        pagina_principal.close()
        
        print(f"\n🚀 Fase 2: Processamento paralelo de {numero_paginas} páginas...")
        print("💡 Cada página será processada em uma aba separada!")
        
        # SEGUNDA FASE: Processamento paralelo
        resultado_final = []
        deve_parar_global = False
        
        # Função para processar com delay escalonado
        def processar_com_delay(numero_pagina):
            # Delay escalonado: página 1 = 0s, página 2 = 5s, página 3 = 10s, etc.
            delay = (numero_pagina - 1) * 5
            if delay > 0:
                print(f"⏰ [Página {numero_pagina}] Aguardando {delay}s antes de iniciar...")
                time.sleep(delay)
            
            return processar_pagina_especifica(navegador, numero_pagina, cookies_compartilhados)
        
        # Usar ThreadPoolExecutor para controlar concorrência
        max_workers = min(5, numero_paginas)  # Máximo 5 abas simultâneas
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submeter todas as tarefas
            futures = {}
            for num_pag in range(1, numero_paginas + 1):
                future = executor.submit(processar_com_delay, num_pag)
                futures[future] = num_pag
            
            # Coletar resultados conforme vão terminando
            for future in as_completed(futures):
                numero_pagina = futures[future]
                try:
                    resultado_pagina, deve_parar = future.result()
                    
                    with lock:
                        resultado_final.extend(resultado_pagina)
                        if deve_parar:
                            deve_parar_global = True
                    
                    print(f"✅ [Página {numero_pagina}] Finalizada: {len(resultado_pagina)} aulas coletadas")
                    
                    # Se encontrou 2024, cancelar tarefas restantes
                    if deve_parar_global:
                        print("🛑 Ano 2024 encontrado - cancelando páginas restantes...")
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        break
                        
                except Exception as e:
                    print(f"❌ [Página {numero_pagina}] Erro: {e}")
        
        print(f"\n📊 Coleta finalizada! Total de aulas processadas: {len(resultado_final)}")
        
        # Preparar e enviar dados
        if resultado_final:
            headers = [
                "CONGREGAÇÃO", "CURSO", "TURMA", "DATA", "PRESENTES IDs", 
                "PRESENTES Nomes", "AUSENTES IDs", "AUSENTES Nomes", "TEM PRESENÇA", "ATA DA AULA"
            ]
            
            body = {
                "tipo": "historico_aulas_paralelo",
                "dados": resultado_final,
                "headers": headers,
                "resumo": {
                    "total_aulas": len(resultado_final),
                    "tempo_processamento": f"{(time.time() - tempo_inicio) / 60:.1f} minutos",
                    "paginas_processadas": numero_paginas
                }
            }
            
            try:
                print("📤 Enviando dados para Google Sheets...")
                resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
                print("✅ Dados enviados!")
                print("Status code:", resposta_post.status_code)
                print("Resposta do Apps Script:", resposta_post.text)
            except Exception as e:
                print(f"❌ Erro ao enviar para Apps Script: {e}")
        
        # Resumo final
        print("\n🎉 RESUMO DA COLETA PARALELA:")
        print(f"   🎯 Total de aulas: {len(resultado_final)}")
        print(f"   📄 Páginas processadas: {numero_paginas}")
        print(f"   ⏱️ Tempo total: {(time.time() - tempo_inicio) / 60:.1f} minutos")
        print(f"   🚀 Velocidade: ~{len(resultado_final) / ((time.time() - tempo_inicio) / 60):.1f} aulas/min")
        
        if resultado_final:
            total_presentes = sum(len(linha[4].split('; ')) if linha[4] else 0 for linha in resultado_final)
            total_ausentes = sum(len(linha[6].split('; ')) if linha[6] else 0 for linha in resultado_final)
            aulas_com_ata = sum(1 for linha in resultado_final if linha[9] == "OK")
            
            print(f"   👥 Total de presenças registradas: {total_presentes}")
            print(f"   ❌ Total de ausências registradas: {total_ausentes}")
            print(f"   📝 Aulas com ATA: {aulas_com_ata}/{len(resultado_final)}")
        
        navegador.close()

if __name__ == "__main__":
    main()
