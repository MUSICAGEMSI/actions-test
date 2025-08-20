# script_historico_aulas_paralelo.py
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

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_PAINEL = "https://musical.congregacao.org.br/painel"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

# Lock para thread-safe printing e resultados
print_lock = threading.Lock()
resultado_global = []
resultado_lock = threading.Lock()

def safe_print(message):
    """Print thread-safe"""
    with print_lock:
        print(message)

def adicionar_resultado(linhas):
    """Adiciona resultados de forma thread-safe"""
    with resultado_lock:
        resultado_global.extend(linhas)

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
        safe_print(f"⚠️ Erro ao extrair detalhes da aula {aula_id}: {e}")
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
        safe_print(f"⚠️ Erro ao processar frequência: {e}")
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
        safe_print(f"⚠️ Erro ao extrair dados da linha {indice_linha}: {e}")
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
        safe_print(f"⚠️ Erro ao clicar no botão da linha {indice_linha}: {e}")
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
        safe_print("🔍 Navegando para G.E.M...")
        
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
                    safe_print(f"✅ Menu G.E.M encontrado: {seletor}")
                    elemento_gem.click()
                    menu_gem_clicado = True
                    break
            except Exception as e:
                continue
        
        if not menu_gem_clicado:
            safe_print("❌ Não foi possível encontrar o menu G.E.M")
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
        safe_print(f"❌ Erro durante navegação: {e}")
        return False

def ir_para_pagina_especifica(pagina, numero_pagina):
    """Navega para uma página específica do histórico"""
    try:
        if numero_pagina == 1:
            return True  # Já estamos na primeira página
        
        # Procurar o link da página específica
        link_pagina = pagina.query_selector(f'a:has-text("{numero_pagina}")')
        
        if link_pagina:
            safe_print(f"📄 Navegando para página {numero_pagina}...")
            link_pagina.click()
            
            # Aguardar nova página carregar
            time.sleep(3)
            
            try:
                pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=10000)
                safe_print(f"✅ Página {numero_pagina} carregada!")
                return True
            except:
                safe_print(f"⚠️ Timeout aguardando página {numero_pagina}")
                return True  # Continuar mesmo assim
                
        else:
            safe_print(f"❌ Link para página {numero_pagina} não encontrado")
            return False
            
    except Exception as e:
        safe_print(f"❌ Erro ao navegar para página {numero_pagina}: {e}")
        return False

def processar_pagina_worker(navegador_context, numero_pagina, cookies_dict):
    """Worker que processa uma página específica"""
    thread_id = threading.current_thread().name
    safe_print(f"🔄 [{thread_id}] Iniciando processamento da página {numero_pagina}")
    
    try:
        # Criar nova página no contexto do navegador
        pagina = navegador_context.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
        # Navegar direto para o painel
        safe_print(f"🌐 [{thread_id}] Navegando para painel...")
        pagina.goto(URL_PAINEL)
        
        # Aguardar carregamento
        time.sleep(2)
        
        # Navegar para histórico
        if not navegar_para_historico_aulas(pagina):
            safe_print(f"❌ [{thread_id}] Falha ao navegar para histórico")
            pagina.close()
            return []
        
        # Configurar para 100 registros
        try:
            pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
            pagina.select_option('select[name="listagem_length"]', "100")
            time.sleep(3)
            
            # Aguardar recarregamento
            pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=15000)
            safe_print(f"✅ [{thread_id}] Configurado para 100 registros")
            
        except Exception as e:
            safe_print(f"⚠️ [{thread_id}] Erro ao configurar registros: {e}")
        
        # Navegar para página específica
        if not ir_para_pagina_especifica(pagina, numero_pagina):
            safe_print(f"❌ [{thread_id}] Falha ao ir para página {numero_pagina}")
            pagina.close()
            return []
        
        # Aguardar 5 segundos como solicitado
        safe_print(f"⏳ [{thread_id}] Aguardando 5 segundos antes da coleta...")
        time.sleep(5)
        
        # Criar sessão requests com cookies
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        resultado_pagina = []
        
        # Contar linhas na página
        total_linhas = contar_linhas_na_pagina(pagina)
        
        if total_linhas == 0:
            safe_print(f"❌ [{thread_id}] Página {numero_pagina} não tem linhas")
            pagina.close()
            return []
        
        safe_print(f"📊 [{thread_id}] Processando {total_linhas} aulas na página {numero_pagina}")
        
        # Processar cada linha
        deve_parar = False
        for i in range(total_linhas):
            if deve_parar:
                break
                
            dados_aula, deve_parar_ano = extrair_dados_de_linha_por_indice(pagina, i)
            
            if deve_parar_ano:
                safe_print(f"🛑 [{thread_id}] Encontrado 2024 na página {numero_pagina} - finalizando!")
                deve_parar = True
                break
            
            if not dados_aula:
                continue
            
            safe_print(f"🎯 [{thread_id}] P{numero_pagina} Aula {i+1}/{total_linhas}: {dados_aula['data']} - {dados_aula['curso']}")
            
            try:
                # Fechar modal anterior se existir
                try:
                    pagina.wait_for_selector("#modalFrequencia", state="hidden", timeout=3000)
                except:
                    try:
                        btn_fechar = pagina.query_selector('button[data-dismiss="modal"]')
                        if btn_fechar:
                            btn_fechar.click()
                        else:
                            pagina.evaluate("$('#modalFrequencia').modal('hide')")
                        pagina.wait_for_selector("#modalFrequencia", state="hidden", timeout=5000)
                    except:
                        pagina.keyboard.press("Escape")
                        time.sleep(1)
                
                # Clicar no botão de frequência
                if clicar_botao_frequencia_por_indice(pagina, i):
                    time.sleep(1)
                    
                    # Processar frequência
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
                    
                    # Obter ATA
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
                    
                    # Mostrar resumo
                    total_alunos = len(freq_data['presentes_ids']) + len(freq_data['ausentes_ids'])
                    safe_print(f"✓ [{thread_id}] P{numero_pagina}: {len(freq_data['presentes_ids'])} presentes, {len(freq_data['ausentes_ids'])} ausentes - ATA: {ata_status}")
                
                else:
                    safe_print(f"❌ [{thread_id}] Falha ao clicar no botão de frequência")
                    
            except Exception as e:
                safe_print(f"⚠️ [{thread_id}] Erro ao processar aula: {e}")
                continue
            
            time.sleep(0.5)
        
        safe_print(f"✅ [{thread_id}] Página {numero_pagina} finalizada! {len(resultado_pagina)} aulas processadas")
        
        pagina.close()
        return resultado_pagina
        
    except Exception as e:
        safe_print(f"❌ [{thread_id}] Erro geral na página {numero_pagina}: {e}")
        return []

def descobrir_total_paginas(pagina_principal):
    """Descobre quantas páginas existem no total"""
    try:
        # Procurar por links de paginação
        links_paginacao = pagina_principal.query_selector_all("ul.pagination li a")
        
        numeros_pagina = []
        for link in links_paginacao:
            texto = link.inner_text().strip()
            if texto.isdigit():
                numeros_pagina.append(int(texto))
        
        if numeros_pagina:
            max_pagina = max(numeros_pagina)
            safe_print(f"📄 Total de páginas descobertas: {max_pagina}")
            return max_pagina
        
        # Se não encontrar, assumir pelo menos algumas páginas para começar
        safe_print("⚠️ Não foi possível determinar total de páginas, assumindo 50")
        return 50
        
    except Exception as e:
        safe_print(f"⚠️ Erro ao descobrir total de páginas: {e}")
        return 50

def main():
    tempo_inicio = time.time()
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        contexto = navegador.new_context()
        pagina_principal = contexto.new_page()
        
        pagina_principal.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
        safe_print("🔐 Fazendo login...")
        pagina_principal.goto(URL_INICIAL)
        
        # Login
        pagina_principal.fill('input[name="login"]', EMAIL)
        pagina_principal.fill('input[name="password"]', SENHA)
        pagina_principal.click('button[type="submit"]')
        
        try:
            pagina_principal.wait_for_selector("nav", timeout=15000)
            safe_print("✅ Login realizado com sucesso!")
        except PlaywrightTimeoutError:
            safe_print("❌ Falha no login. Verifique suas credenciais.")
            navegador.close()
            return
        
        # Navegar para histórico na página principal
        if not navegar_para_historico_aulas(pagina_principal):
            safe_print("❌ Falha na navegação para histórico de aulas.")
            navegador.close()
            return
        
        # Configurar para 100 registros na página principal
        try:
            pagina_principal.wait_for_selector('select[name="listagem_length"]', timeout=10000)
            pagina_principal.select_option('select[name="listagem_length"]', "100")
            time.sleep(3)
            pagina_principal.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=15000)
            safe_print("✅ Página principal configurada para 100 registros")
        except Exception as e:
            safe_print(f"⚠️ Erro ao configurar registros: {e}")
        
        # Descobrir total de páginas
        total_paginas = descobrir_total_paginas(pagina_principal)
        
        # Extrair cookies para sessões requests
        cookies_dict = extrair_cookies_playwright(pagina_principal)
        
        safe_print("🚀 Iniciando processamento paralelo...")
        safe_print(f"📄 Processaremos até {total_paginas} páginas ou até encontrar 2024")
        
        # Processar páginas em paralelo
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Submeter páginas para processamento
            futures = []
            
            for pagina_num in range(1, total_paginas + 1):
                future = executor.submit(processar_pagina_worker, contexto, pagina_num, cookies_dict)
                futures.append((pagina_num, future))
                
                # Adicionar delay entre submissões para não sobrecarregar
                time.sleep(5)  # 5 segundos entre cada nova aba como solicitado
            
            # Coletar resultados conforme ficam prontos
            for pagina_num, future in futures:
                try:
                    resultado_pagina = future.result(timeout=300)  # 5 minutos timeout por página
                    if resultado_pagina:
                        adicionar_resultado(resultado_pagina)
                        safe_print(f"📊 Página {pagina_num} adicionada ao resultado global: {len(resultado_pagina)} aulas")
                    
                    # Se encontrou 2024 em qualquer thread, parar de processar
                    # (isso seria mais complexo de implementar completamente, mas a lógica individual já para)
                    
                except Exception as e:
                    safe_print(f"⚠️ Erro ao processar página {pagina_num}: {e}")
        
        safe_print(f"\n📊 Coleta finalizada! Total de aulas processadas: {len(resultado_global)}")
        
        # Preparar dados para envio
        headers = [
            "CONGREGAÇÃO", "CURSO", "TURMA", "DATA", "PRESENTES IDs", 
            "PRESENTES Nomes", "AUSENTES IDs", "AUSENTES Nomes", "TEM PRESENÇA", "ATA DA AULA"
        ]
        
        body = {
            "tipo": "historico_aulas_paralelo",
            "dados": resultado_global,
            "headers": headers,
            "resumo": {
                "total_aulas": len(resultado_global),
                "tempo_processamento": f"{(time.time() - tempo_inicio) / 60:.1f} minutos",
                "paginas_processadas": f"Processamento paralelo até página {total_paginas}"
            }
        }
        
        # Enviar dados para Apps Script
        if resultado_global:
            try:
                safe_print("📤 Enviando dados para Google Sheets...")
                resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
                safe_print("✅ Dados enviados!")
                safe_print("Status code:", resposta_post.status_code)
                safe_print("Resposta do Apps Script:", resposta_post.text)
            except Exception as e:
                safe_print(f"❌ Erro ao enviar para Apps Script: {e}")
        
        # Resumo final
        safe_print("\n📈 RESUMO DA COLETA PARALELA:")
        safe_print(f"   🎯 Total de aulas: {len(resultado_global)}")
        safe_print(f"   ⏱️ Tempo total: {(time.time() - tempo_inicio) / 60:.1f} minutos")
        
        if resultado_global:
            total_presentes = sum(len(linha[4].split('; ')) if linha[4] else 0 for linha in resultado_global)
            total_ausentes = sum(len(linha[6].split('; ')) if linha[6] else 0 for linha in resultado_global)
            aulas_com_ata = sum(1 for linha in resultado_global if linha[9] == "OK")
            
            safe_print(f"   👥 Total de presenças registradas: {total_presentes}")
            safe_print(f"   ❌ Total de ausências registradas: {total_ausentes}")
            safe_print(f"   📝 Aulas com ATA: {aulas_com_ata}/{len(resultado_global)}")
        
        navegador.close()

if __name__ == "__main__":
    main()
