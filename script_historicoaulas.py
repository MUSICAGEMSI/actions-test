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
import concurrent.futures
from threading import Lock
import asyncio

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

# Lock para thread safety
resultado_lock = Lock()

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para usar em requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def extrair_frequencia_via_request(session, aula_id, professor_id):
    """Extrai frequência diretamente via requisição HTTP (mais rápido que modal)"""
    try:
        # URL que o modal usa para carregar os dados
        url_frequencia = f"https://musical.congregacao.org.br/aulas_abertas/ajax_listar_frequencias/{aula_id}/{professor_id}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'X-Requested-With': 'XMLHttpRequest',  # Importante para requisições AJAX
            'Referer': 'https://musical.congregacao.org.br/aulas_abertas',
        }
        
        resp = session.get(url_frequencia, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            # Parse do HTML retornado
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            presentes_ids = []
            presentes_nomes = []
            ausentes_ids = []
            ausentes_nomes = []
            
            # Buscar todas as linhas da tabela
            linhas = soup.find_all('tr')
            
            for linha in linhas:
                colunas = linha.find_all('td')
                if len(colunas) >= 2:  # Nome + Status
                    nome_completo = colunas[0].get_text(strip=True)
                    
                    # Ignorar linhas vazias
                    if not nome_completo:
                        continue
                    
                    # Buscar link de presença
                    link_presenca = colunas[-1].find('a')
                    if link_presenca:
                        id_membro = link_presenca.get('data-id-membro')
                        
                        if not id_membro:
                            continue
                        
                        # Verificar ícone de presença
                        icone = link_presenca.find('i')
                        if icone:
                            classes = icone.get('class', [])
                            classes_str = ' '.join(classes)
                            
                            if 'fa-check' in classes_str and 'text-success' in classes_str:
                                presentes_ids.append(id_membro)
                                presentes_nomes.append(nome_completo)
                            elif 'fa-remove' in classes_str and 'text-danger' in classes_str:
                                ausentes_ids.append(id_membro)
                                ausentes_nomes.append(nome_completo)
            
            return {
                'presentes_ids': presentes_ids,
                'presentes_nomes': presentes_nomes,
                'ausentes_ids': ausentes_ids,
                'ausentes_nomes': ausentes_nomes,
                'tem_presenca': "OK" if presentes_ids else "FANTASMA"
            }
        
        return {
            'presentes_ids': [],
            'presentes_nomes': [],
            'ausentes_ids': [],
            'ausentes_nomes': [],
            'tem_presenca': "ERRO"
        }
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair frequência da aula {aula_id}: {e}")
        return {
            'presentes_ids': [],
            'presentes_nomes': [],
            'ausentes_ids': [],
            'ausentes_nomes': [],
            'tem_presenca': "ERRO"
        }

def extrair_detalhes_aula(session, aula_id):
    """Extrai detalhes da aula via requests para verificar ATA"""
    try:
        url_detalhes = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
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

def processar_aula_completa(session, dados_aula):
    """Processa uma aula completa (frequência + ATA) via requests"""
    try:
        # Extrair frequência
        freq_data = extrair_frequencia_via_request(session, dados_aula['aula_id'], dados_aula['professor_id'])
        
        # Extrair ATA
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
        
        return linha_resultado
        
    except Exception as e:
        print(f"⚠️ Erro ao processar aula {dados_aula['aula_id']}: {e}")
        return None

def extrair_todas_aulas_da_pagina_html(html_content):
    """Extrai dados de todas as aulas de uma página usando BeautifulSoup"""
    soup = BeautifulSoup(html_content, 'html.parser')
    aulas = []
    
    # Buscar todas as linhas da tabela
    linhas = soup.find('table').find('tbody').find_all('tr')
    
    for linha in linhas:
        colunas = linha.find_all('td')
        
        if len(colunas) >= 6:
            # Extrair dados básicos
            data_aula = colunas[1].get_text(strip=True)
            
            # Verificar se é 2024 - parar processamento
            if "2024" in data_aula:
                return aulas, True  # Retorna aulas coletadas + sinal de parada
            
            congregacao = colunas[2].get_text(strip=True)
            curso = colunas[3].get_text(strip=True)
            turma = colunas[4].get_text(strip=True)
            
            # Extrair IDs do botão de frequência
            btn_freq = linha.find('button', {'onclick': re.compile(r'visualizarFrequencias')})
            if btn_freq:
                onclick = btn_freq.get('onclick')
                match = re.search(r'visualizarFrequencias\((\d+),\s*(\d+)\)', onclick)
                if match:
                    aula_id = match.group(1)
                    professor_id = match.group(2)
                    
                    aulas.append({
                        'aula_id': aula_id,
                        'professor_id': professor_id,
                        'data': data_aula,
                        'congregacao': congregacao,
                        'curso': curso,
                        'turma': turma
                    })
    
    return aulas, False

def processar_lote_aulas(session, aulas_lote, thread_id):
    """Processa um lote de aulas em thread separada"""
    resultados_lote = []
    
    for i, aula in enumerate(aulas_lote):
        print(f"   [Thread {thread_id}] Processando {i+1}/{len(aulas_lote)}: {aula['data']} - {aula['curso']}")
        
        resultado = processar_aula_completa(session, aula)
        if resultado:
            resultados_lote.append(resultado)
            
            # Log do resultado
            total_presentes = len(resultado[4].split('; ')) if resultado[4] else 0
            total_ausentes = len(resultado[6].split('; ')) if resultado[6] else 0
            print(f"     ✓ {total_presentes} presentes, {total_ausentes} ausentes - ATA: {resultado[9]}")
        
        # Pequena pausa para não sobrecarregar servidor
        time.sleep(0.1)
    
    return resultados_lote

def navegar_para_historico_aulas(pagina):
    """Navega pelos menus para chegar ao histórico de aulas"""
    try:
        print("🔍 Navegando para G.E.M...")
        
        # Aguardar o menu carregar após login
        pagina.wait_for_selector("nav", timeout=15000)
        
        # Tentar estratégias de navegação
        estrategias = [
            lambda: pagina.click('a:has-text("G.E.M")'),
            lambda: pagina.goto("https://musical.congregacao.org.br/aulas_abertas")
        ]
        
        sucesso = False
        for i, estrategia in enumerate(estrategias):
            try:
                print(f"   Tentativa {i+1}...")
                estrategia()
                if i == 0:  # Se clicou no menu
                    time.sleep(2)
                    # Tentar clicar em histórico
                    try:
                        pagina.wait_for_selector('a:has-text("Histórico de Aulas")', timeout=5000)
                        pagina.click('a:has-text("Histórico de Aulas")')
                    except:
                        # Navegar direto
                        pagina.goto("https://musical.congregacao.org.br/aulas_abertas")
                
                # Verificar se chegou na página certa
                pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
                sucesso = True
                break
                
            except Exception as e:
                print(f"     Falhou: {e}")
                continue
        
        if not sucesso:
            return False
            
        print("✅ Navegação bem-sucedida!")
        return True
                
    except Exception as e:
        print(f"❌ Erro durante navegação: {e}")
        return False

def main():
    tempo_inicio = time.time()
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        # Configurações do navegador
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        print("🔐 Fazendo login...")
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
        
        # Navegar para histórico de aulas
        if not navegar_para_historico_aulas(pagina):
            print("❌ Falha na navegação para histórico de aulas.")
            navegador.close()
            return
        
        # Configurar para mostrar 2000 registros
        print("⚙️ Configurando para mostrar 2000 registros...")
        try:
            pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
            pagina.select_option('select[name="listagem_length"]', "2000")
            print("✅ Configurado para 2000 registros")
            time.sleep(3)
        except Exception as e:
            print(f"⚠️ Erro ao configurar registros: {e}")
        
        # Criar sessão requests com cookies
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        resultado = []
        pagina_atual = 1
        deve_parar = False
        
        # OTIMIZAÇÃO PRINCIPAL: Extrair todas as aulas por página e processar em lotes
        while not deve_parar:
            print(f"📖 Processando página {pagina_atual}...")
            
            # Aguardar página carregar
            try:
                pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=10000)
                time.sleep(2)
            except:
                print("⚠️ Timeout aguardando página carregar")
            
            # Extrair TODAS as aulas da página atual usando HTML
            html_content = pagina.content()
            aulas_pagina, deve_parar_ano = extrair_todas_aulas_da_pagina_html(html_content)
            
            if deve_parar_ano:
                print("🛑 Encontrado ano 2024 - finalizando coleta!")
                deve_parar = True
            
            if not aulas_pagina:
                print("🏁 Não há mais aulas para processar.")
                break
            
            print(f"   📊 Encontradas {len(aulas_pagina)} aulas nesta página")
            
            # PROCESSAR EM PARALELO COM THREADS
            num_threads = 4  # Ajuste conforme necessário
            tamanho_lote = max(1, len(aulas_pagina) // num_threads)
            
            # Dividir aulas em lotes
            lotes = []
            for i in range(0, len(aulas_pagina), tamanho_lote):
                lotes.append(aulas_pagina[i:i + tamanho_lote])
            
            print(f"   🚀 Processando {len(lotes)} lotes em paralelo...")
            
            # Processar lotes em paralelo
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                # Criar sessão para cada thread
                futures = []
                for i, lote in enumerate(lotes):
                    # Criar nova sessão com os mesmos cookies
                    thread_session = requests.Session()
                    thread_session.cookies.update(cookies_dict)
                    
                    future = executor.submit(processar_lote_aulas, thread_session, lote, i+1)
                    futures.append(future)
                
                # Coletar resultados
                for future in concurrent.futures.as_completed(futures):
                    try:
                        resultados_lote = future.result()
                        with resultado_lock:
                            resultado.extend(resultados_lote)
                    except Exception as e:
                        print(f"⚠️ Erro em thread: {e}")
            
            print(f"   ✅ Página {pagina_atual} processada! Total coletado: {len(resultado)} aulas")
            
            if deve_parar:
                break
            
            # Tentar avançar para próxima página
            try:
                time.sleep(2)
                btn_proximo = pagina.query_selector("a:has(i.fa-chevron-right)")
                
                if btn_proximo:
                    parent = btn_proximo.query_selector("..")
                    parent_class = parent.get_attribute("class") if parent else ""
                    
                    if "disabled" not in parent_class:
                        print("➡️ Avançando para próxima página...")
                        btn_proximo.click()
                        pagina_atual += 1
                        time.sleep(3)
                    else:
                        print("🏁 Última página alcançada.")
                        break
                else:
                    print("🏁 Botão próximo não encontrado.")
                    break
                    
            except Exception as e:
                print(f"⚠️ Erro ao navegar: {e}")
                break
        
        print(f"\n📊 Coleta finalizada! Total de aulas processadas: {len(resultado)}")
        
        # Preparar dados para envio
        headers = [
            "CONGREGAÇÃO", "CURSO", "TURMA", "DATA", "PRESENTES IDs", 
            "PRESENTES Nomes", "AUSENTES IDs", "AUSENTES Nomes", "TEM PRESENÇA", "ATA DA AULA"
        ]
        
        body = {
            "tipo": "historico_aulas",
            "dados": resultado,
            "headers": headers,
            "resumo": {
                "total_aulas": len(resultado),
                "tempo_processamento": f"{(time.time() - tempo_inicio) / 60:.1f} minutos",
                "paginas_processadas": pagina_atual
            }
        }
        
        # Enviar dados para Apps Script
        if resultado:
            try:
                print("📤 Enviando dados para Google Sheets...")
                resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
                print("✅ Dados enviados!")
                print("Status code:", resposta_post.status_code)
                print("Resposta do Apps Script:", resposta_post.text)
            except Exception as e:
                print(f"❌ Erro ao enviar para Apps Script: {e}")
        
        # Resumo final
        tempo_total = (time.time() - tempo_inicio) / 60
        print("\n📈 RESUMO DA COLETA:")
        print(f"   🎯 Total de aulas: {len(resultado)}")
        print(f"   📄 Páginas processadas: {pagina_atual}")
        print(f"   ⏱️ Tempo total: {tempo_total:.1f} minutos")
        print(f"   ⚡ Velocidade: {len(resultado)/tempo_total:.1f} aulas/minuto")
        
        if resultado:
            total_presentes = sum(len(linha[4].split('; ')) if linha[4] else 0 for linha in resultado)
            total_ausentes = sum(len(linha[6].split('; ')) if linha[6] else 0 for linha in resultado)
            aulas_com_ata = sum(1 for linha in resultado if linha[9] == "OK")
            
            print(f"   👥 Total de presenças: {total_presentes}")
            print(f"   ❌ Total de ausências: {total_ausentes}")
            print(f"   📝 Aulas com ATA: {aulas_com_ata}/{len(resultado)}")
        
        navegador.close()

if __name__ == "__main__":
    main()
