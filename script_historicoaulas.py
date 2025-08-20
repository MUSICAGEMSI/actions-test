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
import math

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

# Lock para thread safety
resultado_lock = Lock()
contador_aulas = 0

def fazer_login_navegador(navegador):
    """Faz login e retorna uma página autenticada"""
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
        return pagina
    except PlaywrightTimeoutError:
        print("❌ Falha no login. Verifique suas credenciais.")
        return None

def navegar_para_historico_manual(pagina):
    """Navega manualmente através dos menus para o histórico"""
    try:
        print("🔍 Navegando para histórico através dos menus...")
        
        # Aguardar menu carregar
        pagina.wait_for_selector("nav", timeout=15000)
        time.sleep(2)
        
        # Estratégia 1: Tentar encontrar e clicar no menu G.E.M
        gem_clicado = False
        seletores_gem = [
            'a:has-text("G.E.M")',
            'a:has(.fa-graduation-cap)',
            'a[href="#"]:has(span:text("G.E.M"))',
            'a:has(span):has-text("G.E.M")',
            'a:text("G.E.M")',
            'li:has-text("G.E.M") a'
        ]
        
        for seletor in seletores_gem:
            try:
                elemento = pagina.query_selector(seletor)
                if elemento:
                    print(f"   📍 Menu G.E.M encontrado com: {seletor}")
                    elemento.click()
                    gem_clicado = True
                    break
            except Exception as e:
                continue
        
        if gem_clicado:
            time.sleep(2)  # Aguardar submenu expandir
            
            # Tentar clicar em Histórico de Aulas
            seletores_historico = [
                'a:has-text("Histórico de Aulas")',
                'a:text("Histórico de Aulas")',
                'a[href*="aulas_abertas"]'
            ]
            
            historico_clicado = False
            for seletor in seletores_historico:
                try:
                    elemento = pagina.query_selector(seletor)
                    if elemento:
                        print(f"   📍 Histórico encontrado com: {seletor}")
                        elemento.click()
                        historico_clicado = True
                        break
                except Exception as e:
                    continue
            
            if not historico_clicado:
                print("   ⚠️ Link histórico não encontrado, tentando navegação direta...")
                pagina.goto("https://musical.congregacao.org.br/aulas_abertas")
        else:
            print("   ⚠️ Menu G.E.M não encontrado, tentando navegação direta...")
            pagina.goto("https://musical.congregacao.org.br/aulas_abertas")
        
        # Verificar se chegamos na página certa
        try:
            # Aguardar elementos característicos da página de histórico
            pagina.wait_for_selector('table', timeout=15000)
            
            # Tentar aguardar o seletor de quantidade também
            try:
                pagina.wait_for_selector('select[name="listagem_length"]', timeout=5000)
                print("✅ Página do histórico carregada com sucesso!")
                return True
            except:
                # Se não tem o seletor, pode ser que a página seja diferente
                # Mas se tem tabela, vamos tentar continuar
                if pagina.query_selector('table'):
                    print("✅ Página com tabela encontrada (sem seletor de quantidade)")
                    return True
                else:
                    return False
                    
        except Exception as e:
            print(f"❌ Erro ao verificar página do histórico: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Erro durante navegação manual: {e}")
        return False

def criar_url_pagina_especifica(pagina_num, registros_por_pagina=100):
    """Cria URL para uma página específica do histórico"""
    start = (pagina_num - 1) * registros_por_pagina
    return f"https://musical.congregacao.org.br/aulas_abertas?start={start}&length={registros_por_pagina}"

def processar_frequencia_modal_otimizado(pagina, aula_id, professor_id, max_tentativas=3):
    """Processa a frequência via modal de forma otimizada"""
    for tentativa in range(max_tentativas):
        try:
            # Aguardar modal carregar
            pagina.wait_for_selector("table.table-bordered tbody tr", timeout=8000)
            
            presentes_ids = []
            presentes_nomes = []
            ausentes_ids = []
            ausentes_nomes = []
            
            # Extrair dados da tabela
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
            print(f"         ⚠️ Tentativa {tentativa + 1} falhou: {e}")
            if tentativa < max_tentativas - 1:
                time.sleep(1)
                continue
            else:
                return {
                    'presentes_ids': [],
                    'presentes_nomes': [],
                    'ausentes_ids': [],
                    'ausentes_nomes': [],
                    'tem_presenca': "ERRO"
                }

def extrair_detalhes_aula_requests(cookies_dict, aula_id):
    """Extrai detalhes da aula via requests usando cookies do navegador"""
    try:
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        url_detalhes = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://musical.congregacao.org.br/aulas_abertas',
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

def processar_pagina_completa(navegador, cookies_dict, pagina_num, registros_por_pagina=100):
    """Processa uma página completa de aulas em uma nova aba"""
    global contador_aulas
    
    try:
        print(f"🚀 [Aba {pagina_num}] Iniciando processamento...")
        
        # Criar nova aba
        nova_aba = navegador.new_page()
        nova_aba.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Navegar para a página específica
        url_pagina = criar_url_pagina_especifica(pagina_num, registros_por_pagina)
        print(f"   📍 [Aba {pagina_num}] Navegando para: {url_pagina}")
        nova_aba.goto(url_pagina)
        
        # Aguardar página carregar
        try:
            nova_aba.wait_for_selector("table tbody tr", timeout=15000)
        except:
            print(f"   ❌ [Aba {pagina_num}] Timeout aguardando tabela")
            nova_aba.close()
            return []
        
        # Extrair todas as aulas da página
        linhas = nova_aba.query_selector_all("table tbody tr")
        print(f"   📊 [Aba {pagina_num}] Encontradas {len(linhas)} aulas")
        
        if len(linhas) == 0:
            print(f"   🏁 [Aba {pagina_num}] Página vazia - finalizando")
            nova_aba.close()
            return []
        
        resultados_pagina = []
        
        for i, linha in enumerate(linhas):
            try:
                colunas = linha.query_selector_all("td")
                
                if len(colunas) >= 6:
                    # Extrair dados básicos
                    data_aula = colunas[1].inner_text().strip()
                    
                    # Verificar se chegamos em 2024
                    if "2024" in data_aula:
                        print(f"   🛑 [Aba {pagina_num}] Encontrado 2024 - parando processamento")
                        break
                    
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
                            
                            print(f"      🎯 [Aba {pagina_num}] Aula {i+1}/{len(linhas)}: {data_aula} - {curso}")
                            
                            # Clicar no botão de frequência
                            try:
                                # Fechar modal anterior se existir
                                try:
                                    nova_aba.evaluate("$('#modalFrequencia').modal('hide')")
                                    time.sleep(0.5)
                                except:
                                    pass
                                
                                btn_freq.click()
                                time.sleep(0.8)  # Aguardar modal abrir
                                
                                # Processar frequência
                                freq_data = processar_frequencia_modal_otimizado(nova_aba, aula_id, professor_id)
                                
                                # Fechar modal rapidamente
                                try:
                                    nova_aba.keyboard.press("Escape")
                                    time.sleep(0.3)
                                except:
                                    pass
                                
                                # Extrair ATA via requests (mais rápido)
                                ata_status = extrair_detalhes_aula_requests(cookies_dict, aula_id)
                                
                                # Montar resultado
                                linha_resultado = [
                                    congregacao, curso, turma, data_aula,
                                    "; ".join(freq_data['presentes_ids']),
                                    "; ".join(freq_data['presentes_nomes']),
                                    "; ".join(freq_data['ausentes_ids']),
                                    "; ".join(freq_data['ausentes_nomes']),
                                    freq_data['tem_presenca'],
                                    ata_status
                                ]
                                
                                resultados_pagina.append(linha_resultado)
                                
                                with resultado_lock:
                                    contador_aulas += 1
                                
                                # Log do progresso
                                total_presentes = len(freq_data['presentes_ids'])
                                total_ausentes = len(freq_data['ausentes_ids'])
                                print(f"         ✓ {total_presentes} presentes, {total_ausentes} ausentes - ATA: {ata_status} [Total geral: {contador_aulas}]")
                                
                            except Exception as e:
                                print(f"         ❌ Erro ao processar: {e}")
                                continue
                            
                            # Pequena pausa entre aulas para não sobrecarregar
                            time.sleep(0.1)
                
            except Exception as e:
                print(f"      ⚠️ [Aba {pagina_num}] Erro na linha {i}: {e}")
                continue
        
        print(f"   ✅ [Aba {pagina_num}] Concluída! {len(resultados_pagina)} aulas processadas")
        nova_aba.close()
        return resultados_pagina
        
    except Exception as e:
        print(f"❌ [Aba {pagina_num}] Erro geral: {e}")
        try:
            nova_aba.close()
        except:
            pass
        return []

def main():
    tempo_inicio = time.time()
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)  # Modo invisível para máxima performance
        
        # Fazer login na aba principal
        pagina_principal = fazer_login_navegador(navegador)
        if not pagina_principal:
            navegador.close()
            return
        
        # Navegar para histórico para estabelecer sessão
        if not navegar_para_historico_manual(pagina_principal):
            print("❌ Não foi possível acessar o histórico de aulas")
            navegador.close()
            return
        
        # Configurar para 100 registros por página
        print("⚙️ Configurando 100 registros por página...")
        try:
            pagina_principal.select_option('select[name="listagem_length"]', "100")
            time.sleep(2)
        except Exception as e:
            print(f"⚠️ Não foi possível alterar registros por página: {e}")
            print("📋 Continuando com configuração padrão...")
        
        # Extrair cookies para usar em requests
        cookies_dict = {cookie['name']: cookie['value'] 
                       for cookie in pagina_principal.context.cookies()}
        
        # Definir quantas páginas processar simultaneamente
        NUM_ABAS_SIMULTANEAS = 5  # 5 abas simultâneas para máxima velocidade
        REGISTROS_POR_PAGINA = 100
        MAX_PAGINAS = 100  # Limite aumentado para cobrir mais dados
        
        resultado_final = []
        
        # Processar páginas em lotes
        for lote_inicio in range(1, MAX_PAGINAS + 1, NUM_ABAS_SIMULTANEAS):
            lote_fim = min(lote_inicio + NUM_ABAS_SIMULTANEAS - 1, MAX_PAGINAS)
            paginas_lote = list(range(lote_inicio, lote_fim + 1))
            
            print(f"\n🚀 Processando lote de páginas: {paginas_lote} (5 abas simultâneas)")
            
            # Processar páginas do lote em paralelo
            with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_ABAS_SIMULTANEAS) as executor:
                futures = []
                for pagina_num in paginas_lote:
                    future = executor.submit(processar_pagina_completa, navegador, cookies_dict, 
                                           pagina_num, REGISTROS_POR_PAGINA)
                    futures.append(future)
                
                # Coletar resultados do lote
                resultados_lote = []
                for future in concurrent.futures.as_completed(futures):
                    try:
                        resultado_pagina = future.result()
                        resultados_lote.extend(resultado_pagina)
                    except Exception as e:
                        print(f"⚠️ Erro em thread: {e}")
                
                resultado_final.extend(resultados_lote)
            
            print(f"✅ Lote concluído! Total acumulado: {len(resultado_final)} aulas")
            
            # Verificar se alguma página retornou dados de 2024 (sinal para parar)
            if any("2024" in str(linha[3]) for linha in resultados_lote):
                print("🛑 Encontrado 2024 no lote - finalizando coleta!")
                break
            
            # Se alguma página não retornou dados, pode ter chegado ao fim
            if not all(len(resultado) > 0 for resultado in [future.result() for future in futures]):
                print("🏁 Páginas vazias detectadas - finalizando coleta!")
                break
        
        pagina_principal.close()
        
        print(f"\n📊 Coleta finalizada! Total de aulas processadas: {len(resultado_final)}")
        
        # Preparar dados para envio
        headers = [
            "CONGREGAÇÃO", "CURSO", "TURMA", "DATA", "PRESENTES IDs", 
            "PRESENTES Nomes", "AUSENTES IDs", "AUSENTES Nomes", "TEM PRESENÇA", "ATA DA AULA"
        ]
        
        body = {
            "tipo": "historico_aulas",
            "dados": resultado_final,
            "headers": headers,
            "resumo": {
                "total_aulas": len(resultado_final),
                "tempo_processamento": f"{(time.time() - tempo_inicio) / 60:.1f} minutos",
                "metodo": "multi_abas_paralelo"
            }
        }
        
        # Enviar dados para Apps Script
        if resultado_final:
            try:
                print("📤 Enviando dados para Google Sheets...")
                resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
                print("✅ Dados enviados!")
                print("Status code:", resposta_post.status_code)
                print("Resposta do Apps Script:", resposta_post.text[:200])
            except Exception as e:
                print(f"❌ Erro ao enviar para Apps Script: {e}")
        
        # Resumo final
        tempo_total = (time.time() - tempo_inicio) / 60
        print("\n📈 RESUMO DA COLETA:")
        print(f"   🎯 Total de aulas: {len(resultado_final)}")
        print(f"   ⏱️ Tempo total: {tempo_total:.1f} minutos")
        print(f"   ⚡ Velocidade: {len(resultado_final)/tempo_total:.1f} aulas/minuto")
        print(f"   🚀 Método: Múltiplas abas paralelas")
        
        if resultado_final:
            total_presentes = sum(len(linha[4].split('; ')) if linha[4] else 0 for linha in resultado_final)
            total_ausentes = sum(len(linha[6].split('; ')) if linha[6] else 0 for linha in resultado_final)
            aulas_com_ata = sum(1 for linha in resultado_final if linha[9] == "OK")
            
            print(f"   👥 Total de presenças: {total_presentes}")
            print(f"   ❌ Total de ausências: {total_ausentes}")
            print(f"   📝 Aulas com ATA: {aulas_com_ata}/{len(resultado_final)}")
        
        navegador.close()

if __name__ == "__main__":
    main()
