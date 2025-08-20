from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from datetime import datetime

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

# Configurações simples
TOTAL_ABAS = 29  # Quantas abas vão trabalhar ao mesmo tempo

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

# Variáveis globais para sincronizar as abas
resultado_global = []
lock_resultado = threading.Lock()
cookies_compartilhados = None

def extrair_cookies_playwright(pagina):
    """Copia as funções originais - sem mudanças"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def extrair_detalhes_aula(session, aula_id):
    """Copia as funções originais - sem mudanças"""
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
    """Copia as funções originais - sem mudanças"""
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

def trabalhar_em_uma_pagina(navegador_principal, numero_pagina):
    """Cada aba vai trabalhar em UMA página específica - fazendo exatamente igual o original"""
    print(f"🚀 Aba {numero_pagina}: Começando...")
    
    try:
        # Criar nova aba (herda os cookies do login automaticamente)
        nova_aba = navegador_principal.new_page()
        
        # Ir direto para o histórico de aulas
        nova_aba.goto("https://musical.congregacao.org.br/aulas_abertas")
        
        # Aguardar carregar e configurar 100 registros (igual o original)
        nova_aba.wait_for_selector('select[name="listagem_length"]', timeout=15000)
        nova_aba.select_option('select[name="listagem_length"]', "100")
        time.sleep(3)
        
        # Se não é página 1, navegar até ela
        if numero_pagina > 1:
            print(f"   Aba {numero_pagina}: Navegando para página {numero_pagina}")
            
            # Aguardar paginação aparecer
            nova_aba.wait_for_selector(".pagination", timeout=10000)
            
            # Tentar clicar direto no número da página
            try:
                link_pagina = nova_aba.query_selector(f'a:has-text("{numero_pagina}")')
                if link_pagina:
                    link_pagina.click()
                    time.sleep(3)
                else:
                    # Se não achou, navegar clicando em "próximo" várias vezes
                    for _ in range(numero_pagina - 1):
                        btn_proximo = nova_aba.query_selector("a:has(i.fa-chevron-right)")
                        if btn_proximo:
                            parent = btn_proximo.query_selector("..")
                            parent_class = parent.get_attribute("class") if parent else ""
                            if "disabled" not in parent_class:
                                btn_proximo.click()
                                time.sleep(1)
                            else:
                                break
                        else:
                            break
            except Exception as e:
                print(f"   Aba {numero_pagina}: Erro na navegação: {e}")
        
        # Aguardar a página carregar
        nova_aba.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=15000)
        time.sleep(2)
        
        # Criar sessão requests com cookies para ATA (igual o original)
        cookies_dict = extrair_cookies_playwright(nova_aba)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        resultado_desta_aba = []
        
        # Agora fazer EXATAMENTE igual o original - linha por linha
        linhas = nova_aba.query_selector_all("table tbody tr")
        total_linhas = len(linhas)
        
        print(f"   Aba {numero_pagina}: Processando {total_linhas} aulas")
        
        for i in range(total_linhas):
            try:
                # Buscar a linha novamente para evitar elementos velhos
                linhas = nova_aba.query_selector_all("table tbody tr")
                if i >= len(linhas):
                    break
                
                linha = linhas[i]
                colunas = linha.query_selector_all("td")
                
                if len(colunas) >= 6:
                    data_aula = colunas[1].inner_text().strip()
                    
                    # Parar se chegou em 2024
                    if "2024" in data_aula:
                        print(f"   Aba {numero_pagina}: Encontrou 2024 - parando")
                        break
                    
                    congregacao = colunas[2].inner_text().strip()
                    curso = colunas[3].inner_text().strip()
                    turma = colunas[4].inner_text().strip()
                    
                    # Extrair IDs do botão
                    btn_freq = linha.query_selector("button[onclick*='visualizarFrequencias']")
                    if btn_freq:
                        onclick = btn_freq.get_attribute("onclick")
                        match = re.search(r'visualizarFrequencias\((\d+),\s*(\d+)\)', onclick)
                        if match:
                            aula_id = match.group(1)
                            professor_id = match.group(2)
                            
                            # Fechar qualquer modal que esteja aberto
                            try:
                                nova_aba.wait_for_selector("#modalFrequencia", state="hidden", timeout=3000)
                            except:
                                try:
                                    btn_fechar = nova_aba.query_selector('button[data-dismiss="modal"]')
                                    if btn_fechar:
                                        btn_fechar.click()
                                    nova_aba.keyboard.press("Escape")
                                    time.sleep(1)
                                except:
                                    pass
                            
                            # Clicar no botão (igual o original)
                            btn_freq.click()
                            time.sleep(1)
                            
                            # Processar frequência (igual o original)
                            freq_data = processar_frequencia_modal(nova_aba, aula_id, professor_id)
                            
                            # Fechar modal (igual o original)
                            try:
                                btn_fechar = nova_aba.query_selector('button.btn-warning[data-dismiss="modal"]:has-text("Fechar")')
                                if btn_fechar:
                                    btn_fechar.click()
                                else:
                                    nova_aba.evaluate("$('#modalFrequencia').modal('hide')")
                                
                                nova_aba.wait_for_selector("#modalFrequencia", state="hidden", timeout=5000)
                            except:
                                nova_aba.keyboard.press("Escape")
                            
                            time.sleep(0.5)
                            
                            # Obter ATA (igual o original)
                            ata_status = extrair_detalhes_aula(session, aula_id)
                            
                            # Montar resultado (igual o original)
                            linha_resultado = [
                                congregacao,
                                curso,
                                turma,
                                data_aula,
                                "; ".join(freq_data['presentes_ids']),
                                "; ".join(freq_data['presentes_nomes']),
                                "; ".join(freq_data['ausentes_ids']),
                                "; ".join(freq_data['ausentes_nomes']),
                                freq_data['tem_presenca'],
                                ata_status
                            ]
                            
                            resultado_desta_aba.append(linha_resultado)
                            
                            # Log de progresso
                            total_alunos = len(freq_data['presentes_ids']) + len(freq_data['ausentes_ids'])
                            print(f"   Aba {numero_pagina} [{i+1}/{total_linhas}]: {data_aula} - {total_alunos} alunos - ATA: {ata_status}")
            
            except Exception as e:
                print(f"   Aba {numero_pagina}: Erro na linha {i}: {e}")
                continue
        
        # Adicionar resultados ao global de forma segura
        with lock_resultado:
            resultado_global.extend(resultado_desta_aba)
            print(f"✅ Aba {numero_pagina}: Concluída! {len(resultado_desta_aba)} aulas processadas")
        
        nova_aba.close()
        
    except Exception as e:
        print(f"❌ Aba {numero_pagina}: Erro geral: {e}")

def navegar_para_historico_aulas(pagina):
    """Copia a função original - sem mudanças"""
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
                    print(f"✅ Menu G.E.M encontrado: {seletor}")
                    elemento_gem.click()
                    menu_gem_clicado = True
                    break
            except Exception as e:
                continue
        
        if not menu_gem_clicado:
            print("❌ Não foi possível encontrar o menu G.E.M")
            return False
        
        time.sleep(3)
        
        print("🔍 Procurando por Histórico de Aulas...")
        
        historico_clicado = False
        try:
            historico_link = pagina.wait_for_selector('a:has-text("Histórico de Aulas")', 
                                                     state="visible", timeout=10000)
            if historico_link:
                print("✅ Histórico de Aulas visível - clicando...")
                historico_link.click()
                historico_clicado = True
        except Exception as e:
            print(f"⚠️ Estratégia 1 falhou: {e}")
        
        if not historico_clicado:
            try:
                print("🔧 Tentando forçar clique com JavaScript...")
                elemento = pagina.query_selector('a:has-text("Histórico de Aulas")')
                if elemento:
                    pagina.evaluate("element => element.click()", elemento)
                    historico_clicado = True
                    print("✅ Clique forçado com JavaScript")
            except Exception as e:
                print(f"⚠️ Estratégia 2 falhou: {e}")
        
        if not historico_clicado:
            try:
                print("🌐 Navegando diretamente para URL do histórico...")
                pagina.goto("https://musical.congregacao.org.br/aulas_abertas")
                historico_clicado = True
                print("✅ Navegação direta bem-sucedida")
            except Exception as e:
                print(f"⚠️ Estratégia 3 falhou: {e}")
        
        if not historico_clicado:
            print("❌ Todas as estratégias falharam")
            return False
        
        print("⏳ Aguardando página do histórico carregar...")
        
        try:
            pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=20000)
            print("✅ Tabela do histórico carregada!")
            return True
        except PlaywrightTimeoutError:
            print("⚠️ Timeout aguardando tabela - tentando continuar...")
            try:
                pagina.wait_for_selector("table", timeout=5000)
                print("✅ Tabela encontrada (sem checkboxes)")
                return True
            except:
                print("❌ Nenhuma tabela encontrada")
                return False
                
    except Exception as e:
        print(f"❌ Erro durante navegação: {e}")
        return False

def descobrir_quantas_paginas(pagina):
    """Descobre quantas páginas existem para dividir o trabalho entre as abas"""
    try:
        # Configurar 100 primeiro
        pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
        pagina.select_option('select[name="listagem_length"]', "100")
        time.sleep(3)
        
        # Aguardar paginação
        pagina.wait_for_selector(".pagination", timeout=10000)
        
        # Buscar todos os números de página
        links_pagina = pagina.query_selector_all(".pagination a")
        numeros_pagina = []
        
        for link in links_pagina:
            texto = link.inner_text().strip()
            if texto.isdigit():
                numeros_pagina.append(int(texto))
        
        if numeros_pagina:
            total = max(numeros_pagina)
            print(f"📊 Descobriu que existem {total} páginas")
            return total
        else:
            print("⚠️ Não conseguiu descobrir - assumindo 1 página")
            return 1
            
    except Exception as e:
        print(f"⚠️ Erro ao descobrir páginas: {e} - assumindo 1")
        return 1

def main():
    tempo_inicio = time.time()
    
    print("🚀 VERSÃO SIMPLES COM MÚLTIPLAS ABAS")
    print("=" * 50)
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        
        # FAZER LOGIN UMA ÚNICA VEZ
        print("🔐 Fazendo login único...")
        pagina_principal = navegador.new_page()
        
        pagina_principal.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
        pagina_principal.goto(URL_INICIAL)
        
        # Login
        pagina_principal.fill('input[name="login"]', EMAIL)
        pagina_principal.fill('input[name="password"]', SENHA)
        pagina_principal.click('button[type="submit"]')
        
        try:
            pagina_principal.wait_for_selector("nav", timeout=15000)
            print("✅ Login OK! Todas as abas vão herdar esse login")
        except PlaywrightTimeoutError:
            print("❌ Falha no login")
            navegador.close()
            return
        
        # Navegar para histórico
        if not navegar_para_historico_aulas(pagina_principal):
            print("❌ Falha na navegação")
            navegador.close()
            return
        
        # Descobrir quantas páginas existem
        total_paginas = descobrir_quantas_paginas(pagina_principal)
        
        print(f"🎯 Vou criar {TOTAL_ABAS} abas para processar {total_paginas} páginas")
        print(f"📄 Cada aba vai pegar páginas: ", end="")
        
        # Dividir as páginas entre as abas
        paginas_por_aba = []
        for aba_num in range(TOTAL_ABAS):
            paginas_desta_aba = []
            for pagina_num in range(aba_num + 1, total_paginas + 1, TOTAL_ABAS):
                paginas_desta_aba.append(pagina_num)
            
            if paginas_desta_aba:
                paginas_por_aba.append(paginas_desta_aba)
                print(f"Aba{aba_num+1}: {paginas_desta_aba} ", end="")
        
        print()
        
        # Fechar página principal (já fez login, agora as novas abas herdam)
        pagina_principal.close()
        
        print("🚀 INICIANDO ABAS EM PARALELO...")
        
        # Usar ThreadPoolExecutor para gerenciar as abas
        with ThreadPoolExecutor(max_workers=TOTAL_ABAS) as executor:
            futures = []
            
            for aba_num, paginas_desta_aba in enumerate(paginas_por_aba):
                for pagina_num in paginas_desta_aba:
                    future = executor.submit(trabalhar_em_uma_pagina, navegador, pagina_num)
                    futures.append(future)
            
            # Aguardar todas as abas terminarem
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Erro em uma aba: {e}")
        
        navegador.close()
        
        # RESULTADO FINAL (igual o original)
        tempo_total = (time.time() - tempo_inicio) / 60
        
        print(f"\n📊 RESULTADO FINAL:")
        print(f"   🎯 Total de aulas coletadas: {len(resultado_global)}")
        print(f"   ⏱️ Tempo total: {tempo_total:.1f} minutos")
        print(f"   ⚡ Velocidade: {len(resultado_global) / tempo_total:.0f} aulas/min")
        
        # Enviar para Google Sheets (igual o original)
        if resultado_global:
            headers = [
                "CONGREGAÇÃO", "CURSO", "TURMA", "DATA", "PRESENTES IDs", 
                "PRESENTES Nomes", "AUSENTES IDs", "AUSENTES Nomes", "TEM PRESENÇA", "ATA DA AULA"
            ]
            
            body = {
                "tipo": "historico_aulas_multiplas_abas",
                "dados": resultado_global,
                "headers": headers,
                "resumo": {
                    "total_aulas": len(resultado_global),
                    "tempo_processamento": f"{tempo_total:.1f} minutos",
                    "abas_utilizadas": TOTAL_ABAS,
                    "paginas_processadas": total_paginas
                }
            }
            
            try:
                print("📤 Enviando para Google Sheets...")
                resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
                print("✅ Dados enviados!")
                print("Status code:", resposta_post.status_code)
                print("Resposta:", resposta_post.text[:100])
            except Exception as e:
                print(f"❌ Erro ao enviar: {e}")
        
        print("\n🎉 CONCLUÍDO!")

if __name__ == "__main__":
    main()
