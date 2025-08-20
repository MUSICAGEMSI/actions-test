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

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

# PERÍODO DO SEGUNDO SEMESTRE 2025
DATA_INICIO = "04/07/2025"
DATA_FIM = "31/12/2025"

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def data_esta_no_periodo(data_str):
    """Verifica se a data está no período do segundo semestre de 2025"""
    try:
        # Tentar diferentes formatos de data que podem aparecer
        formatos_data = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]
        data_obj = None
        
        for formato in formatos_data:
            try:
                data_obj = datetime.strptime(data_str.strip(), formato)
                break
            except ValueError:
                continue
        
        if not data_obj:
            print(f"⚠️ Formato de data não reconhecido: {data_str}")
            return False, False  # (no_periodo, data_anterior)
        
        # Definir limites do período
        inicio = datetime.strptime("04/07/2025", "%d/%m/%Y")
        fim = datetime.strptime("31/12/2025", "%d/%m/%Y")
        
        if inicio <= data_obj <= fim:
            return True, False  # Está no período
        elif data_obj < inicio:
            return False, True  # Data anterior ao período - PARAR!
        else:
            return False, False  # Data posterior ao período
        
    except Exception as e:
        print(f"⚠️ Erro ao verificar data {data_str}: {e}")
        return False, False

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
            # Buscar por "ATA DA AULA" no conteúdo
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
        # Aguardar o modal carregar completamente
        pagina.wait_for_selector("table.table-bordered tbody tr", timeout=5000)
        
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
        print(f"⚠️ Erro ao processar frequência: {e}")
        return {
            'presentes_ids': [],
            'presentes_nomes': [],
            'ausentes_ids': [],
            'ausentes_nomes': [],
            'tem_presenca': "ERRO"
        }

def extrair_dados_de_linha_por_indice(pagina, indice_linha):
    """Extrai dados de uma linha específica pelo índice, filtrando por período"""
    try:
        # Buscar NOVAMENTE todas as linhas para evitar elementos coletados
        linhas = pagina.query_selector_all("table tbody tr")
        
        if indice_linha >= len(linhas):
            return None, False, False
        
        linha = linhas[indice_linha]
        colunas = linha.query_selector_all("td")
        
        if len(colunas) >= 6:
            # CORREÇÃO: Vamos identificar qual coluna tem a data
            # Vamos testar cada coluna para ver qual contém uma data válida
            data_aula = None
            congregacao = None
            curso = None
            turma = None
            
            # Tentar identificar as colunas dinamicamente
            for i, coluna in enumerate(colunas):
                texto = coluna.inner_text().strip()
                
                # Verificar se parece uma data (contém / ou - e números)
                if re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', texto):
                    data_aula = texto
                    print(f"      🔍 Data encontrada na coluna {i}: {data_aula}")
                    
                    # Com base na posição da data, definir as outras colunas
                    try:
                        if i == 0:  # Data na primeira coluna
                            congregacao = colunas[1].inner_text().strip() if len(colunas) > 1 else ""
                            curso = colunas[2].inner_text().strip() if len(colunas) > 2 else ""
                            turma = colunas[3].inner_text().strip() if len(colunas) > 3 else ""
                        elif i == 1:  # Data na segunda coluna (original)
                            congregacao = colunas[2].inner_text().strip() if len(colunas) > 2 else ""
                            curso = colunas[3].inner_text().strip() if len(colunas) > 3 else ""
                            turma = colunas[4].inner_text().strip() if len(colunas) > 4 else ""
                        elif i == 2:  # Data na terceira coluna
                            congregacao = colunas[1].inner_text().strip() if len(colunas) > 1 else ""
                            curso = colunas[3].inner_text().strip() if len(colunas) > 3 else ""
                            turma = colunas[4].inner_text().strip() if len(colunas) > 4 else ""
                        else:
                            # Para outras posições, tentar usar as colunas seguintes
                            congregacao = colunas[i+1].inner_text().strip() if len(colunas) > i+1 else ""
                            curso = colunas[i+2].inner_text().strip() if len(colunas) > i+2 else ""
                            turma = colunas[i+3].inner_text().strip() if len(colunas) > i+3 else ""
                    except Exception as col_error:
                        print(f"      ⚠️ Erro ao extrair colunas: {col_error}")
                        congregacao = "N/A"
                        curso = "N/A"
                        turma = "N/A"
                    
                    break
            
            # Se não encontrou data válida, mostrar conteúdo das colunas para debug
            if not data_aula:
                print(f"      🔍 DEBUG - Conteúdo das colunas da linha {indice_linha}:")
                for i, coluna in enumerate(colunas[:6]):  # Mostrar apenas as primeiras 6 colunas
                    texto = coluna.inner_text().strip()
                    print(f"         Coluna {i}: '{texto}'")
                return None, False, False
            
            # NOVA LÓGICA: Verificar se está no período do segundo semestre 2025
            no_periodo, data_anterior = data_esta_no_periodo(data_aula)
            
            if data_anterior:
                # Data anterior ao período - PARAR TUDO!
                print(f"🛑 FINALIZANDO: Encontrada data anterior ao período ({data_aula})")
                print("   Todas as próximas aulas serão anteriores. Parando coleta!")
                return None, True, False  # Sinal para parar tudo
            
            if not no_periodo:
                # Se não está no período (mas não é anterior), pular esta aula
                return None, False, False
            
            # Extrair IDs do botão de frequência
            btn_freq = linha.query_selector("button[onclick*='visualizarFrequencias']")
            if btn_freq:
                onclick = btn_freq.get_attribute("onclick")
                # Extrair os dois IDs: visualizarFrequencias(aula_id, professor_id)
                match = re.search(r'visualizarFrequencias\((\d+),\s*(\d+)\)', onclick)
                if match:
                    aula_id = match.group(1)
                    professor_id = match.group(2)
                    
                    return {
                        'aula_id': aula_id,
                        'professor_id': professor_id,
                        'data': data_aula,
                        'congregacao': congregacao or "N/A",
                        'curso': curso or "N/A",
                        'turma': turma or "N/A"
                    }, False, True  # É válida
        
        return None, False, False
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair dados da linha {indice_linha}: {e}")
        return None, False, False

def clicar_botao_frequencia_por_indice(pagina, indice_linha):
    """Clica no botão de frequência de uma linha específica pelo índice"""
    try:
        # Buscar NOVAMENTE todas as linhas para evitar elementos coletados
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
        
        # Aguardar o menu carregar após login
        pagina.wait_for_selector("nav", timeout=15000)
        
        # Buscar e clicar no menu G.E.M
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
                print(f"⚠️ Tentativa com seletor {seletor} falhou: {e}")
                continue
        
        if not menu_gem_clicado:
            print("❌ Não foi possível encontrar o menu G.E.M")
            return False
        
        # Aguardar submenu aparecer
        print("⏳ Aguardando submenu expandir...")
        time.sleep(1)
        
        print("🔍 Procurando por Histórico de Aulas...")
        
        # Estratégia 1: Tentar aguardar elemento ficar visível
        historico_clicado = False
        try:
            # Aguardar elemento aparecer e ficar visível
            historico_link = pagina.wait_for_selector('a:has-text("Histórico de Aulas")', 
                                                     state="visible", timeout=10000)
            if historico_link:
                print("✅ Histórico de Aulas visível - clicando...")
                historico_link.click()
                historico_clicado = True
        except Exception as e:
            print(f"⚠️ Estratégia 1 falhou: {e}")
        
        # Estratégia 2: Forçar visibilidade com JavaScript
        if not historico_clicado:
            try:
                print("🔧 Tentando forçar clique com JavaScript...")
                # Buscar elemento mesmo que não visível
                elemento = pagina.query_selector('a:has-text("Histórico de Aulas")')
                if elemento:
                    # Forçar clique via JavaScript
                    pagina.evaluate("element => element.click()", elemento)
                    historico_clicado = True
                    print("✅ Clique forçado com JavaScript")
            except Exception as e:
                print(f"⚠️ Estratégia 2 falhou: {e}")
        
        # Estratégia 3: Navegar diretamente via URL
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
        
        # Aguardar indicador de carregamento da tabela
        try:
            # Aguardar pelo menos um checkbox aparecer (indica que a tabela carregou)
            pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=20000)
            print("✅ Tabela do histórico carregada!")
            return True
        except PlaywrightTimeoutError:
            print("⚠️ Timeout aguardando tabela - tentando continuar...")
            # Verificar se pelo menos temos uma tabela
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

def main():
    tempo_inicio = time.time()
    
    print(f"🎯 COLETANDO DADOS DO SEGUNDO SEMESTRE 2025")
    print(f"📅 Período: {DATA_INICIO} a {DATA_FIM}")
    print("=" * 50)
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        # Configurações do navegador
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
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
        
        # Navegar pelos menus para histórico de aulas
        if not navegar_para_historico_aulas(pagina):
            print("❌ Falha na navegação para histórico de aulas.")
            navegador.close()
            return
        
        # Configurar para mostrar 2000 registros
        print("⚙️ Configurando para mostrar 2000 registros...")
        try:
            # Aguardar o seletor de quantidade aparecer
            pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
            pagina.select_option('select[name="listagem_length"]', "2000")
            print("✅ Configurado para 2000 registros")
            
            # Aguardar a página recarregar com 2000 registros
            time.sleep(1)
            
        except Exception as e:
            print(f"⚠️ Erro ao configurar registros: {e}")
            print("📋 Continuando com configuração padrão...")
        
        # Aguardar carregamento da tabela após mudança de quantidade
        print("⏳ Aguardando nova configuração carregar...")
        try:
            # Aguardar pelo menos um checkbox aparecer novamente
            pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=15000)
            print("✅ Tabela recarregada com nova configuração!")
        except:
            print("⚠️ Timeout aguardando recarregamento - continuando...")
        
        # Criar sessão requests com cookies para detalhes das aulas
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        resultado = []
        pagina_atual = 1
        aulas_ignoradas = 0
        deve_parar_coleta = False
        
        while not deve_parar_coleta:
            print(f"📖 Processando página {pagina_atual}...")
            
            # Aguardar linhas carregarem
            try:
                # Aguardar checkboxes que indicam linhas carregadas
                pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=5000)
                time.sleep(0.5)  # Aguardar estabilização
            except:
                print("⚠️ Timeout aguardando linhas - tentando continuar...")
            
            # Contar quantas linhas temos na página atual
            total_linhas = contar_linhas_na_pagina(pagina)
            
            if total_linhas == 0:
                print("🏁 Não há mais linhas para processar.")
                break
            
            print(f"   📊 Encontradas {total_linhas} aulas nesta página")
            
            aulas_processadas_pagina = 0
            aulas_encontradas_periodo = 0  # Contador de aulas no período
            
            # Processar cada linha POR ÍNDICE (evita referências antigas)
            for i in range(total_linhas):
                # Extrair dados da linha atual pelo índice
                dados_aula, deve_parar_coleta, aula_valida = extrair_dados_de_linha_por_indice(pagina, i)
                
                # Se encontrou data anterior ao período, PARAR TUDO!
                if deve_parar_coleta:
                    break
                
                if not aula_valida:
                    # Aula fora do período - ignorar silenciosamente
                    aulas_ignoradas += 1
                    continue
                
                if not dados_aula:
                    continue
                
                aulas_processadas_pagina += 1
                aulas_encontradas_periodo += 1
                print(f"      🎯 Aula {aulas_processadas_pagina}: {dados_aula['data']} - {dados_aula['curso']}")
                
                # Clicar no botão de frequência para abrir modal
                try:
                    # Aguardar que não haja modal aberto antes de clicar
                    try:
                        pagina.wait_for_selector("#modalFrequencia", state="hidden", timeout=1000)
                    except:
                        # Se ainda há modal, forçar fechamento
                        print("⚠️ Modal anterior ainda aberto - forçando fechamento...")
                        try:
                            # Tentar múltiplas formas de fechar modal
                            btn_fechar = pagina.query_selector('button[data-dismiss="modal"], .modal-footer button')
                            if btn_fechar:
                                btn_fechar.click()
                            else:
                                # Forçar fechamento via JavaScript
                                pagina.evaluate("$('#modalFrequencia').modal('hide')")
                            
                            # Aguardar fechar
                            pagina.wait_for_selector("#modalFrequencia", state="hidden", timeout=2000)
                        except:
                            # Último recurso: recarregar página
                            print("⚠️ Forçando escape...")
                            pagina.keyboard.press("Escape")
                            time.sleep(0.2)
                    
                    # Agora clicar no botão de frequência PELO ÍNDICE
                    print(f"         🖱️ Clicando em frequência...")
                    if clicar_botao_frequencia_por_indice(pagina, i):
                        # Aguardar modal carregar
                        time.sleep(0.3)
                        
                        # Processar dados de frequência
                        freq_data = processar_frequencia_modal(pagina, dados_aula['aula_id'], dados_aula['professor_id'])
                        
                        # Fechar modal de forma mais robusta
                        print(f"         🚪 Fechando modal...")
                        try:
                            # Tentar diferentes formas de fechar
                            fechou = False
                            
                            # 1. Botão Fechar específico
                            btn_fechar = pagina.query_selector('button.btn-warning[data-dismiss="modal"]:has-text("Fechar")')
                            if btn_fechar:
                                btn_fechar.click()
                                fechou = True
                            
                            # 2. Qualquer botão de fechar modal
                            if not fechou:
                                btn_fechar = pagina.query_selector('button[data-dismiss="modal"]')
                                if btn_fechar:
                                    btn_fechar.click()
                                    fechou = True
                            
                            # 3. Via JavaScript
                            if not fechou:
                                pagina.evaluate("$('#modalFrequencia').modal('hide')")
                                fechou = True
                            
                            # 4. ESC como último recurso
                            if not fechou:
                                pagina.keyboard.press("Escape")
                            
                            # Aguardar modal fechar completamente
                            try:
                                pagina.wait_for_selector("#modalFrequencia", state="hidden", timeout=2000)
                                print(f"         ✅ Modal fechado com sucesso")
                            except:
                                print(f"         ⚠️ Modal pode não ter fechado completamente")
                                
                        except Exception as close_error:
                            print(f"         ⚠️ Erro ao fechar modal: {close_error}")
                            pagina.keyboard.press("Escape")
                        
                        # Pausa adicional para estabilizar
                        time.sleep(0.2)
                        
                        # Obter detalhes da ATA via requests
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
                        
                        resultado.append(linha_resultado)
                        
                        # Mostrar resumo da aula
                        total_alunos = len(freq_data['presentes_ids']) + len(freq_data['ausentes_ids'])
                        print(f"         ✓ {len(freq_data['presentes_ids'])} presentes, {len(freq_data['ausentes_ids'])} ausentes (Total: {total_alunos}) - ATA: {ata_status}")
                    
                    else:
                        print(f"         ❌ Falha ao clicar no botão de frequência")
                        
                except Exception as e:
                    print(f"⚠️ Erro ao processar aula: {e}")
                    continue
                
                # Pequena pausa entre aulas
                time.sleep(0.1)
            
            print(f"   ✅ {aulas_processadas_pagina} aulas válidas processadas nesta página")
            
            # Se deve parar a coleta, sair do loop principal
            if deve_parar_coleta:
                break
            
            # 🛑 LÓGICA ANTIGA: Se não encontrou NENHUMA aula no período nesta página, PARAR!
            # (Mantida como backup, mas agora para na primeira data anterior)
            if aulas_encontradas_periodo == 0:
                print("🛑 FINALIZANDO: Nenhuma aula do período encontrada nesta página!")
                print("   Todas as aulas restantes são anteriores ao período desejado.")
                break
            
            # Tentar avançar para próxima página
            try:
                # Aguardar um pouco para garantir que a página atual está estável
                time.sleep(0.5)
                
                # Buscar botão próximo
                btn_proximo = pagina.query_selector("a:has(i.fa-chevron-right)")
                
                if btn_proximo:
                    # Verificar se o botão não está desabilitado
                    parent = btn_proximo.query_selector("..")
                    parent_class = parent.get_attribute("class") if parent else ""
                    
                    if "disabled" not in parent_class:
                        print("➡️ Avançando para próxima página...")
                        btn_proximo.click()
                        pagina_atual += 1
                        
                        # Aguardar nova página carregar
                        time.sleep(1)
                        
                        # Aguardar checkboxes da nova página
                        try:
                            pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=10000)
                        except:
                            print("⚠️ Timeout aguardando nova página")
                        
                    else:
                        print("🏁 Botão próximo desabilitado - não há mais páginas.")
                        break
                else:
                    print("🏁 Botão próximo não encontrado - não há mais páginas.")
                    break
                    
            except Exception as e:
                print(f"⚠️ Erro ao navegar para próxima página: {e}")
                break
        
        print(f"\n📊 Coleta finalizada!")
        print(f"🎯 Aulas do 2º semestre 2025: {len(resultado)}")
        print(f"⏭️ Aulas fora do período: {aulas_ignoradas}")
        
        # Preparar dados para envio
        headers = [
            "CONGREGAÇÃO", "CURSO", "TURMA", "DATA", "PRESENTES IDs", 
            "PRESENTES Nomes", "AUSENTES IDs", "AUSENTES Nomes", "TEM PRESENÇA", "ATA DA AULA"
        ]
        
        body = {
            "tipo": "historico_aulas_2sem_2025",
            "dados": resultado,
            "headers": headers,
            "resumo": {
                "total_aulas": len(resultado),
                "aulas_ignoradas": aulas_ignoradas,
                "periodo": f"{DATA_INICIO} a {DATA_FIM}",
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
        else:
            print("ℹ️ Nenhuma aula encontrada no período especificado.")
        
        # Resumo final
        print("\n📈 RESUMO DA COLETA:")
        print(f"   📅 Período: {DATA_INICIO} a {DATA_FIM}")
        print(f"   🎯 Aulas coletadas: {len(resultado)}")
        print(f"   ⏭️ Aulas ignoradas: {aulas_ignoradas}")
        print(f"   📄 Páginas processadas: {pagina_atual}")
        print(f"   ⏱️ Tempo total: {(time.time() - tempo_inicio) / 60:.1f} minutos")
        
        if resultado:
            total_presentes = sum(len(linha[4].split('; ')) if linha[4] else 0 for linha in resultado)
            total_ausentes = sum(len(linha[6].split('; ')) if linha[6] else 0 for linha in resultado)
            aulas_com_ata = sum(1 for linha in resultado if linha[9] == "OK")
            
            print(f"   👥 Total de presenças registradas: {total_presentes}")
            print(f"   ❌ Total de ausências registradas: {total_ausentes}")
            print(f"   📝 Aulas com ATA: {aulas_com_ata}/{len(resultado)}")
        
        navegador.close()

if __name__ == "__main__":
    main()
