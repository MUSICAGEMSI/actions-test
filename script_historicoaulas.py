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
        formatos_data = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y", "%d-%m-%y"]
        data_obj = None
        
        for formato in formatos_data:
            try:
                data_obj = datetime.strptime(data_str.strip(), formato)
                if data_obj.year < 100:
                    data_obj = data_obj.replace(year=data_obj.year + 2000)
                break
            except ValueError:
                continue
        
        if not data_obj:
            print(f"⚠️ Formato de data não reconhecido: {data_str}")
            return False, False
        
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

def extrair_frequencia_via_http(session, aula_id, professor_id):
    """Extrai dados de frequência via requisição HTTP direta (NOVO MÉTODO)"""
    try:
        url_freq = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_frequencias/{aula_id}/{professor_id}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/aulas_abertas/listagem',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        resp = session.get(url_freq, headers=headers, timeout=15)
        
        if resp.status_code != 200:
            print(f"⚠️ Erro HTTP {resp.status_code} ao acessar frequência da aula {aula_id}")
            return {
                'presentes_ids': [],
                'presentes_nomes': [],
                'ausentes_ids': [],
                'ausentes_nomes': [],
                'tem_presenca': "ERRO"
            }
        
        # Parsear HTML com BeautifulSoup
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        presentes_ids = []
        presentes_nomes = []
        ausentes_ids = []
        ausentes_nomes = []
        
        # Buscar tabela de frequência
        tabela = soup.find('table', class_='table-bordered')
        if not tabela:
            print(f"⚠️ Tabela de frequência não encontrada para aula {aula_id}")
            return {
                'presentes_ids': [],
                'presentes_nomes': [],
                'ausentes_ids': [],
                'ausentes_nomes': [],
                'tem_presenca': "FANTASMA"
            }
        
        # Processar linhas da tabela
        tbody = tabela.find('tbody')
        if not tbody:
            print(f"⚠️ Corpo da tabela não encontrado para aula {aula_id}")
            return {
                'presentes_ids': [],
                'presentes_nomes': [],
                'ausentes_ids': [],
                'ausentes_nomes': [],
                'tem_presenca': "FANTASMA"
            }
        
        linhas = tbody.find_all('tr')
        for linha in linhas:
            colunas = linha.find_all('td')
            if len(colunas) < 2:
                continue
            
            # Nome do aluno (primeira coluna)
            nome_completo = colunas[0].get_text(strip=True)
            if not nome_completo:
                continue
            
            # Status de presença (última coluna)
            link_presenca = colunas[-1].find('a')
            if not link_presenca:
                continue
            
            # Extrair ID do membro
            id_membro = link_presenca.get('data-id-membro')
            if not id_membro:
                continue
            
            # Verificar ícone de presença/ausência
            icone = link_presenca.find('i')
            if icone:
                classes = icone.get('class', [])
                classes_str = ' '.join(classes) if isinstance(classes, list) else str(classes)
                
                if 'fa-check' in classes_str and 'text-success' in classes_str:
                    # Presente
                    presentes_ids.append(id_membro)
                    presentes_nomes.append(nome_completo)
                elif 'fa-remove' in classes_str and 'text-danger' in classes_str:
                    # Ausente
                    ausentes_ids.append(id_membro)
                    ausentes_nomes.append(nome_completo)
        
        # Determinar status da presença
        tem_presenca_status = "OK" if (presentes_ids or ausentes_ids) else "FANTASMA"
        
        return {
            'presentes_ids': presentes_ids,
            'presentes_nomes': presentes_nomes,
            'ausentes_ids': ausentes_ids,
            'ausentes_nomes': ausentes_nomes,
            'tem_presenca': tem_presenca_status
        }
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair frequência via HTTP da aula {aula_id}: {e}")
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

def extrair_dados_da_linha(linha_elemento):
    """Extrai dados de uma linha da tabela de forma mais robusta"""
    try:
        colunas = linha_elemento.query_selector_all("td")
        
        if len(colunas) < 4:
            return None
        
        # Buscar data primeiro para estabelecer estrutura
        data_aula = None
        data_col_index = -1
        
        for i, coluna in enumerate(colunas):
            texto = coluna.inner_text().strip()
            # Verificar se é uma data válida
            if re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', texto) and not re.search(r'[a-zA-Z]', texto):
                data_aula = texto
                data_col_index = i
                break
        
        if data_col_index == -1 or not data_aula:
            return None
        
        # Inferir outras colunas baseado na posição da data
        congregacao = "N/A"
        curso = "N/A" 
        turma = "N/A"
        
        if data_col_index >= 3:
            congregacao = colunas[data_col_index-3].inner_text().strip()
            curso = colunas[data_col_index-2].inner_text().strip()
            turma = colunas[data_col_index-1].inner_text().strip()
        elif data_col_index == 2:
            congregacao = colunas[0].inner_text().strip()
            curso = colunas[1].inner_text().strip()
        elif data_col_index == 1:
            congregacao = colunas[0].inner_text().strip()
        
        # Limpar campos de botões/ações
        def limpar_campo(texto):
            botoes_conhecidos = ["frequência", "detalhes", "reabrir", "visualizar", "editar", "excluir"]
            texto_lower = texto.lower()
            for botao in botoes_conhecidos:
                if botao in texto_lower:
                    return "N/A"
            return texto if texto else "N/A"
        
        congregacao = limpar_campo(congregacao)
        curso = limpar_campo(curso)
        turma = limpar_campo(turma)
        
        # Extrair IDs do botão de frequência
        btn_freq = linha_elemento.query_selector("button[onclick*='visualizarFrequencias']")
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
                }
        
        return None
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair dados da linha: {e}")
        return None

def navegar_para_historico_aulas(pagina):
    """Navega pelos menus para chegar ao histórico de aulas"""
    try:
        print("🔍 Navegando para G.E.M...")
        
        pagina.wait_for_selector("nav", timeout=15000)
        
        # Buscar menu G.E.M
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
                    print(f"✅ Menu G.E.M encontrado")
                    elemento_gem.click()
                    menu_gem_clicado = True
                    break
            except Exception:
                continue
        
        if not menu_gem_clicado:
            print("❌ Menu G.E.M não encontrado")
            return False
        
        time.sleep(1)
        
        print("🔍 Procurando Histórico de Aulas...")
        
        # Estratégias para encontrar Histórico de Aulas
        historico_clicado = False
        
        try:
            historico_link = pagina.wait_for_selector('a:has-text("Histórico de Aulas")', 
                                                     state="visible", timeout=10000)
            if historico_link:
                historico_link.click()
                historico_clicado = True
                print("✅ Histórico encontrado via seletor")
        except Exception:
            pass
        
        if not historico_clicado:
            try:
                elemento = pagina.query_selector('a:has-text("Histórico de Aulas")')
                if elemento:
                    pagina.evaluate("element => element.click()", elemento)
                    historico_clicado = True
                    print("✅ Histórico encontrado via JavaScript")
            except Exception:
                pass
        
        if not historico_clicado:
            try:
                pagina.goto("https://musical.congregacao.org.br/aulas_abertas")
                historico_clicado = True
                print("✅ Navegação direta para histórico")
            except Exception:
                pass
        
        if not historico_clicado:
            return False
        
        print("⏳ Aguardando página carregar...")
        
        try:
            pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=20000)
            print("✅ Tabela carregada!")
            return True
        except PlaywrightTimeoutError:
            try:
                pagina.wait_for_selector("table", timeout=5000)
                print("✅ Tabela encontrada")
                return True
            except:
                return False
                
    except Exception as e:
        print(f"❌ Erro na navegação: {e}")
        return False

def processar_pagina_atual(pagina, session):
    """Processa todas as aulas da página atual"""
    try:
        # Aguardar linhas carregarem
        pagina.wait_for_selector('table tbody tr', timeout=10000)
        time.sleep(1)
        
        linhas = pagina.query_selector_all("table tbody tr")
        aulas_processadas = []
        deve_parar = False
        
        print(f"   📊 Processando {len(linhas)} linhas...")
        
        for i, linha in enumerate(linhas):
            # Extrair dados da aula
            dados_aula = extrair_dados_da_linha(linha)
            
            if not dados_aula:
                continue
            
            # Verificar período
            no_periodo, data_anterior = data_esta_no_periodo(dados_aula['data'])
            
            if data_anterior:
                print(f"🛑 Data anterior ao período encontrada: {dados_aula['data']}")
                deve_parar = True
                break
            
            if not no_periodo:
                continue
            
            print(f"      🎯 Processando: {dados_aula['data']} - {dados_aula['curso']}")
            
            # Extrair frequência via HTTP (NOVO MÉTODO - SEM MODAL!)
            freq_data = extrair_frequencia_via_http(session, dados_aula['aula_id'], dados_aula['professor_id'])
            
            # Extrair ATA
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
            
            aulas_processadas.append(linha_resultado)
            
            # Log do resultado
            total_alunos = len(freq_data['presentes_ids']) + len(freq_data['ausentes_ids'])
            print(f"         ✓ {len(freq_data['presentes_ids'])} presentes, {len(freq_data['ausentes_ids'])} ausentes - ATA: {ata_status}")
            
            # Pausa entre requisições para não sobrecarregar servidor
            time.sleep(0.1)
        
        return aulas_processadas, deve_parar
        
    except Exception as e:
        print(f"⚠️ Erro ao processar página: {e}")
        return [], False

def avancar_pagina(pagina):
    """Tenta avançar para a próxima página"""
    try:
        time.sleep(1)
        
        btn_proximo = pagina.query_selector("a:has(i.fa-chevron-right)")
        
        if btn_proximo:
            parent = btn_proximo.query_selector("..")
            parent_class = parent.get_attribute("class") if parent else ""
            
            if "disabled" not in parent_class:
                print("➡️ Avançando para próxima página...")
                btn_proximo.click()
                
                time.sleep(2)
                
                try:
                    pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=10000)
                    return True
                except:
                    pagina.wait_for_selector("table tbody tr", timeout=5000)
                    return True
            else:
                print("🏁 Última página alcançada")
                return False
        else:
            print("🏁 Botão próximo não encontrado")
            return False
            
    except Exception as e:
        print(f"⚠️ Erro ao avançar página: {e}")
        return False

def main():
    tempo_inicio = time.time()
    
    print(f"🎯 COLETANDO DADOS DO SEGUNDO SEMESTRE 2025")
    print(f"📅 Período: {DATA_INICIO} a {DATA_FIM}")
    print(f"🚀 VERSÃO OTIMIZADA - Sem modais, requisições HTTP diretas")
    print("=" * 60)
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
        print("🔐 Fazendo login...")
        pagina.goto(URL_INICIAL)
        
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')
        
        try:
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado!")
        except PlaywrightTimeoutError:
            print("❌ Falha no login")
            navegador.close()
            return
        
        if not navegar_para_historico_aulas(pagina):
            print("❌ Falha na navegação")
            navegador.close()
            return
        
        # Configurar 2000 registros
        print("⚙️ Configurando 2000 registros...")
        try:
            pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
            pagina.select_option('select[name="listagem_length"]', "2000")
            time.sleep(2)
            print("✅ Configurado para 2000 registros")
        except Exception as e:
            print(f"⚠️ Erro ao configurar registros: {e}")
        
        # Criar sessão requests
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # Variáveis de controle
        resultado = []
        pagina_atual = 1
        deve_parar_coleta = False
        
        # Loop principal - processar páginas
        while not deve_parar_coleta:
            print(f"\n📖 PÁGINA {pagina_atual}")
            print("-" * 30)
            
            # Processar página atual
            aulas_pagina, deve_parar = processar_pagina_atual(pagina, session)
            
            if deve_parar:
                deve_parar_coleta = True
                break
            
            if not aulas_pagina:
                print("🛑 Nenhuma aula válida encontrada - finalizando")
                break
            
            resultado.extend(aulas_pagina)
            print(f"✅ {len(aulas_pagina)} aulas coletadas nesta página")
            
            # Tentar avançar para próxima página
            if not avancar_pagina(pagina):
                break
            
            pagina_atual += 1
        
        # Resumo e envio dos dados
        print(f"\n📊 COLETA FINALIZADA!")
        print(f"🎯 Total de aulas coletadas: {len(resultado)}")
        print(f"📄 Páginas processadas: {pagina_atual}")
        print(f"⏱️ Tempo total: {(time.time() - tempo_inicio) / 60:.1f} minutos")
        
        if resultado:
            # Calcular estatísticas
            total_presentes = sum(len(linha[4].split('; ')) if linha[4] else 0 for linha in resultado)
            total_ausentes = sum(len(linha[6].split('; ')) if linha[6] else 0 for linha in resultado)
            aulas_com_ata = sum(1 for linha in resultado if linha[9] == "OK")
            
            print(f"👥 Presenças registradas: {total_presentes}")
            print(f"❌ Ausências registradas: {total_ausentes}")
            print(f"📝 Aulas com ATA: {aulas_com_ata}/{len(resultado)}")
            
            # Preparar dados para envio
            headers = [
                "CONGREGAÇÃO", "CURSO", "TURMA", "DATA", "PRESENTES IDs", 
                "PRESENTES Nomes", "AUSENTES IDs", "AUSENTES Nomes", "TEM PRESENÇA", "ATA DA AULA"
            ]
            
            body = {
                "tipo": "historico_aulas_2sem_2025_otimizado",
                "dados": resultado,
                "headers": headers,
                "resumo": {
                    "total_aulas": len(resultado),
                    "periodo": f"{DATA_INICIO} a {DATA_FIM}",
                    "tempo_processamento": f"{(time.time() - tempo_inicio) / 60:.1f} minutos",
                    "paginas_processadas": pagina_atual,
                    "total_presentes": total_presentes,
                    "total_ausentes": total_ausentes,
                    "aulas_com_ata": aulas_com_ata,
                    "versao": "otimizada_http_direto"
                }
            }
            
            # Enviar para Google Sheets
            try:
                print("\n📤 Enviando para Google Sheets...")
                resposta = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
                print("✅ Dados enviados!")
                print(f"Status: {resposta.status_code}")
                print(f"Resposta: {resposta.text}")
            except Exception as e:
                print(f"❌ Erro no envio: {e}")
        else:
            print("ℹ️ Nenhuma aula encontrada no período")
        
        navegador.close()

if __name__ == "__main__":
    main()
