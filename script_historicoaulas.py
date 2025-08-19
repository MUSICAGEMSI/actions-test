# script_historico_aulas.py
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
URL_HISTORICO_AULAS = "https://musical.congregacao.org.br/aulas_abertas/listagem"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

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
            'Referer': URL_HISTORICO_AULAS,
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
            
            # Extrair status de presença
            link_presenca = linha.query_selector("td:last-child a")
            
            if link_presenca:
                # Extrair ID do membro do data-id-membro
                id_membro = link_presenca.get_attribute("data-id-membro")
                
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

def extrair_dados_linha_aula(linha):
    """Extrai dados básicos de uma linha de aula"""
    try:
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
                # Extrair os dois IDs: visualizarFrequencias(aula_id, professor_id)
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
        print(f"⚠️ Erro ao extrair dados da linha: {e}")
        return None, False

def main():
    tempo_inicio = time.time()
    
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
        
        # Navegar para histórico de aulas
        print("📚 Navegando para histórico de aulas...")
        pagina.goto(URL_HISTORICO_AULAS)
        
        # Aguardar carregamento da página
        pagina.wait_for_selector("select[name='listagem_length']", timeout=10000)
        
        # Configurar para mostrar 2000 registros
        print("⚙️ Configurando para mostrar 2000 registros...")
        pagina.select_option("select[name='listagem_length']", "2000")
        
        # Aguardar carregamento da primeira linha
        pagina.wait_for_selector("table tbody tr:first-child", timeout=15000)
        
        # Criar sessão requests com cookies para detalhes das aulas
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        resultado = []
        pagina_atual = 1
        deve_parar = False
        
        while not deve_parar:
            print(f"📖 Processando página {pagina_atual}...")
            
            # Aguardar linhas carregarem
            try:
                pagina.wait_for_selector("table tbody tr", timeout=10000)
                time.sleep(2)  # Aguardar estabilização
            except:
                print("⚠️ Timeout aguardando linhas - tentando continuar...")
            
            # Obter todas as linhas da página atual
            linhas = pagina.query_selector_all("table tbody tr")
            
            if not linhas:
                print("🏁 Não há mais linhas para processar.")
                break
            
            print(f"   📊 Encontradas {len(linhas)} aulas nesta página")
            
            # Processar cada linha
            for i, linha in enumerate(linhas):
                dados_aula, deve_parar_ano = extrair_dados_linha_aula(linha)
                
                if deve_parar_ano:
                    print("🛑 Encontrado ano 2024 - finalizando coleta!")
                    deve_parar = True
                    break
                
                if not dados_aula:
                    continue
                
                print(f"      🎯 Aula {i+1}/{len(linhas)}: {dados_aula['data']} - {dados_aula['curso']}")
                
                # Clicar no botão de frequência para abrir modal
                try:
                    btn_freq = linha.query_selector("button[onclick*='visualizarFrequencias']")
                    if btn_freq:
                        btn_freq.click()
                        
                        # Processar dados de frequência
                        freq_data = processar_frequencia_modal(pagina, dados_aula['aula_id'], dados_aula['professor_id'])
                        
                        # Fechar modal (buscar botão de fechar ou pressionar ESC)
                        try:
                            btn_fechar = pagina.query_selector("button.close, .modal-header button, [data-dismiss='modal']")
                            if btn_fechar:
                                btn_fechar.click()
                            else:
                                pagina.keyboard.press("Escape")
                        except:
                            pagina.keyboard.press("Escape")
                        
                        # Aguardar modal fechar
                        time.sleep(1)
                        
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
                        
                except Exception as e:
                    print(f"⚠️ Erro ao processar aula: {e}")
                    continue
                
                # Pequena pausa entre aulas
                time.sleep(0.5)
            
            if deve_parar:
                break
            
            # Tentar avançar para próxima página
            try:
                btn_proximo = pagina.query_selector("a:has(i.fa-chevron-right)")
                if btn_proximo and not btn_proximo.get_attribute("class") or "disabled" not in btn_proximo.get_attribute("class"):
                    print("➡️ Avançando para próxima página...")
                    btn_proximo.click()
                    pagina_atual += 1
                    
                    # Aguardar nova página carregar
                    time.sleep(3)
                else:
                    print("🏁 Não há mais páginas para processar.")
                    break
            except Exception as e:
                print(f"⚠️ Erro ao navegar para próxima página: {e}")
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
        print("\n📈 RESUMO DA COLETA:")
        print(f"   🎯 Total de aulas: {len(resultado)}")
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
