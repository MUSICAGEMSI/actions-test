# script_historico_aulas_otimizado.py
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

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

# Cache para evitar consultas repetidas de ATA
ata_cache = {}
ata_lock = Lock()

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para usar em requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def extrair_detalhes_aula_otimizado(session, aula_id):
    """Extrai detalhes da aula via requests para verificar ATA com cache"""
    with ata_lock:
        if aula_id in ata_cache:
            return ata_cache[aula_id]
    
    try:
        url_detalhes = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/aulas_abertas/listagem',
        }
        
        resp = session.get(url_detalhes, headers=headers, timeout=5)  # Timeout reduzido
        
        if resp.status_code == 200:
            resultado = "OK" if "ATA DA AULA" in resp.text else "FANTASMA"
        else:
            resultado = "ERRO"
        
        # Cache o resultado
        with ata_lock:
            ata_cache[aula_id] = resultado
        
        return resultado
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair detalhes da aula {aula_id}: {e}")
        resultado = "ERRO"
        with ata_lock:
            ata_cache[aula_id] = resultado
        return resultado

def processar_frequencia_modal_rapido(pagina):
    """Processa a frequência de forma mais rápida"""
    try:
        # Aguardar modal com timeout menor
        pagina.wait_for_selector("table.table-bordered tbody tr", timeout=5000)
        
        presentes_ids = []
        presentes_nomes = []
        ausentes_ids = []
        ausentes_nomes = []
        
        # Extrair dados de uma vez usando JavaScript para maior velocidade
        dados_frequencia = pagina.evaluate("""
            () => {
                const linhas = document.querySelectorAll("table.table-bordered tbody tr");
                const dados = {
                    presentes_ids: [],
                    presentes_nomes: [],
                    ausentes_ids: [],
                    ausentes_nomes: []
                };
                
                linhas.forEach(linha => {
                    const nomeCell = linha.querySelector("td:first-child");
                    const linkPresenca = linha.querySelector("td:last-child a");
                    
                    if (!nomeCell || !linkPresenca) return;
                    
                    const nome = nomeCell.textContent.trim();
                    const idMembro = linkPresenca.getAttribute("data-id-membro");
                    const icone = linkPresenca.querySelector("i");
                    
                    if (!nome || !idMembro || !icone) return;
                    
                    const classes = icone.className;
                    
                    if (classes.includes("fa-check text-success")) {
                        dados.presentes_ids.push(idMembro);
                        dados.presentes_nomes.push(nome);
                    } else if (classes.includes("fa-remove text-danger")) {
                        dados.ausentes_ids.push(idMembro);
                        dados.ausentes_nomes.push(nome);
                    }
                });
                
                return dados;
            }
        """)
        
        return {
            'presentes_ids': dados_frequencia['presentes_ids'],
            'presentes_nomes': dados_frequencia['presentes_nomes'],
            'ausentes_ids': dados_frequencia['ausentes_ids'],
            'ausentes_nomes': dados_frequencia['ausentes_nomes'],
            'tem_presenca': "OK" if dados_frequencia['presentes_ids'] else "FANTASMA"
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

def extrair_dados_batch_javascript(pagina):
    """Extrai TODOS os dados da página atual de uma vez usando JavaScript"""
    try:
        dados_pagina = pagina.evaluate("""
            () => {
                const linhas = document.querySelectorAll("table tbody tr");
                const dados = [];
                
                linhas.forEach((linha, indice) => {
                    const colunas = linha.querySelectorAll("td");
                    
                    if (colunas.length >= 6) {
                        const data_aula = colunas[1].textContent.trim();
                        const congregacao = colunas[2].textContent.trim();
                        const curso = colunas[3].textContent.trim();
                        const turma = colunas[4].textContent.trim();
                        
                        const btnFreq = linha.querySelector("button[onclick*='visualizarFrequencias']");
                        
                        if (btnFreq) {
                            const onclick = btnFreq.getAttribute("onclick");
                            const match = onclick.match(/visualizarFrequencias\\((\\d+),\\s*(\\d+)\\)/);
                            
                            if (match) {
                                dados.push({
                                    indice: indice,
                                    aula_id: match[1],
                                    professor_id: match[2],
                                    data: data_aula,
                                    congregacao: congregacao,
                                    curso: curso,
                                    turma: turma,
                                    deve_parar: data_aula.includes("2024")
                                });
                            }
                        }
                    }
                });
                
                return dados;
            }
        """)
        
        return dados_pagina
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair dados em batch: {e}")
        return []

def fechar_modal_rapido(pagina):
    """Fecha o modal de forma mais rápida e robusta"""
    try:
        # Tentar JavaScript primeiro (mais rápido)
        pagina.evaluate("$('#modalFrequencia').modal('hide')")
        
        # Aguardar apenas 1 segundo
        try:
            pagina.wait_for_selector("#modalFrequencia", state="hidden", timeout=1000)
        except:
            # Se não funcionou, usar ESC
            pagina.keyboard.press("Escape")
            
    except Exception as e:
        pagina.keyboard.press("Escape")

def navegar_para_historico_aulas(pagina):
    """Navega pelos menus para chegar ao histórico de aulas"""
    try:
        print("🔍 Navegando para G.E.M...")
        
        # Aguardar o menu carregar após login
        pagina.wait_for_selector("nav", timeout=15000)
        
        # Navegar diretamente via URL (mais rápido)
        print("🌐 Navegando diretamente para URL do histórico...")
        pagina.goto("https://musical.congregacao.org.br/aulas_abertas")
        print("✅ Navegação direta bem-sucedida")
        
        print("⏳ Aguardando página do histórico carregar...")
        
        # Aguardar indicador de carregamento da tabela
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

def processar_lote_atas(session, aula_ids):
    """Processa múltiplas ATAs em paralelo"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(extrair_detalhes_aula_otimizado, session, aula_id): aula_id 
                  for aula_id in aula_ids}
        
        resultados = {}
        for future in concurrent.futures.as_completed(futures):
            aula_id = futures[future]
            try:
                resultado = future.result()
                resultados[aula_id] = resultado
            except Exception as e:
                print(f"⚠️ Erro processando ATA {aula_id}: {e}")
                resultados[aula_id] = "ERRO"
        
        return resultados

def main():
    tempo_inicio = time.time()
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        # Configurações do navegador otimizadas
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
            time.sleep(2)  # Reduzido de 3 para 2
        except Exception as e:
            print(f"⚠️ Erro ao configurar registros: {e}")
        
        # Aguardar carregamento da tabela
        try:
            pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=15000)
            print("✅ Tabela recarregada!")
        except:
            print("⚠️ Timeout aguardando recarregamento - continuando...")
        
        # Criar sessão requests com cookies
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        resultado = []
        pagina_atual = 1
        deve_parar = False
        
        while not deve_parar:
            print(f"📖 Processando página {pagina_atual}...")
            
            # Aguardar com timeout menor
            try:
                pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=5000)
                time.sleep(0.5)  # Reduzido drasticamente
            except:
                print("⚠️ Timeout aguardando linhas - continuando...")
            
            # Extrair TODOS os dados da página de uma vez
            dados_pagina = extrair_dados_batch_javascript(pagina)
            
            if not dados_pagina:
                print("🏁 Não há mais linhas para processar.")
                break
            
            print(f"   📊 Encontradas {len(dados_pagina)} aulas nesta página")
            
            # Verificar se deve parar (encontrou 2024)
            for dados_aula in dados_pagina:
                if dados_aula['deve_parar']:
                    print("🛑 Encontrado ano 2024 - finalizando coleta!")
                    deve_parar = True
                    dados_pagina = [d for d in dados_pagina if not d['deve_parar']]
                    break
            
            # Coletar IDs das aulas para processamento paralelo de ATAs
            aula_ids = [dados['aula_id'] for dados in dados_pagina]
            
            # Processar ATAs em paralelo ENQUANTO processa frequências
            print(f"   🚀 Iniciando processamento paralelo de ATAs...")
            atas_resultados = processar_lote_atas(session, aula_ids)
            
            # Processar frequências de forma otimizada
            for i, dados_aula in enumerate(dados_pagina):
                print(f"      🎯 Aula {i+1}/{len(dados_pagina)}: {dados_aula['data']} - {dados_aula['curso']}")
                
                try:
                    # Clicar no botão usando JavaScript (mais rápido)
                    script_clique = f"""
                        const linhas = document.querySelectorAll("table tbody tr");
                        const linha = linhas[{dados_aula['indice']}];
                        if (linha) {{
                            const btn = linha.querySelector("button[onclick*='visualizarFrequencias']");
                            if (btn) btn.click();
                        }}
                    """
                    
                    pagina.evaluate(script_clique)
                    time.sleep(0.2)  # Pausa mínima
                    
                    # Processar frequência
                    freq_data = processar_frequencia_modal_rapido(pagina)
                    
                    # Fechar modal rapidamente
                    fechar_modal_rapido(pagina)
                    time.sleep(0.1)  # Pausa mínima
                    
                    # Obter status da ATA do resultado paralelo
                    ata_status = atas_resultados.get(dados_aula['aula_id'], "ERRO")
                    
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
            
            if deve_parar:
                break
            
            # Navegar para próxima página de forma otimizada
            try:
                time.sleep(0.5)  # Pausa mínima
                
                # Usar JavaScript para verificar e clicar (mais rápido)
                proxima_pagina = pagina.evaluate("""
                    () => {
                        const btnProximo = document.querySelector("a:has(i.fa-chevron-right)");
                        if (btnProximo) {
                            const parent = btnProximo.parentElement;
                            if (!parent.classList.contains("disabled")) {
                                btnProximo.click();
                                return true;
                            }
                        }
                        return false;
                    }
                """)
                
                if proxima_pagina:
                    print("➡️ Avançando para próxima página...")
                    pagina_atual += 1
                    time.sleep(1)  # Reduzido drasticamente
                    
                    try:
                        pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=5000)
                    except:
                        print("⚠️ Timeout aguardando nova página")
                else:
                    print("🏁 Não há mais páginas.")
                    break
                    
            except Exception as e:
                print(f"⚠️ Erro ao navegar para próxima página: {e}")
                break
        
        tempo_final = time.time()
        tempo_total = (tempo_final - tempo_inicio) / 60
        
        print(f"\n📊 Coleta finalizada! Total de aulas processadas: {len(resultado)}")
        print(f"⏱️ Tempo total: {tempo_total:.1f} minutos ({tempo_total/60:.1f} horas)")
        
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
                "tempo_processamento": f"{tempo_total:.1f} minutos",
                "paginas_processadas": pagina_atual,
                "atas_em_cache": len(ata_cache)
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
        print(f"   ⏱️ Tempo total: {tempo_total:.1f} minutos")
        print(f"   🚀 ATAs processadas em cache: {len(ata_cache)}")
        
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
