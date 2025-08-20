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

# Configurações otimizadas
MAX_WORKERS = 8  # Aumentar workers simultâneos
TIMEOUT_MODAL = 3000  # Reduzir timeout dos modais
DELAY_BETWEEN_CLICKS = 0.2  # Reduzir delay entre cliques
MAX_RETRIES = 2  # Reduzir tentativas

# Locks e estruturas globais
print_lock = threading.Lock()
resultado_global = []
resultado_lock = threading.Lock()
stop_processing = threading.Event()
cookies_global = None

def safe_print(message):
    """Print thread-safe"""
    with print_lock:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

def adicionar_resultado(linhas):
    """Adiciona resultados de forma thread-safe"""
    with resultado_lock:
        resultado_global.extend(linhas)

def extrair_detalhes_aula_batch(session, aulas_ids):
    """Extrai detalhes de múltiplas aulas de uma vez - OTIMIZADO"""
    resultados = {}
    
    for aula_id in aulas_ids:
        try:
            url_detalhes = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                'Referer': 'https://musical.congregacao.org.br/aulas_abertas/listagem',
            }
            
            resp = session.get(url_detalhes, headers=headers, timeout=5)  # Timeout reduzido
            
            if resp.status_code == 200:
                resultados[aula_id] = "OK" if "ATA DA AULA" in resp.text else "FANTASMA"
            else:
                resultados[aula_id] = "ERRO"
                
        except Exception as e:
            resultados[aula_id] = "ERRO"
    
    return resultados

def processar_frequencia_modal_rapido(pagina, aula_id):
    """Versão SUPER OTIMIZADA do processamento de modal"""
    try:
        # Aguardar apenas a tabela aparecer - timeout reduzido
        pagina.wait_for_selector("table.table-bordered tbody tr", timeout=TIMEOUT_MODAL)
        
        # Extrair TODOS os dados de uma vez usando JavaScript - MUITO MAIS RÁPIDO!
        dados_freq = pagina.evaluate("""
            () => {
                const linhas = document.querySelectorAll('table.table-bordered tbody tr');
                const presentes_ids = [];
                const presentes_nomes = [];
                const ausentes_ids = [];
                const ausentes_nomes = [];
                
                linhas.forEach(linha => {
                    const nomeCell = linha.querySelector('td:first-child');
                    const nomeCompleto = nomeCell ? nomeCell.innerText.trim() : '';
                    
                    if (!nomeCompleto) return;
                    
                    const linkPresenca = linha.querySelector('td:last-child a');
                    if (!linkPresenca) return;
                    
                    const idMembro = linkPresenca.getAttribute('data-id-membro');
                    if (!idMembro) return;
                    
                    const icone = linkPresenca.querySelector('i');
                    if (!icone) return;
                    
                    const classes = icone.getAttribute('class');
                    
                    if (classes && classes.includes('fa-check text-success')) {
                        presentes_ids.push(idMembro);
                        presentes_nomes.push(nomeCompleto);
                    } else if (classes && classes.includes('fa-remove text-danger')) {
                        ausentes_ids.push(idMembro);
                        ausentes_nomes.push(nomeCompleto);
                    }
                });
                
                return {
                    presentes_ids,
                    presentes_nomes,
                    ausentes_ids,
                    ausentes_nomes,
                    tem_presenca: presentes_ids.length > 0 ? 'OK' : 'FANTASMA'
                };
            }
        """)
        
        return dados_freq
        
    except Exception as e:
        return {
            'presentes_ids': [],
            'presentes_nomes': [],
            'ausentes_ids': [],
            'ausentes_nomes': [],
            'tem_presenca': "ERRO"
        }

def extrair_todas_aulas_da_pagina(pagina):
    """Extrai TODAS as informações das aulas da página atual de uma vez - SUPER OTIMIZADO"""
    try:
        # Usar JavaScript para extrair todos os dados de uma vez - MUITO mais rápido que Playwright
        dados_aulas = pagina.evaluate("""
            () => {
                const linhas = document.querySelectorAll('table tbody tr');
                const aulas = [];
                
                linhas.forEach((linha, indice) => {
                    const colunas = linha.querySelectorAll('td');
                    
                    if (colunas.length >= 6) {
                        const dataAula = colunas[1].innerText.trim();
                        const congregacao = colunas[2].innerText.trim();
                        const curso = colunas[3].innerText.trim();
                        const turma = colunas[4].innerText.trim();
                        
                        const btnFreq = linha.querySelector('button[onclick*="visualizarFrequencias"]');
                        
                        if (btnFreq) {
                            const onclick = btnFreq.getAttribute('onclick');
                            const match = onclick.match(/visualizarFrequencias\\((\\d+),\\s*(\\d+)\\)/);
                            
                            if (match) {
                                aulas.push({
                                    indice: indice,
                                    aula_id: match[1],
                                    professor_id: match[2],
                                    data: dataAula,
                                    congregacao: congregacao,
                                    curso: curso,
                                    turma: turma,
                                    tem_2024: dataAula.includes('2024')
                                });
                            }
                        }
                    }
                });
                
                return aulas;
            }
        """)
        
        return dados_aulas
        
    except Exception as e:
        safe_print(f"❌ Erro ao extrair dados da página: {e}")
        return []

def clicar_botao_por_indice_otimizado(pagina, indice):
    """Versão otimizada para clicar no botão"""
    try:
        # Usar JavaScript direto - mais rápido
        sucesso = pagina.evaluate(f"""
            () => {{
                const linhas = document.querySelectorAll('table tbody tr');
                if ({indice} >= linhas.length) return false;
                
                const linha = linhas[{indice}];
                const btnFreq = linha.querySelector('button[onclick*="visualizarFrequencias"]');
                
                if (btnFreq) {{
                    btnFreq.click();
                    return true;
                }}
                return false;
            }}
        """)
        
        return sucesso
        
    except Exception as e:
        return False

def processar_pagina_ultra_otimizada(contexto, numero_pagina, cookies_dict):
    """Worker ULTRA OTIMIZADO para processar página"""
    safe_print(f"🚀 [P{numero_pagina}] INICIANDO processamento ultra rápido")
    
    pagina = None
    try:
        # Criar nova aba
        pagina = contexto.new_page()
        
        # Configurar timeouts menores
        pagina.set_default_timeout(10000)  # 10 segundos
        
        # Headers otimizados
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
        # IR DIRETO para a página específica do histórico
        url_direto = f"https://musical.congregacao.org.br/aulas_abertas?page={numero_pagina}&length=100"
        pagina.goto(url_direto, timeout=15000)
        
        # Aguardar tabela aparecer
        try:
            pagina.wait_for_selector('table tbody tr', timeout=10000)
        except:
            # Se não carregar, tentar navegação tradicional
            if not navegar_para_historico_aulas_rapido(pagina, numero_pagina):
                safe_print(f"❌ [P{numero_pagina}] Falha na navegação")
                return []
        
        # Extrair TODOS os dados da página de uma vez
        aulas_dados = extrair_todas_aulas_da_pagina(pagina)
        
        if not aulas_dados:
            safe_print(f"📭 [P{numero_pagina}] Página vazia")
            return []
        
        safe_print(f"📊 [P{numero_pagina}] {len(aulas_dados)} aulas encontradas")
        
        # Verificar se tem 2024 - se sim, parar tudo
        tem_2024 = any(aula['tem_2024'] for aula in aulas_dados)
        if tem_2024:
            safe_print(f"🛑 [P{numero_pagina}] ENCONTROU 2024! Parando tudo!")
            stop_processing.set()
            return []
        
        # Criar sessão requests
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # Coletar IDs das aulas para busca em lote da ATA
        aulas_ids = [aula['aula_id'] for aula in aulas_dados]
        safe_print(f"📋 [P{numero_pagina}] Buscando ATAs em lote...")
        atas_resultados = extrair_detalhes_aula_batch(session, aulas_ids)
        
        resultado_pagina = []
        
        # Processar cada aula
        for i, aula in enumerate(aulas_dados):
            if stop_processing.is_set():
                break
            
            try:
                # Fechar modal anterior
                pagina.evaluate("try { $('#modalFrequencia').modal('hide'); } catch(e) {}")
                
                # Clicar no botão
                if clicar_botao_por_indice_otimizado(pagina, aula['indice']):
                    time.sleep(DELAY_BETWEEN_CLICKS)  # Delay mínimo
                    
                    # Processar frequência
                    freq_data = processar_frequencia_modal_rapido(pagina, aula['aula_id'])
                    
                    # Fechar modal rapidamente
                    pagina.evaluate("try { $('#modalFrequencia').modal('hide'); } catch(e) {}")
                    
                    # Pegar ATA do resultado em lote
                    ata_status = atas_resultados.get(aula['aula_id'], "ERRO")
                    
                    # Montar resultado
                    linha_resultado = [
                        aula['congregacao'],
                        aula['curso'],
                        aula['turma'],
                        aula['data'],
                        "; ".join(freq_data['presentes_ids']),
                        "; ".join(freq_data['presentes_nomes']),
                        "; ".join(freq_data['ausentes_ids']),
                        "; ".join(freq_data['ausentes_nomes']),
                        freq_data['tem_presenca'],
                        ata_status
                    ]
                    
                    resultado_pagina.append(linha_resultado)
                    
                    if (i + 1) % 20 == 0:
                        safe_print(f"⚡ [P{numero_pagina}] {i+1}/{len(aulas_dados)} processadas")
                
            except Exception as e:
                safe_print(f"⚠️ [P{numero_pagina}] Erro aula {i+1}: {e}")
                continue
        
        safe_print(f"✅ [P{numero_pagina}] FINALIZADA! {len(resultado_pagina)} aulas coletadas")
        return resultado_pagina
        
    except Exception as e:
        safe_print(f"❌ [P{numero_pagina}] ERRO CRÍTICO: {e}")
        return []
    
    finally:
        if pagina:
            try:
                pagina.close()
            except:
                pass

def navegar_para_historico_aulas_rapido(pagina, numero_pagina):
    """Navegação rápida para histórico"""
    try:
        # Tentar URL direta primeiro
        pagina.goto("https://musical.congregacao.org.br/aulas_abertas", timeout=15000)
        
        # Configurar 100 registros
        try:
            pagina.select_option('select[name="listagem_length"]', "100")
            time.sleep(1)
        except:
            pass
        
        # Navegar para página específica se não for a primeira
        if numero_pagina > 1:
            try:
                link_pagina = pagina.query_selector(f'a:has-text("{numero_pagina}")')
                if link_pagina:
                    link_pagina.click()
                    time.sleep(1)
            except:
                pass
        
        # Verificar se carregou
        try:
            pagina.wait_for_selector('table tbody tr', timeout=5000)
            return True
        except:
            return False
            
    except Exception as e:
        return False

def descobrir_total_paginas_rapido(pagina_principal):
    """Versão rápida para descobrir total de páginas"""
    try:
        # Usar JavaScript para pegar todos os números da paginação
        max_pagina = pagina_principal.evaluate("""
            () => {
                const links = document.querySelectorAll('ul.pagination li a');
                let maxNum = 1;
                
                links.forEach(link => {
                    const texto = link.innerText.trim();
                    if (/^\\d+$/.test(texto)) {
                        maxNum = Math.max(maxNum, parseInt(texto));
                    }
                });
                
                return maxNum;
            }
        """)
        
        safe_print(f"📄 Total de páginas detectadas: {max_pagina}")
        return max_pagina if max_pagina > 1 else 30  # Fallback
        
    except Exception as e:
        safe_print(f"⚠️ Erro ao detectar páginas: {e}")
        return 30

def main():
    tempo_inicio = time.time()
    
    safe_print("🚀 INICIANDO COLETA ULTRA OTIMIZADA!")
    
    try:
        with sync_playwright() as p:
            # Navegador otimizado para velocidade
            navegador = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-images',  # Não carregar imagens
                    '--disable-javascript-harmony-shipping',
                    '--disable-extensions',
                    '--disable-plugins',
                    '--no-first-run',
                    '--disable-default-apps',
                    '--disable-popup-blocking',
                    '--disable-translate',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding',
                    '--disable-features=TranslateUI,VizDisplayCompositor'
                ]
            )
            
            contexto = navegador.new_context()
            pagina_principal = contexto.new_page()
            
            # LOGIN ÚNICO E RÁPIDO
            safe_print("🔐 Login único ultra rápido...")
            pagina_principal.goto(URL_INICIAL, timeout=30000)
            
            pagina_principal.fill('input[name="login"]', EMAIL)
            pagina_principal.fill('input[name="password"]', SENHA)
            pagina_principal.click('button[type="submit"]')
            
            try:
                pagina_principal.wait_for_selector("nav", timeout=15000)
                safe_print("✅ Login OK!")
            except:
                safe_print("❌ Falha no login")
                navegador.close()
                return
            
            # Navegar para histórico
            if not navegar_para_historico_aulas_rapido(pagina_principal, 1):
                safe_print("❌ Falha na navegação inicial")
                navegador.close()
                return
            
            # Descobrir total de páginas
            total_paginas = descobrir_total_paginas_rapido(pagina_principal)
            
            # Extrair cookies
            cookies_dict = {cookie['name']: cookie['value'] for cookie in pagina_principal.context.cookies()}
            
            safe_print(f"🎯 PROCESSAMENTO PARALELO ULTRA OTIMIZADO")
            safe_print(f"📄 Páginas: {total_paginas} | Workers: {MAX_WORKERS}")
            
            # Reset flag
            stop_processing.clear()
            
            # EXECUÇÃO PARALELA ULTRA OTIMIZADA
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {}
                
                # Submeter todas as tarefas
                for pagina_num in range(1, total_paginas + 1):
                    future = executor.submit(processar_pagina_ultra_otimizada, contexto, pagina_num, cookies_dict)
                    futures[future] = pagina_num
                
                safe_print(f"🚀 {len(futures)} workers executando!")
                
                # Coletar resultados
                processadas = 0
                for future in as_completed(futures):
                    pagina_num = futures[future]
                    try:
                        resultado = future.result(timeout=300)  # 5 min timeout
                        if resultado:
                            adicionar_resultado(resultado)
                        
                        processadas += 1
                        safe_print(f"📊 [{processadas}/{len(futures)}] P{pagina_num}: {len(resultado) if resultado else 0} aulas")
                        
                    except Exception as e:
                        processadas += 1
                        safe_print(f"⚠️ [{processadas}/{len(futures)}] P{pagina_num}: ERRO - {e}")
            
            # Resumo e envio
            tempo_total = (time.time() - tempo_inicio) / 60
            
            safe_print(f"\n🎉 COLETA ULTRA RÁPIDA FINALIZADA!")
            safe_print(f"   ⏱️ Tempo: {tempo_total:.1f} minutos")
            safe_print(f"   🎯 Aulas coletadas: {len(resultado_global)}")
            safe_print(f"   ⚡ Velocidade: {len(resultado_global)/tempo_total:.0f} aulas/min")
            
            # Enviar para Apps Script
            if resultado_global:
                headers = [
                    "CONGREGAÇÃO", "CURSO", "TURMA", "DATA", "PRESENTES IDs", 
                    "PRESENTES Nomes", "AUSENTES IDs", "AUSENTES Nomes", "TEM PRESENÇA", "ATA DA AULA"
                ]
                
                body = {
                    "tipo": "historico_aulas_ultra_otimizado",
                    "dados": resultado_global,
                    "headers": headers,
                    "resumo": {
                        "total_aulas": len(resultado_global),
                        "tempo_processamento": f"{tempo_total:.1f} minutos",
                        "workers_utilizados": MAX_WORKERS,
                        "velocidade_aulas_por_minuto": f"{len(resultado_global)/tempo_total:.0f}"
                    }
                }
                
                try:
                    safe_print("📤 Enviando para Google Sheets...")
                    resposta = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
                    safe_print(f"✅ Enviado! Status: {resposta.status_code}")
                    safe_print(f"Resposta: {resposta.text}")
                except Exception as e:
                    safe_print(f"❌ Erro no envio: {e}")
            
            navegador.close()
            
    except Exception as e:
        safe_print(f"❌ ERRO CRÍTICO: {e}")
        if resultado_global:
            safe_print(f"💾 Dados coletados até o erro: {len(resultado_global)} aulas")

if __name__ == "__main__":
    main()
