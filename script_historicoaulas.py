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
from datetime import datetime
from queue import Queue

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

# Configurações otimizadas para GitHub Actions
MAX_ABAS_SIMULTANEAS = 16  # GitHub Actions tem recursos robustos
TIMEOUT_RAPIDO = 8         # Timeouts reduzidos para CI/CD
TIMEOUT_MODAL = 5
PAUSA_ENTRE_MODAIS = 0.1   # Pausa mínima entre operações

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

class ColetorTurbo:
    def __init__(self):
        self.resultado_global = []
        self.lock = threading.Lock()
        self.session_cookies = None
        self.contexto_principal = None
        self.total_processadas = 0
        
    def extrair_cookies_playwright(self, pagina):
        """Extrai cookies do Playwright para usar em requests"""
        cookies = pagina.context.cookies()
        return {cookie['name']: cookie['value'] for cookie in cookies}

    def extrair_detalhes_aula_batch(self, aulas_ids):
        """Extrai detalhes de múltiplas aulas de uma vez via requests"""
        resultados = {}
        
        if not self.session_cookies:
            return {aula_id: "ERRO" for aula_id in aulas_ids}
            
        session = requests.Session()
        session.cookies.update(self.session_cookies)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://musical.congregacao.org.br/aulas_abertas/listagem',
            'Connection': 'keep-alive'
        }
        
        for aula_id in aulas_ids:
            try:
                url_detalhes = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
                resp = session.get(url_detalhes, headers=headers, timeout=3)
                
                if resp.status_code == 200:
                    resultados[aula_id] = "OK" if "ATA DA AULA" in resp.text else "FANTASMA"
                else:
                    resultados[aula_id] = "ERRO"
                    
            except Exception:
                resultados[aula_id] = "ERRO"
        
        return resultados

    def processar_frequencia_modal_rapido(self, pagina):
        """Versão ultra otimizada do processamento de frequência"""
        try:
            # Aguardar com timeout mínimo
            pagina.wait_for_selector("table.table-bordered tbody tr", timeout=TIMEOUT_MODAL * 1000)
            
            # Extrair dados via JavaScript para máxima velocidade
            dados_frequencia = pagina.evaluate("""
                () => {
                    const linhas = document.querySelectorAll('table.table-bordered tbody tr');
                    const presentes_ids = [], presentes_nomes = [];
                    const ausentes_ids = [], ausentes_nomes = [];
                    
                    linhas.forEach(linha => {
                        const nomeTd = linha.querySelector('td:first-child');
                        if (!nomeTd) return;
                        
                        const nome = nomeTd.innerText.trim();
                        if (!nome) return;
                        
                        const linkPresenca = linha.querySelector('td:last-child a');
                        if (!linkPresenca) return;
                        
                        const idMembro = linkPresenca.getAttribute('data-id-membro');
                        if (!idMembro) return;
                        
                        const icone = linkPresenca.querySelector('i');
                        if (!icone) return;
                        
                        const classes = icone.className;
                        
                        if (classes.includes('fa-check text-success')) {
                            presentes_ids.push(idMembro);
                            presentes_nomes.push(nome);
                        } else if (classes.includes('fa-remove text-danger')) {
                            ausentes_ids.push(idMembro);
                            ausentes_nomes.push(nome);
                        }
                    });
                    
                    return {
                        presentes_ids, presentes_nomes,
                        ausentes_ids, ausentes_nomes,
                        tem_presenca: presentes_ids.length > 0 ? 'OK' : 'FANTASMA'
                    };
                }
            """)
            
            return dados_frequencia
            
        except Exception as e:
            return {
                'presentes_ids': [], 'presentes_nomes': [],
                'ausentes_ids': [], 'ausentes_nomes': [],
                'tem_presenca': "ERRO"
            }

    def processar_pagina_turbo(self, numero_pagina, total_paginas):
        """Versão turbo do processamento de página com contexto compartilhado"""
        print(f"🚀 [P{numero_pagina:02d}] Iniciando processamento turbo")
        
        try:
            # Criar nova página no MESMO contexto (compartilha cookies automaticamente)
            nova_aba = self.contexto_principal.new_page()
            
            # Configurações de performance para CI/CD
            nova_aba.set_default_timeout(TIMEOUT_RAPIDO * 1000)
            nova_aba.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
                'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8'
            })
            
            # URL direta com parâmetros de página
            if numero_pagina == 1:
                url_pagina = "https://musical.congregacao.org.br/aulas_abertas"
            else:
                # Calcular start baseado na página (assumindo 2000 por página)
                start = (numero_pagina - 1) * 2000
                url_pagina = f"https://musical.congregacao.org.br/aulas_abertas?start={start}&length=2000"
            
            nova_aba.goto(url_pagina, wait_until="domcontentloaded")
            
            # Configurar 2000 registros imediatamente
            try:
                nova_aba.wait_for_selector('select[name="listagem_length"]', timeout=TIMEOUT_RAPIDO * 1000)
                nova_aba.select_option('select[name="listagem_length"]', "2000")
                time.sleep(1.5)  # Reduzido para CI/CD
            except:
                pass
            
            # Se não é página 1, navegar para ela
            if numero_pagina > 1:
                try:
                    # Tentar clique direto no número
                    link_pagina = nova_aba.query_selector(f'a:has-text("{numero_pagina}")')
                    if link_pagina:
                        link_pagina.click()
                        time.sleep(2)
                    else:
                        # Navegar sequencialmente (mais lento mas confiável)
                        for _ in range(numero_pagina - 1):
                            btn_proximo = nova_aba.query_selector("a:has(i.fa-chevron-right)")
                            if btn_proximo:
                                btn_proximo.click()
                                time.sleep(0.8)
                            else:
                                break
                except:
                    pass
            
            # Aguardar tabela carregar
            nova_aba.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=TIMEOUT_RAPIDO * 1000)
            time.sleep(0.5)
            
            # Extrair TODOS os dados da página via JavaScript (ultra rápido)
            dados_pagina = nova_aba.evaluate("""
                () => {
                    const linhas = document.querySelectorAll('table tbody tr');
                    const dados = [];
                    
                    linhas.forEach((linha, index) => {
                        const colunas = linha.querySelectorAll('td');
                        if (colunas.length >= 6) {
                            const data = colunas[1]?.innerText?.trim() || '';
                            const congregacao = colunas[2]?.innerText?.trim() || '';
                            const curso = colunas[3]?.innerText?.trim() || '';
                            const turma = colunas[4]?.innerText?.trim() || '';
                            
                            const btnFreq = linha.querySelector('button[onclick*="visualizarFrequencias"]');
                            if (btnFreq) {
                                const onclick = btnFreq.getAttribute('onclick');
                                const match = onclick.match(/visualizarFrequencias\\((\\d+),\\s*(\\d+)\\)/);
                                if (match) {
                                    dados.push({
                                        index: index,
                                        data: data,
                                        congregacao: congregacao,
                                        curso: curso,
                                        turma: turma,
                                        aula_id: match[1],
                                        professor_id: match[2]
                                    });
                                }
                            }
                        }
                    });
                    
                    return dados;
                }
            """)
            
            resultado_pagina = []
            aulas_ids = [item['aula_id'] for item in dados_pagina]
            
            print(f"   📊 [P{numero_pagina:02d}] {len(dados_pagina)} aulas encontradas")
            
            # Verificar se chegou em 2024 (parar se necessário)
            if dados_pagina and "2024" in dados_pagina[0]['data']:
                print(f"   🛑 [P{numero_pagina:02d}] Ano 2024 detectado - parando")
                nova_aba.close()
                return
            
            # Obter ATAs em batch (paralelamente)
            atas_status = self.extrair_detalhes_aula_batch(aulas_ids)
            
            # Processar cada aula na página
            for i, item in enumerate(dados_pagina):
                try:
                    # Parar se chegou em 2024
                    if "2024" in item['data']:
                        print(f"   🛑 [P{numero_pagina:02d}] Parando em 2024")
                        break
                    
                    # Fechar qualquer modal anterior
                    nova_aba.evaluate("$('#modalFrequencia').modal('hide')")
                    
                    # Clicar no botão de frequência via JavaScript (mais rápido)
                    clicou = nova_aba.evaluate(f"""
                        () => {{
                            const linhas = document.querySelectorAll('table tbody tr');
                            const linha = linhas[{i}];
                            const btn = linha?.querySelector('button[onclick*="visualizarFrequencias"]');
                            if (btn) {{
                                btn.click();
                                return true;
                            }}
                            return false;
                        }}
                    """)
                    
                    if clicou:
                        time.sleep(PAUSA_ENTRE_MODAIS)
                        
                        # Processar frequência
                        freq_data = self.processar_frequencia_modal_rapido(nova_aba)
                        
                        # Fechar modal rapidamente
                        nova_aba.evaluate("$('#modalFrequencia').modal('hide')")
                        time.sleep(PAUSA_ENTRE_MODAIS)
                        
                        # Montar resultado
                        linha_resultado = [
                            item['congregacao'], item['curso'], item['turma'], item['data'],
                            "; ".join(freq_data['presentes_ids']),
                            "; ".join(freq_data['presentes_nomes']),
                            "; ".join(freq_data['ausentes_ids']),
                            "; ".join(freq_data['ausentes_nomes']),
                            freq_data['tem_presenca'],
                            atas_status.get(item['aula_id'], "ERRO")
                        ]
                        
                        resultado_pagina.append(linha_resultado)
                        
                        # Log compacto para CI/CD
                        total_alunos = len(freq_data['presentes_ids']) + len(freq_data['ausentes_ids'])
                        if i % 5 == 0 or i == len(dados_pagina) - 1:  # Log a cada 5 ou última
                            print(f"   ⚡ [P{numero_pagina:02d}] {i+1}/{len(dados_pagina)} - {item['data'][:5]} - {total_alunos}alunos")
                
                except Exception as e:
                    print(f"   ⚠️ [P{numero_pagina:02d}] Erro linha {i}: {str(e)[:50]}")
                    continue
            
            # Thread-safe: adicionar ao resultado global
            with self.lock:
                self.resultado_global.extend(resultado_pagina)
                self.total_processadas += len(resultado_pagina)
                print(f"   ✅ [P{numero_pagina:02d}] Concluída: {len(resultado_pagina)} aulas | Total global: {self.total_processadas}")
            
            nova_aba.close()
            
        except Exception as e:
            print(f"   ❌ [P{numero_pagina:02d}] Erro crítico: {e}")

    def descobrir_total_paginas(self, pagina):
        """Descobre total de páginas de forma otimizada"""
        try:
            # Configurar 2000 primeiro
            pagina.wait_for_selector('select[name="listagem_length"]', timeout=TIMEOUT_RAPIDO * 1000)
            pagina.select_option('select[name="listagem_length"]', "2000")
            time.sleep(2)
            
            # Descobrir via JavaScript
            total_paginas = pagina.evaluate("""
                () => {
                    const links = document.querySelectorAll('.pagination a');
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
            
            print(f"📊 Total de páginas descobertas: {total_paginas}")
            return total_paginas
                
        except Exception as e:
            print(f"⚠️ Erro ao descobrir páginas: {e} - usando 1")
            return 1

    def main(self):
        tempo_inicio = time.time()
        
        with sync_playwright() as p:
            # Configurações otimizadas para GitHub Actions
            navegador = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-extensions',
                    '--disable-plugins',
                    '--disable-images',  # Não carregar imagens (mais rápido)
                    '--disable-javascript-harmony-shipping',
                    '--disable-background-timer-throttling',
                    '--disable-renderer-backgrounding',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-ipc-flooding-protection',
                    '--memory-pressure-off'
                ]
            )
            
            # Criar contexto compartilhado
            self.contexto_principal = navegador.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
            )
            
            # Página principal para login
            pagina_principal = self.contexto_principal.new_page()
            pagina_principal.set_default_timeout(TIMEOUT_RAPIDO * 1000)
            
            print("🔐 Login ultra-rápido...")
            pagina_principal.goto(URL_INICIAL, wait_until="domcontentloaded")
            
            # Login
            pagina_principal.fill('input[name="login"]', EMAIL)
            pagina_principal.fill('input[name="password"]', SENHA)
            pagina_principal.click('button[type="submit"]')
            
            try:
                pagina_principal.wait_for_selector("nav", timeout=TIMEOUT_RAPIDO * 1000)
                print("✅ Login realizado!")
            except PlaywrightTimeoutError:
                print("❌ Falha no login.")
                navegador.close()
                return
            
            # Navegar para histórico
            print("🔍 Navegando para histórico...")
            try:
                # Tentar menu primeiro
                elemento_gem = pagina_principal.query_selector('a:has-text("G.E.M")')
                if elemento_gem:
                    elemento_gem.click()
                    time.sleep(1.5)
            except:
                pass
            
            # URL direta
            pagina_principal.goto("https://musical.congregacao.org.br/aulas_abertas", wait_until="domcontentloaded")
            
            # Extrair cookies para requests
            self.session_cookies = self.extrair_cookies_playwright(pagina_principal)
            
            # Descobrir total de páginas
            total_paginas = self.descobrir_total_paginas(pagina_principal)
            
            # Fechar página principal (já temos o contexto)
            pagina_principal.close()
            
            print(f"🚀 COLETA TURBO: {total_paginas} páginas com {MAX_ABAS_SIMULTANEAS} abas simultâneas!")
            print(f"⚡ Estimativa: {total_paginas * 2000 / MAX_ABAS_SIMULTANEAS / 60:.1f} minutos")
            
            # Processamento paralelo TURBO
            with ThreadPoolExecutor(max_workers=MAX_ABAS_SIMULTANEAS) as executor:
                futures = []
                for pagina_num in range(1, total_paginas + 1):
                    future = executor.submit(self.processar_pagina_turbo, pagina_num, total_paginas)
                    futures.append(future)
                
                # Aguardar todas as threads
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"❌ Thread error: {e}")
            
            navegador.close()
            
            # Resultado final
            tempo_total = (time.time() - tempo_inicio) / 60
            
            print(f"\n🎯 COLETA TURBO FINALIZADA!")
            print(f"   📊 Total de aulas: {len(self.resultado_global)}")
            print(f"   ⏱️ Tempo: {tempo_total:.1f} minutos")
            print(f"   ⚡ Velocidade: {len(self.resultado_global) / tempo_total:.0f} aulas/min")
            print(f"   🚀 Speedup: ~{30*60 / tempo_total:.0f}x mais rápido que o original!")
            
            # Enviar para Google Sheets
            if self.resultado_global:
                headers = [
                    "CONGREGAÇÃO", "CURSO", "TURMA", "DATA", "PRESENTES IDs", 
                    "PRESENTES Nomes", "AUSENTES IDs", "AUSENTES Nomes", "TEM PRESENÇA", "ATA DA AULA"
                ]
                
                body = {
                    "tipo": "historico_aulas_turbo",
                    "dados": self.resultado_global,
                    "headers": headers,
                    "resumo": {
                        "total_aulas": len(self.resultado_global),
                        "tempo_processamento_minutos": tempo_total,
                        "paginas_processadas": total_paginas,
                        "abas_simultaneas": MAX_ABAS_SIMULTANEAS,
                        "aulas_por_minuto": len(self.resultado_global) / tempo_total
                    }
                }
                
                try:
                    print("📤 Enviando para Google Sheets...")
                    resposta = requests.post(URL_APPS_SCRIPT, json=body, timeout=180)
                    print(f"✅ Enviado! Status: {resposta.status_code}")
                    if resposta.status_code != 200:
                        print(f"⚠️ Resposta: {resposta.text[:200]}")
                except Exception as e:
                    print(f"❌ Erro no envio: {e}")
                    # Salvar localmente como backup
                    with open('backup_dados.json', 'w') as f:
                        json.dump(body, f, indent=2)
                    print("💾 Dados salvos em backup_dados.json")

if __name__ == "__main__":
    print("🚀 INICIANDO COLETOR TURBO PARA GITHUB ACTIONS")
    print("=" * 60)
    
    coletor = ColetorTurbo()
    coletor.main()
