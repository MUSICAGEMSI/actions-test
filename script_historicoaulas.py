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
import asyncio

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

DATA_INICIO = "04/07/2025"
DATA_FIM = "31/12/2025"

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
            return False, False
        
        inicio = datetime.strptime("04/07/2025", "%d/%m/%Y")
        fim = datetime.strptime("31/12/2025", "%d/%m/%Y")
        
        if inicio <= data_obj <= fim:
            return True, False
        elif data_obj < inicio:
            return False, True
        else:
            return False, False
        
    except Exception as e:
        return False, False

class AjaxFrequenciaCollector:
    """Coletor que intercepta chamadas AJAX de frequência"""
    
    def __init__(self):
        self.ajax_responses = {}
        self.intercepted_data = {}
        
    def setup_page_interception(self, pagina):
        """Configura interceptação de requests na página"""
        
        def handle_response(response):
            try:
                url = response.url
                
                # Interceptar chamadas AJAX relacionadas à frequência
                if any(keyword in url for keyword in [
                    'visualizar_frequencias', 'frequencia', 'presenca', 
                    'ajax', 'carregarFrequencia', 'obterFrequencia'
                ]):
                    print(f"🕵️ AJAX interceptado: {url}")
                    
                    # Extrair IDs da URL
                    match = re.search(r'/(\d+)/(\d+)/?$', url)
                    if match:
                        aula_id = match.group(1)
                        professor_id = match.group(2)
                        
                        # Salvar resposta
                        try:
                            content = response.text()
                            self.ajax_responses[f"{aula_id}_{professor_id}"] = {
                                'url': url,
                                'status': response.status,
                                'content': content,
                                'headers': dict(response.headers)
                            }
                            print(f"   ✅ Dados salvos para aula {aula_id}")
                        except Exception as e:
                            print(f"   ⚠️ Erro ao salvar resposta: {e}")
                            
            except Exception as e:
                print(f"⚠️ Erro no interceptor: {e}")
        
        # Configurar interceptação
        pagina.on("response", handle_response)
        
        # JavaScript para interceptar chamadas AJAX
        js_interceptor = """
        // Interceptar XMLHttpRequest
        (function() {
            const originalOpen = XMLHttpRequest.prototype.open;
            const originalSend = XMLHttpRequest.prototype.send;
            
            XMLHttpRequest.prototype.open = function(method, url, ...args) {
                this._url = url;
                this._method = method;
                return originalOpen.call(this, method, url, ...args);
            };
            
            XMLHttpRequest.prototype.send = function(data) {
                this.addEventListener('load', function() {
                    if (this._url && (this._url.includes('frequencia') || this._url.includes('presenca'))) {
                        console.log('🔍 XHR Interceptado:', this._method, this._url);
                        console.log('📊 Resposta:', this.responseText);
                        
                        // Armazenar dados globalmente
                        if (!window._interceptedData) window._interceptedData = {};
                        window._interceptedData[this._url] = {
                            method: this._method,
                            response: this.responseText,
                            status: this.status
                        };
                    }
                });
                
                return originalSend.call(this, data);
            };
            
            // Interceptar fetch também
            const originalFetch = window.fetch;
            window.fetch = function(url, options = {}) {
                const promise = originalFetch.call(this, url, options);
                
                if (typeof url === 'string' && (url.includes('frequencia') || url.includes('presenca'))) {
                    console.log('🔍 Fetch Interceptado:', options.method || 'GET', url);
                    
                    promise.then(response => {
                        return response.clone().text().then(text => {
                            console.log('📊 Fetch Resposta:', text);
                            
                            if (!window._interceptedData) window._interceptedData = {};
                            window._interceptedData[url] = {
                                method: options.method || 'GET',
                                response: text,
                                status: response.status
                            };
                        });
                    });
                }
                
                return promise;
            };
            
            console.log('🚀 Interceptação AJAX configurada!');
        })();
        """
        
        pagina.evaluate(js_interceptor)
    
    def processar_frequencia_otimizada(self, pagina, aula_id, professor_id):
        """Processa frequência usando método otimizado com interceptação"""
        
        try:
            print(f"      🎯 Processando aula {aula_id}...")
            
            # 1. MÉTODO: Tentar obter dados interceptados primeiro
            dados_interceptados = self.obter_dados_interceptados(pagina, aula_id, professor_id)
            if dados_interceptados:
                return dados_interceptados
            
            # 2. MÉTODO: JavaScript direto para carregar dados
            script_carregar = f"""
            // Tentar carregar dados via JavaScript
            if (typeof visualizarFrequencias === 'function') {{
                visualizarFrequencias({aula_id}, {professor_id});
                return 'modal_triggered';
            }} else if (typeof carregarFrequencia === 'function') {{
                carregarFrequencia({aula_id}, {professor_id});
                return 'ajax_triggered';
            }} else {{
                return 'no_function';
            }}
            """
            
            trigger_result = pagina.evaluate(script_carregar)
            print(f"         🔧 Trigger result: {trigger_result}")
            
            # Aguardar dados carregarem
            time.sleep(0.5)
            
            # 3. MÉTODO: Extrair dados do DOM após carregamento
            dados_dom = self.extrair_dados_dom_completo(pagina)
            if dados_dom['tem_dados']:
                return dados_dom
            
            # 4. MÉTODO FALLBACK: Verificar dados interceptados novamente
            time.sleep(0.5)
            dados_interceptados = self.obter_dados_interceptados(pagina, aula_id, professor_id)
            if dados_interceptados:
                return dados_interceptados
            
            # 5. ÚLTIMO RECURSO: Modal tradicional
            return self.modal_fallback(pagina, aula_id, professor_id)
            
        except Exception as e:
            print(f"         ❌ Erro ao processar: {e}")
            return self.resultado_erro()
    
    def obter_dados_interceptados(self, pagina, aula_id, professor_id):
        """Obtém dados das chamadas AJAX interceptadas"""
        
        # Verificar dados interceptados via JavaScript
        script_obter = """
        if (window._interceptedData) {
            const dados = {};
            for (const [url, data] of Object.entries(window._interceptedData)) {
                dados[url] = data;
            }
            return dados;
        }
        return null;
        """
        
        intercepted = pagina.evaluate(script_obter)
        
        if intercepted:
            print(f"         📡 {len(intercepted)} chamadas AJAX interceptadas")
            
            # Processar dados interceptados
            for url, data in intercepted.items():
                if aula_id in url:
                    try:
                        # Parsear HTML da resposta AJAX
                        soup = BeautifulSoup(data['response'], 'html.parser')
                        return self.extrair_frequencia_html(soup)
                    except Exception as e:
                        print(f"         ⚠️ Erro ao parsear AJAX: {e}")
        
        # Verificar também dados salvos na classe
        key = f"{aula_id}_{professor_id}"
        if key in self.ajax_responses:
            try:
                content = self.ajax_responses[key]['content']
                soup = BeautifulSoup(content, 'html.parser')
                return self.extrair_frequencia_html(soup)
            except Exception as e:
                print(f"         ⚠️ Erro ao processar resposta salva: {e}")
        
        return None
    
    def extrair_dados_dom_completo(self, pagina):
        """Extrai dados diretamente do DOM após JavaScript executar"""
        
        script_extrair = """
        function extrairFrequenciaCompleta() {
            const presentes_ids = [];
            const presentes_nomes = [];
            const ausentes_ids = [];
            const ausentes_nomes = [];
            
            // Procurar em modal aberto
            let tabela = document.querySelector('#modalFrequencia table tbody');
            
            // Se modal não está aberto, procurar em qualquer tabela
            if (!tabela) {
                tabela = document.querySelector('table tbody');
            }
            
            // Procurar também em elementos específicos
            if (!tabela) {
                const tabelas = document.querySelectorAll('table');
                for (const t of tabelas) {
                    const linhas = t.querySelectorAll('tbody tr');
                    if (linhas.length > 0) {
                        // Verificar se tem estrutura de frequência
                        const primeiraLinha = linhas[0];
                        if (primeiraLinha.querySelector('a[data-id-membro]')) {
                            tabela = t.querySelector('tbody');
                            break;
                        }
                    }
                }
            }
            
            if (!tabela) {
                return {
                    presentes_ids, presentes_nomes, ausentes_ids, ausentes_nomes,
                    tem_dados: false, motivo: 'tabela_nao_encontrada'
                };
            }
            
            const linhas = tabela.querySelectorAll('tr');
            
            for (const linha of linhas) {
                try {
                    const nome = linha.querySelector('td:first-child')?.textContent?.trim();
                    const link = linha.querySelector('td:last-child a[data-id-membro]');
                    
                    if (nome && link) {
                        const idMembro = link.getAttribute('data-id-membro');
                        const icone = link.querySelector('i');
                        
                        if (idMembro && icone) {
                            const classes = icone.className;
                            
                            if (classes.includes('fa-check') && classes.includes('text-success')) {
                                presentes_ids.push(idMembro);
                                presentes_nomes.push(nome);
                            } else if (classes.includes('fa-remove') || classes.includes('fa-times')) {
                                ausentes_ids.push(idMembro);
                                ausentes_nomes.push(nome);
                            }
                        }
                    }
                } catch (e) {
                    console.error('Erro ao processar linha:', e);
                }
            }
            
            return {
                presentes_ids, presentes_nomes, ausentes_ids, ausentes_nomes,
                tem_dados: presentes_ids.length > 0 || ausentes_ids.length > 0,
                total_linhas: linhas.length,
                motivo: 'dom_extraido'
            };
        }
        
        return extrairFrequenciaCompleta();
        """
        
        try:
            resultado = pagina.evaluate(script_extrair)
            
            if resultado['tem_dados']:
                print(f"         ✅ DOM: {len(resultado['presentes_ids'])} presentes, {len(resultado['ausentes_ids'])} ausentes")
                return {
                    'presentes_ids': resultado['presentes_ids'],
                    'presentes_nomes': resultado['presentes_nomes'],
                    'ausentes_ids': resultado['ausentes_ids'],
                    'ausentes_nomes': resultado['ausentes_nomes'],
                    'tem_presenca': "OK",
                    'metodo': 'dom_otimizado'
                }
            else:
                print(f"         ⚠️ DOM sem dados: {resultado.get('motivo', 'unknown')}")
            
        except Exception as e:
            print(f"         ❌ Erro na extração DOM: {e}")
        
        return {'tem_dados': False}
    
    def extrair_frequencia_html(self, soup):
        """Extrai dados de frequência de HTML parseado"""
        
        presentes_ids = []
        presentes_nomes = []
        ausentes_ids = []
        ausentes_nomes = []
        
        # Procurar tabela de frequência
        tabela = soup.find('table')
        if tabela:
            tbody = tabela.find('tbody')
            if tbody:
                linhas = tbody.find_all('tr')
                
                for linha in linhas:
                    cels = linha.find_all('td')
                    if len(cels) >= 2:
                        # Nome
                        nome = cels[0].get_text(strip=True)
                        
                        # Link com dados
                        link = cels[-1].find('a', {'data-id-membro': True})
                        if link and nome:
                            id_membro = link.get('data-id-membro')
                            icone = link.find('i')
                            
                            if id_membro and icone:
                                classes = ' '.join(icone.get('class', []))
                                
                                if 'fa-check' in classes and 'text-success' in classes:
                                    presentes_ids.append(id_membro)
                                    presentes_nomes.append(nome)
                                elif 'fa-remove' in classes or 'fa-times' in classes:
                                    ausentes_ids.append(id_membro)
                                    ausentes_nomes.append(nome)
        
        return {
            'presentes_ids': presentes_ids,
            'presentes_nomes': presentes_nomes,
            'ausentes_ids': ausentes_ids,
            'ausentes_nomes': ausentes_nomes,
            'tem_presenca': "OK" if presentes_ids or ausentes_ids else "FANTASMA",
            'metodo': 'html_parsed'
        }
    
    def modal_fallback(self, pagina, aula_id, professor_id):
        """Método fallback usando modal tradicional"""
        
        print(f"         🔄 Usando fallback modal...")
        
        try:
            # Tentar clicar no modal via JavaScript
            script_modal = f"""
            // Procurar botão de frequência
            const botoes = document.querySelectorAll('button[onclick*="visualizarFrequencias"]');
            for (const botao of botoes) {{
                const onclick = botao.getAttribute('onclick');
                if (onclick.includes('{aula_id}') && onclick.includes('{professor_id}')) {{
                    botao.click();
                    return 'clicked';
                }}
            }}
            return 'not_found';
            """
            
            click_result = pagina.evaluate(script_modal)
            
            if click_result == 'clicked':
                # Aguardar modal carregar
                pagina.wait_for_selector("#modalFrequencia table tbody tr", timeout=2000)
                
                # Extrair dados
                dados = self.extrair_dados_dom_completo(pagina)
                
                # Fechar modal
                pagina.evaluate("$('#modalFrequencia').modal('hide');")
                
                if dados['tem_dados']:
                    return dados
                    
        except Exception as e:
            print(f"         ⚠️ Fallback modal falhou: {e}")
        
        return self.resultado_erro()
    
    def resultado_erro(self):
        """Retorna resultado de erro padrão"""
        return {
            'presentes_ids': [],
            'presentes_nomes': [],
            'ausentes_ids': [],
            'ausentes_nomes': [],
            'tem_presenca': "ERRO",
            'metodo': 'erro'
        }

def extrair_aulas_da_pagina_super_rapido(pagina):
    """Extrai todas as aulas de uma página rapidamente"""
    
    script_extracao = """
    function extrairTodasAulas() {
        const aulas = [];
        const linhas = document.querySelectorAll('table tbody tr');
        
        for (let i = 0; i < linhas.length; i++) {
            try {
                const linha = linhas[i];
                const colunas = linha.querySelectorAll('td');
                
                if (colunas.length < 4) continue;
                
                // Encontrar data
                let data = null;
                let dataColIndex = -1;
                
                for (let j = 0; j < colunas.length; j++) {
                    const texto = colunas[j].textContent.trim();
                    if (/\\d{1,2}[/-]\\d{1,2}[/-]\\d{2,4}/.test(texto) && !/[a-zA-Z]/.test(texto)) {
                        data = texto;
                        dataColIndex = j;
                        break;
                    }
                }
                
                if (!data) continue;
                
                // Extrair outras informações
                const congregacao = dataColIndex >= 3 ? colunas[dataColIndex-3].textContent.trim() : "N/A";
                const curso = dataColIndex >= 2 ? colunas[dataColIndex-2].textContent.trim() : "N/A";
                const turma = dataColIndex >= 1 ? colunas[dataColIndex-1].textContent.trim() : "N/A";
                
                // Encontrar botão de frequência
                const btnFreq = linha.querySelector('button[onclick*="visualizarFrequencias"]');
                if (btnFreq) {
                    const onclick = btnFreq.getAttribute('onclick');
                    const match = onclick.match(/visualizarFrequencias\\((\\d+),\\s*(\\d+)\\)/);
                    
                    if (match) {
                        aulas.push({
                            aula_id: match[1],
                            professor_id: match[2],
                            data: data,
                            congregacao: congregacao,
                            curso: curso,
                            turma: turma,
                            linha_index: i
                        });
                    }
                }
            } catch (e) {
                console.error('Erro na linha', i, ':', e);
            }
        }
        
        return aulas;
    }
    
    return extrairTodasAulas();
    """
    
    try:
        aulas = pagina.evaluate(script_extracao)
        
        # Filtrar por período
        aulas_validas = []
        deve_parar = False
        
        for aula in aulas:
            no_periodo, data_anterior = data_esta_no_periodo(aula['data'])
            
            if data_anterior:
                deve_parar = True
                break
            
            if no_periodo:
                aulas_validas.append(aula)
        
        print(f"   📊 {len(aulas_validas)} aulas válidas de {len(aulas)} totais")
        return aulas_validas, deve_parar
        
    except Exception as e:
        print(f"❌ Erro na extração JavaScript: {e}")
        return [], False

def main_ajax_interceptor():
    tempo_inicio = time.time()
    
    print(f"🚀 COLETOR COM INTERCEPTAÇÃO AJAX")
    print(f"📅 Período: {DATA_INICIO} a {DATA_FIM}")
    print("=" * 60)
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=False)  # Deixar visível para debug
        
        contexto = navegador.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        
        pagina = contexto.new_page()
        
        # Configurar coletor
        collector = AjaxFrequenciaCollector()
        collector.setup_page_interception(pagina)
        
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
            print("❌ Falha no login.")
            return
        
        # Navegar para histórico (código de navegação aqui)
        print("🔍 Navegando para histórico...")
        # ... implementar navegação ...
        
        # Processar páginas
        todas_aulas = []
        pagina_atual = 1
        deve_parar = False
        
        print("📥 Coletando informações básicas...")
        
        while not deve_parar:
            print(f"📖 Página {pagina_atual}...")
            
            aulas_pagina, deve_parar = extrair_aulas_da_pagina_super_rapido(pagina)
            todas_aulas.extend(aulas_pagina)
            
            if deve_parar or not aulas_pagina:
                break
            
            # Navegar próxima página
            try:
                btn_proximo = pagina.query_selector("a:has(i.fa-chevron-right)")
                if btn_proximo and "disabled" not in (btn_proximo.query_selector("..").get_attribute("class") or ""):
                    btn_proximo.click()
                    pagina_atual += 1
                    time.sleep(2)
                else:
                    break
            except:
                break
        
        print(f"✅ {len(todas_aulas)} aulas coletadas!")
        
        # Processar frequências
        if todas_aulas:
            print("⚡ Processando frequências...")
            resultado = []
            
            for i, aula in enumerate(todas_aulas):
                print(f"🎯 Aula {i+1}/{len(todas_aulas)}: {aula['data']} - {aula['curso']}")
                
                # Processar frequência
                freq_data = collector.processar_frequencia_otimizada(
                    pagina, aula['aula_id'], aula['professor_id']
                )
                
                # ATA via request
                session = requests.Session()
                cookies_dict = {c['name']: c['value'] for c in contexto.cookies()}
                session.cookies.update(cookies_dict)
                
                url_ata = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula['aula_id']}"
                try:
                    resp_ata = session.get(url_ata, timeout=5)
                    ata_status = "OK" if "ATA DA AULA" in resp_ata.text else "FANTASMA"
                except:
                    ata_status = "ERRO"
                
                # Montar linha resultado
                linha = [
                    aula['congregacao'], aula['curso'], aula['turma'], aula['data'],
                    "; ".join(freq_data['presentes_ids']),
                    "; ".join(freq_data['presentes_nomes']),
                    "; ".join(freq_data['ausentes_ids']),
                    "; ".join(freq_data['ausentes_nomes']),
                    freq_data['tem_presenca'],
                    ata_status
                ]
                
                resultado.append(linha)
                
                # Log resumo
                total = len(freq_data['presentes_ids']) + len(freq_data['ausentes_ids'])
                print(f"      ✓ {len(freq_data['presentes_ids'])} presentes, {len(freq_data['ausentes_ids'])} ausentes (Total: {total}) - {freq_data['metodo']}")
                
                # Pausa entre aulas
                time.sleep(0.2)
            
            # Salvar resultados
            tempo_total = (time.time() - tempo_inicio) / 60
            
            print(f"\n🎉 COLETA FINALIZADA!")
            print(f"⏱️ Tempo: {tempo_total:.1f} minutos")
            print(f"🎯 Aulas: {len(resultado)}")
            print(f"⚡ Velocidade: {len(resultado) / tempo_total:.1f} aulas/min")
            
            # Enviar para Google Sheets
            if resultado:
                headers = [
                    "CONGREGAÇÃO", "CURSO", "TURMA", "DATA", "PRESENTES IDs", 
                    "PRESENTES Nomes", "AUSENTES IDs", "AUSENTES Nomes", "TEM PRESENÇA", "ATA DA AULA"
                ]
                
                body = {
                    "tipo": "historico_aulas_ajax_interceptor",
                    "dados": resultado,
                    "headers": headers,
                    "resumo": {
                        "total_aulas": len(resultado),
                        "tempo_processamento": f"{tempo_total:.1f} minutos",
                        "metodo": "ajax_interceptor"
                    }
                }
                
                try:
                    resposta = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
                    print(f"✅ Enviado! Status: {resposta.status_code}")
                except Exception as e:
                    print(f"❌ Erro ao enviar: {e}")
        
        contexto.close()
        navegador.close()

if __name__ == "__main__":
    main_ajax_interceptor()
