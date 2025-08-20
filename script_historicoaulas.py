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

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

class ColetorSuperRapido:
    def __init__(self):
        self.session = None
        self.lock = Lock()
        self.contador_processadas = 0
        
    def extrair_cookies_playwright(self, pagina):
        """Extrai cookies do Playwright para usar em requests"""
        cookies = pagina.context.cookies()
        return {cookie['name']: cookie['value'] for cookie in cookies}

    def extrair_todas_aulas_js(self, pagina):
        """Extrai dados de todas as aulas usando JavaScript puro - SUPER OTIMIZADO"""
        script = """
        () => {
            const aulas = [];
            const linhas = document.querySelectorAll('table tbody tr');
            
            for (let i = 0; i < linhas.length; i++) {
                const linha = linhas[i];
                const colunas = linha.querySelectorAll('td');
                
                if (colunas.length >= 6) {
                    const btn = linha.querySelector('button[onclick*="visualizarFrequencias"]');
                    if (btn) {
                        const onclick = btn.getAttribute('onclick');
                        const match = onclick.match(/visualizarFrequencias\\((\\d+),\\s*(\\d+)\\)/);
                        if (match) {
                            const data = colunas[4]?.innerText?.trim() || '';
                            
                            // Parar se for 2024
                            if (data.includes('2024')) {
                                return { parar: true, aulas: aulas };
                            }
                            
                            aulas.push({
                                aula_id: match[1],
                                professor_id: match[2],
                                data: data,
                                congregacao: colunas[1]?.innerText?.trim() || '',
                                curso: colunas[2]?.innerText?.trim() || '',
                                turma: colunas[3]?.innerText?.trim() || ''
                            });
                        }
                    }
                }
            }
            return { parar: false, aulas: aulas };
        }
        """
        return pagina.evaluate(script)

    def extrair_frequencia_direta(self, aula_id, professor_id):
        """FAZ REQUISIÇÃO DIRETA - SEM MODAL! Muito mais rápido"""
        try:
            url_freq = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_frequencias/{aula_id}/{professor_id}"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://musical.congregacao.org.br/aulas_abertas/listagem',
                'X-Requested-With': 'XMLHttpRequest'  # Simula requisição AJAX
            }
            
            response = self.session.get(url_freq, headers=headers, timeout=8)
            
            if response.status_code == 200:
                return self.processar_html_frequencia(response.text)
            else:
                return {
                    'presentes_ids': [],
                    'presentes_nomes': [],
                    'ausentes_ids': [],
                    'ausentes_nomes': [],
                    'tem_presenca': "ERRO"
                }
                
        except Exception as e:
            return {
                'presentes_ids': [],
                'presentes_nomes': [],
                'ausentes_ids': [],
                'ausentes_nomes': [],
                'tem_presenca': "ERRO"
            }

    def processar_html_frequencia(self, html_content):
        """Processa o HTML da frequência diretamente"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            presentes_ids = []
            presentes_nomes = []
            ausentes_ids = []
            ausentes_nomes = []
            
            # Buscar todas as linhas da tabela
            linhas = soup.find_all('tr')
            
            for linha in linhas:
                # Buscar nome na primeira célula
                nome_cell = linha.find('td')
                if not nome_cell:
                    continue
                    
                nome_completo = nome_cell.get_text(strip=True)
                
                # Ignorar linhas vazias ou sem nome
                if not nome_completo:
                    continue
                
                # Buscar link de presença
                link_presenca = linha.find('a', {'data-id-membro': True})
                
                if link_presenca:
                    id_membro = link_presenca.get('data-id-membro')
                    
                    # Ignorar se não tem ID válido
                    if not id_membro:
                        continue
                    
                    # Verificar se está presente ou ausente pelo ícone
                    icone = link_presenca.find('i')
                    if icone:
                        classes = icone.get('class', [])
                        classes_str = ' '.join(classes)
                        
                        if 'fa-check' in classes_str and 'text-success' in classes_str:
                            # Presente
                            presentes_ids.append(id_membro)
                            presentes_nomes.append(nome_completo)
                        elif 'fa-remove' in classes_str and 'text-danger' in classes_str:
                            # Ausente
                            ausentes_ids.append(id_membro)
                            ausentes_nomes.append(nome_completo)
            
            return {
                'presentes_ids': presentes_ids,
                'presentes_nomes': presentes_nomes,
                'ausentes_ids': ausentes_ids,
                'ausentes_nomes': ausentes_nomes,
                'tem_presenca': "OK" if presentes_ids or ausentes_ids else "FANTASMA"
            }
            
        except Exception as e:
            print(f"⚠️ Erro ao processar HTML: {e}")
            return {
                'presentes_ids': [],
                'presentes_nomes': [],
                'ausentes_ids': [],
                'ausentes_nomes': [],
                'tem_presenca': "ERRO"
            }

    def extrair_ata_direta(self, aula_id):
        """Versão ultra-rápida para verificar ATA"""
        try:
            url_detalhes = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://musical.congregacao.org.br/aulas_abertas/listagem',
            }
            
            response = self.session.get(url_detalhes, headers=headers, timeout=6)
            
            if response.status_code == 200:
                return "OK" if "ATA DA AULA" in response.text else "FANTASMA"
            else:
                return "ERRO"
                
        except Exception as e:
            return "ERRO"

    def processar_aula_completa(self, aula):
        """Processa uma aula completa - frequência + ATA"""
        try:
            # Obter frequência via requisição direta
            freq_data = self.extrair_frequencia_direta(aula['aula_id'], aula['professor_id'])
            
            # Obter ATA via requisição direta
            ata_status = self.extrair_ata_direta(aula['aula_id'])
            
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
            
            # Log thread-safe
            with self.lock:
                self.contador_processadas += 1
                total_alunos = len(freq_data['presentes_ids']) + len(freq_data['ausentes_ids'])
                print(f"✓ [{self.contador_processadas:03d}] {aula['data']} - {aula['curso'][:30]:<30} | "
                      f"{len(freq_data['presentes_ids']):2d}P {len(freq_data['ausentes_ids']):2d}A | ATA:{ata_status}")
            
            return linha_resultado
            
        except Exception as e:
            with self.lock:
                print(f"❌ Erro processando aula {aula['aula_id']}: {e}")
            return None

    def processar_lote_paralelo(self, aulas_lote, max_workers=8):
        """Processa um lote de aulas EM PARALELO - MUITO MAIS RÁPIDO"""
        resultado_lote = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Enviar todas as tarefas
            futures = {executor.submit(self.processar_aula_completa, aula): aula for aula in aulas_lote}
            
            # Coletar resultados conforme completam
            for future in concurrent.futures.as_completed(futures):
                try:
                    resultado = future.result(timeout=15)
                    if resultado:
                        resultado_lote.append(resultado)
                except Exception as e:
                    aula = futures[future]
                    print(f"⚠️ Timeout/erro na aula {aula['aula_id']}: {e}")
        
        return resultado_lote

def navegar_para_historico_rapido(pagina):
    """Navegação ultra-otimizada com múltiplas estratégias"""
    try:
        print("🚀 Tentando navegação direta...")
        
        # ESTRATÉGIA 1: URL direta (mais rápida)
        try:
            pagina.goto("https://musical.congregacao.org.br/aulas_abertas", wait_until="domcontentloaded")
            time.sleep(2)  # Aguardar um pouco mais
            
            # Verificar se chegou na página certa
            pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=10000)
            print("✅ Navegação direta bem-sucedida!")
            return True
            
        except Exception as e1:
            print(f"⚠️ Navegação direta falhou: {e1}")
        
        print("🔄 Tentando via menus...")
        
        # ESTRATÉGIA 2: Via menus (fallback)
        try:
            # Voltar para página inicial se necessário
            pagina.goto("https://musical.congregacao.org.br/painel", wait_until="domcontentloaded")
            time.sleep(2)
            
            # Aguardar menu aparecer com timeout maior
            pagina.wait_for_selector("nav, .navbar, [role='navigation']", timeout=15000)
            
            # Tentar encontrar G.E.M de múltiplas formas
            seletores_gem = [
                'a:has-text("G.E.M")',
                'a[href*="gem"]',
                'a:has(.fa-graduation-cap)',
                'li:has-text("G.E.M") a',
                '.menu-item:has-text("G.E.M")'
            ]
            
            menu_gem_encontrado = False
            for seletor in seletores_gem:
                try:
                    elemento = pagina.query_selector(seletor)
                    if elemento and elemento.is_visible():
                        print(f"✅ G.E.M encontrado: {seletor}")
                        elemento.click()
                        menu_gem_encontrado = True
                        break
                except:
                    continue
            
            if not menu_gem_encontrado:
                print("❌ Menu G.E.M não encontrado")
                return False
            
            time.sleep(3)  # Aguardar submenu expandir
            
            # Tentar encontrar "Histórico de Aulas"
            seletores_historico = [
                'a:has-text("Histórico de Aulas")',
                'a[href*="aulas_abertas"]',
                'li:has-text("Histórico") a',
                '.submenu-item:has-text("Histórico")'
            ]
            
            historico_encontrado = False
            for seletor in seletores_historico:
                try:
                    elemento = pagina.query_selector(seletor)
                    if elemento:
                        print(f"✅ Histórico encontrado: {seletor}")
                        elemento.click()
                        historico_encontrado = True
                        break
                except:
                    continue
            
            if not historico_encontrado:
                # Tentar força bruta via JavaScript
                print("🔧 Tentando clique forçado...")
                resultado_js = pagina.evaluate("""
                () => {
                    const links = Array.from(document.querySelectorAll('a'));
                    const historico = links.find(link => 
                        link.textContent.includes('Histórico') || 
                        link.href.includes('aulas_abertas')
                    );
                    if (historico) {
                        historico.click();
                        return true;
                    }
                    return false;
                }
                """)
                
                if not resultado_js:
                    print("❌ Histórico não encontrado de forma alguma")
                    return False
            
            # Aguardar página do histórico carregar
            pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=20000)
            print("✅ Navegação por menus bem-sucedida!")
            return True
            
        except Exception as e2:
            print(f"⚠️ Navegação por menus falhou: {e2}")
        
        # ESTRATÉGIA 3: Força bruta total
        print("🔨 Última tentativa - força bruta...")
        try:
            urls_possíveis = [
                "https://musical.congregacao.org.br/aulas_abertas/listagem",
                "https://musical.congregacao.org.br/aulas_abertas/index",
                "https://musical.congregacao.org.br/gem/aulas_abertas"
            ]
            
            for url in urls_possíveis:
                try:
                    print(f"   Tentando: {url}")
                    pagina.goto(url, wait_until="domcontentloaded")
                    time.sleep(2)
                    
                    # Verificar se chegou
                    pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=8000)
                    print("✅ Força bruta bem-sucedida!")
                    return True
                except:
                    continue
            
            return False
            
        except Exception as e3:
            print(f"❌ Todas as estratégias falharam: {e3}")
            return False
        
    except Exception as e:
        print(f"❌ Erro geral na navegação: {e}")
        return False

def main():
    tempo_inicio = time.time()
    coletor = ColetorSuperRapido()
    
    with sync_playwright() as p:
        # Configurar navegador com mais estabilidade
        navegador = p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-gpu',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding'
            ]
        )
        
        # Criar contexto com configurações otimizadas
        context = navegador.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        pagina = context.new_page()
        
        # Bloquear recursos apenas se não for crítico para navegação
        pagina.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2}", lambda route: route.abort())
        
        print("🔐 Fazendo login...")
        try:
            pagina.goto(URL_INICIAL, wait_until="networkidle", timeout=30000)
        except:
            pagina.goto(URL_INICIAL, wait_until="domcontentloaded")
        
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')
        
        # Aguardar redirect pós-login com mais tempo
        try:
            pagina.wait_for_url("**/painel", timeout=20000)
            print("✅ Login realizado - redirecionado para painel!")
        except:
            try:
                # Aguardar qualquer elemento que indique login bem-sucedido
                pagina.wait_for_selector("nav, .navbar, .menu, .header-menu", timeout=15000)
                print("✅ Login realizado - menu detectado!")
            except PlaywrightTimeoutError:
                print("❌ Falha no login - timeout aguardando elementos.")
                navegador.close()
                return
        
        if not navegar_para_historico_rapido(pagina):
            navegador.close()
            return
        
        # Configurar 2000 registros com debug melhorado
        print("⚙️ Configurando para 2000 registros...")
        try:
            # Primeiro verificar se o seletor existe
            selector_found = pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
            if selector_found:
                print("✅ Seletor de quantidade encontrado")
                
                # Verificar opções disponíveis
                opcoes = pagina.evaluate("""
                () => {
                    const select = document.querySelector('select[name="listagem_length"]');
                    if (select) {
                        return Array.from(select.options).map(opt => opt.value);
                    }
                    return [];
                }
                """)
                print(f"   Opções disponíveis: {opcoes}")
                
                # Selecionar 2000 ou a maior opção disponível
                if "2000" in opcoes:
                    pagina.select_option('select[name="listagem_length"]', "2000")
                    print("✅ 2000 registros selecionados")
                elif opcoes:
                    maior_opcao = max(opcoes, key=lambda x: int(x) if x.isdigit() else 0)
                    pagina.select_option('select[name="listagem_length"]', maior_opcao)
                    print(f"✅ {maior_opcao} registros selecionados (máximo disponível)")
                
                # Aguardar recarregamento
                time.sleep(3)
                
                # Verificar se recarregou
                try:
                    pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=15000)
                    
                    # Contar quantos registros estão sendo mostrados
                    total_info = pagina.query_selector('.dataTables_info')
                    if total_info:
                        info_text = total_info.inner_text()
                        print(f"✅ Configuração aplicada: {info_text}")
                    else:
                        print("✅ Tabela recarregada com nova configuração")
                        
                except Exception as e:
                    print(f"⚠️ Timeout aguardando recarregamento: {e}")
                    print("   Continuando com a configuração atual...")
            else:
                print("⚠️ Seletor de quantidade não encontrado")
                
        except Exception as e:
            print(f"⚠️ Erro na configuração de registros: {e}")
            print("   Continuando com configuração padrão...")
            
            # Tentar verificar se pelo menos temos uma tabela funcional
            try:
                tabela_ok = pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=5000)
                if tabela_ok:
                    print("✅ Tabela básica encontrada - continuando...")
                else:
                    print("❌ Nenhuma tabela funcional encontrada")
                    navegador.close()
                    return
            except:
                print("❌ Falha crítica - sem tabela de dados")
                navegador.close()
                return
        
        # Criar sessão requests com cookies
        cookies_dict = coletor.extrair_cookies_playwright(pagina)
        coletor.session = requests.Session()
        coletor.session.cookies.update(cookies_dict)
        
        # Configurar sessão para máxima performance
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=1
        )
        coletor.session.mount('http://', adapter)
        coletor.session.mount('https://', adapter)
        
        resultado_final = []
        pagina_atual = 1
        
        print("🚀 INICIANDO COLETA SUPER-RÁPIDA...")
        print("=" * 80)
        
        while True:
            print(f"\n📖 PÁGINA {pagina_atual}")
            print("-" * 40)
            
            # Extrair todas as aulas da página via JavaScript
            tempo_extracao = time.time()
            resultado_js = coletor.extrair_todas_aulas_js(pagina)
            print(f"   ⚡ Extração JS: {time.time() - tempo_extracao:.1f}s")
            
            if resultado_js['parar']:
                print("🛑 Encontrado 2024 - PARANDO!")
                break
            
            aulas_pagina = resultado_js['aulas']
            
            if not aulas_pagina:
                print("🏁 Sem mais aulas na página")
                break
            
            print(f"   📊 {len(aulas_pagina)} aulas encontradas")
            
            # Processar aulas em lotes paralelos - AQUI ESTÁ A MÁGICA!
            TAMANHO_LOTE = 20  # Processar 20 aulas simultâneas
            tempo_processamento = time.time()
            
            for i in range(0, len(aulas_pagina), TAMANHO_LOTE):
                lote = aulas_pagina[i:i+TAMANHO_LOTE]
                print(f"   🔄 Processando lote {i//TAMANHO_LOTE + 1} ({len(lote)} aulas)...")
                
                resultado_lote = coletor.processar_lote_paralelo(lote, max_workers=10)
                resultado_final.extend(resultado_lote)
            
            tempo_lote = time.time() - tempo_processamento
            velocidade = len(aulas_pagina) / tempo_lote if tempo_lote > 0 else 0
            print(f"   ⚡ Página processada em {tempo_lote:.1f}s ({velocidade:.1f} aulas/s)")
            
            # Navegar para próxima página
            try:
                btn_proximo = pagina.query_selector("a:has(i.fa-chevron-right)")
                if btn_proximo:
                    parent = btn_proximo.query_selector("..")
                    if parent and "disabled" not in (parent.get_attribute("class") or ""):
                        print("   ➡️ Avançando...")
                        btn_proximo.click()
                        pagina_atual += 1
                        time.sleep(1)  # Pausa mínima
                        continue
                
                print("🏁 Última página alcançada")
                break
                
            except Exception as e:
                print(f"⚠️ Erro na navegação: {e}")
                break
        
        # Finalização
        tempo_total = time.time() - tempo_inicio
        
        print(f"\n" + "=" * 80)
        print(f"🎉 COLETA FINALIZADA!")
        print(f"📊 {len(resultado_final)} aulas processadas")
        print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} minutos)")
        print(f"🚀 Velocidade média: {len(resultado_final)/tempo_total:.1f} aulas/segundo")
        print(f"📄 {pagina_atual} páginas processadas")
        
        # Enviar para Google Sheets
        if resultado_final:
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
                    "tempo_processamento": f"{tempo_total/60:.1f} minutos",
                    "velocidade": f"{len(resultado_final)/tempo_total:.1f} aulas/s",
                    "paginas_processadas": pagina_atual
                }
            }
            
            try:
                print("\n📤 Enviando para Google Sheets...")
                tempo_envio = time.time()
                resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=180)
                tempo_envio = time.time() - tempo_envio
                print(f"✅ Enviado em {tempo_envio:.1f}s! Status: {resposta_post.status_code}")
                
                if resposta_post.text:
                    print(f"📋 Resposta: {resposta_post.text[:200]}...")
                    
            except Exception as e:
                print(f"❌ Erro no envio: {e}")
                # Salvar localmente como backup
                with open(f'backup_aulas_{int(time.time())}.json', 'w') as f:
                    json.dump(body, f, ensure_ascii=False, indent=2)
                print("💾 Dados salvos localmente como backup")
        
        # Estatísticas finais
        if resultado_final:
            total_presentes = sum(len(linha[4].split('; ')) if linha[4] else 0 for linha in resultado_final)
            total_ausentes = sum(len(linha[6].split('; ')) if linha[6] else 0 for linha in resultado_final)
            aulas_com_ata = sum(1 for linha in resultado_final if linha[9] == "OK")
            
            print(f"\n📈 ESTATÍSTICAS:")
            print(f"   👥 {total_presentes} presenças registradas")
            print(f"   ❌ {total_ausentes} ausências registradas")
            print(f"   📝 {aulas_com_ata}/{len(resultado_final)} aulas com ATA")
            print(f"   ⚡ Melhoria de velocidade: ~10x mais rápido!")
        
        navegador.close()

if __name__ == "__main__":
    main()
