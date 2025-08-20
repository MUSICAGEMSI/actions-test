from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

import os
import re
import requests
import time
import json
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

# Configurações otimizadas para GitHub Actions
MAX_ABAS_SIMULTANEAS = 12  # Reduzido para estabilidade
TIMEOUT_RAPIDO = 10000     # 10s em ms
TIMEOUT_MODAL = 6000       # 6s em ms
PAUSA_ENTRE_MODAIS = 200   # 0.2s em ms

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

class ColetorAssincrono:
    def __init__(self):
        self.resultado_global = []
        self.session_cookies = None
        self.total_processadas = 0
        self.semaforo = None  # Para controlar concorrência
        
    async def extrair_cookies_playwright(self, pagina):
        """Extrai cookies do Playwright para usar em requests"""
        cookies = await pagina.context.cookies()
        return {cookie['name']: cookie['value'] for cookie in cookies}

    def extrair_detalhes_aula_batch(self, aulas_ids):
        """Extrai detalhes de múltiplas aulas via requests síncrono"""
        resultados = {}
        
        if not self.session_cookies:
            return {aula_id: "ERRO" for aula_id in aulas_ids}
            
        session = requests.Session()
        session.cookies.update(self.session_cookies)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
            'Referer': 'https://musical.congregacao.org.br/aulas_abertas/listagem',
        }
        
        for aula_id in aulas_ids:
            try:
                url_detalhes = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
                resp = session.get(url_detalhes, headers=headers, timeout=4)
                
                if resp.status_code == 200:
                    resultados[aula_id] = "OK" if "ATA DA AULA" in resp.text else "FANTASMA"
                else:
                    resultados[aula_id] = "ERRO"
                    
            except Exception:
                resultados[aula_id] = "ERRO"
        
        return resultados

    async def processar_frequencia_modal_async(self, pagina):
        """Versão assíncrona do processamento de frequência"""
        try:
            # Aguardar modal carregar
            await pagina.wait_for_selector("table.table-bordered tbody tr", timeout=TIMEOUT_MODAL)
            
            # Extrair dados via JavaScript
            dados_frequencia = await pagina.evaluate("""
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
            print(f"⚠️ Erro no modal: {e}")
            return {
                'presentes_ids': [], 'presentes_nomes': [],
                'ausentes_ids': [], 'ausentes_nomes': [],
                'tem_presenca': "ERRO"
            }

    async def processar_pagina_async(self, contexto, numero_pagina, total_paginas):
        """Processamento assíncrono de uma página específica"""
        async with self.semaforo:  # Controla concorrência
            print(f"🚀 [P{numero_pagina:02d}] Iniciando...")
            
            try:
                # Criar nova página
                pagina = await contexto.new_page()
                
                # Configurar timeouts
                pagina.set_default_timeout(TIMEOUT_RAPIDO)
                
                # Navegar diretamente para a página
                if numero_pagina == 1:
                    url = "https://musical.congregacao.org.br/aulas_abertas"
                else:
                    # Tentar URL com parâmetros de paginação
                    start = (numero_pagina - 1) * 2000
                    url = f"https://musical.congregacao.org.br/aulas_abertas"
                
                await pagina.goto(url, wait_until="domcontentloaded")
                
                # Configurar para 2000 registros
                try:
                    await pagina.wait_for_selector('select[name="listagem_length"]', timeout=TIMEOUT_RAPIDO)
                    await pagina.select_option('select[name="listagem_length"]', "2000")
                    await asyncio.sleep(2)
                except Exception as e:
                    print(f"   ⚠️ [P{numero_pagina:02d}] Erro ao configurar registros: {e}")
                
                # Navegar para página específica se necessário
                if numero_pagina > 1:
                    try:
                        # Aguardar paginação aparecer
                        await pagina.wait_for_selector(".pagination", timeout=TIMEOUT_RAPIDO)
                        
                        # Tentar clicar diretamente no número da página
                        link_pagina = await pagina.query_selector(f'a:has-text("{numero_pagina}")')
                        if link_pagina:
                            await link_pagina.click()
                            await asyncio.sleep(2)
                        else:
                            # Navegar sequencialmente
                            for _ in range(numero_pagina - 1):
                                btn_proximo = await pagina.query_selector("a:has(i.fa-chevron-right)")
                                if btn_proximo:
                                    # Verificar se não está desabilitado
                                    parent = await btn_proximo.query_selector("..")
                                    if parent:
                                        parent_class = await parent.get_attribute("class") or ""
                                        if "disabled" not in parent_class:
                                            await btn_proximo.click()
                                            await asyncio.sleep(1)
                                        else:
                                            break
                                else:
                                    break
                    except Exception as e:
                        print(f"   ⚠️ [P{numero_pagina:02d}] Erro na navegação: {e}")
                
                # Aguardar tabela carregar
                await pagina.wait_for_selector('input[type="checkbox"][name="item[]"]', timeout=TIMEOUT_RAPIDO)
                await asyncio.sleep(1)
                
                # Extrair dados da página via JavaScript
                dados_pagina = await pagina.evaluate("""
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
                
                print(f"   📊 [P{numero_pagina:02d}] {len(dados_pagina)} aulas encontradas")
                
                if not dados_pagina:
                    await pagina.close()
                    return
                
                # Verificar se chegou em 2024
                if dados_pagina and "2024" in dados_pagina[0]['data']:
                    print(f"   🛑 [P{numero_pagina:02d}] Ano 2024 detectado")
                    await pagina.close()
                    return
                
                # Obter ATAs em batch
                aulas_ids = [item['aula_id'] for item in dados_pagina]
                atas_status = self.extrair_detalhes_aula_batch(aulas_ids)
                
                resultado_pagina = []
                
                # Processar cada aula
                for i, item in enumerate(dados_pagina):
                    try:
                        if "2024" in item['data']:
                            break
                        
                        # Fechar modal anterior
                        await pagina.evaluate("$('#modalFrequencia').modal('hide')")
                        await asyncio.sleep(0.1)
                        
                        # Clicar no botão de frequência
                        clicou = await pagina.evaluate(f"""
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
                            await asyncio.sleep(0.3)
                            
                            # Processar frequência
                            freq_data = await self.processar_frequencia_modal_async(pagina)
                            
                            # Fechar modal
                            await pagina.evaluate("$('#modalFrequencia').modal('hide')")
                            await asyncio.sleep(0.1)
                            
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
                            
                            # Log compacto
                            if i % 10 == 0 or i == len(dados_pagina) - 1:
                                total_alunos = len(freq_data['presentes_ids']) + len(freq_data['ausentes_ids'])
                                print(f"   ⚡ [P{numero_pagina:02d}] {i+1}/{len(dados_pagina)} - {total_alunos}alunos")
                    
                    except Exception as e:
                        print(f"   ⚠️ [P{numero_pagina:02d}] Erro item {i}: {str(e)[:30]}")
                        continue
                
                # Adicionar aos resultados globais
                self.resultado_global.extend(resultado_pagina)
                self.total_processadas += len(resultado_pagina)
                
                print(f"   ✅ [P{numero_pagina:02d}] {len(resultado_pagina)} aulas | Total: {self.total_processadas}")
                
                await pagina.close()
                
            except Exception as e:
                print(f"   ❌ [P{numero_pagina:02d}] Erro crítico: {e}")

    async def descobrir_total_paginas(self, pagina):
        """Descobre total de páginas"""
        try:
            # Configurar 2000 primeiro
            await pagina.wait_for_selector('select[name="listagem_length"]', timeout=TIMEOUT_RAPIDO)
            await pagina.select_option('select[name="listagem_length"]', "2000")
            await asyncio.sleep(3)
            
            # Descobrir via JavaScript
            total_paginas = await pagina.evaluate("""
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
            
            print(f"📊 Total de páginas: {total_paginas}")
            return total_paginas
                
        except Exception as e:
            print(f"⚠️ Erro ao descobrir páginas: {e}")
            return 1

    async def main(self):
        tempo_inicio = time.time()
        
        async with async_playwright() as p:
            # Configurações do navegador
            navegador = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-images',
                    '--disable-plugins',
                    '--disable-extensions'
                ]
            )
            
            # Criar contexto
            contexto = await navegador.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
            )
            
            # Criar semáforo para controlar concorrência
            self.semaforo = asyncio.Semaphore(MAX_ABAS_SIMULTANEAS)
            
            print("🔐 Login assíncrono...")
            
            # Página principal para login
            pagina_principal = await contexto.new_page()
            await pagina_principal.goto(URL_INICIAL, wait_until="domcontentloaded")
            
            # Login
            await pagina_principal.fill('input[name="login"]', EMAIL)
            await pagina_principal.fill('input[name="password"]', SENHA)
            await pagina_principal.click('button[type="submit"]')
            
            try:
                await pagina_principal.wait_for_selector("nav", timeout=TIMEOUT_RAPIDO)
                print("✅ Login realizado!")
            except Exception as e:
                print(f"❌ Falha no login: {e}")
                await navegador.close()
                return
            
            # Navegar para histórico
            print("🔍 Navegando para histórico...")
            try:
                # Tentar menu primeiro
                elemento_gem = await pagina_principal.query_selector('a:has-text("G.E.M")')
                if elemento_gem:
                    await elemento_gem.click()
                    await asyncio.sleep(2)
            except:
                pass
            
            await pagina_principal.goto("https://musical.congregacao.org.br/aulas_abertas", wait_until="domcontentloaded")
            
            # Extrair cookies para requests
            self.session_cookies = await self.extrair_cookies_playwright(pagina_principal)
            
            # Descobrir total de páginas
            total_paginas = await self.descobrir_total_paginas(pagina_principal)
            
            print(f"🚀 COLETA ASSÍNCRONA: {total_paginas} páginas com até {MAX_ABAS_SIMULTANEAS} abas!")
            
            # Fechar página principal
            await pagina_principal.close()
            
            # Criar tarefas assíncronas para cada página
            tasks = []
            for pagina_num in range(1, total_paginas + 1):
                task = asyncio.create_task(
                    self.processar_pagina_async(contexto, pagina_num, total_paginas)
                )
                tasks.append(task)
            
            # Aguardar todas as tarefas concluírem
            await asyncio.gather(*tasks, return_exceptions=True)
            
            await navegador.close()
            
            # Resultado final
            tempo_total = (time.time() - tempo_inicio) / 60
            
            print(f"\n🎯 COLETA ASSÍNCRONA FINALIZADA!")
            print(f"   📊 Total de aulas: {len(self.resultado_global)}")
            print(f"   ⏱️ Tempo: {tempo_total:.1f} minutos")
            if self.resultado_global:
                print(f"   ⚡ Velocidade: {len(self.resultado_global) / tempo_total:.0f} aulas/min")
            
            # Enviar para Google Sheets
            if self.resultado_global:
                headers = [
                    "CONGREGAÇÃO", "CURSO", "TURMA", "DATA", "PRESENTES IDs", 
                    "PRESENTES Nomes", "AUSENTES IDs", "AUSENTES Nomes", "TEM PRESENÇA", "ATA DA AULA"
                ]
                
                body = {
                    "tipo": "historico_aulas_async",
                    "dados": self.resultado_global,
                    "headers": headers,
                    "resumo": {
                        "total_aulas": len(self.resultado_global),
                        "tempo_processamento_minutos": tempo_total,
                        "paginas_processadas": total_paginas,
                        "max_abas_simultaneas": MAX_ABAS_SIMULTANEAS
                    }
                }
                
                try:
                    print("📤 Enviando para Google Sheets...")
                    resposta = requests.post(URL_APPS_SCRIPT, json=body, timeout=180)
                    print(f"✅ Enviado! Status: {resposta.status_code}")
                except Exception as e:
                    print(f"❌ Erro no envio: {e}")
                    # Backup local
                    with open('backup_dados.json', 'w', encoding='utf-8') as f:
                        json.dump(body, f, indent=2, ensure_ascii=False)
                    print("💾 Backup salvo em backup_dados.json")
            else:
                print("⚠️ Nenhum dado coletado")

def run():
    coletor = ColetorAssincrono()
    asyncio.run(coletor.main())

if __name__ == "__main__":
    print("🚀 COLETOR ASSÍNCRONO PARA GITHUB ACTIONS")
    print("=" * 60)
    
    run()
