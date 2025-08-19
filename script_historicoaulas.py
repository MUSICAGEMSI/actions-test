# script_historico_SEM_MODALS.py
from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
import json
from datetime import datetime
import concurrent.futures
from threading import Lock
from urllib.parse import urljoin
import asyncio

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

# Caches
cache_lock = Lock()
frequencia_cache = {}
ata_cache = {}

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def interceptar_requisicoes_ajax(pagina, session):
    """
    ABORDAGEM 1: Interceptar requisições AJAX que os modais fazem
    Esta é a mais eficiente - captura os dados direto da API
    """
    dados_interceptados = []
    
    def handle_response(response):
        # Interceptar chamadas para visualizar frequência
        if "visualizar_frequencia" in response.url or "frequencia" in response.url:
            try:
                if response.status == 200:
                    data = response.json()
                    dados_interceptados.append({
                        'url': response.url,
                        'data': data
                    })
                    print(f"🎯 Interceptada API: {response.url}")
            except:
                pass
    
    pagina.on("response", handle_response)
    return dados_interceptados

def extrair_via_requests_diretas(session, aula_id, professor_id):
    """
    ABORDAGEM 2: Fazer requisições diretas para APIs que os modals usam
    Reverse engineering das chamadas AJAX
    """
    possíveis_endpoints = [
        f"https://musical.congregacao.org.br/ajax/frequencia/{aula_id}",
        f"https://musical.congregacao.org.br/aulas_abertas/frequencia_ajax/{aula_id}",
        f"https://musical.congregacao.org.br/frequencia/listar/{aula_id}",
        f"https://musical.congregacao.org.br/api/frequencia/{aula_id}",
        f"https://musical.congregacao.org.br/aulas_abertas/ajax_frequencia?aula_id={aula_id}&professor_id={professor_id}",
    ]
    
    headers = {
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Referer': 'https://musical.congregacao.org.br/aulas_abertas',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    for endpoint in possíveis_endpoints:
        try:
            # Tentar GET
            resp = session.get(endpoint, headers=headers, timeout=3)
            if resp.status_code == 200 and resp.text.strip():
                try:
                    data = resp.json()
                    return processar_dados_api(data)
                except:
                    # Se não é JSON, pode ser HTML com dados
                    if "fa-check" in resp.text or "fa-remove" in resp.text:
                        return processar_html_frequencia(resp.text)
            
            # Tentar POST
            resp = session.post(endpoint, headers=headers, 
                              data={'aula_id': aula_id, 'professor_id': professor_id}, 
                              timeout=3)
            if resp.status_code == 200 and resp.text.strip():
                try:
                    data = resp.json()
                    return processar_dados_api(data)
                except:
                    if "fa-check" in resp.text or "fa-remove" in resp.text:
                        return processar_html_frequencia(resp.text)
                        
        except:
            continue
    
    return None

def processar_dados_api(data):
    """Processa dados vindos da API JSON"""
    if isinstance(data, dict):
        # Diferentes formatos possíveis da API
        if 'frequencia' in data:
            return extrair_frequencia_do_json(data['frequencia'])
        elif 'alunos' in data:
            return extrair_frequencia_do_json(data['alunos'])
        elif 'presentes' in data and 'ausentes' in data:
            return {
                'presentes_ids': [str(p.get('id', '')) for p in data.get('presentes', [])],
                'presentes_nomes': [p.get('nome', '') for p in data.get('presentes', [])],
                'ausentes_ids': [str(a.get('id', '')) for a in data.get('ausentes', [])],
                'ausentes_nomes': [a.get('nome', '') for a in data.get('ausentes', [])],
                'tem_presenca': "OK" if data.get('presentes') else "FANTASMA"
            }
    
    return None

def extrair_frequencia_do_json(alunos_data):
    """Extrai frequência de dados JSON"""
    presentes_ids, presentes_nomes = [], []
    ausentes_ids, ausentes_nomes = [], []
    
    for aluno in alunos_data:
        if aluno.get('presente', False) or aluno.get('status') == 'presente':
            presentes_ids.append(str(aluno.get('id', '')))
            presentes_nomes.append(aluno.get('nome', ''))
        else:
            ausentes_ids.append(str(aluno.get('id', '')))
            ausentes_nomes.append(aluno.get('nome', ''))
    
    return {
        'presentes_ids': presentes_ids,
        'presentes_nomes': presentes_nomes,
        'ausentes_ids': ausentes_ids,
        'ausentes_nomes': ausentes_nomes,
        'tem_presenca': "OK" if presentes_ids else "FANTASMA"
    }

def processar_html_frequencia(html_content):
    """Processa HTML retornado por requisições diretas"""
    from bs4 import BeautifulSoup
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        presentes_ids, presentes_nomes = [], []
        ausentes_ids, ausentes_nomes = [], []
        
        # Buscar por linhas da tabela
        linhas = soup.find_all('tr')
        
        for linha in linhas:
            colunas = linha.find_all(['td', 'th'])
            if len(colunas) >= 2:
                nome = colunas[0].get_text(strip=True)
                
                # Buscar por ícones ou links de presença
                link = linha.find('a', {'data-id-membro': True})
                if link and nome:
                    id_membro = link.get('data-id-membro')
                    icone = link.find('i')
                    
                    if icone:
                        classes = icone.get('class', [])
                        classes_str = ' '.join(classes)
                        
                        if 'fa-check' in classes_str and 'text-success' in classes_str:
                            presentes_ids.append(id_membro)
                            presentes_nomes.append(nome)
                        elif 'fa-remove' in classes_str and 'text-danger' in classes_str:
                            ausentes_ids.append(id_membro)
                            ausentes_nomes.append(nome)
        
        return {
            'presentes_ids': presentes_ids,
            'presentes_nomes': presentes_nomes,
            'ausentes_ids': ausentes_ids,
            'ausentes_nomes': ausentes_nomes,
            'tem_presenca': "OK" if presentes_ids else "FANTASMA"
        }
    
    except Exception as e:
        print(f"⚠️ Erro processando HTML: {e}")
        return None

def extrair_via_urls_diretas(session, dados_aula):
    """
    ABORDAGEM 3: Tentar acessar URLs diretas da frequência
    Algumas aplicações expõem URLs diretas para cada aula
    """
    possíveis_urls = [
        f"https://musical.congregacao.org.br/aulas_abertas/frequencia/{dados_aula['aula_id']}",
        f"https://musical.congregacao.org.br/frequencia/aula/{dados_aula['aula_id']}",
        f"https://musical.congregacao.org.br/gem/frequencia/{dados_aula['aula_id']}",
        f"https://musical.congregacao.org.br/aula/{dados_aula['aula_id']}/frequencia",
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://musical.congregacao.org.br/aulas_abertas',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }
    
    for url in possíveis_urls:
        try:
            resp = session.get(url, headers=headers, timeout=5)
            if resp.status_code == 200:
                # Verificar se tem dados de frequência
                if "table" in resp.text and ("fa-check" in resp.text or "presente" in resp.text.lower()):
                    return processar_html_frequencia(resp.text)
        except:
            continue
    
    return None

def descobrir_endpoints_dinamicamente(pagina):
    """
    ABORDAGEM 4: Descobrir endpoints dinamicamente monitorando network
    """
    endpoints_descobertos = set()
    
    def capture_request(request):
        url = request.url
        if any(keyword in url.lower() for keyword in ['frequencia', 'ajax', 'api', 'aluno']):
            endpoints_descobertos.add(url)
            print(f"🔍 Endpoint descoberto: {url}")
    
    pagina.on("request", capture_request)
    
    # Fazer algumas ações na página para descobrir endpoints
    try:
        # Clicar em alguns botões para ver que requisições são feitas
        botoes = pagina.query_selector_all("button[onclick*='visualizarFrequencias']")
        if botoes:
            # Clicar no primeiro para descobrir o endpoint
            print("🔍 Descobrindo endpoints...")
            botoes[0].click()
            time.sleep(2)
            
            # Fechar modal
            try:
                pagina.keyboard.press("Escape")
            except:
                pass
    except:
        pass
    
    return list(endpoints_descobertos)

def processar_batch_sem_modals(session, aulas_lote):
    """
    Processa um lote de aulas tentando todas as abordagens sem modal
    """
    resultados = []
    
    # Usar threading para paralelizar as requisições
    def processar_aula_individual(dados_aula):
        aula_id = dados_aula['aula_id']
        professor_id = dados_aula['professor_id']
        
        # Verificar cache
        cache_key = f"{aula_id}_{professor_id}"
        with cache_lock:
            if cache_key in frequencia_cache:
                return dados_aula, frequencia_cache[cache_key]
        
        # Tentar abordagem 2: Requisições diretas para APIs
        freq_data = extrair_via_requests_diretas(session, aula_id, professor_id)
        
        if not freq_data:
            # Tentar abordagem 3: URLs diretas
            freq_data = extrair_via_urls_diretas(session, dados_aula)
        
        if not freq_data:
            # Se tudo falhar, retornar dados vazios
            freq_data = {
                'presentes_ids': [],
                'presentes_nomes': [],
                'ausentes_ids': [],
                'ausentes_nomes': [],
                'tem_presenca': "ERRO"
            }
        
        # Salvar no cache
        with cache_lock:
            frequencia_cache[cache_key] = freq_data
        
        return dados_aula, freq_data
    
    # Processar em paralelo
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(processar_aula_individual, dados) for dados in aulas_lote]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                dados_aula, freq_data = future.result()
                resultados.append({
                    'dados': dados_aula,
                    'freq': freq_data
                })
            except Exception as e:
                print(f"⚠️ Erro processando aula: {e}")
    
    return resultados

def extrair_dados_pagina_otimizado(pagina):
    """Extrai dados da página atual (mantido do código original)"""
    return pagina.evaluate("""
        () => {
            const dados = [];
            const linhas = document.querySelectorAll('table tbody tr');
            
            for (let i = 0; i < linhas.length; i++) {
                const linha = linhas[i];
                const colunas = linha.querySelectorAll('td');
                
                if (colunas.length < 6) continue;
                
                const data = colunas[1]?.textContent?.trim();
                if (!data) continue;
                
                if (data.includes('2024')) {
                    dados.push({ deve_parar: true });
                    break;
                }
                
                const btnFreq = linha.querySelector('button[onclick*="visualizarFrequencias"]');
                if (!btnFreq) continue;
                
                const onclick = btnFreq.getAttribute('onclick');
                const match = onclick?.match(/visualizarFrequencias\\((\\d+),\\s*(\\d+)\\)/);
                
                if (match) {
                    dados.push({
                        aula_id: match[1],
                        professor_id: match[2],
                        data: data,
                        congregacao: colunas[2]?.textContent?.trim() || '',
                        curso: colunas[3]?.textContent?.trim() || '',
                        turma: colunas[4]?.textContent?.trim() || ''
                    });
                }
            }
            
            return dados;
        }
    """)

def processar_atas_paralelo(session, aula_ids):
    """Processa ATAs em paralelo (mantido do código anterior)"""
    def processar_ata(aula_id):
        with cache_lock:
            if aula_id in ata_cache:
                return aula_id, ata_cache[aula_id]
        
        try:
            url = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
            resp = session.get(url, timeout=3)
            resultado = "OK" if resp.status_code == 200 and "ATA DA AULA" in resp.text else "FANTASMA"
            
            with cache_lock:
                ata_cache[aula_id] = resultado
            
            return aula_id, resultado
        except:
            with cache_lock:
                ata_cache[aula_id] = "ERRO"
            return aula_id, "ERRO"
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        futures = [executor.submit(processar_ata, aula_id) for aula_id in aula_ids]
        
        resultados = {}
        for future in concurrent.futures.as_completed(futures):
            try:
                aula_id, status = future.result()
                resultados[aula_id] = status
            except:
                pass
        
        return resultados

def main():
    tempo_inicio = time.time()
    
    print("🚀 MODO SEM MODALS - Abordagens Alternativas")
    print("🎯 Meta: Eliminar gargalo dos modals")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        print("🔐 Login...")
        pagina.goto(URL_INICIAL)
        
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')
        
        try:
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login OK!")
        except:
            print("❌ Login falhou")
            return
        
        # Navegação direta para histórico
        print("🌐 Navegando para histórico...")
        pagina.goto("https://musical.congregacao.org.br/aulas_abertas")
        
        # Configurar 2000 registros
        try:
            pagina.wait_for_selector('select[name="listagem_length"]', timeout=10000)
            pagina.select_option('select[name="listagem_length"]', "2000")
            time.sleep(2)
            print("✅ Configurado para 2000 registros")
        except:
            print("⚠️ Não conseguiu configurar 2000")
        
        # Setup session
        cookies = {cookie['name']: cookie['value'] for cookie in pagina.context.cookies()}
        session = requests.Session()
        session.cookies.update(cookies)
        
        # DISCOVERY PHASE: Descobrir endpoints
        print("\n🔍 FASE 1: Descobrindo endpoints...")
        endpoints = descobrir_endpoints_dinamicamente(pagina)
        
        if endpoints:
            print(f"✅ Descobertos {len(endpoints)} endpoints:")
            for endpoint in endpoints[:5]:  # Mostrar apenas os primeiros 5
                print(f"   • {endpoint}")
        else:
            print("⚠️ Nenhum endpoint descoberto - usando abordagens diretas")
        
        resultado_final = []
        pagina_atual = 1
        total_processado = 0
        
        # Interceptar requisições AJAX (Abordagem 1)
        dados_interceptados = interceptar_requisicoes_ajax(pagina, session)
        
        print("\n🔄 FASE 2: Processamento...")
        
        while True:
            print(f"\n📖 Página {pagina_atual}")
            
            time.sleep(1)
            
            # Extrair dados da página
            dados_pagina = extrair_dados_pagina_otimizado(pagina)
            
            if not dados_pagina:
                break
            
            # Verificar parada
            deve_parar = any(d.get('deve_parar') for d in dados_pagina)
            if deve_parar:
                dados_pagina = [d for d in dados_pagina if not d.get('deve_parar')]
                print("🛑 Encontrou 2024")
            
            total_aulas = len(dados_pagina)
            if total_aulas == 0:
                break
            
            print(f"   📊 {total_aulas} aulas encontradas")
            
            # PROCESSAR SEM MODALS
            print("   🚀 Processando sem modals...")
            
            # Processar frequências em lote (SEM MODALS!)
            resultados_freq = processar_batch_sem_modals(session, dados_pagina)
            
            # Processar ATAs em paralelo
            aula_ids = [d['aula_id'] for d in dados_pagina]
            resultados_ata = processar_atas_paralelo(session, aula_ids)
            
            # Montar resultados finais
            for resultado_freq in resultados_freq:
                dados = resultado_freq['dados']
                freq = resultado_freq['freq']
                ata_status = resultados_ata.get(dados['aula_id'], "ERRO")
                
                linha = [
                    dados['congregacao'],
                    dados['curso'],
                    dados['turma'],
                    dados['data'],
                    "; ".join(freq['presentes_ids']),
                    "; ".join(freq['presentes_nomes']),
                    "; ".join(freq['ausentes_ids']),
                    "; ".join(freq['ausentes_nomes']),
                    freq['tem_presenca'],
                    ata_status
                ]
                
                resultado_final.append(linha)
                total_processado += 1
            
            # Stats
            freq_ok = sum(1 for r in resultados_freq if r['freq']['tem_presenca'] == 'OK')
            freq_erro = sum(1 for r in resultados_freq if r['freq']['tem_presenca'] == 'ERRO')
            
            print(f"   ✅ {len(resultados_freq)} processadas | OK: {freq_ok} | ERRO: {freq_erro}")
            print(f"   📊 Total acumulado: {total_processado}")
            
            if deve_parar:
                break
            
            # Próxima página
            proxima = pagina.evaluate("""
                () => {
                    const btn = document.querySelector('a:has(i.fa-chevron-right)');
                    if (btn && !btn.parentElement.classList.contains('disabled')) {
                        btn.click();
                        return true;
                    }
                    return false;
                }
            """)
            
            if not proxima:
                break
            
            pagina_atual += 1
            time.sleep(1.5)

        # Resultados finais
        tempo_total = (time.time() - tempo_inicio) / 60
        
        print(f"\n🏁 CONCLUÍDO SEM MODALS!")
        print(f"   ⏱️ Tempo: {tempo_total:.1f} minutos")
        print(f"   🎯 Aulas: {len(resultado_final)}")
        print(f"   📈 Performance: {len(resultado_final)/tempo_total:.1f} aulas/min")
        print(f"   💾 Cache frequências: {len(frequencia_cache)}")
        print(f"   💾 Cache ATAs: {len(ata_cache)}")
        
        # Análise de eficácia das abordagens
        freq_ok = sum(1 for linha in resultado_final if linha[8] == 'OK')
        freq_fantasma = sum(1 for linha in resultado_final if linha[8] == 'FANTASMA')
        freq_erro = sum(1 for linha in resultado_final if linha[8] == 'ERRO')
        
        print(f"\n📊 EFICÁCIA DAS ABORDAGENS:")
        print(f"   ✅ Sucessos: {freq_ok} ({freq_ok/len(resultado_final)*100:.1f}%)")
        print(f"   👻 Sem dados: {freq_fantasma} ({freq_fantasma/len(resultado_final)*100:.1f}%)")
        print(f"   ❌ Erros: {freq_erro} ({freq_erro/len(resultado_final)*100:.1f}%)")
        
        # Salvar dados
        if resultado_final:
            headers = [
                "CONGREGAÇÃO", "CURSO", "TURMA", "DATA", "PRESENTES IDs", 
                "PRESENTES Nomes", "AUSENTES IDs", "AUSENTES Nomes", "TEM PRESENÇA", "ATA DA AULA"
            ]
            
            body = {
                "tipo": "historico_aulas_sem_modals",
                "dados": resultado_final,
                "headers": headers,
                "resumo": {
                    "total_aulas": len(resultado_final),
                    "tempo_minutos": round(tempo_total, 1),
                    "performance_aulas_por_minuto": round(len(resultado_final)/tempo_total, 1),
                    "paginas": pagina_atual,
                    "cache_frequencias": len(frequencia_cache),
                    "cache_atas": len(ata_cache),
                    "sucessos": freq_ok,
                    "erros": freq_erro,
                    "modo": "SEM_MODALS",
                    "endpoints_descobertos": len(endpoints) if endpoints else 0
                }
            }
            
            try:
                print("📤 Enviando...")
                resp = requests.post(URL_APPS_SCRIPT, json=body, timeout=60)
                print(f"✅ Enviado! {resp.status_code}")
            except Exception as e:
                print(f"❌ Erro envio: {e}")
        
        navegador.close()

if __name__ == "__main__":
    main()
