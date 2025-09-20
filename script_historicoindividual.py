# script_historico_alunos_ultra_otimizado.py
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
import asyncio
import aiohttp
from urllib.parse import urlencode

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_LISTAGEM_ALUNOS = "https://musical.congregacao.org.br/alunos/listagem"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxVW_i69_DL_UQQqVjxLsAcEv5edorXSD4g-PZUu4LC9TkGd9yEfNiTL0x92ELDNm8M/exec'

# Lock para thread safety
print_lock = Lock()
processados_count = 0
total_alunos = 0

def safe_print(*args, **kwargs):
    """Print thread-safe"""
    with print_lock:
        print(*args, **kwargs)

def update_progress():
    """Atualiza contador de progresso de forma thread-safe"""
    global processados_count
    with print_lock:
        processados_count += 1
        if processados_count % 10 == 0 or processados_count <= 5:
            progresso = (processados_count / total_alunos) * 100
            safe_print(f"📈 Progresso: {processados_count}/{total_alunos} ({progresso:.1f}%)")

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para usar em requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def obter_lista_alunos(session):
    """Obtém a lista completa de alunos da API"""
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/alunos/listagem',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }
        
        data = {
            'draw': '1',
            'start': '0',
            'length': '10000',
            'search[value]': '',
            'search[regex]': 'false'
        }
        
        resp = session.post(URL_LISTAGEM_ALUNOS, data=data, headers=headers, timeout=30)
        
        if resp.status_code == 200:
            dados_json = resp.json()
            alunos = []
            
            for linha in dados_json.get('data', []):
                if len(linha) >= 8:
                    id_aluno = linha[0]
                    nome_info = linha[1]
                    comum_info = linha[2]
                    ministerio = linha[3]
                    instrumento = linha[4]
                    nivel = linha[5]
                    
                    comum_limpo = re.sub(r'<[^>]+>', '', comum_info).strip()
                    nome_limpo = nome_info.strip()
                    
                    alunos.append({
                        'id': id_aluno,
                        'nome': nome_limpo,
                        'comum': comum_limpo,
                        'ministerio': ministerio,
                        'instrumento': instrumento,
                        'nivel': nivel
                    })
            
            safe_print(f"✅ Encontrados {len(alunos)} alunos")
            return alunos
            
    except Exception as e:
        safe_print(f"❌ Erro ao obter lista de alunos: {e}")
        return []

# Cache de regex compilado para melhor performance
REGEX_DATA = re.compile(r'\b(\d{1,2}/\d{1,2}/\d{4})\b')
REGEX_DATA_CELL = re.compile(r'^\d{1,2}/\d{1,2}/\d{4}$')

def extrair_datas_super_otimizada(html_content):
    """
    Versão super otimizada da extração de datas
    """
    if not html_content or len(html_content) < 10:
        return ""
    
    # Usar regex diretamente no HTML para maior velocidade
    datas_encontradas = set(REGEX_DATA.findall(html_content))
    
    if not datas_encontradas:
        return ""
    
    # Ordenar cronologicamente (otimizada)
    try:
        datas_ordenadas = sorted(
            list(datas_encontradas), 
            key=lambda x: datetime.strptime(x, '%d/%m/%Y')
        )
        return "; ".join(datas_ordenadas)
    except:
        return "; ".join(sorted(list(datas_encontradas)))

def identificar_secoes_otimizada(html):
    """
    Versão otimizada que usa regex em vez de BeautifulSoup para maior velocidade
    """
    secoes = {
        'mts': "",
        'mts_grupo': "",
        'msa': "",
        'msa_grupo': "",
        'provas': "",
        'metodo': "",
        'hinario': "",
        'hinario_grupo': "",
        'escalas': "",
        'escalas_grupo': ""
    }
    
    # Usar regex para encontrar seções específicas
    patterns = {
        'mts': r'<div[^>]*id="mts"[^>]*>.*?</div>(?=<div[^>]*class="tab-pane"|$)',
        'msa': r'<div[^>]*id="msa"[^>]*>.*?</div>(?=<div[^>]*class="tab-pane"|$)',
        'provas': r'<div[^>]*id="provas"[^>]*>.*?</div>(?=<div[^>]*class="tab-pane"|$)',
        'metodos': r'<div[^>]*id="metodos"[^>]*>.*?</div>(?=<div[^>]*class="tab-pane"|$)',
        'hinario': r'<div[^>]*id="hinario"[^>]*>.*?</div>(?=<div[^>]*class="tab-pane"|$)',
        'escalas': r'<div[^>]*id="escalas"[^>]*>.*?</div>(?=<div[^>]*class="tab-pane"|$)'
    }
    
    for secao_key, pattern in patterns.items():
        match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if match:
            conteudo = match.group(0)
            
            if secao_key == 'mts':
                # Primeira tabela para MTS individual
                tabela_match = re.search(r'<table[^>]*id="datatable1"[^>]*>.*?</table>', conteudo, re.DOTALL)
                if tabela_match:
                    secoes['mts'] = tabela_match.group(0)
                
                # Tabela grupo
                grupo_match = re.search(r'<table[^>]*id="datatable_mts_grupo"[^>]*>.*?</table>', conteudo, re.DOTALL)
                if grupo_match:
                    secoes['mts_grupo'] = grupo_match.group(0)
            
            elif secao_key == 'msa':
                # Primeira tabela para MSA individual
                tabela_match = re.search(r'<table[^>]*id="datatable1"[^>]*>.*?</table>', conteudo, re.DOTALL)
                if tabela_match:
                    secoes['msa'] = tabela_match.group(0)
                
                # MSA grupo (depois do h3 com "grupo")
                if 'grupo' in conteudo.lower():
                    grupo_match = re.search(r'<h3[^>]*>.*?grupo.*?</h3>.*?<table[^>]*>.*?</table>', conteudo, re.DOTALL | re.IGNORECASE)
                    if grupo_match:
                        table_match = re.search(r'<table[^>]*>.*?</table>', grupo_match.group(0), re.DOTALL)
                        if table_match:
                            secoes['msa_grupo'] = table_match.group(0)
            
            elif secao_key == 'provas':
                tabela_match = re.search(r'<table[^>]*id="datatable2"[^>]*>.*?</table>', conteudo, re.DOTALL)
                if tabela_match:
                    secoes['provas'] = tabela_match.group(0)
            
            elif secao_key == 'metodos':
                tabela_match = re.search(r'<table[^>]*id="datatable3"[^>]*>.*?</table>', conteudo, re.DOTALL)
                if tabela_match:
                    secoes['metodo'] = tabela_match.group(0)
            
            elif secao_key == 'hinario':
                # Hinário individual
                tabela_match = re.search(r'<table[^>]*id="datatable4"[^>]*>.*?</table>', conteudo, re.DOTALL)
                if tabela_match:
                    secoes['hinario'] = tabela_match.group(0)
                
                # Hinário grupo
                if 'grupo' in conteudo.lower():
                    grupo_match = re.search(r'<h3[^>]*>.*?grupo.*?</h3>.*?<table[^>]*>.*?</table>', conteudo, re.DOTALL | re.IGNORECASE)
                    if grupo_match:
                        table_match = re.search(r'<table[^>]*>.*?</table>', grupo_match.group(0), re.DOTALL)
                        if table_match:
                            secoes['hinario_grupo'] = table_match.group(0)
            
            elif secao_key == 'escalas':
                # Escalas individual
                tabela_match = re.search(r'<table[^>]*id="datatable4"[^>]*>.*?</table>', conteudo, re.DOTALL)
                if tabela_match:
                    secoes['escalas'] = tabela_match.group(0)
                
                # Escalas grupo
                if 'grupo' in conteudo.lower():
                    grupo_match = re.search(r'<h3[^>]*>.*?grupo.*?</h3>.*?<table[^>]*>.*?</table>', conteudo, re.DOTALL | re.IGNORECASE)
                    if grupo_match:
                        table_match = re.search(r'<table[^>]*>.*?</table>', grupo_match.group(0), re.DOTALL)
                        if table_match:
                            secoes['escalas_grupo'] = table_match.group(0)
    
    return secoes

def obter_historico_aluno_super_otimizado(session, aluno_id, aluno_nome=""):
    """Versão super otimizada para obter histórico do aluno"""
    try:
        url_historico = f"https://musical.congregacao.org.br/licoes/index/{aluno_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/alunos/listagem',
            'Connection': 'keep-alive',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br'
        }
        
        resp = session.get(url_historico, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            return {}
        
        # Usar a versão otimizada de identificação de seções
        secoes_html = identificar_secoes_otimizada(resp.text)
        
        # Extrair datas de cada seção de forma mais rápida
        historico = {}
        total_datas = 0
        
        for secao_nome, conteudo_html in secoes_html.items():
            if conteudo_html:
                datas = extrair_datas_super_otimizada(conteudo_html)
                historico[secao_nome] = datas
                if datas:
                    total_datas += len(datas.split('; '))
            else:
                historico[secao_nome] = ""
        
        # Atualizar progresso
        update_progress()
        
        return historico
        
    except Exception as e:
        update_progress()
        return {}

def processar_aluno_individual(session, aluno):
    """Processa um único aluno - versão otimizada"""
    try:
        # Obter histórico do aluno
        historico = obter_historico_aluno_super_otimizado(session, aluno['id'], aluno['nome'])
        
        # Montar linha de dados
        linha = [
            aluno['nome'],
            aluno['id'],
            aluno['comum'],
            aluno['ministerio'],
            aluno['instrumento'],
            aluno['nivel'],
            historico.get('mts', ''),
            historico.get('mts_grupo', ''),
            historico.get('msa', ''),
            historico.get('msa_grupo', ''),
            historico.get('provas', ''),
            historico.get('metodo', ''),
            historico.get('hinario', ''),
            historico.get('hinario_grupo', ''),
            historico.get('escalas', ''),
            historico.get('escalas_grupo', '')
        ]
        
        return linha
        
    except Exception as e:
        # Adicionar linha vazia em caso de erro
        linha_vazia = [aluno['nome'], aluno['id'], aluno['comum'], 
                      aluno['ministerio'], aluno['instrumento'], aluno['nivel']] + [''] * 10
        return linha_vazia

def criar_sessoes_otimizadas(cookies_dict, num_sessoes=8):
    """Cria múltiplas sessões otimizadas"""
    sessoes = []
    for i in range(num_sessoes):
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # Otimizações de sessão
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Keep-Alive': 'timeout=30, max=100'
        })
        
        # Configurar adapter para connection pooling
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=2
        )
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        
        sessoes.append(session)
    
    return sessoes

def main():
    global total_alunos, processados_count
    tempo_inicio = time.time()
    
    with sync_playwright() as p:
        # Usar Chrome com otimizações
        navegador = p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
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
        
        # Criar sessão principal
        cookies_dict = extrair_cookies_playwright(pagina)
        session_principal = requests.Session()
        session_principal.cookies.update(cookies_dict)
        
        # Obter lista de alunos
        print("🔍 Obtendo lista de alunos...")
        alunos = obter_lista_alunos(session_principal)
        
        if not alunos:
            print("❌ Nenhum aluno encontrado.")
            navegador.close()
            return
        
        total_alunos = len(alunos)
        processados_count = 0
        
        # Teste com alguns alunos primeiro (opcional)
        # alunos = alunos[:20]  # Descomente para testar
        # total_alunos = len(alunos)
        
        print(f"📊 Processando {len(alunos)} alunos com paralelização ultra otimizada...")
        
        resultado_final = []
        
        # Criar múltiplas sessões otimizadas
        max_workers = 8  # Aumentado para maior paralelismo
        sessoes = criar_sessoes_otimizadas(cookies_dict, max_workers)
        
        # Processar todos os alunos em paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Mapear cada aluno para uma sessão
            futures = []
            
            for i, aluno in enumerate(alunos):
                session_para_aluno = sessoes[i % len(sessoes)]
                future = executor.submit(processar_aluno_individual, session_para_aluno, aluno)
                futures.append(future)
            
            # Coletar resultados conforme ficam prontos
            for future in concurrent.futures.as_completed(futures):
                try:
                    resultado = future.result()
                    resultado_final.append(resultado)
                except Exception as e:
                    safe_print(f"⚠️ Erro em future: {e}")
                
                # Status a cada 25 alunos processados
                if len(resultado_final) % 25 == 0:
                    progresso = (len(resultado_final) / len(alunos)) * 100
                    tempo_decorrido = (time.time() - tempo_inicio) / 60
                    velocidade = len(resultado_final) / tempo_decorrido if tempo_decorrido > 0 else 0
                    safe_print(f"🚀 {len(resultado_final)}/{len(alunos)} ({progresso:.1f}%) - {velocidade:.1f} alunos/min")
        
        print(f"\n📊 Processamento concluído: {len(resultado_final)} alunos")
        tempo_total = (time.time() - tempo_inicio) / 60
        print(f"⏱️ Tempo total: {tempo_total:.1f} minutos")
        print(f"🚀 Velocidade média: {len(resultado_final)/tempo_total:.1f} alunos/min")
        
        # Preparar dados para envio
        headers = [
            "NOME", "ID", "COMUM", "MINISTERIO", "INSTRUMENTO", "NIVEL",
            "MTS", "MTS GRUPO", "MSA", "MSA GRUPO", "PROVAS", "MÉTODO",
            "HINÁRIO", "HINÁRIO GRUPO", "ESCALAS", "ESCALAS GRUPO"
        ]
        
        # Calcular estatísticas
        total_datas = 0
        stats_secoes = {}
        
        for linha in resultado_final:
            for i, campo_data in enumerate(linha[6:]):
                secao_nome = headers[i + 6]
                if campo_data:
                    num_datas = len(campo_data.split('; '))
                    stats_secoes[secao_nome] = stats_secoes.get(secao_nome, 0) + num_datas
                    total_datas += num_datas
        
        body = {
            "tipo": "historico_alunos",
            "dados": resultado_final,
            "headers": headers,
            "resumo": {
                "total_alunos": len(resultado_final),
                "tempo_processamento": f"{tempo_total:.1f} minutos",
                "velocidade": f"{len(resultado_final)/tempo_total:.1f} alunos/min",
                "total_datas": total_datas,
                "media_datas_por_aluno": f"{total_datas/len(resultado_final):.1f}" if resultado_final else "0",
                "stats_secoes": stats_secoes
            }
        }
        
        # Enviar dados para Apps Script
        try:
            print("📤 Enviando dados para Google Sheets...")
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
            print(f"✅ Dados enviados! Status: {resposta_post.status_code}")
            print(f"Resposta: {resposta_post.text[:200]}...")
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
            
            # Salvar backup local
            with open(f'backup_historico_{int(time.time())}.json', 'w', encoding='utf-8') as f:
                json.dump(body, f, ensure_ascii=False, indent=2)
            print("💾 Backup salvo localmente")
        
        # Resumo final
        print(f"\n📈 RESUMO FINAL:")
        print(f"   🎯 Total de alunos processados: {len(resultado_final)}")
        print(f"   📅 Total de datas coletadas: {total_datas}")
        print(f"   📊 Média de datas por aluno: {total_datas/len(resultado_final):.1f}")
        print(f"   ⏱️ Tempo total: {tempo_total:.1f} minutos")
        print(f"   🚀 Velocidade: {len(resultado_final)/tempo_total:.1f} alunos/min")
        
        if stats_secoes:
            print("   📋 Datas por seção:")
            for secao, count in sorted(stats_secoes.items(), key=lambda x: x[1], reverse=True):
                if count > 0:
                    print(f"      - {secao}: {count} datas")
        
        navegador.close()

if __name__ == "__main__":
    if not EMAIL or not SENHA:
        print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
        exit(1)
    
    main()
