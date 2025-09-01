# script_historico_alunos_otimizado.py
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
URL_LISTAGEM_ALUNOS = "https://musical.congregacao.org.br/alunos/listagem"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxVW_i69_DL_UQQqVjxLsAcEv5edorXSD4g-PZUu4LC9TkGd9yEfNiTL0x92ELDNm8M/exec'

# Lock para thread safety
print_lock = Lock()

def safe_print(*args, **kwargs):
    """Print thread-safe"""
    with print_lock:
        print(*args, **kwargs)

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

def extrair_datas_otimizada(html_content, secao_nome=""):
    """
    Extração otimizada de datas focando na estrutura real do HTML
    """
    if not html_content:
        return ""
    
    soup = BeautifulSoup(html_content, 'html.parser')
    datas_encontradas = set()  # Usar set para evitar duplicatas automaticamente
    
    # Estratégia 1: Buscar datas em células de tabela <td>
    for td in soup.find_all('td'):
        texto = td.get_text().strip()
        # Padrão para datas DD/MM/YYYY
        if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', texto):
            datas_encontradas.add(texto)
    
    # Estratégia 2: Buscar datas no texto usando regex mais amplo
    texto_completo = soup.get_text()
    pattern_data = r'\b(\d{1,2}/\d{1,2}/\d{4})\b'
    datas_regex = re.findall(pattern_data, texto_completo)
    datas_encontradas.update(datas_regex)
    
    # Estratégia 3: Buscar especificamente em tr com id (para MSA, provas, etc.)
    for tr in soup.find_all('tr', id=True):
        if any(prefix in tr.get('id', '') for prefix in ['msa_', 'prova_', 'escala_']):
            for td in tr.find_all('td'):
                texto = td.get_text().strip()
                if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', texto):
                    datas_encontradas.add(texto)
    
    if not datas_encontradas:
        return ""
    
    # Ordenar cronologicamente
    try:
        datas_ordenadas = sorted(
            list(datas_encontradas), 
            key=lambda x: datetime.strptime(x, '%d/%m/%Y')
        )
        return "; ".join(datas_ordenadas)
    except:
        return "; ".join(sorted(list(datas_encontradas)))

def identificar_secoes_html(html):
    """
    Identifica as diferentes seções no HTML de forma mais robusta
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
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Buscar por divs com id específico das abas
    tab_panes = soup.find_all('div', class_='tab-pane')
    
    for pane in tab_panes:
        pane_id = pane.get('id', '')
        
        if pane_id == 'mts':
            # MTS individual - primeira tabela
            primeira_tabela = pane.find('table', id='datatable1')
            if primeira_tabela:
                secoes['mts'] = str(primeira_tabela)
            
            # MTS grupo - tabela com id datatable_mts_grupo
            tabela_grupo = pane.find('table', id='datatable_mts_grupo')
            if tabela_grupo:
                secoes['mts_grupo'] = str(tabela_grupo)
        
        elif pane_id == 'msa':
            # MSA individual - primeira tabela
            primeira_tabela = pane.find('table', id='datatable1')
            if primeira_tabela:
                secoes['msa'] = str(primeira_tabela)
            
            # MSA grupo - buscar por h3 "MSA - Aulas em grupo"
            h3_elements = pane.find_all('h3')
            for h3 in h3_elements:
                if 'MSA' in h3.get_text() and 'grupo' in h3.get_text():
                    # Buscar próxima tabela após o h3
                    next_table = h3.find_next('table')
                    if next_table:
                        secoes['msa_grupo'] = str(next_table)
                    break
        
        elif pane_id == 'provas':
            tabela_provas = pane.find('table', id='datatable2')
            if tabela_provas:
                secoes['provas'] = str(tabela_provas)
        
        elif pane_id == 'metodos':
            tabela_metodos = pane.find('table', id='datatable3')
            if tabela_metodos:
                secoes['metodo'] = str(tabela_metodos)
        
        elif pane_id == 'hinario':
            # Hinário individual
            primeira_tabela = pane.find('table', id='datatable4')
            if primeira_tabela:
                secoes['hinario'] = str(primeira_tabela)
            
            # Hinário grupo
            h3_elements = pane.find_all('h3')
            for h3 in h3_elements:
                if 'Hinos' in h3.get_text() and 'grupo' in h3.get_text():
                    next_table = h3.find_next('table')
                    if next_table:
                        secoes['hinario_grupo'] = str(next_table)
                    break
        
        elif pane_id == 'escalas':
            # Escalas individual
            primeira_tabela = pane.find('table', id='datatable4')
            if primeira_tabela:
                secoes['escalas'] = str(primeira_tabela)
            
            # Escalas grupo
            h3_elements = pane.find_all('h3')
            for h3 in h3_elements:
                if 'Escalas' in h3.get_text() and 'grupo' in h3.get_text():
                    next_table = h3.find_next('table')
                    if next_table:
                        secoes['escalas_grupo'] = str(next_table)
                    break
    
    return secoes

def obter_historico_aluno_otimizado(session, aluno_id, aluno_nome=""):
    """Versão otimizada para obter histórico do aluno"""
    try:
        url_historico = f"https://musical.congregacao.org.br/licoes/index/{aluno_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/alunos/listagem',
            'Connection': 'keep-alive',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        
        resp = session.get(url_historico, headers=headers, timeout=15)
        
        if resp.status_code != 200:
            safe_print(f"      ⚠️ Status HTTP {resp.status_code} para aluno {aluno_id}")
            return {}
        
        # Identificar seções de forma mais estruturada
        secoes_html = identificar_secoes_html(resp.text)
        
        # Extrair datas de cada seção
        historico = {}
        total_datas = 0
        
        for secao_nome, conteudo_html in secoes_html.items():
            if conteudo_html:
                datas = extrair_datas_otimizada(conteudo_html, secao_nome)
                historico[secao_nome] = datas
                if datas:
                    total_datas += len(datas.split('; '))
            else:
                historico[secao_nome] = ""
        
        if total_datas > 0:
            safe_print(f"      ✓ {aluno_nome[:30]}... - {total_datas} datas coletadas")
        
        return historico
        
    except Exception as e:
        safe_print(f"      ⚠️ Erro ao processar aluno {aluno_id}: {e}")
        return {}

def processar_lote_alunos(session, lote_alunos, lote_numero):
    """Processa um lote de alunos"""
    resultado_lote = []
    
    for i, aluno in enumerate(lote_alunos):
        try:
            # Obter histórico do aluno
            historico = obter_historico_aluno_otimizado(session, aluno['id'], aluno['nome'])
            
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
            
            resultado_lote.append(linha)
            
            # Pequena pausa entre alunos
            time.sleep(0.1)
            
        except Exception as e:
            safe_print(f"      ⚠️ Erro ao processar aluno {aluno['id']}: {e}")
            # Adicionar linha vazia em caso de erro
            linha_vazia = [aluno['nome'], aluno['id'], aluno['comum'], 
                          aluno['ministerio'], aluno['instrumento'], aluno['nivel']] + [''] * 10
            resultado_lote.append(linha_vazia)
    
    safe_print(f"   📦 Lote {lote_numero} concluído ({len(resultado_lote)} alunos)")
    return resultado_lote

def criar_sessoes_multiplas(cookies_dict, num_sessoes=3):
    """Cria múltiplas sessões com os mesmos cookies"""
    sessoes = []
    for i in range(num_sessoes):
        session = requests.Session()
        session.cookies.update(cookies_dict)
        sessoes.append(session)
    return sessoes

def main():
    tempo_inicio = time.time()
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
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
        
        # Teste com alguns alunos primeiro (opcional)
        # alunos = alunos[:50]  # Descomente para testar com apenas 50 alunos
        
        print(f"📊 Processando {len(alunos)} alunos...")
        
        # Dividir alunos em lotes menores
        batch_size = 10  # Lotes menores para melhor controle
        lotes = [alunos[i:i + batch_size] for i in range(0, len(alunos), batch_size)]
        
        resultado_final = []
        
        # Criar múltiplas sessões para paralelização
        sessoes = criar_sessoes_multiplas(cookies_dict, 3)
        
        # Processar lotes com threading
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            
            for i, lote in enumerate(lotes):
                # Rotacionar entre as sessões
                session_para_lote = sessoes[i % len(sessoes)]
                
                future = executor.submit(
                    processar_lote_alunos, 
                    session_para_lote, 
                    lote, 
                    i + 1
                )
                futures.append(future)
                
                # Não sobrecarregar - processar em grupos
                if len(futures) >= 3:
                    # Aguardar conclusão dos futures atuais
                    for future in concurrent.futures.as_completed(futures):
                        resultado_lote = future.result()
                        resultado_final.extend(resultado_lote)
                    
                    futures = []
                    
                    # Status
                    alunos_processados = len(resultado_final)
                    progresso = (alunos_processados / len(alunos)) * 100
                    tempo_decorrido = (time.time() - tempo_inicio) / 60
                    print(f"📈 Progresso: {alunos_processados}/{len(alunos)} ({progresso:.1f}%) - {tempo_decorrido:.1f}min")
            
            # Processar futures restantes
            for future in concurrent.futures.as_completed(futures):
                resultado_lote = future.result()
                resultado_final.extend(resultado_lote)
        
        print(f"\n📊 Processamento concluído: {len(resultado_final)} alunos")
        tempo_total = (time.time() - tempo_inicio) / 60
        print(f"⏱️ Tempo total: {tempo_total:.1f} minutos")
        
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
            
            # Salvar backup local em caso de erro
            import json
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
