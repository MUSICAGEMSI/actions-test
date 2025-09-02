# script_historicoindividual.py - Versão Supabase Simplificada
from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from supabase import create_client, Client
import os
import re
import requests
import time
import json
from bs4 import BeautifulSoup
from datetime import datetime
import concurrent.futures
from threading import Lock

# Credenciais
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

URL_INICIAL = "https://musical.congregacao.org.br/"
URL_LISTAGEM_ALUNOS = "https://musical.congregacao.org.br/alunos/listagem"

# Lock para thread safety
print_lock = Lock()

def safe_print(*args, **kwargs):
    """Print thread-safe"""
    with print_lock:
        print(*args, **kwargs)

def init_supabase():
    """Inicializa cliente Supabase"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise Exception("SUPABASE_URL ou SUPABASE_KEY não definidos")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def parsear_nome_info(nome_completo):
    """Extrai nome, estado civil e idade"""
    if ' - ' in nome_completo:
        nome, info_extra = nome_completo.split(' - ', 1)
        estado_civil = ""
        idade = None
        
        if '/' in info_extra:
            estado_civil = info_extra.split('/')[0].strip()
            try:
                idade = int(info_extra.split('/')[1].strip())
            except:
                pass
        else:
            estado_civil = info_extra.strip()
        
        return nome.strip(), estado_civil, idade
    return nome_completo.strip(), "", None

def parsear_endereco(comum_info):
    """Extrai endereço, cidade e estado"""
    endereco = ""
    cidade = ""
    estado = ""
    
    if '|' in comum_info:
        endereco = comum_info.split('|')[0].strip()
        localizacao = comum_info.split('|')[1].strip()
        
        if '-' in localizacao:
            partes = localizacao.split('-')
            if len(partes) >= 2:
                estado = partes[1]
            if len(partes) >= 4:
                cidade = f"{partes[2]}-{partes[3]}"
    else:
        endereco = comum_info
    
    return endereco, cidade, estado

def processar_e_inserir_pessoa(supabase, linha_dados):
    """Processa e insere/atualiza uma pessoa no Supabase"""
    try:
        # Mapear dados
        nome_completo = linha_dados[0]
        id_comum = int(linha_dados[1]) if linha_dados[1] else None
        comum_info = linha_dados[2]
        ministerio = linha_dados[3]
        instrumento = linha_dados[4]
        nivel = linha_dados[5]
        mts = linha_dados[6]
        mts_grupo = linha_dados[7]
        msa = linha_dados[8]
        msa_grupo = linha_dados[9]
        provas = linha_dados[10]
        metodo = linha_dados[11]
        hinario = linha_dados[12]
        hinario_grupo = linha_dados[13]
        escalas = linha_dados[14]
        escalas_grupo = linha_dados[15]
        
        # Parsear informações
        nome, estado_civil, idade = parsear_nome_info(nome_completo)
        endereco, cidade, estado = parsear_endereco(comum_info)
        
        # Preparar dados completos
        dados_pessoa = {
            'nome': nome,
            'id_comum': id_comum,
            'estado_civil': estado_civil,
            'idade': idade,
            'endereco': endereco,
            'cidade': cidade,
            'estado': estado,
            'pais': 'BR',
            'ministerio': ministerio,
            'instrumento': instrumento,
            'nivel': nivel,
            'mts': mts,
            'mts_grupo': mts_grupo,
            'msa': msa,
            'msa_grupo': msa_grupo,
            'provas': provas,
            'metodo': metodo,
            'hinario': hinario,
            'hinario_grupo': hinario_grupo,
            'escalas': escalas,
            'escalas_grupo': escalas_grupo,
            'status': 'CANDIDATO(A)'
        }
        
        # Verificar se pessoa já existe
        resultado_busca = supabase.table('pessoas').select('id').eq('id_comum', id_comum).execute()
        
        if resultado_busca.data:
            # Atualizar pessoa existente
            pessoa_id = resultado_busca.data[0]['id']
            supabase.table('pessoas').update(dados_pessoa).eq('id', pessoa_id).execute()
            safe_print(f"      ✓ {nome[:30]}... atualizado")
            return 'atualizado'
        else:
            # Inserir nova pessoa
            supabase.table('pessoas').insert(dados_pessoa).execute()
            safe_print(f"      ✓ {nome[:30]}... inserido")
            return 'inserido'
            
    except Exception as e:
        safe_print(f"      ⚠️ Erro ao processar: {e}")
        return 'erro'

def log_coleta_supabase(supabase, stats, tempo_execucao):
    """Registra log da coleta"""
    try:
        observacoes = f"Inseridos: {stats['inseridos']}, Atualizados: {stats['atualizados']}, Erros: {stats['erros']}"
        
        supabase.table('logs_coleta').insert({
            'total_registros': stats['total'],
            'tempo_execucao': f"{tempo_execucao:.2f} minutos",
            'status': 'SUCESSO' if stats['erros'] == 0 else 'COM_ERROS',
            'observacoes': observacoes
        }).execute()
        
        safe_print("📋 Log de coleta registrado")
    except Exception as e:
        safe_print(f"⚠️ Erro ao registrar log: {e}")

# ============================================================================
# FUNÇÕES DE SCRAPING (MANTIDAS DO SCRIPT ORIGINAL)
# ============================================================================

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
    """Extração otimizada de datas"""
    if not html_content:
        return ""
    
    soup = BeautifulSoup(html_content, 'html.parser')
    datas_encontradas = set()
    
    # Estratégia 1: Buscar em células de tabela
    for td in soup.find_all('td'):
        texto = td.get_text().strip()
        if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', texto):
            datas_encontradas.add(texto)
    
    # Estratégia 2: Regex no texto completo
    texto_completo = soup.get_text()
    pattern_data = r'\b(\d{1,2}/\d{1,2}/\d{4})\b'
    datas_regex = re.findall(pattern_data, texto_completo)
    datas_encontradas.update(datas_regex)
    
    # Estratégia 3: tr com id específico
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
    """Identifica seções no HTML"""
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
    tab_panes = soup.find_all('div', class_='tab-pane')
    
    for pane in tab_panes:
        pane_id = pane.get('id', '')
        
        if pane_id == 'mts':
            primeira_tabela = pane.find('table', id='datatable1')
            if primeira_tabela:
                secoes['mts'] = str(primeira_tabela)
            
            tabela_grupo = pane.find('table', id='datatable_mts_grupo')
            if tabela_grupo:
                secoes['mts_grupo'] = str(tabela_grupo)
        
        elif pane_id == 'msa':
            primeira_tabela = pane.find('table', id='datatable1')
            if primeira_tabela:
                secoes['msa'] = str(primeira_tabela)
            
            h3_elements = pane.find_all('h3')
            for h3 in h3_elements:
                if 'MSA' in h3.get_text() and 'grupo' in h3.get_text():
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
            primeira_tabela = pane.find('table', id='datatable4')
            if primeira_tabela:
                secoes['hinario'] = str(primeira_tabela)
            
            h3_elements = pane.find_all('h3')
            for h3 in h3_elements:
                if 'Hinos' in h3.get_text() and 'grupo' in h3.get_text():
                    next_table = h3.find_next('table')
                    if next_table:
                        secoes['hinario_grupo'] = str(next_table)
                    break
        
        elif pane_id == 'escalas':
            primeira_tabela = pane.find('table', id='datatable4')
            if primeira_tabela:
                secoes['escalas'] = str(primeira_tabela)
            
            h3_elements = pane.find_all('h3')
            for h3 in h3_elements:
                if 'Escalas' in h3.get_text() and 'grupo' in h3.get_text():
                    next_table = h3.find_next('table')
                    if next_table:
                        secoes['escalas_grupo'] = str(next_table)
                    break
    
    return secoes

def obter_historico_aluno_otimizado(session, aluno_id, aluno_nome=""):
    """Obtém histórico do aluno"""
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
        
        secoes_html = identificar_secoes_html(resp.text)
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
            historico = obter_historico_aluno_otimizado(session, aluno['id'], aluno['nome'])
            
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
            time.sleep(0.1)
            
        except Exception as e:
            safe_print(f"      ⚠️ Erro ao processar aluno {aluno['id']}: {e}")
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
    
    # Inicializar Supabase
    try:
        supabase = init_supabase()
        safe_print("✅ Conexão com Supabase estabelecida")
    except Exception as e:
        safe_print(f"❌ Erro ao conectar com Supabase: {e}")
        return
    
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
        
        print(f"📊 Processando {len(alunos)} alunos...")
        
        # Processar em lotes
        batch_size = 10
        lotes = [alunos[i:i + batch_size] for i in range(0, len(alunos), batch_size)]
        resultado_final = []
        sessoes = criar_sessoes_multiplas(cookies_dict, 3)
        
        # Processar lotes com threading
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            
            for i, lote in enumerate(lotes):
                session_para_lote = sessoes[i % len(sessoes)]
                future = executor.submit(processar_lote_alunos, session_para_lote, lote, i + 1)
                futures.append(future)
                
                if len(futures) >= 3:
                    for future in concurrent.futures.as_completed(futures):
                        resultado_lote = future.result()
                        resultado_final.extend(resultado_lote)
                    futures = []
                    
                    # Progress
                    alunos_processados = len(resultado_final)
                    progresso = (alunos_processados / len(alunos)) * 100
                    tempo_decorrido = (time.time() - tempo_inicio) / 60
                    print(f"📈 Progresso: {alunos_processados}/{len(alunos)} ({progresso:.1f}%) - {tempo_decorrido:.1f}min")
            
            # Processar futures restantes
            for future in concurrent.futures.as_completed(futures):
                resultado_lote = future.result()
                resultado_final.extend(resultado_lote)
        
        print(f"\n📊 Scraping concluído: {len(resultado_final)} alunos")
        
        # INSERIR NO SUPABASE (SIMPLIFICADO)
        print("💾 Inserindo no Supabase...")
        stats = {'inseridos': 0, 'atualizados': 0, 'erros': 0, 'total': len(resultado_final)}
        
        for i, linha in enumerate(resultado_final):
            resultado = processar_e_inserir_pessoa(supabase, linha)
            stats[resultado] += 1
            
            if (i + 1) % 50 == 0:
                progresso = ((i + 1) / len(resultado_final)) * 100
                print(f"   💾 Progresso: {i + 1}/{len(resultado_final)} ({progresso:.1f}%)")
        
        tempo_total = (time.time() - tempo_inicio) / 60
        
        # Log final
        log_coleta_supabase(supabase, stats, tempo_total)
        
        # Resumo
        print(f"\n🎉 PROCESSAMENTO CONCLUÍDO!")
        print(f"   📊 Total processados: {stats['total']}")
        print(f"   ✅ Inseridos: {stats['inseridos']}")
        print(f"   🔄 Atualizados: {stats['atualizados']}")
        print(f"   ❌ Erros: {stats['erros']}")
        print(f"   ⏱️ Tempo total: {tempo_total:.1f} minutos")
        print(f"   🚀 Velocidade: {stats['total']/tempo_total:.1f} registros/min")
        
        navegador.close()

if __name__ == "__main__":
    if not EMAIL or not SENHA:
        print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
        exit(1)
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Erro: SUPABASE_URL ou SUPABASE_KEY não definidos.")
        exit(1)
    
    main()
