"""
TESTE ISOLADO - MÓDULO 2: COLETA DE TURMAS
Coleta turmas de Hortolândia filtrando por IDs de instrutores
"""

from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
import json

# ==================== CONFIGURAÇÕES ====================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT_TURMAS = 'https://script.google.com/macros/s/AKfycbyw2E0QH0ucHRdCMNOY_La7r4ElK6xcf0OWlnQGa9w7yCcg82mG_bJV_5fxbhuhbfuY/exec'

# ==================== FUNÇÕES AUXILIARES ====================

def criar_sessao_robusta():
    """Cria sessão HTTP com retry automático"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "HEAD"]
    )
    
    adapter = HTTPAdapter(
        pool_connections=20,
        pool_maxsize=20,
        max_retries=retry_strategy
    )
    
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    
    return session

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def gerar_timestamp():
    """Gera timestamp no formato DD_MM_YYYY-HH:MM"""
    return datetime.now().strftime('%d_%m_%Y-%H_%M')

# ==================== LOGIN ====================

def fazer_login_unico():
    """Realiza login único via Playwright"""
    print("\n" + "=" * 80)
    print("🔐 REALIZANDO LOGIN")
    print("=" * 80)
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        print("   Acessando página de login...")
        pagina.goto(URL_INICIAL, timeout=20000)
        
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')
        
        try:
            pagina.wait_for_selector("nav", timeout=20000)
            print("   ✅ Login realizado com sucesso!")
        except PlaywrightTimeoutError:
            print("   ❌ Falha no login. Verifique as credenciais.")
            navegador.close()
            return None, None
        
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
    
    session = criar_sessao_robusta()
    session.cookies.update(cookies_dict)
    
    print("   ✅ Sessão configurada\n")
    return session, cookies_dict

# ==================== MÓDULO 2: TURMAS ====================

def carregar_ids_instrutores_hortolandia(session, max_tentativas=5):
    """Carrega IDs de todos os instrutores de Hortolândia"""
    print("\n" + "=" * 80)
    print("👥 CARREGANDO INSTRUTORES DE HORTOLÂNDIA")
    print("=" * 80)
    
    for tentativa in range(1, max_tentativas + 1):
        try:
            url = "https://musical.congregacao.org.br/licoes/instrutores?q=a"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive'
            }
            
            timeout = 15 + (tentativa * 5)
            print(f"   Tentativa {tentativa}/{max_tentativas} (timeout: {timeout}s)...", end=" ")
            
            resp = session.get(url, headers=headers, timeout=timeout)
            
            if resp.status_code != 200:
                print(f"HTTP {resp.status_code}")
                continue
            
            instrutores = json.loads(resp.text)
            
            ids_instrutores = {}
            
            for instrutor in instrutores:
                id_instrutor = instrutor['id']
                texto_completo = instrutor['text']
                ids_instrutores[id_instrutor] = texto_completo
            
            print(f"✅ {len(ids_instrutores)} instrutores carregados!")
            
            print(f"\n📋 Amostra (primeiros 5):")
            for i, (id_inst, nome) in enumerate(list(ids_instrutores.items())[:5], 1):
                print(f"   {i}. ID {id_inst}: {nome}")
            
            return ids_instrutores
            
        except requests.Timeout:
            print(f"Timeout")
            if tentativa < max_tentativas:
                time.sleep(tentativa * 2)
        except Exception as e:
            print(f"Erro: {e}")
            if tentativa < max_tentativas:
                time.sleep(tentativa * 2)
    
    print("\n❌ Falha ao carregar instrutores\n")
    return {}

def buscar_todas_turmas_sistema(session, id_min=1, id_max=50000):
    """Busca TODAS as turmas do sistema por varredura de IDs"""
    print(f"\n🔍 Buscando turmas por varredura de IDs ({id_min} até {id_max})...")
    
    ids_turmas_existentes = []
    
    LOTE_SIZE = 500
    MAX_WORKERS = 20
    
    for lote_inicio in range(id_min, id_max + 1, LOTE_SIZE):
        lote_fim = min(lote_inicio + LOTE_SIZE - 1, id_max)
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(verificar_turma_existe, session, turma_id): turma_id 
                for turma_id in range(lote_inicio, lote_fim + 1)
            }
            
            for future in as_completed(futures):
                turma_id = futures[future]
                existe = future.result()
                
                if existe:
                    ids_turmas_existentes.append(turma_id)
        
        if lote_inicio % 5000 == 0:
            print(f"   Progresso: {lote_inicio}/{id_max} | Encontradas: {len(ids_turmas_existentes)}")
        
        time.sleep(0.2)
    
    return ids_turmas_existentes

def verificar_turma_existe(session, turma_id):
    """Verifica se uma turma existe"""
    try:
        url = f"https://musical.congregacao.org.br/turmas/editar/{turma_id}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = session.get(url, headers=headers, timeout=5)
        
        if resp.status_code == 200 and 'form' in resp.text.lower():
            return True
        return False
    except:
        return False

def coletar_dados_turma_completo(session, turma_id):
    """Coleta todos os dados de uma turma"""
    try:
        url = f"https://musical.congregacao.org.br/turmas/editar/{turma_id}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        resp = session.get(url, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        form = soup.find('form', id='turmas')
        if not form:
            return None
        
        dados = {
            'id_turma': turma_id,
            'curso': '',
            'descricao': '',
            'comum': '',
            'dia_semana': '',
            'data_inicio': '',
            'data_encerramento': '',
            'hora_inicio': '',
            'hora_termino': '',
            'responsavel_1_id': '',
            'responsavel_1_nome': '',
            'responsavel_2_id': '',
            'responsavel_2_nome': '',
            'destinado_ao': '',
            'ativo': 'Não',
            'cadastrado_em': '',
            'cadastrado_por': '',
            'atualizado_em': '',
            'atualizado_por': ''
        }
        
        # Curso
        curso_select = soup.find('select', {'name': 'id_curso'})
        if curso_select:
            curso_option = curso_select.find('option', selected=True)
            if curso_option:
                dados['curso'] = curso_option.get_text(strip=True)
        
        # Descrição
        descricao_input = soup.find('input', {'name': 'descricao'})
        if descricao_input:
            dados['descricao'] = descricao_input.get('value', '').strip()
        
        # Comum
        comum_select = soup.find('select', {'name': 'id_igreja'})
        if comum_select:
            comum_option = comum_select.find('option', selected=True)
            if comum_option:
                texto_completo = comum_option.get_text(strip=True)
                dados['comum'] = texto_completo.split('|')[0].strip()
        
        # Dia da Semana
        dia_select = soup.find('select', {'name': 'dia_semana'})
        if dia_select:
            dia_option = dia_select.find('option', selected=True)
            if dia_option:
                dados['dia_semana'] = dia_option.get_text(strip=True)
        
        # Data de Início
        dt_inicio_input = soup.find('input', {'name': 'dt_inicio'})
        if dt_inicio_input:
            dados['data_inicio'] = dt_inicio_input.get('value', '').strip()
        
        # Data de Encerramento
        dt_fim_input = soup.find('input', {'name': 'dt_fim'})
        if dt_fim_input:
            dados['data_encerramento'] = dt_fim_input.get('value', '').strip()
        
        # Hora de Início
        hr_inicio_input = soup.find('input', {'name': 'hr_inicio'})
        if hr_inicio_input:
            hora_completa = hr_inicio_input.get('value', '').strip()
            dados['hora_inicio'] = hora_completa[:5] if hora_completa else ''
        
        # Hora de Término
        hr_fim_input = soup.find('input', {'name': 'hr_fim'})
        if hr_fim_input:
            hora_completa = hr_fim_input.get('value', '').strip()
            dados['hora_termino'] = hora_completa[:5] if hora_completa else ''
        
        # RESPONSÁVEIS - Extrair do JavaScript
        script_tags = soup.find_all('script')
        for script in script_tags:
            script_text = script.string
            if script_text and 'id_responsavel' in script_text:
                # Responsável 1
                match1 = re.search(r"const option = '<option value=\"(\d+)\" selected>(.*?)</option>'", script_text)
                if match1:
                    dados['responsavel_1_id'] = match1.group(1)
                    nome_completo = match1.group(2).strip()
                    dados['responsavel_1_nome'] = nome_completo.split(' - ')[0].strip()
                
                # Responsável 2
                match2 = re.search(r"const option2 = '<option value=\"(\d+)\" selected>(.*?)</option>'", script_text)
                if match2:
                    dados['responsavel_2_id'] = match2.group(1)
                    nome_completo = match2.group(2).strip()
                    dados['responsavel_2_nome'] = nome_completo.split(' - ')[0].strip()
        
        # Destinado ao
        genero_select = soup.find('select', {'name': 'id_turma_genero'})
        if genero_select:
            genero_option = genero_select.find('option', selected=True)
            if genero_option:
                dados['destinado_ao'] = genero_option.get_text(strip=True)
        
        # Ativo
        status_checkbox = soup.find('input', {'name': 'status'})
        if status_checkbox and status_checkbox.has_attr('checked'):
            dados['ativo'] = 'Sim'
        
        # Histórico
        historico_div = soup.find('div', id='collapseOne')
        if historico_div:
            paragrafos = historico_div.find_all('p')
            
            for p in paragrafos:
                texto = p.get_text(strip=True)
                
                if 'Cadastrado em:' in texto:
                    partes = texto.split('por:')
                    if len(partes) >= 2:
                        dados['cadastrado_em'] = partes[0].replace('Cadastrado em:', '').strip()
                        dados['cadastrado_por'] = partes[1].strip()
                
                elif 'Atualizado em:' in texto:
                    partes = texto.split('por:')
                    if len(partes) >= 2:
                        dados['atualizado_em'] = partes[0].replace('Atualizado em:', '').strip()
                        dados['atualizado_por'] = partes[1].strip()
        
        return dados
        
    except Exception as e:
        return None

def filtrar_turmas_hortolandia(dados_turma, ids_instrutores_htl):
    """Verifica se a turma é de Hortolândia"""
    if not dados_turma:
        return False
    
    resp1_id = dados_turma.get('responsavel_1_id', '')
    resp2_id = dados_turma.get('responsavel_2_id', '')
    
    if resp1_id in ids_instrutores_htl or resp2_id in ids_instrutores_htl:
        return True
    
    return False

def executar_teste_modulo2(session):
    """TESTE: Módulo 2 - Coleta turmas de Hortolândia"""
    tempo_inicio = time.time()
    timestamp_execucao = gerar_timestamp()
    
    print("\n" + "=" * 80)
    print("🎓 TESTE: MÓDULO 2 - DADOS DE TURMAS")
    print("=" * 80)
    
    # PASSO 1: Carregar instrutores
    ids_instrutores_htl = carregar_ids_instrutores_hortolandia(session)
    
    if not ids_instrutores_htl:
        print("❌ Não foi possível carregar instrutores. Abortando.")
        return
    
    # PASSO 2: Buscar todas as turmas
    print("\n" + "=" * 80)
    print("🔍 BUSCANDO TODAS AS TURMAS DO SISTEMA")
    print("=" * 80)
    
    ids_turmas_sistema = buscar_todas_turmas_sistema(session, id_min=27000, id_max=54000)
    
    if not ids_turmas_sistema:
        print("❌ Nenhuma turma encontrada. Abortando.")
        return
    
    print(f"\n✅ {len(ids_turmas_sistema)} turmas encontradas no sistema")
    
    # PASSO 3: Coletar e filtrar
    print("\n" + "=" * 80)
    print("📊 COLETANDO E FILTRANDO TURMAS DE HORTOLÂNDIA")
    print("=" * 80)
    
    resultado = []
    processadas = 0
    turmas_htl = 0
    erros = 0
    
    MAX_WORKERS = 15
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(coletar_dados_turma_completo, session, turma_id): turma_id 
            for turma_id in ids_turmas_sistema
        }
        
        for future in as_completed(futures):
            processadas += 1
            turma_id = futures[future]
            
            dados = future.result()
            
            if dados and filtrar_turmas_hortolandia(dados, ids_instrutores_htl):
                turmas_htl += 1
                
                resultado.append([
                    dados['id_turma'],
                    dados['curso'],
                    dados['descricao'],
                    dados['comum'],
                    dados['dia_semana'],
                    dados['data_inicio'],
                    dados['data_encerramento'],
                    dados['hora_inicio'],
                    dados['hora_termino'],
                    dados['responsavel_1_id'],
                    dados['responsavel_1_nome'],
                    dados['responsavel_2_id'],
                    dados['responsavel_2_nome'],
                    dados['destinado_ao'],
                    dados['ativo'],
                    dados['cadastrado_em'],
                    dados['cadastrado_por'],
                    dados['atualizado_em'],
                    dados['atualizado_por'],
                    'Coletado',
                    time.strftime('%d/%m/%Y %H:%M:%S')
                ])
                
                print(f"[{turmas_htl:3d}] ID {turma_id:5d} ✅ HTL | {dados['curso']} | {dados['responsavel_1_nome']}")
            
            elif dados:
                pass
            else:
                erros += 1
            
            if processadas % 100 == 0:
                print(f"\n   Progresso: {processadas}/{len(ids_turmas_sistema)} | HTL: {turmas_htl} | Erros: {erros}\n")
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n" + "=" * 80)
    print(f"✅ COLETA FINALIZADA")
    print(f"=" * 80)
    print(f"📊 Turmas processadas: {processadas}")
    print(f"🎯 Turmas de Hortolândia: {turmas_htl}")
    print(f"⏱️ Tempo total: {tempo_total/60:.2f} minutos")
    print(f"=" * 80)
    
    # Backup local
    body = {
        "tipo": "dados_turmas",
        "timestamp": timestamp_execucao,
        "dados": resultado,
        "headers": [
            "ID_Turma", "Curso", "Descricao", "Comum", "Dia_Semana",
            "Data_Inicio", "Data_Encerramento", "Hora_Inicio", "Hora_Termino",
            "Responsavel_1_ID", "Responsavel_1_Nome",
            "Responsavel_2_ID", "Responsavel_2_Nome",
            "Destinado_ao", "Ativo",
            "Cadastrado_em", "Cadastrado_por", "Atualizado_em", "Atualizado_por",
            "Status_Coleta", "Data_Coleta"
        ],
        "resumo": {
            "total_processadas": processadas,
            "turmas_hortolandia": turmas_htl,
            "erros": erros,
            "total_instrutores_htl": len(ids_instrutores_htl),
            "tempo_minutos": round(tempo_total/60, 2)
        }
    }
    
    backup_file = f"backup_turmas_{timestamp_execucao.replace(':', '-')}.json"
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(body, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo: {backup_file}")
    
    # Enviar para Google Sheets
    print("\n📤 Enviando para Google Sheets...")
    
    try:
        resposta_post = requests.post(URL_APPS_SCRIPT_TURMAS, json=body, timeout=120)
        
        if resposta_post.status_code == 200:
            resposta_json = resposta_post.json()
            
            if resposta_json.get('status') == 'sucesso':
                planilha_info = resposta_json.get('planilha', {})
                
                print(f"\n✅ PLANILHA DE TURMAS CRIADA!")
                print(f"   Nome: {planilha_info.get('nome')}")
                print(f"   ID: {planilha_info.get('id')}")
                print(f"   URL: {planilha_info.get('url')}")
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")

# ==================== MAIN ====================
def main():
    print("\n" + "=" * 80)
    print("🧪 TESTE ISOLADO - MÓDULO 2: TURMAS")
    print("=" * 80)
    
    session, cookies = fazer_login_unico()
    
    if not session:
        print("\n❌ Falha no login. Encerrando.")
        return
    
    executar_teste_modulo2(session)
    
    print("\n" + "=" * 80)
    print("🎉 TESTE FINALIZADO!")
    print("=" * 80 + "\n")
    
if __name__ == "__main__":
    main()
