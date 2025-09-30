# script_turmas_detalhado.py
from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time
import re
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_TURMA_BASE = "https://musical.congregacao.org.br/turmas/editar"
URL_MATRICULADOS_BASE = "https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbyw2E0QH0ucHRdCMNOY_La7r4ElK6xcf0OWlnQGa9w7yCcg82mG_bJV_5fxbhuhbfuY/exec'

RANGE_INICIO = 43000
RANGE_FIM = 53000
MAX_VAZIOS_SEQUENCIAIS = 500
MAX_WORKERS = 10
TIMEOUT_REQUEST = 10

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

# Controle de progresso thread-safe
lock = threading.Lock()
progresso = {
    'processados': 0,
    'validos': 0,
    'invalidos': 0,
    'erros': 0
}

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para usar em requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def criar_sessao_com_cookies(cookies_dict):
    """Cria sessão requests com cookies do Playwright"""
    session = requests.Session()
    session.cookies.update(cookies_dict)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        'Referer': 'https://musical.congregacao.org.br/painel'
    })
    return session

def extrair_dados_turma(session, turma_id):
    """
    Extrai dados completos de uma turma específica
    Retorna dict com dados ou None se turma não existir
    """
    try:
        url = f"{URL_TURMA_BASE}/{turma_id}"
        resp = session.get(url, timeout=TIMEOUT_REQUEST)
        
        # Verificar se turma existe
        if resp.status_code != 200 or 'Turmas' not in resp.text:
            return None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Verificar se é página de edição válida
        form = soup.find('form', {'id': 'turmas'})
        if not form:
            return None
        
        dados = {
            'id': turma_id,
            'curso': '',
            'descricao': '',
            'dia_semana': '',
            'data_inicio': '',
            'data_fim': '',
            'hora_inicio': '',
            'hora_fim': '',
            'matriculados': 0
        }
        
        # Extrair CURSO
        curso_select = soup.find('select', {'name': 'id_curso'})
        if curso_select:
            curso_option = curso_select.find('option', {'selected': True})
            if curso_option:
                dados['curso'] = curso_option.text.strip()
        
        # Extrair DESCRIÇÃO
        descricao_input = soup.find('input', {'name': 'descricao'})
        if descricao_input and descricao_input.get('value'):
            dados['descricao'] = descricao_input['value'].strip()
        
        # Extrair DIA DA SEMANA
        dia_select = soup.find('select', {'name': 'dia_semana'})
        if dia_select:
            dia_option = dia_select.find('option', {'selected': True})
            if dia_option:
                dados['dia_semana'] = dia_option.text.strip()
        
        # Extrair DATA DE INÍCIO
        dt_inicio_input = soup.find('input', {'name': 'dt_inicio'})
        if dt_inicio_input and dt_inicio_input.get('value'):
            dados['data_inicio'] = dt_inicio_input['value'].strip()
        
        # Extrair DATA DE FIM
        dt_fim_input = soup.find('input', {'name': 'dt_fim'})
        if dt_fim_input and dt_fim_input.get('value'):
            dados['data_fim'] = dt_fim_input['value'].strip()
        
        # Extrair HORA DE INÍCIO
        hr_inicio_input = soup.find('input', {'name': 'hr_inicio'})
        if hr_inicio_input and hr_inicio_input.get('value'):
            hora_raw = hr_inicio_input['value'].strip()
            dados['hora_inicio'] = hora_raw[:5] if len(hora_raw) >= 5 else hora_raw
        
        # Extrair HORA DE FIM
        hr_fim_input = soup.find('input', {'name': 'hr_fim'})
        if hr_fim_input and hr_fim_input.get('value'):
            hora_raw = hr_fim_input['value'].strip()
            dados['hora_fim'] = hora_raw[:5] if len(hora_raw) >= 5 else hora_raw
        
        # Obter número de MATRICULADOS
        dados['matriculados'] = obter_matriculados(session, turma_id)
        
        return dados
        
    except requests.Timeout:
        return None
    except Exception as e:
        return None

def obter_matriculados(session, turma_id):
    """Conta o número real de alunos matriculados em uma turma"""
    try:
        url = f"{URL_MATRICULADOS_BASE}/{turma_id}"
        resp = session.get(url, timeout=TIMEOUT_REQUEST)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Método 1: Info do DataTable
            info_div = soup.find('div', {'class': 'dataTables_info'})
            if info_div:
                match = re.search(r'de um total de (\d+) registros', info_div.text)
                if match:
                    return int(match.group(1))
            
            # Método 2: Contar linhas do tbody
            tbody = soup.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                valid_rows = [r for r in rows if len(r.find_all('td')) >= 3]
                return len(valid_rows)
            
            # Método 3: Contar botões "Desmatricular"
            desmatricular_count = resp.text.count('Desmatricular')
            if desmatricular_count > 0:
                return desmatricular_count
        
        return 0
        
    except Exception:
        return 0

def processar_lote(session, ids):
    """Processa um lote de IDs e retorna resultados"""
    resultados = []
    
    for turma_id in ids:
        dados = extrair_dados_turma(session, turma_id)
        
        with lock:
            progresso['processados'] += 1
            
            if dados:
                progresso['validos'] += 1
                resultados.append(dados)
                
                if progresso['validos'] % 10 == 0:
                    print(f"📊 [{progresso['processados']}/{RANGE_FIM}] Encontradas: {progresso['validos']} | "
                          f"Inválidas: {progresso['invalidos']} | Última: ID {turma_id}")
            else:
                progresso['invalidos'] += 1
    
    return resultados

def main():
    tempo_inicio = time.time()
    print("=" * 70)
    print("🎵 COLETA DETALHADA DE TURMAS - Sistema Musical Congregação")
    print("=" * 70)
    print(f"📍 Range: {RANGE_INICIO} a {RANGE_FIM}")
    print(f"⚡ Workers paralelos: {MAX_WORKERS}")
    print(f"🛑 Parada automática: {MAX_VAZIOS_SEQUENCIAIS} IDs vazios sequenciais")
    print("-" * 70)
    
    # Login via Playwright (igual ao script original)
    with sync_playwright() as p:
        print("🌐 Abrindo navegador...")
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
        print("🔐 Realizando login...")
        pagina.goto(URL_INICIAL)
        
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
        
        # Extrair cookies para usar com requests
        print("🍪 Extraindo cookies da sessão...")
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
    
    print(f"✅ Cookies obtidos: {len(cookies_dict)} cookies")
    print("-" * 70)
    
    # Preparar lotes de IDs
    todos_ids = list(range(RANGE_INICIO, RANGE_FIM + 1))
    tamanho_lote = 100
    lotes = [todos_ids[i:i + tamanho_lote] for i in range(0, len(todos_ids), tamanho_lote)]
    
    print(f"📦 Total de lotes: {len(lotes)} (de {tamanho_lote} IDs cada)")
    print("🚀 Iniciando coleta paralela...\n")
    
    todos_resultados = []
    vazios_sequenciais = 0
    ultimo_valido_id = 0
    
    # Processar em paralelo
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Criar sessões para cada worker com os cookies
        sessoes = [criar_sessao_com_cookies(cookies_dict) for _ in range(MAX_WORKERS)]
        
        # Submeter tarefas
        futures = {}
        sessao_idx = 0
        
        for lote in lotes:
            sessao = sessoes[sessao_idx % len(sessoes)]
            future = executor.submit(processar_lote, sessao, lote)
            futures[future] = lote[0]
            sessao_idx += 1
        
        # Coletar resultados
        for future in as_completed(futures):
            try:
                resultados_lote = future.result()
                
                if resultados_lote:
                    todos_resultados.extend(resultados_lote)
                    ultimo_valido_id = max([r['id'] for r in resultados_lote])
                    vazios_sequenciais = 0
                else:
                    primeiro_id_lote = futures[future]
                    if primeiro_id_lote > ultimo_valido_id:
                        vazios_sequenciais += tamanho_lote
                        
                        if vazios_sequenciais >= MAX_VAZIOS_SEQUENCIAIS:
                            print(f"\n⏹️  {MAX_VAZIOS_SEQUENCIAIS} IDs vazios sequenciais detectados.")
                            print("   Encerrando coleta (fim dos dados)...")
                            executor.shutdown(wait=False, cancel_futures=True)
                            break
                
            except Exception as e:
                with lock:
                    progresso['erros'] += 1
        
        for sessao in sessoes:
            sessao.close()
    
    # Resumo final
    print("\n" + "=" * 70)
    print("📈 RESUMO DA COLETA")
    print("=" * 70)
    print(f"✅ Turmas válidas encontradas: {progresso['validos']}")
    print(f"❌ IDs inválidos/vazios: {progresso['invalidos']}")
    print(f"🔢 Total processados: {progresso['processados']}")
    print(f"⚠️  Erros: {progresso['erros']}")
    print(f"⏱️  Tempo total: {time.time() - tempo_inicio:.2f}s")
    print("=" * 70)
    
    if not todos_resultados:
        print("\n❌ Nenhuma turma encontrada!")
        return
    
    # Converter para formato de envio
    dados_envio = []
    for r in todos_resultados:
        dados_envio.append([
            r['id'],
            r['curso'],
            r['descricao'],
            r['dia_semana'],
            r['data_inicio'],
            r['data_fim'],
            r['hora_inicio'],
            r['hora_fim'],
            r['matriculados']
        ])
    
    body = {
        "tipo": "turmas_detalhado",
        "dados": dados_envio,
        "headers": [
            "ID_Turma", "Curso", "Descrição", "Dia_Semana", 
            "Data_Início", "Data_Fim", "Hora_Início", "Hora_Fim", "Matriculados"
        ],
        "resumo": {
            "total_turmas": len(todos_resultados),
            "ids_processados": progresso['processados'],
            "tempo_segundos": round(time.time() - tempo_inicio, 2)
        }
    }
    
    # Enviar para Apps Script
    print("\n📤 Enviando dados para Google Apps Script...")
    try:
        resp = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
        print(f"   Status: {resp.status_code}")
        print(f"   Resposta: {resp.text[:200]}")
        print("\n✅ Dados enviados com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
        print("\n💾 Salvando backup local...")
        import json
        with open('turmas_backup.json', 'w', encoding='utf-8') as f:
            json.dump(body, f, ensure_ascii=False, indent=2)
        print("✅ Backup salvo: turmas_backup.json")

if __name__ == "__main__":
    main()
