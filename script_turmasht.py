# script_turmas_detalhado.py
from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

import os
import requests
import time
import re
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_LOGIN = "https://musical.congregacao.org.br/login/logar"
URL_TURMA_BASE = "https://musical.congregacao.org.br/turmas/editar"
URL_MATRICULADOS_BASE = "https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbyw2E0QH0ucHRdCMNOY_La7r4ElK6xcf0OWlnQGa9w7yCcg82mG_bJV_5fxbhuhbfuY/exec'

RANGE_INICIO = 1
RANGE_FIM = 60000
MAX_VAZIOS_SEQUENCIAIS = 500  # Para após 500 IDs vazios seguidos
MAX_WORKERS = 10  # Threads paralelas
TIMEOUT_REQUEST = 10

if not EMAIL or not SENHA:
    print("Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

# Controle de progresso thread-safe
lock = threading.Lock()
progresso = {
    'processados': 0,
    'validos': 0,
    'invalidos': 0,
    'erros': 0
}

def criar_sessao_autenticada():
    """Cria uma sessão requests autenticada"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    })
    
    try:
        print(f"  Tentando login com usuário: {EMAIL[:3]}***")
        
        # Primeiro: acessar a página de login para obter cookies/tokens
        print("  [1/3] Acessando página inicial...")
        resp_inicial = session.get("https://musical.congregacao.org.br/", timeout=15)
        print(f"      Status: {resp_inicial.status_code}")
        
        # Fazer login
        print("  [2/3] Enviando credenciais...")
        payload = {
            'login': EMAIL,
            'password': SENHA
        }
        
        resp = session.post(URL_LOGIN, data=payload, timeout=15, allow_redirects=True)
        
        print(f"      Status: {resp.status_code}")
        print(f"      URL final: {resp.url}")
        print(f"      Cookies: {len(session.cookies)}")
        
        # Verificar se login foi bem sucedido
        print("  [3/3] Verificando autenticação...")
        
        # Tentar acessar o painel
        resp_painel = session.get("https://musical.congregacao.org.br/painel", timeout=15)
        
        if resp_painel.status_code == 200 and 'painel' in resp_painel.url.lower():
            print("  ✓ Autenticação bem-sucedida!")
            return session
        else:
            print(f"  ✗ Falha: redirecionado para {resp_painel.url}")
            print(f"  HTML contém 'login': {'login' in resp_painel.text.lower()}")
            
            # Debug adicional
            if 'incorreto' in resp_painel.text.lower() or 'inválid' in resp_painel.text.lower():
                print("  MOTIVO: Credenciais incorretas")
            
            return None
            
    except Exception as e:
        print(f"  ✗ Erro ao criar sessão: {e}")
        import traceback
        traceback.print_exc()
        return None

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
            # Limpar formato HH:MM:SS para HH:MM
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
        print(f"Erro ao processar turma {turma_id}: {e}")
        return None

def obter_matriculados(session, turma_id):
    """
    Conta o número real de alunos matriculados em uma turma
    """
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
                
                # Log a cada turma válida encontrada
                if progresso['validos'] % 10 == 0:
                    print(f"[{progresso['processados']}/{RANGE_FIM}] Encontradas: {progresso['validos']} | "
                          f"Inválidas: {progresso['invalidos']} | Última: ID {turma_id}")
            else:
                progresso['invalidos'] += 1
    
    return resultados

def main():
    tempo_inicio = time.time()
    print("Iniciando coleta detalhada de turmas...")
    print(f"Range: {RANGE_INICIO} a {RANGE_FIM}")
    print(f"Workers: {MAX_WORKERS}")
    print("-" * 60)
    
    # ÚNICO LOGIN - Criar sessão autenticada que será compartilhada
    print("Realizando login único...")
    sessao_master = criar_sessao_autenticada()
    if not sessao_master:
        print("Falha na
    
    # Preparar lotes de IDs
    todos_ids = list(range(RANGE_INICIO, RANGE_FIM + 1))
    tamanho_lote = 50  # Cada worker processa 50 IDs por vez
    lotes = [todos_ids[i:i + tamanho_lote] for i in range(0, len(todos_ids), tamanho_lote)]
    
    print(f"Total de lotes: {len(lotes)}")
    print("-" * 60)
    
    todos_resultados = []
    vazios_sequenciais = 0
    ultimo_valido_id = 0
    
    # Processar em paralelo
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Criar sessões para cada worker
        sessoes = []
        for _ in range(MAX_WORKERS):
            s = criar_sessao_autenticada()
            if s:
                sessoes.append(s)
        
        if len(sessoes) < MAX_WORKERS:
            print(f"Aviso: apenas {len(sessoes)} sessões criadas de {MAX_WORKERS}")
        
        # Submeter tarefas
        futures = {}
        sessao_idx = 0
        
        for lote in lotes:
            sessao = sessoes[sessao_idx % len(sessoes)]
            future = executor.submit(processar_lote, sessao, lote)
            futures[future] = lote[0]  # Guarda primeiro ID do lote
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
                    # Verificar se devemos parar (muitos vazios seguidos)
                    primeiro_id_lote = futures[future]
                    if primeiro_id_lote > ultimo_valido_id:
                        vazios_sequenciais += len(lote)
                        
                        if vazios_sequenciais >= MAX_VAZIOS_SEQUENCIAIS:
                            print(f"\n{MAX_VAZIOS_SEQUENCIAIS} IDs vazios sequenciais detectados.")
                            print("Encerrando coleta prematuramente...")
                            executor.shutdown(wait=False, cancel_futures=True)
                            break
                
            except Exception as e:
                print(f"Erro no processamento do lote: {e}")
                with lock:
                    progresso['erros'] += 1
        
        # Fechar sessões
        for sessao in sessoes:
            sessao.close()
    
    # Preparar dados para envio
    print("\n" + "=" * 60)
    print("COLETA FINALIZADA")
    print("=" * 60)
    print(f"Total processados: {progresso['processados']}")
    print(f"Turmas válidas: {progresso['validos']}")
    print(f"IDs inválidos: {progresso['invalidos']}")
    print(f"Erros: {progresso['erros']}")
    print(f"Tempo decorrido: {time.time() - tempo_inicio:.2f}s")
    
    if not todos_resultados:
        print("\nNenhuma turma encontrada!")
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
    print("\nEnviando dados para Google Apps Script...")
    try:
        resp = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
        print(f"Status: {resp.status_code}")
        print(f"Resposta: {resp.text[:200]}")
        print("\nDados enviados com sucesso!")
    except Exception as e:
        print(f"Erro ao enviar: {e}")
        print("\nSalvando dados localmente como backup...")
        import json
        with open('turmas_backup.json', 'w', encoding='utf-8') as f:
            json.dump(body, f, ensure_ascii=False, indent=2)
        print("Backup salvo em: turmas_backup.json")

if __name__ == "__main__":
    main()
