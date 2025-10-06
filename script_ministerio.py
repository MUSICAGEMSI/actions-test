from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
from bs4 import BeautifulSoup
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwHlf2VUjfwX7KcHGKgvf0v2FlXZ7Y53ubkfcIPxihSb3VVUzbyzlBr5Fyx0OHrxwBx/exec'

# Configurações de alta performance
MAX_WORKERS = 20  # Threads paralelas
BATCH_SIZE = 500  # Envia a cada 500
TIMEOUT = 10      # Timeout por requisição

# Locks para thread-safety
print_lock = Lock()
resultado_lock = Lock()

# Contadores globais
stats = {
    'processados': 0,
    'sucesso': 0,
    'erros': 0,
    'resultado': []
}

def criar_sessao_otimizada():
    """Cria sessão HTTP otimizada com retry e pool de conexões"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS * 2
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def carregar_ids_do_apps_script():
    """Busca IDs via Apps Script"""
    print("Carregando IDs...", end=' ')
    
    try:
        url = f"{URL_APPS_SCRIPT}?acao=obter_ids"
        response = requests.get(url, timeout=60)
        dados = response.json()
        
        if dados['status'] == 'sucesso':
            print(f"OK - {len(dados['ids'])} IDs carregados")
            return dados['ids']
        
        print(f"ERRO - {dados.get('mensagem')}")
        return []
        
    except Exception as e:
        print(f"ERRO - {e}")
        return []

def extrair_dados_ministro(html_content, pessoa_id):
    """Extrai dados do HTML"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        form = soup.find('form', id='ministerio')
        
        if not form:
            return None
        
        dados = {'ID_Ministro': pessoa_id, 'Nome': '', 'Email': '', 'ID_Localidade': '', 
                 'Comum': '', 'Ministerio': '', 'Telefone_Celular': '', 'Telefone_Fixo': '',
                 'Cadastrado_em': '', 'Cadastrado_por': ''}
        
        # Nome
        nome_input = form.find('input', {'name': 'nome'})
        if nome_input and nome_input.get('value'):
            dados['Nome'] = nome_input.get('value', '').strip()
        
        if not dados['Nome']:
            return None
        
        # Email
        email_input = form.find('input', {'name': 'email'})
        if email_input:
            dados['Email'] = email_input.get('value', '').strip()
        
        # Comum
        comum_select = form.find('select', {'name': 'id_igreja'})
        if comum_select:
            comum_option = comum_select.find('option', selected=True)
            if comum_option and comum_option.get('value'):
                dados['ID_Localidade'] = comum_option.get('value', '').strip()
                dados['Comum'] = comum_option.get_text(strip=True)
        
        # Ministério
        cargo_select = form.find('select', {'id': 'id_cargo'})
        if cargo_select:
            cargo_option = cargo_select.find('option', selected=True)
            if cargo_option and cargo_option.get('value'):
                dados['Ministerio'] = cargo_option.get_text(strip=True)
        
        # Telefones
        tel = form.find('input', {'id': 'telefone'})
        if tel:
            dados['Telefone_Celular'] = tel.get('value', '').strip()
        
        tel2 = form.find('input', {'id': 'telefone2'})
        if tel2:
            dados['Telefone_Fixo'] = tel2.get('value', '').strip()
        
        # Histórico
        historico = soup.find('div', id='collapseOne')
        if historico:
            for p in historico.find_all('p'):
                texto = p.get_text(strip=True)
                if 'Cadastrado em:' in texto:
                    texto_limpo = texto.replace('Cadastrado em:', '').strip()
                    partes = texto_limpo.split('por:')
                    if len(partes) >= 2:
                        dados['Cadastrado_em'] = partes[0].strip()
                        dados['Cadastrado_por'] = partes[1].strip()
        
        return dados
        
    except:
        return None

def processar_id(pessoa_id, session, cookies_dict):
    """Processa um ID (função executada em thread)"""
    url = f"https://musical.congregacao.org.br/ministros/editar/{pessoa_id}"
    
    try:
        # Atualiza cookies na sessão
        session.cookies.update(cookies_dict)
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        resp = session.get(url, headers=headers, timeout=TIMEOUT)
        
        dados = extrair_dados_ministro(resp.text, pessoa_id)
        
        if dados:
            linha = [
                dados['ID_Ministro'], dados['Nome'], dados['Email'],
                dados['ID_Localidade'], dados['Comum'], dados['Ministerio'],
                dados['Telefone_Celular'], dados['Telefone_Fixo'],
                dados['Cadastrado_em'], dados['Cadastrado_por'],
                'Coletado', time.strftime('%d/%m/%Y %H:%M:%S')
            ]
            return True, linha, dados['Nome'][:30]
        else:
            linha = [pessoa_id, '', '', '', '', '', '', '', '', '', 
                    'Não encontrado', time.strftime('%d/%m/%Y %H:%M:%S')]
            return False, linha, "Não encontrado"
    
    except Exception as e:
        linha = [pessoa_id, '', '', '', '', '', '', '', '', '', 
                f'Erro: {str(e)[:30]}', time.strftime('%d/%m/%Y %H:%M:%S')]
        return False, linha, str(e)[:30]

def enviar_para_sheets(dados, metadata):
    """Envia dados para Google Sheets"""
    cabecalho = ["ID_Ministro", "Nome", "Email", "ID_Localidade", "Comum",
                 "Ministerio", "Telefone_Celular", "Telefone_Fixo",
                 "Cadastrado_em", "Cadastrado_por", "Status_Coleta", "Data_Coleta"]
    
    payload = {
        "tipo": "ministerio",
        "relatorio_formatado": [cabecalho] + dados,
        "metadata": metadata
    }
    
    try:
        resposta = requests.post(URL_APPS_SCRIPT, json=payload, timeout=180)
        return resposta.status_code == 200
    except:
        return False

def main():
    tempo_inicio = time.time()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("=" * 80)
    print("COLETOR PARALELO - MÁXIMA VELOCIDADE")
    print("=" * 80)
    
    # Carregar IDs
    ids_ministros = carregar_ids_do_apps_script()
    
    if not ids_ministros:
        print("\nERRO: Nenhum ID carregado")
        return
    
    total_ids = len(ids_ministros)
    print(f"\nTotal: {total_ids} IDs | Workers: {MAX_WORKERS} threads | Batch: {BATCH_SIZE}")
    print(f"Estimativa: {total_ids/(MAX_WORKERS*3):.0f} segundos (~{total_ids/(MAX_WORKERS*3)/60:.1f} min)\n")
    
    # Login e obter cookies
    print("Autenticando...", end=' ')
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        try:
            pagina.goto(URL_INICIAL, timeout=30000)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=15000)
            
            cookies = pagina.context.cookies()
            cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}
            print("OK\n")
            
        except:
            print("FALHA")
            navegador.close()
            return
        
        navegador.close()
    
    # Processamento paralelo
    print("=" * 80)
    print("INICIANDO COLETA PARALELA")
    print("=" * 80)
    print()
    
    batch_atual = []
    ultimo_relatorio = time.time()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Cria sessões para cada worker
        sessoes = [criar_sessao_otimizada() for _ in range(MAX_WORKERS)]
        
        # Submete todas as tarefas
        futures = {
            executor.submit(processar_id, pessoa_id, sessoes[i % MAX_WORKERS], cookies_dict): pessoa_id 
            for i, pessoa_id in enumerate(ids_ministros)
        }
        
        # Processa resultados conforme completam
        for future in as_completed(futures):
            pessoa_id = futures[future]
            
            try:
                sucesso, linha, info = future.result()
                
                with resultado_lock:
                    stats['processados'] += 1
                    batch_atual.append(linha)
                    
                    if sucesso:
                        stats['sucesso'] += 1
                    else:
                        stats['erros'] += 1
                    
                    # Print progresso
                    if stats['processados'] % 50 == 0 or time.time() - ultimo_relatorio > 5:
                        tempo_decorrido = time.time() - tempo_inicio
                        taxa = stats['processados'] / tempo_decorrido
                        restantes = total_ids - stats['processados']
                        eta = restantes / taxa if taxa > 0 else 0
                        
                        with print_lock:
                            print(f"[{stats['processados']}/{total_ids}] "
                                  f"OK: {stats['sucesso']} | ERR: {stats['erros']} | "
                                  f"{taxa:.1f} IDs/s | ETA: {eta:.0f}s")
                        
                        ultimo_relatorio = time.time()
                    
                    # Envia batch
                    if len(batch_atual) >= BATCH_SIZE:
                        with print_lock:
                            print(f"\n>>> Enviando batch de {len(batch_atual)} registros...", end=' ')
                        
                        metadata = {
                            "total_processados": stats['processados'],
                            "sucesso": stats['sucesso'],
                            "erros": stats['erros'],
                            "tempo_minutos": round((time.time() - tempo_inicio) / 60, 2),
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        
                        if enviar_para_sheets(batch_atual, metadata):
                            with print_lock:
                                print("OK\n")
                            stats['resultado'].extend(batch_atual)
                            batch_atual = []
                        else:
                            with print_lock:
                                print("FALHA (mantido em memória)\n")
                
            except Exception as e:
                with resultado_lock:
                    stats['processados'] += 1
                    stats['erros'] += 1
    
    # Envio final
    if batch_atual:
        print(f"\n>>> Enviando batch final de {len(batch_atual)} registros...", end=' ')
        
        tempo_total = time.time() - tempo_inicio
        metadata = {
            "total_processados": total_ids,
            "sucesso": stats['sucesso'],
            "erros": stats['erros'],
            "tempo_minutos": round(tempo_total / 60, 2),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        if enviar_para_sheets(batch_atual, metadata):
            print("OK")
            stats['resultado'].extend(batch_atual)
        else:
            print("FALHA")
    
    # Backup local
    backup_file = f"backup_ministerio_{timestamp}.json"
    backup_data = {
        'total': total_ids,
        'sucesso': stats['sucesso'],
        'erros': stats['erros'],
        'tempo_minutos': round((time.time() - tempo_inicio) / 60, 2),
        'dados': stats['resultado'] + batch_atual
    }
    
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(backup_data, f, ensure_ascii=False, indent=2)
    
    # Resumo
    tempo_total = time.time() - tempo_inicio
    
    print("\n" + "=" * 80)
    print("FINALIZADO")
    print("=" * 80)
    print(f"Total: {total_ids} IDs")
    print(f"Sucesso: {stats['sucesso']} ({stats['sucesso']/total_ids*100:.1f}%)")
    print(f"Erros: {stats['erros']} ({stats['erros']/total_ids*100:.1f}%)")
    print(f"Tempo: {tempo_total:.1f}s ({tempo_total/60:.2f} min)")
    print(f"Taxa média: {total_ids/tempo_total:.1f} IDs/s")
    print(f"Backup: {backup_file}")
    print("=" * 80)

if __name__ == "__main__":
    main()
