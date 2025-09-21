import os
import sys
import re
import requests
import time
import concurrent.futures
from playwright.sync_api import sync_playwright
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configurações
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwV-0AChSp5-JyBc3NysUQI0UlFJ7AycvE6CSRKWxldnJ8EBiaNHtj3oYx5jiiHxQbzOw/exec'

# BATCH 1: IDs 1 - 200.000
RANGE_INICIO = 1
RANGE_FIM = 200000
INSTANCIA_ID = "GHA_batch_1"
NUM_THREADS = 15
TIMEOUT_REQUEST = 10
PAUSA_MINIMA = 0.05

def extrair_dados(html_content, membro_id):
    try:
        if not html_content or len(html_content) < 1000:
            return None
        
        dados = {'id': membro_id}
        
        # Nome
        nome_match = re.search(r'name="nome"[^>]*value="([^"]*)"', html_content)
        if not nome_match:
            return None
        dados['nome'] = nome_match.group(1).strip()
        if not dados['nome']:
            return None
        
        # Igreja
        igreja_match = re.search(r'igreja_selecionada\s*\(\s*(\d+)\s*\)', html_content)
        dados['igreja_selecionada'] = igreja_match.group(1) if igreja_match else ''
        
        # Outros campos
        patterns = {
            'cargo_ministerio': r'id_cargo"[^>]*>.*?selected[^>]*>\s*([^<\n]+)',
            'nivel': r'id_nivel"[^>]*>.*?selected[^>]*>\s*([^<\n]+)',
            'instrumento': r'id_instrumento"[^>]*>.*?selected[^>]*>\s*([^<\n]+)',
            'tonalidade': r'id_tonalidade"[^>]*>.*?selected[^>]*>\s*([^<\n]+)'
        }
        
        for campo, pattern in patterns.items():
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            dados[campo] = match.group(1).strip() if match else ''
        
        return dados
    except:
        return None

class Coletor:
    def __init__(self, cookies, thread_id):
        self.thread_id = thread_id
        self.sucessos = 0
        self.falhas = 0
        
        self.session = requests.Session()
        self.session.cookies.update(cookies)
        
        retry_strategy = Retry(total=1, backoff_factor=0.3)
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Connection': 'keep-alive'
        }
    
    def coletar_batch(self, ids_batch):
        membros = []
        for membro_id in ids_batch:
            try:
                url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
                resp = self.session.get(url, headers=self.headers, timeout=TIMEOUT_REQUEST)
                
                if resp.status_code == 200 and len(resp.text) > 5000:
                    dados = extrair_dados(resp.text, membro_id)
                    if dados:
                        membros.append(dados)
                        self.sucessos += 1
                        if self.sucessos % 50 == 0:
                            print(f"T{self.thread_id}: {self.sucessos} coletados")
                    else:
                        self.falhas += 1
                else:
                    self.falhas += 1
                
                time.sleep(PAUSA_MINIMA)
            except:
                self.falhas += 1
        return membros

def login():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
        page = browser.new_page()
        page.goto(URL_INICIAL, timeout=30000)
        page.fill('input[name="login"]', EMAIL)
        page.fill('input[name="password"]', SENHA)
        page.click('button[type="submit"]')
        page.wait_for_selector("nav", timeout=20000)
        cookies = {cookie['name']: cookie['value'] for cookie in page.context.cookies()}
        browser.close()
        return cookies

def executar_coleta(cookies):
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    ids_per_thread = total_ids // NUM_THREADS
    
    thread_ranges = []
    for i in range(NUM_THREADS):
        inicio = RANGE_INICIO + (i * ids_per_thread)
        fim = inicio + ids_per_thread - 1
        if i == NUM_THREADS - 1:
            fim = RANGE_FIM
        thread_ranges.append(list(range(inicio, fim + 1)))
    
    todos_membros = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        coletores = [Coletor(cookies, i) for i in range(NUM_THREADS)]
        futures = [executor.submit(coletores[i].coletar_batch, thread_ranges[i]) for i in range(NUM_THREADS)]
        
        for i, future in enumerate(futures):
            try:
                membros = future.result(timeout=3600)
                todos_membros.extend(membros)
                print(f"Thread {i}: {len(membros)} membros coletados")
            except:
                print(f"Thread {i}: Erro/Timeout")
    
    return todos_membros

def enviar_dados(membros, tempo_total):
    if not membros:
        return
    
    relatorio = [["ID", "NOME", "IGREJA_SELECIONADA", "CARGO/MINISTERIO", "NÍVEL", "INSTRUMENTO", "TONALIDADE"]]
    for membro in membros:
        relatorio.append([
            str(membro.get('id', '')),
            membro.get('nome', ''),
            membro.get('igreja_selecionada', ''),
            membro.get('cargo_ministerio', ''),
            membro.get('nivel', ''),
            membro.get('instrumento', ''),
            membro.get('tonalidade', '')
        ])
    
    payload = {
        "tipo": f"membros_gha_{INSTANCIA_ID}",
        "relatorio_formatado": relatorio,
        "metadata": {
            "instancia": INSTANCIA_ID,
            "range_inicio": RANGE_INICIO,
            "range_fim": RANGE_FIM,
            "total_coletados": len(membros),
            "tempo_execucao_min": round(tempo_total/60, 2),
            "threads_utilizadas": NUM_THREADS,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC")
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
        if response.status_code == 200:
            print("Dados enviados com sucesso!")
        else:
            print(f"Erro no envio: {response.status_code}")
    except Exception as e:
        print(f"Erro no envio: {e}")

def main():
    print(f"BATCH 1: Coletando IDs {RANGE_INICIO:,} - {RANGE_FIM:,}")
    
    if not EMAIL or not SENHA:
        print("Credenciais não encontradas")
        sys.exit(1)
    
    tempo_inicio = time.time()
    
    print("Fazendo login...")
    cookies = login()
    if not cookies:
        sys.exit(1)
    
    print("Iniciando coleta...")
    membros = executar_coleta(cookies)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"Coletados: {len(membros):,} membros em {tempo_total/60:.1f} min")
    
    if membros:
        enviar_dados(membros, tempo_total)
        # Amostras
        for i, m in enumerate(membros[:3], 1):
            print(f"{i}. {m.get('nome', '')[:30]} - {m.get('instrumento', '')}")
    
    print(f"Batch 1 finalizado!")

if __name__ == "__main__":
    main()
