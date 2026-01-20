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

# Range menor para teste de velocidade
RANGE_INICIO = 600000
RANGE_FIM = 605000  # 5K para teste
INSTANCIA_ID = "GHA_speed_test"

# Configurações ULTRA AGRESSIVAS
NUM_THREADS = 30
TIMEOUT_REQUEST = 3
PAUSA_MINIMA = 0.01
BATCH_SIZE = 100

# Regex pré-compiladas
REGEX_NOME = re.compile(r'name="nome"[^>]*value="([^"]*)"')
REGEX_IGREJA = re.compile(r'igreja_selecionada\s*\(\s*(\d+)\s*\)')
REGEX_CARGO = re.compile(r'id_cargo"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_NIVEL = re.compile(r'id_nivel"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_INSTRUMENTO = re.compile(r'id_instrumento"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_TONALIDADE = re.compile(r'id_tonalidade"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)

def extrair_dados_turbo(html_content, membro_id):
    try:
        if len(html_content) < 1000:
            return None
        
        # Nome obrigatório - busca rápida
        nome_match = REGEX_NOME.search(html_content)
        if not nome_match:
            return None
        
        nome = nome_match.group(1).strip()
        if not nome:
            return None
        
        dados = {
            'id': membro_id,
            'nome': nome,
            'igreja_selecionada': '',
            'cargo_ministerio': '',
            'nivel': '',
            'instrumento': '',
            'tonalidade': ''
        }
        
        # Outros campos - busca rápida
        igreja_match = REGEX_IGREJA.search(html_content)
        if igreja_match:
            dados['igreja_selecionada'] = igreja_match.group(1)
        
        cargo_match = REGEX_CARGO.search(html_content)
        if cargo_match:
            dados['cargo_ministerio'] = cargo_match.group(1).strip()
        
        nivel_match = REGEX_NIVEL.search(html_content)
        if nivel_match:
            dados['nivel'] = nivel_match.group(1).strip()
        
        instrumento_match = REGEX_INSTRUMENTO.search(html_content)
        if instrumento_match:
            dados['instrumento'] = instrumento_match.group(1).strip()
        
        tonalidade_match = REGEX_TONALIDADE.search(html_content)
        if tonalidade_match:
            dados['tonalidade'] = tonalidade_match.group(1).strip()
        
        return dados
    except:
        return None

class ColetorTurbo:
    def __init__(self, cookies, thread_id):
        self.thread_id = thread_id
        self.sucessos = 0
        self.falhas = 0
        self.timeouts = 0
        self.start_time = time.time()
        
        # Sessão ultra otimizada
        self.session = requests.Session()
        self.session.cookies.update(cookies)
        
        # Adapter agressivo - SEM RETRY
        adapter = HTTPAdapter(
            pool_connections=40,
            pool_maxsize=40,
            max_retries=0
        )
        self.session.mount("https://", adapter)
        
        # Headers mínimos
        self.headers = {
            'User-Agent': 'Mozilla/5.0',
            'Connection': 'keep-alive'
        }
    
    def coletar_sem_pausa(self, ids_lista):
        membros = []
        
        for i, membro_id in enumerate(ids_lista):
            try:
                url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
                resp = self.session.get(url, headers=self.headers, timeout=TIMEOUT_REQUEST)
                
                if resp.status_code == 200 and len(resp.text) > 3000:
                    dados = extrair_dados_turbo(resp.text, membro_id)
                    if dados:
                        membros.append(dados)
                        self.sucessos += 1
                        
                        # Log apenas a cada 250 sucessos
                        if self.sucessos % 250 == 0:
                            elapsed = time.time() - self.start_time
                            rate = self.sucessos / elapsed * 60
                            print(f"T{self.thread_id}: {self.sucessos} ({rate:.0f}/min)")
                    else:
                        self.falhas += 1
                else:
                    self.falhas += 1
                
                # Pausa MUITO esporádica
                if i % 200 == 0 and i > 0:
                    time.sleep(PAUSA_MINIMA)
                
            except requests.exceptions.Timeout:
                self.timeouts += 1
            except:
                self.falhas += 1
        
        return membros

def fazer_login_rapido():
    print("Login rápido...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-web-security',
                '--disable-background-timer-throttling',
                '--disable-renderer-backgrounding'
            ]
        )
        
        page = browser.new_page()
        page.goto(URL_INICIAL, timeout=20000)
        page.fill('input[name="login"]', EMAIL)
        page.fill('input[name="password"]', SENHA)
        page.click('button[type="submit"]')
        page.wait_for_selector("nav", timeout=15000)
        
        cookies = {cookie['name']: cookie['value'] for cookie in page.context.cookies()}
        browser.close()
        return cookies

def executar_teste_velocidade(cookies):
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    chunk_size = total_ids // NUM_THREADS
    
    chunks = []
    for i in range(NUM_THREADS):
        start = RANGE_INICIO + (i * chunk_size)
        end = start + chunk_size - 1
        if i == NUM_THREADS - 1:
            end = RANGE_FIM
        chunks.append(list(range(start, end + 1)))
    
    print(f"Dividindo {total_ids} IDs em {NUM_THREADS} threads")
    
    todos_membros = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        coletores = [ColetorTurbo(cookies, i) for i in range(NUM_THREADS)]
        futures = [executor.submit(coletores[i].coletar_sem_pausa, chunks[i]) for i in range(NUM_THREADS)]
        
        for i, future in enumerate(futures):
            try:
                membros = future.result(timeout=1800)  # 30 min
                todos_membros.extend(membros)
                
                coletor = coletores[i]
                elapsed = time.time() - coletor.start_time
                rate = coletor.sucessos / elapsed * 60 if elapsed > 0 else 0
                
                print(f"T{i} FINAL: {len(membros)} membros | "
                      f"✅{coletor.sucessos} ❌{coletor.falhas} ⏱️{coletor.timeouts} | "
                      f"{rate:.0f}/min")
                
            except concurrent.futures.TimeoutError:
                print(f"T{i}: TIMEOUT")
            except Exception as e:
                print(f"T{i}: ERRO - {str(e)[:20]}")
    
    return todos_membros

def enviar_resultado(membros, tempo_total):
    if not membros:
        print("Nada para enviar")
        return
    
    relatorio = [["ID", "NOME", "IGREJA_SELECIONADA", "CARGO/MINISTERIO", "NÍVEL", "INSTRUMENTO", "TONALIDADE"]]
    
    for membro in membros:
        relatorio.append([
            str(membro['id']),
            membro['nome'][:80],
            membro['igreja_selecionada'],
            membro['cargo_ministerio'][:40],
            membro['nivel'][:25],
            membro['instrumento'][:25],
            membro['tonalidade'][:15]
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
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "versao": "TURBO_TEST"
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
        if response.status_code == 200:
            print("DADOS ENVIADOS!")
        else:
            print(f"Erro envio: {response.status_code}")
    except Exception as e:
        print(f"Erro: {e}")

def main():
    print("TESTE DE VELOCIDADE TURBO")
    print("=" * 40)
    print(f"Range: {RANGE_INICIO:,} - {RANGE_FIM:,} ({RANGE_FIM-RANGE_INICIO+1:,} IDs)")
    print(f"Config: {NUM_THREADS} threads, timeout {TIMEOUT_REQUEST}s")
    
    if not EMAIL or not SENHA:
        print("ERRO: Sem credenciais")
        sys.exit(1)
    
    tempo_inicio = time.time()
    
    cookies = fazer_login_rapido()
    if not cookies:
        print("ERRO: Login falhou")
        sys.exit(1)
    
    print(f"Cookies: {len(cookies)} obtidos")
    print("\nINICIANDO TESTE...")
    
    membros = executar_teste_velocidade(cookies)
    
    tempo_total = time.time() - tempo_inicio
    
    print("\n" + "=" * 40)
    print("RESULTADO DO TESTE")
    print("=" * 40)
    print(f"Coletados: {len(membros):,} membros")
    print(f"Tempo: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    
    if membros:
        rate_min = len(membros) / (tempo_total/60)
        rate_hour = rate_min * 60
        
        print(f"VELOCIDADE: {rate_min:.0f} membros/min")
        print(f"          : {rate_hour:.0f} membros/hora")
        
        # Análise da velocidade
        if rate_min >= 1000:
            tempo_1m = 1000000 / rate_hour
            print(f"EXCELENTE! 1M em {tempo_1m:.1f} horas")
        elif rate_min >= 500:
            tempo_1m = 1000000 / rate_hour
            print(f"BOA! 1M em {tempo_1m:.1f} horas")
        elif rate_min >= 200:
            print("RAZOÁVEL - pode melhorar")
        else:
            print("MUITO LENTA - precisa otimizar")
        
        # Amostras
        print(f"\nAMOSTRAS:")
        for i, m in enumerate(membros[:3], 1):
            print(f"{i}. ID {m['id']} - {m['nome'][:25]} - {m['instrumento'][:15]}")
        
        enviar_resultado(membros, tempo_total)
    else:
        print("NENHUM MEMBRO COLETADO!")
    
    print(f"\nTESTE FINALIZADO")

if __name__ == "__main__":
    main()
