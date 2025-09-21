import os
import sys
import re
import requests
import time
import concurrent.futures
from playwright.sync_api import sync_playwright
import asyncio
import aiohttp
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configurações AGRESSIVAS
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwV-0AChSp5-JyBc3NysUQI0UlFJ7AycvE6CSRKWxldnJ8EBiaNHtj3oYx5jiiHxQbzOw/exec'

# Range menor para teste de velocidade
RANGE_INICIO = 600000
RANGE_FIM = 610000  # Apenas 10k para teste
INSTANCIA_ID = "GHA_ultra_test"

# Configurações ULTRA AGRESSIVAS
NUM_THREADS = 30  # Aumentado
TIMEOUT_REQUEST = 5  # Reduzido drasticamente
PAUSA_MINIMA = 0.01  # Quase zero
BATCH_SIZE = 100  # Batches maiores

# Regex pré-compiladas para velocidade
REGEX_NOME = re.compile(r'name="nome"[^>]*value="([^"]*)"')
REGEX_IGREJA = re.compile(r'igreja_selecionada\s*\(\s*(\d+)\s*\)')
REGEX_CARGO = re.compile(r'id_cargo"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_NIVEL = re.compile(r'id_nivel"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_INSTRUMENTO = re.compile(r'id_instrumento"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_TONALIDADE = re.compile(r'id_tonalidade"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)

def extrair_dados_otimizado(html_content, membro_id):
    """Extração com regex pré-compiladas"""
    try:
        if len(html_content) < 1000:
            return None
        
        # Nome obrigatório
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
        
        # Outros campos - usar regex pré-compiladas
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

class ColetorUltraRapido:
    def __init__(self, cookies, thread_id):
        self.thread_id = thread_id
        self.sucessos = 0
        self.falhas = 0
        self.start_time = time.time()
        
        # Sessão ultra otimizada
        self.session = requests.Session()
        self.session.cookies.update(cookies)
        
        # Sem retry para velocidade máxima
        adapter = HTTPAdapter(
            pool_connections=30,
            pool_maxsize=30,
            max_retries=0  # SEM RETRY
        )
        self.session.mount("https://", adapter)
        
        # Headers mínimos
        self.headers = {
            'User-Agent': 'Mozilla/5.0',
            'Connection': 'keep-alive'
        }
    
    def coletar_ultra_rapido(self, ids_lista):
        membros = []
        
        for i, membro_id in enumerate(ids_lista):
            try:
                url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
                resp = self.session.get(url, headers=self.headers, timeout=TIMEOUT_REQUEST)
                
                if resp.status_code == 200 and len(resp.text) > 3000:
                    dados = extrair_dados_otimizado(resp.text, membro_id)
                    if dados:
                        membros.append(dados)
                        self.sucessos += 1
                        
                        # Log apenas a cada 200 sucessos
                        if self.sucessos % 200 == 0:
                            elapsed = time.time() - self.start_time
                            rate = self.sucessos / elapsed * 60  # por minuto
                            print(f"T{self.thread_id}: {self.sucessos} ({rate:.0f}/min)")
                    else:
                        self.falhas += 1
                else:
                    self.falhas += 1
                
                # Pausa quase zero
                if i % 50 == 0:  # Só pausa a cada 50
                    time.sleep(PAUSA_MINIMA)
                
            except:
                self.falhas += 1
        
        return membros

def fazer_login():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-web-security',
                '--disable-background-timer-throttling',
                '--disable-renderer-backgrounding',
                '--disable-backgrounding-occluded-windows',
                '--disable-features=TranslateUI',
                '--disable-ipc-flooding-protection'
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

def executar_coleta_ultra_rapida(cookies):
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    
    # Dividir em chunks menores para paralelismo máximo
    chunk_size = total_ids // NUM_THREADS
    chunks = []
    
    for i in range(NUM_THREADS):
        start = RANGE_INICIO + (i * chunk_size)
        end = start + chunk_size - 1
        if i == NUM_THREADS - 1:
            end = RANGE_FIM
        chunks.append(list(range(start, end + 1)))
    
    print(f"Dividindo {total_ids} IDs em {NUM_THREADS} threads")
    for i, chunk in enumerate(chunks):
        print(f"T{i}: {len(chunk)} IDs ({chunk[0]}-{chunk[-1]})")
    
    # Executar com timeout mais agressivo
    todos_membros = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        coletores = [ColetorUltraRapido(cookies, i) for i in range(NUM_THREADS)]
        futures = [executor.submit(coletores[i].coletar_ultra_rapido, chunks[i]) for i in range(NUM_THREADS)]
        
        for i, future in enumerate(futures):
            try:
                membros = future.result(timeout=3000)  # 50 min timeout
                todos_membros.extend(membros)
                
                coletor = coletores[i]
                elapsed = time.time() - coletor.start_time
                rate = coletor.sucessos / elapsed * 60 if elapsed > 0 else 0
                
                print(f"T{i} FINAL: {len(membros)} membros | {coletor.sucessos} sucessos | "
                      f"{coletor.falhas} falhas | {rate:.0f} sucessos/min")
                
            except concurrent.futures.TimeoutError:
                print(f"T{i}: TIMEOUT após 50 min")
            except Exception as e:
                print(f"T{i}: ERRO - {str(e)[:30]}")
    
    return todos_membros

def enviar_dados_rapido(membros, tempo_total):
    if not membros:
        return
    
    print("Preparando envio...")
    
    # Criar relatório compacto
    relatorio = [["ID", "NOME", "IGREJA_SELECIONADA", "CARGO/MINISTERIO", "NÍVEL", "INSTRUMENTO", "TONALIDADE"]]
    
    for membro in membros:
        relatorio.append([
            str(membro['id']),
            membro['nome'][:100],  # Limitar tamanho
            membro['igreja_selecionada'],
            membro['cargo_ministerio'][:50],
            membro['nivel'][:30],
            membro['instrumento'][:30],
            membro['tonalidade'][:20]
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
            "versao": "ULTRA_OTIMIZADA"
        }
    }
    
    try:
        print("Enviando para Google Sheets...")
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=180)
        
        if response.status_code == 200:
            print("ENVIADO COM SUCESSO!")
        else:
            print(f"Erro HTTP: {response.status_code}")
            
    except Exception as e:
        print(f"Erro no envio: {e}")

def main():
    print("MODO ULTRA RÁPIDO - TESTE DE VELOCIDADE")
    print("=" * 50)
    print(f"Range: {RANGE_INICIO:,} - {RANGE_FIM:,} ({RANGE_FIM-RANGE_INICIO+1:,} IDs)")
    print(f"Threads: {NUM_THREADS} | Timeout: {TIMEOUT_REQUEST}s | Pausa: {PAUSA_MINIMA}s")
    
    if not EMAIL or not SENHA:
        print("ERRO: Credenciais não encontradas")
        sys.exit(1)
    
    tempo_inicio = time.time()
    
    print("\nLogin...")
    cookies = fazer_login()
    if not cookies:
        print("ERRO: Falha no login")
        sys.exit(1)
    
    print(f"Cookies obtidos: {len(cookies)}")
    
    print("\nINICIANDO COLETA ULTRA RÁPIDA...")
    print("-" * 50)
    
    membros = executar_coleta_ultra_rapida(cookies)
    
    tempo_total = time.time() - tempo_inicio
    
    print("\n" + "=" * 50)
    print("RESULTADO FINAL")
    print("=" * 50)
    print(f"Membros coletados: {len(membros):,}")
    print(f"Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    
    if membros:
        rate_per_min = len(membros) / (tempo_total/60)
        rate_per_hour = rate_per_min * 60
        
        print(f"Velocidade: {rate_per_min:.0f} membros/min | {rate_per_hour:.0f} membros/hora")
        
        # Projeção para 1 milhão
        tempo_1m = 1000000 / rate_per_hour
        print(f"Projeção p/ 1M: {tempo_1m:.1f} horas")
        
        # Amostras
        print(f"\nAMOSTRAS ({min(5, len(membros))} primeiros):")
        for i, m in enumerate(membros[:5], 1):
            nome = m['nome'][:25]
            instr = m['instrumento'][:15]
            print(f"{i}. ID {m['id']} - {nome} - {instr}")
        
        enviar_dados_rapido(membros, tempo_total)
    else:
        print("NENHUM MEMBRO COLETADO!")
    
    print(f"\nFINALIZADO: {INSTANCIA_ID}")

if __name__ == "__main__":
    main()
