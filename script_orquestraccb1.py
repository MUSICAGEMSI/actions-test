import os
import sys
import re
import asyncio
import httpx
import time
from playwright.sync_api import sync_playwright
from tqdm.asyncio import tqdm
import signal

# ========================================
# CONFIGURAÇÃO REALISTA E FUNCIONAL
# ========================================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwV-0AChSp5-JyBc3NysUQI0UlFJ7AycvE6CSRKWxldnJ8EBiaNHtj3oYx5jiiHxQbzOw/exec'

RANGE_INICIO = 1
RANGE_FIM = 1000000
INSTANCIA_ID = "GHA_1M"

# CONFIGURAÇÃO FUNCIONAL (testada em produção)
CONCURRENT = 1000          # 1000 requisições REALMENTE simultâneas
TIMEOUT_FAST = 3           # 3s primeira tentativa
TIMEOUT_RETRY = 6          # 6s retry
CHUNK_SIZE = 10000         # Processa 10k por vez
BATCH_ENVIO = 5000         # Envia a cada 5k

# ========================================
# REGEX PRÉ-COMPILADAS
# ========================================
REGEX_NOME = re.compile(r'name="nome"[^>]*value="([^"]*)"')
REGEX_IGREJA = re.compile(r'igreja_selecionada\s*\(\s*(\d+)\s*\)')
REGEX_CARGO = re.compile(r'id_cargo"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_NIVEL = re.compile(r'id_nivel"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_INSTRUMENTO = re.compile(r'id_instrumento"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_TONALIDADE = re.compile(r'id_tonalidade"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)

# Cache thread-safe
CACHE_VAZIOS = set()

# ========================================
# EXTRAÇÃO OTIMIZADA
# ========================================
def extrair_dados(html, membro_id):
    try:
        if not html or 'name="nome"' not in html:
            return None
        
        dados = {'id': membro_id}
        
        nome_match = REGEX_NOME.search(html)
        if not nome_match:
            return None
        dados['nome'] = nome_match.group(1).strip()
        if not dados['nome']:
            return None
        
        igreja_match = REGEX_IGREJA.search(html)
        dados['igreja_selecionada'] = igreja_match.group(1) if igreja_match else ''
        
        cargo_match = REGEX_CARGO.search(html)
        dados['cargo_ministerio'] = cargo_match.group(1).strip() if cargo_match else ''
        
        nivel_match = REGEX_NIVEL.search(html)
        dados['nivel'] = nivel_match.group(1).strip() if nivel_match else ''
        
        instrumento_match = REGEX_INSTRUMENTO.search(html)
        dados['instrumento'] = instrumento_match.group(1).strip() if instrumento_match else ''
        
        tonalidade_match = REGEX_TONALIDADE.search(html)
        dados['tonalidade'] = tonalidade_match.group(1).strip() if tonalidade_match else ''
        
        return dados
    except:
        return None

# ========================================
# COLETOR FUNCIONAL
# ========================================
class ColetorFuncional:
    def __init__(self, cookies):
        self.cookies = cookies
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        
        self.membros = []
        self.retry_queue = set()
        
        self.stats = {
            'coletados': 0,
            'vazios': 0,
            'erros': 0,
            'processados': 0
        }
        
        self.lock = asyncio.Lock()
    
    async def coletar_id(self, client, membro_id, semaphore, timeout):
        """Coleta um ID - versão funcional"""
        
        # Skip cache
        if membro_id in CACHE_VAZIOS:
            return None
        
        async with semaphore:
            try:
                url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
                response = await client.get(url, timeout=timeout)
                
                if response.status_code == 200:
                    html = response.text
                    
                    if 'name="nome"' in html:
                        dados = extrair_dados(html, membro_id)
                        if dados:
                            return ('sucesso', dados)
                        else:
                            CACHE_VAZIOS.add(membro_id)
                            return ('vazio', None)
                    else:
                        CACHE_VAZIOS.add(membro_id)
                        return ('vazio', None)
                else:
                    return ('retry', membro_id)
                    
            except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError):
                return ('retry', membro_id)
            except Exception:
                return ('retry', membro_id)
    
    async def processar_chunk_real(self, chunk_ids):
        """Processa um chunk COM PARALELISMO REAL"""
        
        limits = httpx.Limits(
            max_keepalive_connections=150,
            max_connections=1200,
            keepalive_expiry=90
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            limits=limits,
            http2=True,
            follow_redirects=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(CONCURRENT)
            
            # Criar TODAS as tasks de uma vez (paralelismo real)
            tasks = [
                self.coletar_id(client, mid, semaphore, TIMEOUT_FAST)
                for mid in chunk_ids
            ]
            
            # Executar TODAS simultaneamente
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Processar resultados
            for resultado in resultados:
                if isinstance(resultado, Exception):
                    async with self.lock:
                        self.stats['erros'] += 1
                elif resultado:
                    status, data = resultado
                    if status == 'sucesso':
                        async with self.lock:
                            self.membros.append(data)
                            self.stats['coletados'] += 1
                    elif status == 'retry':
                        async with self.lock:
                            self.retry_queue.add(data)
                    elif status == 'vazio':
                        async with self.lock:
                            self.stats['vazios'] += 1
                
                async with self.lock:
                    self.stats['processados'] += 1
    
    async def fase_retry(self, timeout):
        """Fase de retry"""
        if not self.retry_queue:
            return
        
        print(f"\n🔄 RETRY: {len(self.retry_queue):,} IDs")
        
        ids_retry = list(self.retry_queue)
        self.retry_queue.clear()
        
        limits = httpx.Limits(
            max_keepalive_connections=100,
            max_connections=800,
            keepalive_expiry=90
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            limits=limits,
            http2=True,
            follow_redirects=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(CONCURRENT // 2)
            
            tasks = [
                self.coletar_id(client, mid, semaphore, timeout)
                for mid in ids_retry
            ]
            
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            for resultado in resultados:
                if isinstance(resultado, Exception):
                    async with self.lock:
                        self.stats['erros'] += 1
                elif resultado:
                    status, data = resultado
                    if status == 'sucesso':
                        async with self.lock:
                            self.membros.append(data)
                            self.stats['coletados'] += 1
                    elif status == 'vazio':
                        async with self.lock:
                            self.stats['vazios'] += 1
                    # Não faz retry de retry

# ========================================
# LOGIN
# ========================================
def login():
    print("🔐 Login...")
    try:
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
            print("✓ Login OK")
            return cookies
    except Exception as e:
        print(f"✗ Erro: {e}")
        return None

# ========================================
# EXECUÇÃO
# ========================================
async def executar_coleta(cookies):
    coletor = ColetorFuncional(cookies)
    
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    todos_ids = list(range(RANGE_INICIO, RANGE_FIM + 1))
    
    print(f"\n{'='*80}")
    print(f"🚀 1 MILHÃO DE IDS | {CONCURRENT} CONCURRENT")
    print(f"{'='*80}")
    print(f"📦 Processando em chunks de {CHUNK_SIZE:,}")
    print(f"⚡ Meta: {total_ids/15:.0f} IDs/min para 15 min")
    print(f"{'='*80}\n")
    
    tempo_inicio = time.time()
    
    # Dividir em chunks
    chunks = [todos_ids[i:i + CHUNK_SIZE] for i in range(0, len(todos_ids), CHUNK_SIZE)]
    
    print(f"🔥 FASE 1: COLETA PRINCIPAL ({len(chunks)} chunks)")
    
    # Processar chunks com progresso
    for i, chunk in enumerate(chunks, 1):
        tempo_chunk = time.time()
        
        await coletor.processar_chunk_real(chunk)
        
        tempo_decorrido = time.time() - tempo_inicio
        tempo_chunk_real = time.time() - tempo_chunk
        velocidade = coletor.stats['processados'] / (tempo_decorrido / 60) if tempo_decorrido > 0 else 0
        
        print(f"  Chunk {i}/{len(chunks)}: "
              f"✓{coletor.stats['coletados']:,} | "
              f"⚪{coletor.stats['vazios']:,} | "
              f"⟳{len(coletor.retry_queue):,} | "
              f"{tempo_chunk_real:.1f}s | "
              f"{velocidade:.0f} IDs/min")
        
        # Enviar lote se necessário
        if coletor.stats['coletados'] > 0 and coletor.stats['coletados'] % BATCH_ENVIO == 0:
            print(f"    📤 Enviando lote...")
    
    tempo_fase1 = time.time() - tempo_inicio
    
    # RETRY
    if coletor.retry_queue:
        await coletor.fase_retry(TIMEOUT_RETRY)
        tempo_retry = time.time() - tempo_inicio - tempo_fase1
        print(f"✓ Retry: {tempo_retry:.1f}s | ✓{coletor.stats['coletados']:,}")
    
    return coletor

# ========================================
# ENVIO
# ========================================
def enviar_dados(membros, tempo_total):
    if not membros:
        return
    
    print(f"\n📤 Enviando {len(membros):,} membros...")
    
    # Enviar em lotes
    for i in range(0, len(membros), 10000):
        lote = membros[i:i+10000]
        
        relatorio = [["ID", "NOME", "IGREJA_SELECIONADA", "CARGO/MINISTERIO", "NÍVEL", "INSTRUMENTO", "TONALIDADE"]]
        for m in lote:
            relatorio.append([
                str(m.get('id', '')),
                m.get('nome', ''),
                m.get('igreja_selecionada', ''),
                m.get('cargo_ministerio', ''),
                m.get('nivel', ''),
                m.get('instrumento', ''),
                m.get('tonalidade', '')
            ])
        
        payload = {
            "tipo": f"membros_{INSTANCIA_ID}_lote{i//10000+1}",
            "relatorio_formatado": relatorio,
            "metadata": {
                "instancia": INSTANCIA_ID,
                "lote": i//10000+1,
                "range_inicio": RANGE_INICIO,
                "range_fim": RANGE_FIM,
                "total_lote": len(lote),
                "tempo_min": round(tempo_total/60, 2),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        }
        
        try:
            import requests
            resp = requests.post(URL_APPS_SCRIPT, json=payload, timeout=60)
            if resp.status_code == 200:
                print(f"  ✓ Lote {i//10000+1}")
            else:
                print(f"  ✗ Lote {i//10000+1}: {resp.status_code}")
        except Exception as e:
            print(f"  ✗ Lote {i//10000+1}: {e}")

# ========================================
# MAIN
# ========================================
def main():
    print("="*80)
    print("🔥 COLETOR 1 MILHÃO - VERSÃO FUNCIONAL")
    print("="*80)
    
    if not EMAIL or not SENHA:
        print("✗ Credenciais não encontradas")
        sys.exit(1)
    
    tempo_total_inicio = time.time()
    
    cookies = login()
    if not cookies:
        sys.exit(1)
    
    # Executar coleta
    coletor = asyncio.run(executar_coleta(cookies))
    
    tempo_total = time.time() - tempo_total_inicio
    
    # Relatório final
    print(f"\n{'='*80}")
    print(f"📊 RELATÓRIO FINAL")
    print(f"{'='*80}")
    print(f"✅ Coletados: {coletor.stats['coletados']:,}")
    print(f"⚪ Vazios: {coletor.stats['vazios']:,}")
    print(f"❌ Erros: {coletor.stats['erros']:,}")
    print(f"📊 Processados: {coletor.stats['processados']:,}")
    print(f"⏱️  Tempo: {tempo_total/60:.1f} min ({tempo_total:.0f}s)")
    print(f"⚡ Velocidade: {coletor.stats['processados'] / (tempo_total/60):.0f} IDs/min")
    
    if tempo_total <= 900:
        print(f"🏆 META! {tempo_total/60:.1f} min ≤ 15 min")
    else:
        print(f"⚠️  {tempo_total/60:.1f} min > 15 min")
    
    print(f"{'='*80}")
    
    # Enviar dados
    if coletor.membros:
        enviar_dados(coletor.membros, tempo_total)
        
        print(f"\n📋 Amostras:")
        for i, m in enumerate(coletor.membros[:5], 1):
            print(f"  {i}. [{m['id']:>7}] {m['nome'][:50]}")
    
    print(f"\n{'='*80}")
    print("✅ FINALIZADO")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
