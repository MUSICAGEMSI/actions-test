import os
import sys
import re
import asyncio
import httpx
import time
from playwright.sync_api import sync_playwright
from collections import deque
from tqdm import tqdm

# ========================================
# 🔥 CONFIGURAÇÃO
# ========================================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwV-0AChSp5-JyBc3NysUQI0UlFJ7AycvE6CSRKWxldnJ8EBiaNHtj3oYx5jiiHxQbzOw/exec'

# META
RANGE_INICIO = 1
RANGE_FIM = 1000000
INSTANCIA_ID = "GHA_1M_15min"

# FASE 1: ULTRA AGRESSIVA
CONCURRENT_PHASE1 = 2000
TIMEOUT_PHASE1 = 2
WORKERS_PHASE1 = 50

# FASE 2: AGRESSIVA
CONCURRENT_PHASE2 = 1000
TIMEOUT_PHASE2 = 4
WORKERS_PHASE2 = 30

# FASE 3: GARANTIA
CONCURRENT_PHASE3 = 500
TIMEOUT_PHASE3 = 8
WORKERS_PHASE3 = 20

# OTIMIZAÇÕES
CHUNK_SIZE = 20000
BATCH_ENVIO = 5000  # Envia a cada 5k (menor para garantir envio)
CACHE_SIZE_LIMIT = 500000

# ========================================
# REGEX PRÉ-COMPILADAS
# ========================================
REGEX_NOME = re.compile(rb'name="nome"[^>]*value="([^"]*)"')
REGEX_IGREJA = re.compile(rb'igreja_selecionada\s*\(\s*(\d+)\s*\)')
REGEX_CARGO = re.compile(rb'id_cargo"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_NIVEL = re.compile(rb'id_nivel"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_INSTRUMENTO = re.compile(rb'id_instrumento"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_TONALIDADE = re.compile(rb'id_tonalidade"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)

# Cache global
CACHE_VAZIOS = set()
CACHE_LOCK = asyncio.Lock()

# ========================================
# EXTRAÇÃO
# ========================================
def extrair_dados_bytes(html_bytes, membro_id):
    try:
        if not html_bytes or len(html_bytes) < 500:
            return None
        
        if b'name="nome"' not in html_bytes:
            return None
        
        dados = {'id': membro_id}
        
        nome_match = REGEX_NOME.search(html_bytes)
        if not nome_match:
            return None
        
        nome = nome_match.group(1).decode('utf-8', errors='ignore').strip()
        if not nome:
            return None
        dados['nome'] = nome
        
        igreja_match = REGEX_IGREJA.search(html_bytes)
        dados['igreja_selecionada'] = igreja_match.group(1).decode('utf-8', errors='ignore') if igreja_match else ''
        
        cargo_match = REGEX_CARGO.search(html_bytes)
        dados['cargo_ministerio'] = cargo_match.group(1).decode('utf-8', errors='ignore').strip() if cargo_match else ''
        
        nivel_match = REGEX_NIVEL.search(html_bytes)
        dados['nivel'] = nivel_match.group(1).decode('utf-8', errors='ignore').strip() if nivel_match else ''
        
        instrumento_match = REGEX_INSTRUMENTO.search(html_bytes)
        dados['instrumento'] = instrumento_match.group(1).decode('utf-8', errors='ignore').strip() if instrumento_match else ''
        
        tonalidade_match = REGEX_TONALIDADE.search(html_bytes)
        dados['tonalidade'] = tonalidade_match.group(1).decode('utf-8', errors='ignore').strip() if tonalidade_match else ''
        
        return dados
    except:
        return None

# ========================================
# WORKER
# ========================================
class WorkerAsync:
    def __init__(self, worker_id, cookies, semaphore, timeout, fase):
        self.worker_id = worker_id
        self.cookies = cookies
        self.semaphore = semaphore
        self.timeout = timeout
        self.fase = fase
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        
        self.coletados = 0
        self.vazios = 0
        self.erros = 0
    
    async def processar_batch(self, ids_batch, client):
        resultados = []
        
        for membro_id in ids_batch:
            if membro_id in CACHE_VAZIOS:
                self.vazios += 1
                continue
            
            resultado = await self.coletar_id(membro_id, client)
            if resultado:
                resultados.append(resultado)
        
        return resultados
    
    async def coletar_id(self, membro_id, client):
        async with self.semaphore:
            try:
                url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
                response = await client.get(url, timeout=self.timeout)
                
                if response.status_code == 200:
                    html_bytes = response.content
                    
                    if b'name="nome"' in html_bytes:
                        dados = extrair_dados_bytes(html_bytes, membro_id)
                        if dados:
                            self.coletados += 1
                            return ('sucesso', dados)
                        else:
                            if len(CACHE_VAZIOS) < CACHE_SIZE_LIMIT:
                                async with CACHE_LOCK:
                                    CACHE_VAZIOS.add(membro_id)
                            self.vazios += 1
                            return None
                    else:
                        if len(CACHE_VAZIOS) < CACHE_SIZE_LIMIT:
                            async with CACHE_LOCK:
                                CACHE_VAZIOS.add(membro_id)
                        self.vazios += 1
                        return None
                else:
                    self.erros += 1
                    return ('retry', membro_id)
                    
            except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout):
                self.erros += 1
                return ('retry', membro_id)
            except httpx.ConnectError:
                self.erros += 1
                return ('retry', membro_id)
            except Exception:
                self.erros += 1
                return ('retry', membro_id)
        
        return None

# ========================================
# ORQUESTRADOR COM ENVIO REAL
# ========================================
class OrquestradorExtreme:
    def __init__(self, cookies):
        self.cookies = cookies
        self.membros = []
        self.membros_lock = asyncio.Lock()
        
        self.stats = {
            'coletados': 0,
            'vazios': 0,
            'erros_fase1': 0,
            'erros_fase2': 0,
            'erros_fase3': 0,
            'enviados': 0,
            'falhas_envio': 0,
        }
        
        self.retry_fase2 = deque()
        self.retry_fase3 = deque()
        
        self.ultimo_envio = 0
        self.lote_atual = 1
    
    async def enviar_lote_real(self, membros_lote):
        """ENVIO REAL para Google Sheets via Apps Script"""
        if not membros_lote:
            return
        
        # Formato esperado pelo Apps Script
        relatorio = [["ID", "NOME", "IGREJA_SELECIONADA", "CARGO/MINISTERIO", "NÍVEL", "INSTRUMENTO", "TONALIDADE"]]
        for membro in membros_lote:
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
            "tipo": f"membros_gha_{INSTANCIA_ID}_lote_{self.lote_atual}",
            "relatorio_formatado": relatorio,
            "metadata": {
                "instancia": INSTANCIA_ID,
                "lote": self.lote_atual,
                "range_inicio": RANGE_INICIO,
                "range_fim": RANGE_FIM,
                "total_neste_lote": len(membros_lote),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC")
            }
        }
        
        try:
            # Usa httpx ao invés de requests
            async with httpx.AsyncClient(timeout=120) as client:
                response = await client.post(URL_APPS_SCRIPT, json=payload)
                
                if response.status_code == 200:
                    self.stats['enviados'] += len(membros_lote)
                    print(f"\n  ✓ Lote {self.lote_atual} enviado: {len(membros_lote):,} membros")
                    self.lote_atual += 1
                    return True
                else:
                    self.stats['falhas_envio'] += len(membros_lote)
                    print(f"\n  ✗ Erro lote {self.lote_atual}: HTTP {response.status_code}")
                    return False
        except Exception as e:
            self.stats['falhas_envio'] += len(membros_lote)
            print(f"\n  ✗ Erro lote {self.lote_atual}: {e}")
            return False
    
    async def fase1_extreme(self, todos_ids, pbar):
        print(f"\n🔥 FASE 1: {CONCURRENT_PHASE1} CONCURRENT | {WORKERS_PHASE1} WORKERS")
        
        ids_por_worker = len(todos_ids) // WORKERS_PHASE1
        
        limits = httpx.Limits(
            max_keepalive_connections=200,
            max_connections=2500,
            keepalive_expiry=120
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            limits=limits,
            http2=True,
            follow_redirects=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(CONCURRENT_PHASE1)
            
            workers = [
                WorkerAsync(i, self.cookies, semaphore, TIMEOUT_PHASE1, 1)
                for i in range(WORKERS_PHASE1)
            ]
            
            tasks = []
            for i, worker in enumerate(workers):
                inicio = i * ids_por_worker
                fim = inicio + ids_por_worker if i < WORKERS_PHASE1 - 1 else len(todos_ids)
                worker_ids = todos_ids[inicio:fim]
                
                tasks.append(self.processar_worker(worker, worker_ids, client, pbar))
            
            resultados = await asyncio.gather(*tasks)
            
            for worker_resultado in resultados:
                for resultado in worker_resultado:
                    if resultado:
                        if resultado[0] == 'sucesso':
                            async with self.membros_lock:
                                self.membros.append(resultado[1])
                                self.stats['coletados'] += 1
                        elif resultado[0] == 'retry':
                            self.retry_fase2.append(resultado[1])
                            self.stats['erros_fase1'] += 1
            
            for worker in workers:
                self.stats['vazios'] += worker.vazios
    
    async def processar_worker(self, worker, ids, client, pbar):
        resultados = []
        
        for i in range(0, len(ids), 100):
            batch = ids[i:i+100]
            batch_resultado = await worker.processar_batch(batch, client)
            resultados.extend([r for r in batch_resultado if r])
            
            if pbar:
                pbar.update(len(batch))
                pbar.set_postfix({
                    '✓': self.stats['coletados'],
                    '📤': self.stats['enviados'],
                    '⟳': len(self.retry_fase2),
                })
            
            # ENVIO AUTOMÁTICO A CADA BATCH_ENVIO membros
            if self.stats['coletados'] - self.ultimo_envio >= BATCH_ENVIO:
                # Pega lote para enviar
                async with self.membros_lock:
                    lote_envio = self.membros[-BATCH_ENVIO:]
                
                # Envia em background
                asyncio.create_task(self.enviar_lote_real(lote_envio))
                self.ultimo_envio = self.stats['coletados']
        
        return resultados
    
    async def fase2_retry(self, pbar):
        if not self.retry_fase2:
            return
        
        print(f"\n🔄 FASE 2: {len(self.retry_fase2):,} IDs | {CONCURRENT_PHASE2} CONCURRENT")
        
        ids_retry = list(self.retry_fase2)
        self.retry_fase2.clear()
        
        limits = httpx.Limits(
            max_keepalive_connections=120,
            max_connections=1200,
            keepalive_expiry=120
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            limits=limits,
            http2=True,
            follow_redirects=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(CONCURRENT_PHASE2)
            
            workers = [
                WorkerAsync(i, self.cookies, semaphore, TIMEOUT_PHASE2, 2)
                for i in range(WORKERS_PHASE2)
            ]
            
            ids_por_worker = len(ids_retry) // WORKERS_PHASE2
            
            tasks = []
            for i, worker in enumerate(workers):
                inicio = i * ids_por_worker
                fim = inicio + ids_por_worker if i < WORKERS_PHASE2 - 1 else len(ids_retry)
                worker_ids = ids_retry[inicio:fim]
                tasks.append(self.processar_worker(worker, worker_ids, client, pbar))
            
            resultados = await asyncio.gather(*tasks)
            
            for worker_resultado in resultados:
                for resultado in worker_resultado:
                    if resultado:
                        if resultado[0] == 'sucesso':
                            async with self.membros_lock:
                                self.membros.append(resultado[1])
                                self.stats['coletados'] += 1
                        elif resultado[0] == 'retry':
                            self.retry_fase3.append(resultado[1])
                            self.stats['erros_fase2'] += 1
    
    async def fase3_garantia(self, pbar):
        if not self.retry_fase3:
            return
        
        print(f"\n🎯 FASE 3: {len(self.retry_fase3):,} IDs | {CONCURRENT_PHASE3} CONCURRENT")
        
        ids_retry = list(self.retry_fase3)
        self.retry_fase3.clear()
        
        limits = httpx.Limits(
            max_keepalive_connections=80,
            max_connections=600,
            keepalive_expiry=120
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            limits=limits,
            http2=True,
            follow_redirects=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(CONCURRENT_PHASE3)
            
            workers = [
                WorkerAsync(i, self.cookies, semaphore, TIMEOUT_PHASE3, 3)
                for i in range(WORKERS_PHASE3)
            ]
            
            ids_por_worker = len(ids_retry) // WORKERS_PHASE3
            
            tasks = []
            for i, worker in enumerate(workers):
                inicio = i * ids_por_worker
                fim = inicio + ids_por_worker if i < WORKERS_PHASE3 - 1 else len(ids_retry)
                worker_ids = ids_retry[inicio:fim]
                tasks.append(self.processar_worker(worker, worker_ids, client, pbar))
            
            resultados = await asyncio.gather(*tasks)
            
            for worker_resultado in resultados:
                for resultado in worker_resultado:
                    if resultado:
                        if resultado[0] == 'sucesso':
                            async with self.membros_lock:
                                self.membros.append(resultado[1])
                                self.stats['coletados'] += 1
                        elif resultado[0] == 'retry':
                            self.stats['erros_fase3'] += 1

# ========================================
# LOGIN
# ========================================
def login():
    print("🔐 Realizando login...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
            )
            page = browser.new_page()
            page.goto(URL_INICIAL, timeout=30000)
            page.fill('input[name="login"]', EMAIL)
            page.fill('input[name="password"]', SENHA)
            page.click('button[type="submit"]')
            page.wait_for_selector("nav", timeout=20000)
            cookies = {cookie['name']: cookie['value'] for cookie in page.context.cookies()}
            browser.close()
            print("✓ Login realizado")
            return cookies
    except Exception as e:
        print(f"✗ Erro no login: {e}")
        return None

# ========================================
# EXECUÇÃO
# ========================================
async def executar_coleta_extreme(cookies):
    orquestrador = OrquestradorExtreme(cookies)
    
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    todos_ids = list(range(RANGE_INICIO, RANGE_FIM + 1))
    
    print(f"\n{'='*80}")
    print(f"🔥 META: {total_ids:,} IDs | ENVIO AUTOMÁTICO A CADA {BATCH_ENVIO:,}")
    print(f"{'='*80}\n")
    
    tempo_inicio = time.time()
    
    # FASE 1
    with tqdm(total=total_ids, desc="Fase 1", unit="ID", ncols=100, colour='red') as pbar:
        await orquestrador.fase1_extreme(todos_ids, pbar)
    
    tempo_fase1 = time.time() - tempo_inicio
    print(f"✓ F1: {tempo_fase1:.1f}s | ✓{orquestrador.stats['coletados']:,} | 📤{orquestrador.stats['enviados']:,}")
    
    # FASE 2
    if orquestrador.retry_fase2:
        with tqdm(total=len(orquestrador.retry_fase2), desc="Fase 2", unit="ID", ncols=100, colour='yellow') as pbar:
            await orquestrador.fase2_retry(pbar)
    
    # FASE 3
    if orquestrador.retry_fase3:
        with tqdm(total=len(orquestrador.retry_fase3), desc="Fase 3", unit="ID", ncols=100, colour='green') as pbar:
            await orquestrador.fase3_garantia(pbar)
    
    # ENVIO FINAL (resto que não foi enviado ainda)
    if len(orquestrador.membros) > orquestrador.stats['enviados']:
        print(f"\n📤 Enviando últimos {len(orquestrador.membros) - orquestrador.stats['enviados']:,} membros...")
        resto = orquestrador.membros[orquestrador.ultimo_envio:]
        await orquestrador.enviar_lote_real(resto)
    
    return orquestrador

# ========================================
# MAIN
# ========================================
def main():
    print("=" * 80)
    print("🔥 COLETOR COM ENVIO AUTOMÁTICO")
    print("=" * 80)
    
    if not EMAIL or not SENHA:
        print("✗ Credenciais não encontradas")
        sys.exit(1)
    
    tempo_total_inicio = time.time()
    
    cookies = login()
    if not cookies:
        sys.exit(1)
    
    orquestrador = asyncio.run(executar_coleta_extreme(cookies))
    
    tempo_total = time.time() - tempo_total_inicio
    
    print(f"\n{'='*80}")
    print(f"📊 RELATÓRIO FINAL")
    print(f"{'='*80}")
    print(f"✅ Coletados: {orquestrador.stats['coletados']:,}")
    print(f"📤 Enviados: {orquestrador.stats['enviados']:,}")
    print(f"❌ Falhas envio: {orquestrador.stats['falhas_envio']:,}")
    print(f"⚪ Vazios: {orquestrador.stats['vazios']:,}")
    print(f"⏱️  Tempo: {tempo_total/60:.2f} min")
    print(f"⚡ Velocidade: {(RANGE_FIM - RANGE_INICIO + 1) / (tempo_total/60):.0f} IDs/min")
    print(f"{'='*80}")
    
    if orquestrador.membros:
        print(f"\n📋 Amostras (5 primeiros):")
        for i, m in enumerate(orquestrador.membros[:5], 1):
            print(f"  {i}. [{m['id']:>7}] {m['nome'][:45]}")
    
    print(f"\n✅ COLETA FINALIZADA")

if __name__ == "__main__":
    main()
