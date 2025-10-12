import os
import sys
import re
import asyncio
import httpx
import time
import random
from collections import deque, defaultdict
import json

# ========================================
# FORÇA UNBUFFERED OUTPUT (TEMPO REAL)
# ========================================
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

def log(msg, force_flush=True):
    """Print com flush imediato para GitHub Actions"""
    print(msg, flush=force_flush)

# ========================================
# CONFIGURAÇÕES ADAPTATIVAS
# ========================================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzbkdOTDjGJxabnlJNDX7ZKI4_vh-t5d84MDRp-4FO4KmocRPEVs2jkHL3gjKEG-efF/exec'

RANGE_INICIO = 1
RANGE_FIM = 850000
INSTANCIA_ID = "GHA_smart_adaptive_v3"

# 🧠 CONFIGURAÇÕES INTELIGENTES
FASE0_SAMPLES = 2000
CONCURRENT_INICIAL = 500
CONCURRENT_MINIMO = 50
CONCURRENT_MAXIMO = 1500

TIMEOUT_BASE = 3.0
CHUNK_SIZE = 2000
BATCH_ENVIO = 5000

# Circuit Breaker
ERRO_THRESHOLD = 0.3
SUCESSO_THRESHOLD = 0.9

# Timeout do GitHub Actions
MAX_EXECUTION_TIME = 25 * 60  # 25 minutos

# ========================================
# REGEX PRÉ-COMPILADAS
# ========================================
REGEX_NOME = re.compile(r'name="nome"[^>]*value="([^"]*)"')
REGEX_IGREJA = re.compile(r'igreja_selecionada\s*\(\s*(\d+)\s*\)')
REGEX_CARGO = re.compile(r'id_cargo"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_NIVEL = re.compile(r'id_nivel"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_INSTRUMENTO = re.compile(r'id_instrumento"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_TONALIDADE = re.compile(r'id_tonalidade"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)

CACHE_VAZIOS = set()
CHECKPOINT_FILE = "checkpoint_coleta.json"

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

# ========================================
# HEARTBEAT COM OUTPUT IMEDIATO
# ========================================
class Heartbeat:
    def __init__(self, interval=15):  # A cada 15s
        self.interval = interval
        self.last_update = time.time()
        self.running = True
        self.task = None
        self.contador = 0
    
    async def start(self):
        self.task = asyncio.create_task(self._heartbeat_loop())
    
    async def _heartbeat_loop(self):
        while self.running:
            await asyncio.sleep(self.interval)
            self.contador += 1
            elapsed = time.time() - self.last_update
            log(f"💓 [{self.contador}] Sistema ativo - {elapsed:.0f}s desde última atualização")
    
    def update(self):
        self.last_update = time.time()
    
    def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()

# ========================================
# EXTRAÇÃO OTIMIZADA
# ========================================
def extrair_dados_rapido(html, membro_id):
    if not html or len(html) < 400:
        return None
    if 'name="nome"' not in html:
        return None
    
    dados = {'id': membro_id}
    
    nome_match = REGEX_NOME.search(html)
    if not nome_match:
        return None
    dados['nome'] = nome_match.group(1).strip()
    if not dados['nome']:
        return None
    
    igreja = REGEX_IGREJA.search(html)
    dados['igreja_selecionada'] = igreja.group(1) if igreja else ''
    
    cargo = REGEX_CARGO.search(html)
    dados['cargo_ministerio'] = cargo.group(1).strip() if cargo else ''
    
    nivel = REGEX_NIVEL.search(html)
    dados['nivel'] = nivel.group(1).strip() if nivel else ''
    
    instrumento = REGEX_INSTRUMENTO.search(html)
    dados['instrumento'] = instrumento.group(1).strip() if instrumento else ''
    
    tonalidade = REGEX_TONALIDADE.search(html)
    dados['tonalidade'] = tonalidade.group(1).strip() if tonalidade else ''
    
    return dados

# ========================================
# COLETOR ADAPTATIVO COM OUTPUT REAL-TIME
# ========================================
class ColetorAdaptativo:
    def __init__(self, cookies, tempo_inicio):
        self.cookies = cookies
        self.membros = []
        self.retry_queue = deque()
        self.tempo_inicio = tempo_inicio
        self.heartbeat = Heartbeat(interval=15)
        
        self.stats = {
            'coletados': 0,
            'vazios': 0,
            'erros': 0,
            'auth_errors': 0,
            'rate_limits': 0,
            'processados': 0,
            'timeout_errors': 0
        }
        
        self.concurrent_atual = CONCURRENT_INICIAL
        self.lock = asyncio.Lock()
        self.circuit_breaker_ativo = False
        self.ultima_taxa_sucesso = 1.0
        self.ranges_quentes = []
        
        # Contador para relatórios periódicos
        self.ultimo_relatorio = time.time()
        self.relatorio_interval = 10  # A cada 10 segundos
    
    def check_timeout_global(self):
        elapsed = time.time() - self.tempo_inicio
        if elapsed > MAX_EXECUTION_TIME:
            log(f"⚠️ TIMEOUT GLOBAL: {elapsed/60:.1f} min > {MAX_EXECUTION_TIME/60:.1f} min")
            return True
        return False
    
    def get_headers(self):
        return {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        }
    
    async def coletar_id(self, client, membro_id, timeout, semaphore):
        if membro_id in CACHE_VAZIOS:
            return 'cached_empty'
        
        async with semaphore:
            if random.random() < 0.1:
                await asyncio.sleep(random.uniform(0.01, 0.05))
            
            try:
                url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
                response = await client.get(url, timeout=timeout)
                
                if response.status_code == 200:
                    html = response.text
                    
                    if 'name="login"' in html or 'name="password"' in html:
                        async with self.lock:
                            self.stats['auth_errors'] += 1
                        return 'auth_error'
                    
                    if 'name="nome"' in html:
                        dados = extrair_dados_rapido(html, membro_id)
                        if dados:
                            async with self.lock:
                                self.stats['coletados'] += 1
                                self.membros.append(dados)
                            return 'ok'
                    
                    CACHE_VAZIOS.add(membro_id)
                    async with self.lock:
                        self.stats['vazios'] += 1
                    return 'vazio'
                
                elif response.status_code == 429:
                    async with self.lock:
                        self.stats['rate_limits'] += 1
                    return 'rate_limit'
                
                elif response.status_code in [301, 302, 303]:
                    async with self.lock:
                        self.stats['auth_errors'] += 1
                    return 'auth_error'
                
                else:
                    return 'retry'
                    
            except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout):
                async with self.lock:
                    self.stats['timeout_errors'] += 1
                return 'retry'
            except:
                return 'retry'
    
    def relatorio_periodico(self, force=False):
        """Relatório a cada X segundos"""
        agora = time.time()
        if force or (agora - self.ultimo_relatorio) >= self.relatorio_interval:
            elapsed = agora - self.tempo_inicio
            ids_por_min = (self.stats['processados'] / (elapsed / 60)) if elapsed > 0 else 0
            
            log(f"📊 [{elapsed/60:.1f}min] Processados: {self.stats['processados']:,} | "
                f"OK: {self.stats['coletados']:,} | "
                f"Vazios: {self.stats['vazios']:,} | "
                f"Erros: {self.stats['erros']:,} | "
                f"Velocidade: {ids_por_min:.0f}/min | "
                f"Concurrent: {self.concurrent_atual}")
            
            self.ultimo_relatorio = agora
    
    async def fase0_sampling_inteligente(self):
        log(f"\n{'='*80}")
        log(f"🔍 FASE 0: SAMPLING INTELIGENTE ({FASE0_SAMPLES} IDs)")
        log(f"{'='*80}")
        
        total_range = RANGE_FIM - RANGE_INICIO + 1
        subrange_size = total_range // 10
        samples_per_subrange = FASE0_SAMPLES // 10
        
        samples = []
        for i in range(10):
            start = RANGE_INICIO + (i * subrange_size)
            end = start + subrange_size
            range_samples = random.sample(range(start, min(end, RANGE_FIM + 1)), 
                                         min(samples_per_subrange, end - start))
            samples.extend(range_samples)
        
        log(f"📊 Testando {len(samples)} amostras distribuídas...")
        
        limits = httpx.Limits(
            max_keepalive_connections=100,
            max_connections=200,
            keepalive_expiry=60
        )
        
        tempo_sampling_inicio = time.time()
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.get_headers(),
            limits=limits,
            http2=True,
            follow_redirects=False,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(200)
            tasks = [self.coletar_id(client, mid, 3.0, semaphore) for mid in samples]
            
            # Progress durante sampling
            log("⏳ Executando sampling...")
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            validos = sum(1 for r in resultados if r == 'ok')
            vazios = sum(1 for r in resultados if r == 'vazio')
            auth_errors = sum(1 for r in resultados if r == 'auth_error')
            rate_limits = sum(1 for r in resultados if r == 'rate_limit')
            
            densidade_geral = (validos / len(samples)) * 100
            tempo_sampling = time.time() - tempo_sampling_inicio
            
            log(f"\n{'='*80}")
            log(f"📊 RESULTADO DO SAMPLING (concluído em {tempo_sampling:.1f}s)")
            log(f"{'='*80}")
            log(f"  ✅ Válidos: {validos} ({densidade_geral:.2f}%)")
            log(f"  ⚪ Vazios: {vazios}")
            log(f"  ⚠️  Auth errors: {auth_errors}")
            log(f"  🚫 Rate limits: {rate_limits}")
            
            if auth_errors > len(samples) * 0.5:
                log(f"\n❌ CRÍTICO: {auth_errors} erros de autenticação!")
                return -1
            
            if rate_limits > len(samples) * 0.2:
                log(f"\n⚠️  WARNING: {rate_limits} rate limits detectados!")
                log("   Reduzindo concorrência inicial...")
                self.concurrent_atual = CONCURRENT_MINIMO
                self.circuit_breaker_ativo = True
            
            log(f"\n📍 ANÁLISE DE DENSIDADE POR RANGE:")
            range_densities = []
            for i in range(10):
                start = RANGE_INICIO + (i * subrange_size)
                end = start + subrange_size
                
                range_samples = [s for s in samples if start <= s < end]
                range_resultados = [resultados[samples.index(s)] for s in range_samples]
                range_validos = sum(1 for r in range_resultados if r == 'ok')
                
                if len(range_samples) > 0:
                    densidade = (range_validos / len(range_samples)) * 100
                    range_densities.append((start, end, densidade))
                    
                    emoji = "🔥" if densidade > 10 else "⚪" if densidade > 5 else "❄️"
                    log(f"  {emoji} Range {start:>7,}-{end:>7,}: {densidade:>5.1f}% válidos")
            
            self.ranges_quentes = [(s, e) for s, e, d in range_densities if d > 5]
            
            if validos > 0:
                expectativa = int(total_range * densidade_geral / 100)
                log(f"\n✓ Expectativa total: ~{expectativa:,} membros")
            
            self.heartbeat.update()
            return densidade_geral
    
    async def ajustar_concorrencia(self, taxa_sucesso):
        async with self.lock:
            if taxa_sucesso > SUCESSO_THRESHOLD and not self.circuit_breaker_ativo:
                novo = min(self.concurrent_atual + 100, CONCURRENT_MAXIMO)
                if novo != self.concurrent_atual:
                    log(f"  ⬆️  Aumentando concorrência: {self.concurrent_atual} → {novo}")
                    self.concurrent_atual = novo
            
            elif taxa_sucesso < ERRO_THRESHOLD:
                novo = max(self.concurrent_atual - 100, CONCURRENT_MINIMO)
                if novo != self.concurrent_atual:
                    log(f"  ⬇️  Reduzindo concorrência: {self.concurrent_atual} → {novo}")
                    self.concurrent_atual = novo
                    self.circuit_breaker_ativo = True
            
            elif taxa_sucesso > 0.7 and self.circuit_breaker_ativo:
                log(f"  ✅ Circuit breaker desativado")
                self.circuit_breaker_ativo = False
            
            self.ultima_taxa_sucesso = taxa_sucesso
    
    async def coletar_chunk_adaptativo(self, chunk_num, total_chunks, ids_chunk):
        if self.check_timeout_global():
            log(f"⏰ TIMEOUT GLOBAL - Parando na chunk {chunk_num}/{total_chunks}")
            return False
        
        limits = httpx.Limits(
            max_keepalive_connections=min(300, self.concurrent_atual),
            max_connections=self.concurrent_atual + 100,
            keepalive_expiry=90
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.get_headers(),
            limits=limits,
            http2=True,
            follow_redirects=False,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(self.concurrent_atual)
            
            tasks = [self.coletar_id(client, mid, TIMEOUT_BASE, semaphore) 
                     for mid in ids_chunk]
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            sucessos = sum(1 for r in resultados if r == 'ok')
            vazios = sum(1 for r in resultados if r in ['vazio', 'cached_empty'])
            retries = sum(1 for r in resultados if r == 'retry')
            rate_limits = sum(1 for r in resultados if r == 'rate_limit')
            
            for mid, resultado in zip(ids_chunk, resultados):
                if resultado in ['retry', 'rate_limit']:
                    self.retry_queue.append(mid)
            
            taxa_sucesso = (sucessos + vazios) / len(resultados)
            await self.ajustar_concorrencia(taxa_sucesso)
            
            self.stats['processados'] += len(ids_chunk)
            self.heartbeat.update()
            
            # Relatório periódico automático
            self.relatorio_periodico()
            
            # Relatório de chunk a cada 10 chunks
            if chunk_num % 10 == 0:
                log(f"📦 Chunk {chunk_num}/{total_chunks} | "
                    f"OK: {sucessos} | Vazios: {vazios} | "
                    f"Retry: {retries} | Rate: {rate_limits}")
            
            if rate_limits > len(resultados) * 0.1:
                wait_time = random.uniform(0.5, 2.0)
                log(f"🚫 Rate limit detectado, aguardando {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
            
            return True
    
    async def processar_retries(self, max_retries=2):
        if not self.retry_queue:
            return
        
        log(f"\n{'='*80}")
        log(f"🔄 PROCESSANDO RETRIES ({len(self.retry_queue):,} IDs)")
        log(f"{'='*80}")
        
        tentativa = 0
        while self.retry_queue and tentativa < max_retries:
            if self.check_timeout_global():
                log("⏰ TIMEOUT GLOBAL - Pulando retries")
                break
            
            tentativa += 1
            batch_size = min(5000, len(self.retry_queue))
            batch = [self.retry_queue.popleft() for _ in range(batch_size)]
            
            log(f"  Tentativa {tentativa}/{max_retries}: {len(batch):,} IDs")
            
            concurrent_retry = max(CONCURRENT_MINIMO, self.concurrent_atual // 2)
            
            limits = httpx.Limits(
                max_keepalive_connections=150,
                max_connections=concurrent_retry + 50,
                keepalive_expiry=60
            )
            
            async with httpx.AsyncClient(
                cookies=self.cookies,
                headers=self.get_headers(),
                limits=limits,
                http2=True,
                follow_redirects=False,
                timeout=None
            ) as client:
                
                semaphore = asyncio.Semaphore(concurrent_retry)
                
                tasks = [self.coletar_id(client, mid, TIMEOUT_BASE * 2, semaphore) 
                         for mid in batch]
                resultados = await asyncio.gather(*tasks, return_exceptions=True)
                
                if tentativa < max_retries:
                    for mid, resultado in zip(batch, resultados):
                        if resultado in ['retry', 'rate_limit']:
                            self.retry_queue.append(mid)
                else:
                    erros_finais = sum(1 for r in resultados if r in ['retry', 'rate_limit'])
                    self.stats['erros'] += erros_finais
                    log(f"❌ {erros_finais} erros finais após todas tentativas")
            
            self.heartbeat.update()
            
            if self.retry_queue and tentativa < max_retries:
                await asyncio.sleep(2.0)

# ========================================
# LOGIN ASYNC
# ========================================
async def login():
    log("=" * 80)
    log("🔐 INICIANDO LOGIN")
    log("=" * 80)
    try:
        from playwright.async_api import async_playwright
        
        log("  ➤ Iniciando Playwright...")
        async with async_playwright() as p:
            log("  ➤ Abrindo navegador...")
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
            )
            context = await browser.new_context(
                user_agent=random.choice(USER_AGENTS)
            )
            page = await context.new_page()
            
            log(f"  ➤ Navegando para {URL_INICIAL}")
            await page.goto(URL_INICIAL, timeout=30000)
            
            log("  ➤ Preenchendo credenciais...")
            await page.fill('input[name="login"]', EMAIL)
            await page.fill('input[name="password"]', SENHA)
            
            log("  ➤ Submetendo formulário...")
            await page.click('button[type="submit"]')
            
            log("  ➤ Aguardando navegação...")
            await page.wait_for_selector("nav", timeout=20000)
            
            cookies = {c['name']: c['value'] for c in await context.cookies()}
            await browser.close()
            
            log(f"✓ Login realizado com sucesso! {len(cookies)} cookies obtidos")
            log("=" * 80)
            return cookies
    except Exception as e:
        log(f"✗ Erro no login: {e}")
        log("=" * 80)
        return None

# ========================================
# EXECUÇÃO ADAPTATIVA
# ========================================
async def executar_coleta_adaptativa():
    tempo_inicio = time.time()
    
    log("\n" + "=" * 80)
    log("🧠 COLETOR ADAPTATIVO INTELIGENTE v3 - REAL-TIME OUTPUT")
    log("=" * 80)
    log(f"📊 Range: {RANGE_INICIO:,} → {RANGE_FIM:,}")
    log(f"⏰ Timeout máximo: {MAX_EXECUTION_TIME/60:.0f} minutos")
    log(f"🎯 Concorrência: {CONCURRENT_MINIMO}-{CONCURRENT_MAXIMO} (dinâmica)")
    log("=" * 80)
    
    cookies = await login()
    if not cookies:
        sys.exit(1)
    
    coletor = ColetorAdaptativo(cookies, tempo_inicio)
    await coletor.heartbeat.start()
    
    try:
        # FASE 0
        densidade = await coletor.fase0_sampling_inteligente()
        
        if densidade == -1:
            log("\n❌ ABORTANDO: Problema crítico de autenticação")
            return coletor, time.time() - tempo_inicio
        
        # FASE 1
        total_ids = RANGE_FIM - RANGE_INICIO + 1
        todos_ids = list(range(RANGE_INICIO, RANGE_FIM + 1))
        
        if coletor.ranges_quentes:
            log(f"\n🎯 PRIORIZANDO {len(coletor.ranges_quentes)} RANGES QUENTES")
            ids_priorizados = []
            for start, end in coletor.ranges_quentes:
                ids_priorizados.extend(range(start, end))
            ids_resto = [i for i in todos_ids if i not in ids_priorizados]
            todos_ids = ids_priorizados + ids_resto
        
        chunks = [todos_ids[i:i + CHUNK_SIZE] for i in range(0, len(todos_ids), CHUNK_SIZE)]
        
        log(f"\n{'='*80}")
        log(f"🚀 FASE 1: COLETA ADAPTATIVA")
        log(f"{'='*80}")
        log(f"  Total IDs: {total_ids:,}")
        log(f"  Chunks: {len(chunks)}")
        log(f"  Concorrência inicial: {coletor.concurrent_atual}")
        log(f"{'='*80}\n")
        
        for i, chunk in enumerate(chunks, 1):
            continue_coleta = await coletor.coletar_chunk_adaptativo(i, len(chunks), chunk)
            
            if not continue_coleta:
                break
        
        # Relatório final da Fase 1
        tempo_fase1 = time.time() - tempo_inicio
        log(f"\n{'='*80}")
        log(f"✓ FASE 1 CONCLUÍDA")
        log(f"{'='*80}")
        log(f"  Tempo: {tempo_fase1/60:.1f} min")
        log(f"  Coletados: {coletor.stats['coletados']:,}")
        log(f"  Vazios: {coletor.stats['vazios']:,}")
        log(f"  Retry queue: {len(coletor.retry_queue):,}")
        log(f"{'='*80}")
        
        # FASE 2
        tempo_restante = MAX_EXECUTION_TIME - (time.time() - tempo_inicio)
        if tempo_restante > 120:
            await coletor.processar_retries(max_retries=2)
        else:
            log(f"\n⏰ Pulando retries - tempo restante: {tempo_restante:.0f}s")
        
        tempo_total = time.time() - tempo_inicio
        return coletor, tempo_total
    
    finally:
        coletor.heartbeat.stop()

# ========================================
# ENVIO
# ========================================
def enviar_dados(membros, tempo_total, stats):
    if not membros:
        log("⚠️  Nenhum membro coletado")
        return False
    
    log(f"\n📤 Enviando {len(membros):,} membros para Google Sheets...")
    
    relatorio = [["ID", "NOME", "IGREJA_SELECIONADA", "CARGO/MINISTERIO", "NÍVEL", "INSTRUMENTO", "TONALIDADE"]]
    
    for m in membros:
        linha = [
            str(m.get('id', '')),
            m.get('nome', ''),
            m.get('igreja_selecionada', ''),
            m.get('cargo_ministerio', ''),
            m.get('nivel', ''),
            m.get('instrumento', ''),
            m.get('tonalidade', '')
        ]
        relatorio.append(linha[:7])
    
    payload = {
        "tipo": f"membros_{INSTANCIA_ID}",
        "relatorio_formatado": relatorio,
        "metadata": {
            "instancia": INSTANCIA_ID,
            "total_coletados": len(membros),
            "total_vazios": stats['vazios'],
            "total_erros": stats['erros'],
            "auth_errors": stats['auth_errors'],
            "rate_limits": stats['rate_limits'],
            "timeout_errors": stats['timeout_errors'],
            "tempo_min": round(tempo_total/60, 2),
            "estrategia": "adaptive_smart_v3_realtime",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC")
        }
    }
    
    try:
        import requests
        log("  ➤ Enviando payload...")
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
        
        if response.status_code == 200:
            log("✅ Dados enviados com sucesso!")
            return True
        else:
            log(f"✗ Erro HTTP {response.status_code}")
            return False
    except Exception as e:
        log(f"✗ Erro ao enviar: {e}")
        return False

# ========================================
# MAIN
# ========================================
def main():
    log("=" * 80)
    log("🧠 COLETOR ADAPTATIVO v3 - REAL-TIME OUTPUT")
    log("=" * 80)
    log(f"📊 Range: {RANGE_INICIO:,} → {RANGE_FIM:,}")
    log(f"⚡ Estratégia: Adaptativa com Circuit Breaker")
    log(f"🎯 Concorrência: {CONCURRENT_MINIMO}-{CONCURRENT_MAXIMO}")
    log(f"⏰ Timeout: {MAX_EXECUTION_TIME/60:.0f} min")
    log("=" * 80)

if not EMAIL or not SENHA:
        log("✗ Credenciais não encontradas")
        sys.exit(1)
    
    # Executar
    try:
        coletor, tempo_total = asyncio.run(executar_coleta_adaptativa())
    except KeyboardInterrupt:
        log("\n⚠️ EXECUÇÃO INTERROMPIDA PELO USUÁRIO")
        sys.exit(1)
    except Exception as e:
        log(f"\n❌ ERRO CRÍTICO: {e}")
        import traceback
        log(traceback.format_exc())
        sys.exit(1)
    
    # Relatório final
    log("\n" + "=" * 80)
    log("📊 RELATÓRIO FINAL")
    log("=" * 80)
    log(f"✅ Membros coletados: {coletor.stats['coletados']:,}")
    log(f"⚪ IDs vazios: {coletor.stats['vazios']:,}")
    log(f"❌ Erros finais: {coletor.stats['erros']:,}")
    log(f"⚠️  Auth errors: {coletor.stats['auth_errors']:,}")
    log(f"🚫 Rate limits: {coletor.stats['rate_limits']:,}")
    log(f"⏱️  Timeouts: {coletor.stats['timeout_errors']:,}")
    log(f"⏱️  Tempo total: {tempo_total/60:.2f} min")
    
    if tempo_total > 0:
        velocidade = (RANGE_FIM - RANGE_INICIO + 1) / (tempo_total/60)
        log(f"⚡ Velocidade média: {velocidade:.0f} IDs/min")
    
    if coletor.stats['coletados'] > 0:
        taxa_sucesso = (coletor.stats['coletados'] / (RANGE_FIM - RANGE_INICIO + 1)) * 100
        log(f"📈 Taxa de coleta: {taxa_sucesso:.3f}%")
    
    if tempo_total < 1200:  # < 20 min
        log(f"🏆 META ALCANÇADA! {tempo_total/60:.1f} min < 20 min")
    elif tempo_total < MAX_EXECUTION_TIME:
        log(f"✓ Concluído dentro do limite de tempo")
    else:
        log(f"⚠️ Timeout atingido")
    
    log("=" * 80)
    
    # Enviar
    if coletor.membros:
        enviar_dados(coletor.membros, tempo_total, coletor.stats)
        
        log(f"\n📋 Amostras (primeiros 5 membros):")
        for i, m in enumerate(coletor.membros[:5], 1):
            nome_truncado = m['nome'][:40] if len(m['nome']) > 40 else m['nome']
            log(f"  {i}. [ID:{m['id']:>6}] {nome_truncado}")
    
    log("\n✅ COLETA FINALIZADA\n")
    log("=" * 80)

if __name__ == "__main__":
    main()
