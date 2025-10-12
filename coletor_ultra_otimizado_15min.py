import os
import sys
import re
import asyncio
import httpx
import time
import random
from collections import deque, defaultdict
from tqdm import tqdm
import json

# ========================================
# CONFIGURAÇÕES ADAPTATIVAS
# ========================================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzbkdOTDjGJxabnlJNDX7ZKI4_vh-t5d84MDRp-4FO4KmocRPEVs2jkHL3gjKEG-efF/exec'

RANGE_INICIO = 1
RANGE_FIM = 850000
INSTANCIA_ID = "GHA_smart_adaptive"

# 🧠 CONFIGURAÇÕES INTELIGENTES - ADAPTA-SE AUTOMATICAMENTE
FASE0_SAMPLES = 2000           # Sampling maior para melhor análise
CONCURRENT_INICIAL = 500       # Começa moderado
CONCURRENT_MINIMO = 50         # Mínimo se houver rate limit
CONCURRENT_MAXIMO = 1500       # Máximo se tudo OK

TIMEOUT_BASE = 3.0
CHUNK_SIZE = 2000              # Chunks menores para melhor controle
BATCH_ENVIO = 5000

# Circuit Breaker
ERRO_THRESHOLD = 0.3           # Se >30% de erros, reduz velocidade
SUCESSO_THRESHOLD = 0.9        # Se >90% sucesso, aumenta velocidade

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

# Pool de User-Agents para rotação
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
]

# ========================================
# EXTRAÇÃO OTIMIZADA
# ========================================
def extrair_dados_rapido(html, membro_id):
    """Extração ultra otimizada"""
    if not html or len(html) < 400:
        return None
    
    # Early exit
    nome_check = 'name="nome"'
    if nome_check not in html:
        return None
    
    dados = {'id': membro_id}
    
    nome_match = REGEX_NOME.search(html)
    if not nome_match:
        return None
    dados['nome'] = nome_match.group(1).strip()
    if not dados['nome']:
        return None
    
    # Campos opcionais
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
# COLETOR ADAPTATIVO INTELIGENTE
# ========================================
class ColetorAdaptativo:
    def __init__(self, cookies):
        self.cookies = cookies
        self.membros = []
        self.retry_queue = deque()
        
        # Stats detalhados
        self.stats = {
            'coletados': 0,
            'vazios': 0,
            'erros': 0,
            'auth_errors': 0,
            'rate_limits': 0,
            'processados': 0,
            'timeout_errors': 0
        }
        
        # Controle adaptativo
        self.concurrent_atual = CONCURRENT_INICIAL
        self.lock = asyncio.Lock()
        self.circuit_breaker_ativo = False
        self.ultima_taxa_sucesso = 1.0
        
        # Ranges com dados (descoberto no sampling)
        self.ranges_quentes = []
    
    def get_headers(self):
        """Headers randomizados"""
        return {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none'
        }
    
    async def coletar_id(self, client, membro_id, timeout, semaphore):
        """Coleta individual com detecção inteligente"""
        
        if membro_id in CACHE_VAZIOS:
            return 'cached_empty'
        
        async with semaphore:
            # Jitter aleatório (simula humano)
            if random.random() < 0.1:  # 10% das requisições
                await asyncio.sleep(random.uniform(0.01, 0.05))
            
            try:
                url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
                response = await client.get(url, timeout=timeout)
                
                # Análise de resposta
                if response.status_code == 200:
                    html = response.text
                    
                    # Detectar redirecionamento para login
                    if 'name="login"' in html or 'name="password"' in html:
                        async with self.lock:
                            self.stats['auth_errors'] += 1
                        return 'auth_error'
                    
                    # Tentar extrair dados
                    nome_presente = 'name="nome"' in html
                    if nome_presente:
                        dados = extrair_dados_rapido(html, membro_id)
                        if dados:
                            async with self.lock:
                                self.stats['coletados'] += 1
                                self.membros.append(dados)
                            return 'ok'
                    
                    # ID vazio
                    CACHE_VAZIOS.add(membro_id)
                    async with self.lock:
                        self.stats['vazios'] += 1
                    return 'vazio'
                
                elif response.status_code == 429:
                    # Rate limit detectado!
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
            except httpx.ConnectError:
                return 'retry'
            except Exception as e:
                return 'retry'
    
    async def fase0_sampling_inteligente(self):
        """FASE 0: Sampling com análise de densidade por range"""
        print(f"\n🔍 FASE 0: SAMPLING INTELIGENTE ({FASE0_SAMPLES} IDs)")
        
        # Divide o range em 10 sub-ranges
        total_range = RANGE_FIM - RANGE_INICIO + 1
        subrange_size = total_range // 10
        
        samples_per_subrange = FASE0_SAMPLES // 10
        
        samples = []
        range_densities = []
        
        for i in range(10):
            start = RANGE_INICIO + (i * subrange_size)
            end = start + subrange_size
            
            # Amostra aleatória deste sub-range
            range_samples = random.sample(range(start, min(end, RANGE_FIM + 1)), 
                                         min(samples_per_subrange, end - start))
            samples.extend(range_samples)
        
        # Testa as amostras
        limits = httpx.Limits(
            max_keepalive_connections=100,
            max_connections=200,
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
            
            semaphore = asyncio.Semaphore(200)
            tasks = [self.coletar_id(client, mid, 3.0, semaphore) for mid in samples]
            
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Análise de resultados
            validos = sum(1 for r in resultados if r == 'ok')
            vazios = sum(1 for r in resultados if r == 'vazio')
            auth_errors = sum(1 for r in resultados if r == 'auth_error')
            rate_limits = sum(1 for r in resultados if r == 'rate_limit')
            
            densidade_geral = (validos / len(samples)) * 100
            
            print(f"\n📊 RESULTADO DO SAMPLING:")
            print(f"  ✅ Válidos: {validos} ({densidade_geral:.2f}%)")
            print(f"  ⚪ Vazios: {vazios}")
            print(f"  ⚠️ Auth errors: {auth_errors}")
            print(f"  🚫 Rate limits: {rate_limits}")
            
            # Validação crítica
            if auth_errors > len(samples) * 0.5:
                print(f"\n❌ CRÍTICO: {auth_errors} erros de autenticação!")
                return -1
            
            if rate_limits > len(samples) * 0.2:
                print(f"\n⚠️  WARNING: {rate_limits} rate limits detectados!")
                print("   Reduzindo concorrência inicial...")
                self.concurrent_atual = CONCURRENT_MINIMO
                self.circuit_breaker_ativo = True
            
            # Analisa densidade por sub-range
            print(f"\n📍 ANÁLISE DE DENSIDADE POR RANGE:")
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
                    print(f"  {emoji} Range {start:>7,}-{end:>7,}: {densidade:>5.1f}% válidos")
            
            # Marca ranges "quentes" (>5% de densidade)
            self.ranges_quentes = [(s, e) for s, e, d in range_densities if d > 5]
            
            if validos > 0:
                expectativa = int(total_range * densidade_geral / 100)
                print(f"\n✓ Expectativa total: ~{expectativa:,} membros")
            
            return densidade_geral
    
    async def ajustar_concorrencia(self, taxa_sucesso):
        """Ajusta concorrência dinamicamente"""
        async with self.lock:
            if taxa_sucesso > SUCESSO_THRESHOLD and not self.circuit_breaker_ativo:
                # Tudo OK, pode aumentar
                novo = min(self.concurrent_atual + 100, CONCURRENT_MAXIMO)
                if novo != self.concurrent_atual:
                    print(f"  ⬆️  Aumentando concorrência: {self.concurrent_atual} → {novo}")
                    self.concurrent_atual = novo
            
            elif taxa_sucesso < ERRO_THRESHOLD:
                # Muitos erros, reduzir
                novo = max(self.concurrent_atual - 100, CONCURRENT_MINIMO)
                if novo != self.concurrent_atual:
                    print(f"  ⬇️  Reduzindo concorrência: {self.concurrent_atual} → {novo}")
                    self.concurrent_atual = novo
                    self.circuit_breaker_ativo = True
            
            elif taxa_sucesso > 0.7 and self.circuit_breaker_ativo:
                # Recuperando, pode desativar circuit breaker
                print(f"  ✅ Circuit breaker desativado")
                self.circuit_breaker_ativo = False
            
            self.ultima_taxa_sucesso = taxa_sucesso
    
    async def coletar_chunk_adaptativo(self, ids_chunk, pbar):
        """Coleta um chunk com controle adaptativo"""
        
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
            
            # Análise de resultados
            sucessos = sum(1 for r in resultados if r == 'ok')
            vazios = sum(1 for r in resultados if r in ['vazio', 'cached_empty'])
            retries = sum(1 for r in resultados if r == 'retry')
            rate_limits = sum(1 for r in resultados if r == 'rate_limit')
            
            # Adiciona retries à fila
            for mid, resultado in zip(ids_chunk, resultados):
                if resultado in ['retry', 'rate_limit']:
                    self.retry_queue.append(mid)
            
            # Taxa de sucesso (considera vazio como sucesso)
            taxa_sucesso = (sucessos + vazios) / len(resultados)
            
            # Ajusta concorrência
            await self.ajustar_concorrencia(taxa_sucesso)
            
            self.stats['processados'] += len(ids_chunk)
            
            if pbar:
                pbar.update(len(ids_chunk))
                pbar.set_postfix({
                    'OK': self.stats['coletados'],
                    'Concurrent': self.concurrent_atual,
                    'Retry': len(self.retry_queue)
                })
            
            # Backoff se rate limit
            if rate_limits > len(resultados) * 0.1:
                wait_time = random.uniform(0.5, 2.0)
                await asyncio.sleep(wait_time)
    
    async def processar_retries(self, max_retries=2):
        """Processa fila de retry com backoff"""
        if not self.retry_queue:
            return
        
        print(f"\n🔄 PROCESSANDO RETRIES ({len(self.retry_queue):,} IDs)")
        
        tentativa = 0
        while self.retry_queue and tentativa < max_retries:
            tentativa += 1
            batch_size = min(5000, len(self.retry_queue))
            batch = [self.retry_queue.popleft() for _ in range(batch_size)]
            
            print(f"  Tentativa {tentativa}/{max_retries}: {len(batch):,} IDs")
            
            # Reduz concorrência para retries
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
                
                # Re-adiciona falhas (se não for última tentativa)
                if tentativa < max_retries:
                    for mid, resultado in zip(batch, resultados):
                        if resultado in ['retry', 'rate_limit']:
                            self.retry_queue.append(mid)
                else:
                    # Última tentativa, conta como erro
                    erros_finais = sum(1 for r in resultados if r in ['retry', 'rate_limit'])
                    self.stats['erros'] += erros_finais
            
            # Backoff entre tentativas
            if self.retry_queue and tentativa < max_retries:
                await asyncio.sleep(2.0)
    
    def salvar_checkpoint(self, fase, progresso):
        """Salva checkpoint"""
        checkpoint = {
            'fase': fase,
            'progresso': progresso,
            'stats': self.stats,
            'timestamp': time.time()
        }
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f)

# ========================================
# LOGIN ASYNC
# ========================================
async def login():
    """Login com Playwright Async"""
    print("🔐 Realizando login...")
    try:
        from playwright.async_api import async_playwright
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
            )
            context = await browser.new_context(
                user_agent=random.choice(USER_AGENTS)
            )
            page = await context.new_page()
            await page.goto(URL_INICIAL, timeout=30000)
            await page.fill('input[name="login"]', EMAIL)
            await page.fill('input[name="password"]', SENHA)
            await page.click('button[type="submit"]')
            await page.wait_for_selector("nav", timeout=20000)
            cookies = {c['name']: c['value'] for c in await context.cookies()}
            await browser.close()
            print("✓ Login realizado\n")
            return cookies
    except Exception as e:
        print(f"✗ Erro no login: {e}")
        return None

# ========================================
# EXECUÇÃO ADAPTATIVA
# ========================================
async def executar_coleta_adaptativa():
    """Pipeline adaptativo inteligente"""
    
    cookies = await login()
    if not cookies:
        sys.exit(1)
    
    coletor = ColetorAdaptativo(cookies)
    tempo_inicio = time.time()
    
    # FASE 0: Sampling inteligente
    densidade = await coletor.fase0_sampling_inteligente()
    
    if densidade == -1:
        print("\n❌ ABORTANDO: Problema crítico de autenticação")
        sys.exit(1)
    
    # FASE 1: Coleta adaptativa
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    todos_ids = list(range(RANGE_INICIO, RANGE_FIM + 1))
    
    # Prioriza ranges quentes (se detectados)
    if coletor.ranges_quentes:
        print(f"\n🎯 PRIORIZANDO {len(coletor.ranges_quentes)} RANGES QUENTES")
        ids_priorizados = []
        for start, end in coletor.ranges_quentes:
            ids_priorizados.extend(range(start, end))
        
        # Adiciona resto
        ids_resto = [i for i in todos_ids if i not in ids_priorizados]
        todos_ids = ids_priorizados + ids_resto
    
    chunks = [todos_ids[i:i + CHUNK_SIZE] for i in range(0, len(todos_ids), CHUNK_SIZE)]
    
    print(f"\n🚀 FASE 1: COLETA ADAPTATIVA")
    print(f"  Concorrência inicial: {coletor.concurrent_atual}")
    print(f"  Chunks: {len(chunks)}")
    
    with tqdm(total=total_ids, desc="Coleta", unit="ID", ncols=100, colour='cyan') as pbar:
        for i, chunk in enumerate(chunks):
            await coletor.coletar_chunk_adaptativo(chunk, pbar)
            
            # Checkpoint periódico
            if (i + 1) % 50 == 0:
                coletor.salvar_checkpoint('fase1', i + 1)
    
    tempo_fase1 = time.time() - tempo_inicio
    print(f"\n✓ Fase 1: {tempo_fase1:.1f}s | Coletados: {coletor.stats['coletados']:,}")
    print(f"  Vazios: {coletor.stats['vazios']:,} | Retry queue: {len(coletor.retry_queue):,}")
    
    # FASE 2: Retries
    await coletor.processar_retries(max_retries=2)
    
    tempo_total = time.time() - tempo_inicio
    
    return coletor, tempo_total

# ========================================
# ENVIO OTIMIZADO
# ========================================
def enviar_dados(membros, tempo_total, stats):
    """Envio para Google Sheets"""
    if not membros:
        print("⚠️  Nenhum membro coletado")
        return False
    
    print(f"\n📤 Enviando {len(membros):,} membros...")
    
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
            "estrategia": "adaptive_smart_crawler",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC")
        }
    }
    
    try:
        import requests
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
        
        if response.status_code == 200:
            print("✅ Dados enviados com sucesso!")
            return True
        else:
            print(f"✗ Erro HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Erro ao enviar: {e}")
        return False

# ========================================
# MAIN
# ========================================
def main():
    print("=" * 80)
    print("🧠 COLETOR ADAPTATIVO INTELIGENTE - META: <20 MINUTOS")
    print("=" * 80)
    print(f"📊 Range: {RANGE_INICIO:,} → {RANGE_FIM:,}")
    print(f"⚡ Estratégia: Adaptativa com Circuit Breaker")
    print(f"🎯 Concorrência: {CONCURRENT_MINIMO}-{CONCURRENT_MAXIMO} (dinâmica)")
    print("=" * 80)
    
    if not EMAIL or not SENHA:
        print("✗ Credenciais não encontradas")
        sys.exit(1)
    
    # Executar
    coletor, tempo_total = asyncio.run(executar_coleta_adaptativa())
    
    # Relatório final
    print("\n" + "=" * 80)
    print("📊 RELATÓRIO FINAL")
    print("=" * 80)
    print(f"✅ Membros coletados: {coletor.stats['coletados']:,}")
    print(f"⚪ IDs vazios: {coletor.stats['vazios']:,}")
    print(f"❌ Erros finais: {coletor.stats['erros']:,}")
    print(f"⚠️  Auth errors: {coletor.stats['auth_errors']:,}")
    print(f"🚫 Rate limits: {coletor.stats['rate_limits']:,}")
    print(f"⏱️  Timeouts: {coletor.stats['timeout_errors']:,}")
    print(f"⏱️  Tempo total: {tempo_total/60:.2f} min")
    print(f"⚡ Velocidade: {(RANGE_FIM - RANGE_INICIO + 1) / (tempo_total/60):.0f} IDs/min")
    
    taxa_sucesso = (coletor.stats['coletados'] / (RANGE_FIM - RANGE_INICIO + 1)) * 100
    print(f"📈 Taxa coleta: {taxa_sucesso:.3f}%")
    
    if tempo_total < 1200:  # < 20 min
        print(f"🏆 META ALCANÇADA! {tempo_total/60:.1f} min < 20 min")
    
    print("=" * 80)
    
    # Enviar
    if coletor.membros:
        enviar_dados(coletor.membros, tempo_total, coletor.stats)
        
        print(f"\n📋 Amostras (5 primeiros):")
        for i, m in enumerate(coletor.membros[:5], 1):
            print(f"  {i}. [{m['id']:>6}] {m['nome'][:40]:<40}")
    
    print("\n✅ COLETA FINALIZADA\n")

if __name__ == "__main__":
    main()
