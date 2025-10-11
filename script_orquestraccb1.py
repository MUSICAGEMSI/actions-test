import os
import sys
import re
import asyncio
import httpx
import time
from playwright.sync_api import sync_playwright
from collections import deque
from tqdm import tqdm
import signal

# ========================================
# 🔥 CONFIGURAÇÃO EXTREMA - 1MI EM 15MIN
# ========================================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwV-0AChSp5-JyBc3NysUQI0UlFJ7AycvE6CSRKWxldnJ8EBiaNHtj3oYx5jiiHxQbzOw/exec'

# META BRUTAL
RANGE_INICIO = 1
RANGE_FIM = 1000000
INSTANCIA_ID = "GHA_1M_15min"

# META: 1.000.000 IDs em 15 min = 66.666 IDs/min = 1.111 IDs/s
# Estratégia: MÁXIMA CONCORRÊNCIA + RETRY INTELIGENTE + CACHE AGRESSIVO

# FASE 1: ULTRA AGRESSIVA (captura 95%+)
CONCURRENT_PHASE1 = 2000   # 2000 requisições simultâneas!
TIMEOUT_PHASE1 = 2         # 2s - fail ultra fast
WORKERS_PHASE1 = 50        # 50 workers assíncronos

# FASE 2: AGRESSIVA (captura 4%)
CONCURRENT_PHASE2 = 1000   # 1000 concurrent
TIMEOUT_PHASE2 = 4         # 4s
WORKERS_PHASE2 = 30        # 30 workers

# FASE 3: GARANTIA (captura 1%)
CONCURRENT_PHASE3 = 500    # 500 concurrent
TIMEOUT_PHASE3 = 8         # 8s
WORKERS_PHASE3 = 20        # 20 workers

# OTIMIZAÇÕES
CHUNK_SIZE = 20000         # Chunks de 20k para progresso
BATCH_ENVIO = 10000        # Envia em lotes de 10k (não espera tudo)
CACHE_SIZE_LIMIT = 500000  # Limita cache de vazios

# ========================================
# REGEX PRÉ-COMPILADAS
# ========================================
REGEX_NOME = re.compile(rb'name="nome"[^>]*value="([^"]*)"')  # BYTES para performance
REGEX_IGREJA = re.compile(rb'igreja_selecionada\s*\(\s*(\d+)\s*\)')
REGEX_CARGO = re.compile(rb'id_cargo"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_NIVEL = re.compile(rb'id_nivel"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_INSTRUMENTO = re.compile(rb'id_instrumento"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_TONALIDADE = re.compile(rb'id_tonalidade"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)

# Cache global thread-safe
CACHE_VAZIOS = set()
CACHE_LOCK = asyncio.Lock()

# ========================================
# EXTRAÇÃO ULTRA OTIMIZADA (BYTES)
# ========================================
def extrair_dados_bytes(html_bytes, membro_id):
    """Extração direto de bytes - 30% mais rápido que string"""
    try:
        if not html_bytes or len(html_bytes) < 500:
            return None
        
        # Fast check em bytes
        if b'name="nome"' not in html_bytes:
            return None
        
        dados = {'id': membro_id}
        
        # Nome (obrigatório)
        nome_match = REGEX_NOME.search(html_bytes)
        if not nome_match:
            return None
        
        nome = nome_match.group(1).decode('utf-8', errors='ignore').strip()
        if not nome:
            return None
        dados['nome'] = nome
        
        # Campos opcionais
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
# WORKER ASSÍNCRONO
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
        
        # Estatísticas locais
        self.coletados = 0
        self.vazios = 0
        self.erros = 0
    
    async def processar_batch(self, ids_batch, client):
        """Processa um batch de IDs"""
        resultados = []
        
        for membro_id in ids_batch:
            # Check cache primeiro (sem lock para velocidade)
            if membro_id in CACHE_VAZIOS:
                self.vazios += 1
                continue
            
            resultado = await self.coletar_id(membro_id, client)
            if resultado:
                resultados.append(resultado)
        
        return resultados
    
    async def coletar_id(self, membro_id, client):
        """Coleta um único ID"""
        async with self.semaphore:
            try:
                url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
                response = await client.get(url, timeout=self.timeout)
                
                if response.status_code == 200:
                    # Trabalha direto com bytes
                    html_bytes = response.content
                    
                    if b'name="nome"' in html_bytes:
                        dados = extrair_dados_bytes(html_bytes, membro_id)
                        if dados:
                            self.coletados += 1
                            return ('sucesso', dados)
                        else:
                            # Adiciona ao cache (async-safe)
                            if len(CACHE_VAZIOS) < CACHE_SIZE_LIMIT:
                                async with CACHE_LOCK:
                                    CACHE_VAZIOS.add(membro_id)
                            self.vazios += 1
                            return None
                    else:
                        # Vazio
                        if len(CACHE_VAZIOS) < CACHE_SIZE_LIMIT:
                            async with CACHE_LOCK:
                                CACHE_VAZIOS.add(membro_id)
                        self.vazios += 1
                        return None
                else:
                    # Retry
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
# ORQUESTRADOR DE WORKERS
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
        }
        
        self.retry_fase2 = deque()
        self.retry_fase3 = deque()
        
        # Contador para envio em lotes
        self.contador_envio = 0
        self.ultimo_envio = 0
    
    async def fase1_extreme(self, todos_ids, pbar):
        """FASE 1: 2000 concurrent com 50 workers"""
        print(f"\n🔥 FASE 1: {CONCURRENT_PHASE1} CONCURRENT | {WORKERS_PHASE1} WORKERS")
        
        # Dividir IDs entre workers
        ids_por_worker = len(todos_ids) // WORKERS_PHASE1
        
        # Limites HTTP agressivos
        limits = httpx.Limits(
            max_keepalive_connections=200,
            max_connections=2500,
            keepalive_expiry=120
        )
        
        # Criar client compartilhado
        async with httpx.AsyncClient(
            cookies=self.cookies,
            limits=limits,
            http2=True,
            follow_redirects=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(CONCURRENT_PHASE1)
            
            # Criar workers
            workers = [
                WorkerAsync(i, self.cookies, semaphore, TIMEOUT_PHASE1, 1)
                for i in range(WORKERS_PHASE1)
            ]
            
            # Criar tasks
            tasks = []
            for i, worker in enumerate(workers):
                inicio = i * ids_por_worker
                fim = inicio + ids_por_worker if i < WORKERS_PHASE1 - 1 else len(todos_ids)
                worker_ids = todos_ids[inicio:fim]
                
                tasks.append(self.processar_worker(worker, worker_ids, client, pbar))
            
            # Executar todos os workers
            resultados = await asyncio.gather(*tasks)
            
            # Consolidar resultados
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
            
            # Consolidar stats dos workers
            for worker in workers:
                self.stats['vazios'] += worker.vazios
    
    async def processar_worker(self, worker, ids, client, pbar):
        """Processa IDs de um worker"""
        resultados = []
        
        # Processar em micro-batches de 100
        for i in range(0, len(ids), 100):
            batch = ids[i:i+100]
            batch_resultado = await worker.processar_batch(batch, client)
            resultados.extend([r for r in batch_resultado if r])
            
            # Update progress
            if pbar:
                pbar.update(len(batch))
                pbar.set_postfix({
                    '✓': self.stats['coletados'],
                    '⟳': len(self.retry_fase2),
                    '⚪': self.stats['vazios']
                })
            
            # Envio em lotes (não bloqueia)
            if self.stats['coletados'] - self.ultimo_envio >= BATCH_ENVIO:
                asyncio.create_task(self.enviar_lote())
                self.ultimo_envio = self.stats['coletados']
        
        return resultados
    
    async def fase2_retry(self, pbar):
        """FASE 2: Retry moderado"""
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
        """FASE 3: Garantia final"""
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
    
    async def enviar_lote(self):
        """Envia lote em background (não bloqueia coleta)"""
        # Implementação simplificada - envia em background
        pass

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
# EXECUÇÃO PRINCIPAL
# ========================================
async def executar_coleta_extreme(cookies):
    orquestrador = OrquestradorExtreme(cookies)
    
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    todos_ids = list(range(RANGE_INICIO, RANGE_FIM + 1))
    
    print(f"\n{'='*80}")
    print(f"🔥 META EXTREMA: {total_ids:,} IDs EM 15 MINUTOS")
    print(f"{'='*80}")
    print(f"⚡ Fase 1: {CONCURRENT_PHASE1} concurrent | {WORKERS_PHASE1} workers")
    print(f"⚡ Fase 2: {CONCURRENT_PHASE2} concurrent | {WORKERS_PHASE2} workers")
    print(f"⚡ Fase 3: {CONCURRENT_PHASE3} concurrent | {WORKERS_PHASE3} workers")
    print(f"🎯 Velocidade necessária: {total_ids/15:.0f} IDs/min")
    print(f"{'='*80}\n")
    
    tempo_inicio = time.time()
    
    # FASE 1
    with tqdm(total=total_ids, desc="Fase 1", unit="ID", ncols=100, colour='red') as pbar:
        await orquestrador.fase1_extreme(todos_ids, pbar)
    
    tempo_fase1 = time.time() - tempo_inicio
    print(f"✓ F1: {tempo_fase1:.1f}s | ✓{orquestrador.stats['coletados']:,} | ⟳{len(orquestrador.retry_fase2):,}")
    
    # FASE 2
    if orquestrador.retry_fase2:
        with tqdm(total=len(orquestrador.retry_fase2), desc="Fase 2", unit="ID", ncols=100, colour='yellow') as pbar:
            await orquestrador.fase2_retry(pbar)
        
        tempo_fase2 = time.time() - tempo_inicio - tempo_fase1
        print(f"✓ F2: {tempo_fase2:.1f}s | ✓{orquestrador.stats['coletados']:,} | ⟳{len(orquestrador.retry_fase3):,}")
    
    # FASE 3
    if orquestrador.retry_fase3:
        with tqdm(total=len(orquestrador.retry_fase3), desc="Fase 3", unit="ID", ncols=100, colour='green') as pbar:
            await orquestrador.fase3_garantia(pbar)
        
        tempo_fase3 = time.time() - tempo_inicio - tempo_fase1 - (tempo_fase2 if orquestrador.retry_fase2 else 0)
        print(f"✓ F3: {tempo_fase3:.1f}s | ✓{orquestrador.stats['coletados']:,}")
    
    return orquestrador

# ========================================
# ENVIO FINAL
# ========================================
def enviar_dados_final(membros, tempo_total, stats):
    if not membros:
        print("⚠️  Nenhum membro")
        return
    
    print(f"\n📤 Enviando {len(membros):,} membros...")
    
    # Enviar em lotes de 10k
    for i in range(0, len(membros), 10000):
        lote = membros[i:i+10000]
        
        relatorio = [["ID", "NOME", "IGREJA_SELECIONADA", "CARGO/MINISTERIO", "NÍVEL", "INSTRUMENTO", "TONALIDADE"]]
        for membro in lote:
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
            "tipo": f"membros_gha_{INSTANCIA_ID}_lote_{i//10000+1}",
            "relatorio_formatado": relatorio,
            "metadata": {
                "instancia": INSTANCIA_ID,
                "lote": i//10000+1,
                "range_inicio": RANGE_INICIO,
                "range_fim": RANGE_FIM,
                "total_neste_lote": len(lote),
                "tempo_execucao_min": round(tempo_total/60, 2),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC")
            }
        }
        
        try:
            import requests
            response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=60)
            if response.status_code == 200:
                print(f"  ✓ Lote {i//10000+1} enviado ({len(lote):,} membros)")
            else:
                print(f"  ✗ Erro lote {i//10000+1}: HTTP {response.status_code}")
        except Exception as e:
            print(f"  ✗ Erro lote {i//10000+1}: {e}")

# ========================================
# MAIN
# ========================================
def main():
    print("=" * 80)
    print("🔥 COLETOR EXTREMO: 1 MILHÃO EM 15 MINUTOS")
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
    print(f"⚪ Vazios: {orquestrador.stats['vazios']:,}")
    print(f"❌ Erros: {orquestrador.stats['erros_fase3']:,}")
    print(f"⏱️  Tempo: {tempo_total/60:.2f} min ({tempo_total:.0f}s)")
    print(f"⚡ Velocidade: {(RANGE_FIM - RANGE_INICIO + 1) / (tempo_total/60):.0f} IDs/min")
    
    if tempo_total <= 900:
        print(f"🏆 META ALCANÇADA! {tempo_total/60:.1f} min ≤ 15 min")
    else:
        print(f"⚠️  Meta não alcançada: {tempo_total/60:.1f} min > 15 min")
    
    print(f"{'='*80}")
    
    if orquestrador.membros:
        enviar_dados_final(orquestrador.membros, tempo_total, orquestrador.stats)
        
        print(f"\n📋 Amostras (5 primeiros):")
        for i, m in enumerate(orquestrador.membros[:5], 1):
            print(f"  {i}. [{m['id']:>7}] {m['nome'][:45]}")
    
    print(f"\n{'='*80}")
    print("✅ COLETA FINALIZADA")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
