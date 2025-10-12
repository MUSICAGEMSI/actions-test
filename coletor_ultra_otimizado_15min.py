import os
import sys
import re
import asyncio
import httpx
import time
import random
from playwright.sync_api import sync_playwright
from collections import deque, defaultdict
from tqdm import tqdm
import json

# ========================================
# CONFIGURAÇÕES ULTRA OTIMIZADAS
# ========================================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzbkdOTDjGJxabnlJNDX7ZKI4_vh-t5d84MDRp-4FO4KmocRPEVs2jkHL3gjKEG-efF/exec'

RANGE_INICIO = 1
RANGE_FIM = 850000
INSTANCIA_ID = "GHA_ultra_fast"

# 🚀 CONFIGURAÇÕES INSANAS - META: 15 MINUTOS
FASE0_SAMPLES = 1000           # Sampling inicial
FASE1_CONCURRENT = 2000        # Ultra agressivo
FASE1_TIMEOUT = 1.0            # 1 segundo
FASE2_CONCURRENT = 800         # Retry moderado
FASE2_TIMEOUT = 3.0            # 3 segundos
FASE3_CONCURRENT = 300         # Garantia final
FASE3_TIMEOUT = 8.0            # 8 segundos

CHUNK_SIZE = 5000              # Chunks menores para paralelismo
BATCH_ENVIO = 5000             # Envio em batches paralelos
CHECKPOINT_INTERVAL = 100000   # Salva a cada 100K

# ========================================
# REGEX PRÉ-COMPILADAS
# ========================================
REGEX_NOME = re.compile(r'name="nome"[^>]*value="([^"]*)"')
REGEX_IGREJA = re.compile(r'igreja_selecionada\s*\(\s*(\d+)\s*\)')
REGEX_CARGO = re.compile(r'id_cargo"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_NIVEL = re.compile(r'id_nivel"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_INSTRUMENTO = re.compile(r'id_instrumento"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_TONALIDADE = re.compile(r'id_tonalidade"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)

# ========================================
# CACHE E TRACKING
# ========================================
CACHE_VAZIOS = set()
CHECKPOINT_FILE = "checkpoint_coleta.json"

# ========================================
# EXTRAÇÃO OTIMIZADA
# ========================================
def extrair_dados_rapido(html, membro_id):
    """Extração ultra otimizada - early exit"""
    if not html or len(html) < 400:
        return None
    
    # Early exit: verifica só substring crítica
    if 'name="nome"' not in html:
        return None
    
    dados = {'id': membro_id}
    
    # Nome (crítico)
    nome_match = REGEX_NOME.search(html)
    if not nome_match:
        return None
    dados['nome'] = nome_match.group(1).strip()
    if not dados['nome']:
        return None
    
    # Campos opcionais (busca rápida)
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
# COLETOR ULTRA RÁPIDO
# ========================================
class ColetorUltraRapido:
    def __init__(self, cookies):
        self.cookies = cookies
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0'
        }
        
        self.membros = []
        self.retry_fase2 = set()
        self.retry_fase3 = set()
        
        # Stats atômicos (sem lock excessivo)
        self.stats = {
            'coletados': 0,
            'vazios': 0,
            'erros': 0,
            'processados': 0
        }
        
        self.lock = asyncio.Lock()
        self.batch_buffer = []
    
    async def coletar_id(self, client, membro_id, timeout, semaphore):
        """Coleta individual - máxima performance"""
        
        # Cache hit
        if membro_id in CACHE_VAZIOS:
            return None
        
        async with semaphore:
            try:
                url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
                response = await client.get(url, timeout=timeout)
                
                if response.status_code == 200:
                    html = response.text
                    
                    # Validação rápida
                    if 'name="nome"' in html:
                        dados = extrair_dados_rapido(html, membro_id)
                        if dados:
                            async with self.lock:
                                self.stats['coletados'] += 1
                                self.membros.append(dados)
                            return 'ok'
                    
                    # Marcar como vazio
                    CACHE_VAZIOS.add(membro_id)
                    return 'vazio'
                
                # Retry necessário
                return 'retry'
                
            except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout):
                return 'retry'
            except:
                return 'retry'
    
    async def fase0_sampling(self):
        """FASE 0: Sampling rápido para estimar densidade"""
        print(f"\n🔍 FASE 0: SAMPLING ({FASE0_SAMPLES} IDs aleatórios)")
        
        samples = random.sample(range(RANGE_INICIO, RANGE_FIM + 1), FASE0_SAMPLES)
        
        limits = httpx.Limits(
            max_keepalive_connections=100,
            max_connections=200,
            keepalive_expiry=60
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            limits=limits,
            http2=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(200)
            tasks = [self.coletar_id(client, mid, 2.0, semaphore) for mid in samples]
            
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            validos = sum(1 for r in resultados if r == 'ok')
            densidade = (validos / FASE0_SAMPLES) * 100
            
            print(f"✓ Densidade estimada: {densidade:.2f}% de IDs válidos")
            print(f"✓ Expectativa: ~{int((RANGE_FIM - RANGE_INICIO + 1) * densidade / 100):,} membros")
            
            return densidade
    
    async def fase1_ultra_rapida(self, ids_chunk, pbar):
        """FASE 1: Fire and forget - 2K concurrent"""
        
        limits = httpx.Limits(
            max_keepalive_connections=500,
            max_connections=2500,
            keepalive_expiry=120
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            limits=limits,
            http2=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(FASE1_CONCURRENT)
            
            tasks = [self.coletar_id(client, mid, FASE1_TIMEOUT, semaphore) for mid in ids_chunk]
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            for mid, resultado in zip(ids_chunk, resultados):
                if resultado == 'retry':
                    self.retry_fase2.add(mid)
                elif resultado == 'vazio':
                    self.stats['vazios'] += 1
                elif resultado == 'ok':
                    pass  # Já contabilizado
                
                self.stats['processados'] += 1
                
                if pbar and self.stats['processados'] % 100 == 0:
                    pbar.update(100)
                    pbar.set_postfix({
                        'OK': self.stats['coletados'],
                        'Retry': len(self.retry_fase2)
                    })
    
    async def fase2_retry(self, pbar):
        """FASE 2: Retry inteligente - 800 concurrent"""
        
        if not self.retry_fase2:
            return
        
        ids_retry = list(self.retry_fase2)
        self.retry_fase2.clear()
        
        print(f"\n🔄 FASE 2: RETRY ({len(ids_retry):,} IDs)")
        
        limits = httpx.Limits(
            max_keepalive_connections=300,
            max_connections=1000,
            keepalive_expiry=90
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            limits=limits,
            http2=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(FASE2_CONCURRENT)
            
            tasks = [self.coletar_id(client, mid, FASE2_TIMEOUT, semaphore) for mid in ids_retry]
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            for mid, resultado in zip(ids_retry, resultados):
                if resultado == 'retry':
                    self.retry_fase3.add(mid)
                elif resultado == 'vazio':
                    self.stats['vazios'] += 1
                
                if pbar:
                    pbar.update(1)
    
    async def fase3_garantia(self, pbar):
        """FASE 3: Garantia final - 300 concurrent"""
        
        if not self.retry_fase3:
            return
        
        ids_retry = list(self.retry_fase3)
        self.retry_fase3.clear()
        
        print(f"\n🎯 FASE 3: GARANTIA FINAL ({len(ids_retry):,} IDs)")
        
        limits = httpx.Limits(
            max_keepalive_connections=150,
            max_connections=400,
            keepalive_expiry=60
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            limits=limits,
            http2=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(FASE3_CONCURRENT)
            
            tasks = [self.coletar_id(client, mid, FASE3_TIMEOUT, semaphore) for mid in ids_retry]
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            for resultado in resultados:
                if resultado == 'retry':
                    self.stats['erros'] += 1
                elif resultado == 'vazio':
                    self.stats['vazios'] += 1
                
                if pbar:
                    pbar.update(1)
    
    def salvar_checkpoint(self, fase, progresso):
        """Salva checkpoint para recuperação"""
        checkpoint = {
            'fase': fase,
            'progresso': progresso,
            'coletados': self.stats['coletados'],
            'timestamp': time.time()
        }
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f)

# ========================================
# LOGIN OTIMIZADO
# ========================================
def login():
    """Login com Playwright"""
    print("🔐 Realizando login...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
            )
            context = browser.new_context(
                user_agent='Mozilla/5.0 (X11; Linux x64) AppleWebKit/537.36'
            )
            page = context.new_page()
            page.goto(URL_INICIAL, timeout=30000)
            page.fill('input[name="login"]', EMAIL)
            page.fill('input[name="password"]', SENHA)
            page.click('button[type="submit"]')
            page.wait_for_selector("nav", timeout=20000)
            cookies = {c['name']: c['value'] for c in context.cookies()}
            browser.close()
            print("✓ Login realizado\n")
            return cookies
    except Exception as e:
        print(f"✗ Erro no login: {e}")
        return None

# ========================================
# EXECUÇÃO PRINCIPAL
# ========================================
async def executar_coleta_ultra():
    """Pipeline completo ultra otimizado"""
    
    cookies = login()
    if not cookies:
        sys.exit(1)
    
    coletor = ColetorUltraRapido(cookies)
    
    tempo_inicio = time.time()
    
    # FASE 0: Sampling
    await coletor.fase0_sampling()
    
    # FASE 1: Ultra rápida
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    todos_ids = list(range(RANGE_INICIO, RANGE_FIM + 1))
    chunks = [todos_ids[i:i + CHUNK_SIZE] for i in range(0, len(todos_ids), CHUNK_SIZE)]
    
    print(f"\n🚀 FASE 1: COLETA ULTRA RÁPIDA ({FASE1_CONCURRENT} concurrent)")
    with tqdm(total=total_ids, desc="Fase 1", unit="ID", ncols=100, colour='red') as pbar:
        for i, chunk in enumerate(chunks):
            await coletor.fase1_ultra_rapida(chunk, pbar)
            
            # Checkpoint periódico
            if (i + 1) % 20 == 0:
                coletor.salvar_checkpoint('fase1', i + 1)
    
    tempo_fase1 = time.time() - tempo_inicio
    print(f"✓ Fase 1: {tempo_fase1:.1f}s | Coletados: {coletor.stats['coletados']:,} | Retry: {len(coletor.retry_fase2):,}")
    
    # FASE 2: Retry
    if coletor.retry_fase2:
        with tqdm(total=len(coletor.retry_fase2), desc="Fase 2", unit="ID", ncols=100, colour='yellow') as pbar:
            await coletor.fase2_retry(pbar)
        
        tempo_fase2 = time.time() - tempo_inicio - tempo_fase1
        print(f"✓ Fase 2: {tempo_fase2:.1f}s | Coletados: {coletor.stats['coletados']:,} | Retry: {len(coletor.retry_fase3):,}")
    
    # FASE 3: Garantia
    if coletor.retry_fase3:
        with tqdm(total=len(coletor.retry_fase3), desc="Fase 3", unit="ID", ncols=100, colour='green') as pbar:
            await coletor.fase3_garantia(pbar)
        
        tempo_fase3 = time.time() - tempo_inicio
        print(f"✓ Fase 3: {tempo_fase3:.1f}s | Coletados: {coletor.stats['coletados']:,}")
    
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
        "tipo": f"membros_{INSTANCIA_ID}_ultra",
        "relatorio_formatado": relatorio,
        "metadata": {
            "instancia": INSTANCIA_ID,
            "total_coletados": len(membros),
            "total_vazios": stats['vazios'],
            "total_erros": stats['erros'],
            "tempo_min": round(tempo_total/60, 2),
            "velocidade_ids_min": round((RANGE_FIM - RANGE_INICIO + 1) / (tempo_total/60), 0),
            "concurrent_max": FASE1_CONCURRENT,
            "estrategia": "3_fases_ultra_otimizado",
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
    print("🔥 COLETOR ULTRA OTIMIZADO - META: 15 MINUTOS")
    print("=" * 80)
    print(f"📊 Range: {RANGE_INICIO:,} → {RANGE_FIM:,}")
    print(f"⚡ Estratégia: 3 Fases + Sampling")
    print(f"🎯 Max concurrent: {FASE1_CONCURRENT}")
    print("=" * 80)
    
    if not EMAIL or not SENHA:
        print("✗ Credenciais não encontradas")
        sys.exit(1)
    
    # Executar
    coletor, tempo_total = asyncio.run(executar_coleta_ultra())
    
    # Relatório final
    print("\n" + "=" * 80)
    print("📊 RELATÓRIO FINAL")
    print("=" * 80)
    print(f"✅ Membros coletados: {coletor.stats['coletados']:,}")
    print(f"⚪ IDs vazios: {coletor.stats['vazios']:,}")
    print(f"❌ Erros finais: {coletor.stats['erros']:,}")
    print(f"⏱️  Tempo total: {tempo_total/60:.2f} min")
    print(f"⚡ Velocidade: {(RANGE_FIM - RANGE_INICIO + 1) / (tempo_total/60):.0f} IDs/min")
    
    if tempo_total < 900:
        print(f"🏆 META ALCANÇADA! {tempo_total/60:.1f} min < 15 min")
    else:
        print(f"⚠️  {tempo_total/60:.1f} min (ajustar parâmetros)")
    
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
