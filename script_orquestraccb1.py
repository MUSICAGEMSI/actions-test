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
# CONFIGURAÇÕES ULTRA AGRESSIVAS
# ========================================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwV-0AChSp5-JyBc3NysUQI0UlFJ7AycvE6CSRKWxldnJ8EBiaNHtj3oYx5jiiHxQbzOw/exec'

RANGE_INICIO = 1
RANGE_FIM = 10000
INSTANCIA_ID = "GHA_batch_1"

# 🚀 MODO INSANO - META: 15 MINUTOS
CONCURRENT_REQUESTS = 500  # 500 requisições simultâneas
TIMEOUT_ULTRA_FAST = 2     # 2s primeira tentativa
TIMEOUT_FAST = 4           # 4s segunda tentativa
TIMEOUT_CAREFUL = 8        # 8s terceira tentativa
MAX_RETRIES = 3            # 3 tentativas por ID
CHUNK_SIZE = 10000         # Chunks maiores

# Estratégia de Retry Adaptativo
SEMAPHORE_PHASE1 = 500     # Fase 1: Ultra agressivo
SEMAPHORE_PHASE2 = 300     # Fase 2: Moderado
SEMAPHORE_PHASE3 = 150     # Fase 3: Conservador

# ========================================
# REGEX PRÉ-COMPILADAS (OTIMIZADAS)
# ========================================
REGEX_NOME = re.compile(r'name="nome"[^>]*value="([^"]*)"')
REGEX_IGREJA = re.compile(r'igreja_selecionada\s*\(\s*(\d+)\s*\)')
REGEX_CARGO = re.compile(r'id_cargo"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_NIVEL = re.compile(r'id_nivel"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_INSTRUMENTO = re.compile(r'id_instrumento"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_TONALIDADE = re.compile(r'id_tonalidade"[^>]*>.*?selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)

# Cache para IDs já validados como vazios
CACHE_IDS_VAZIOS = set()

# ========================================
# EXTRAÇÃO OTIMIZADA
# ========================================
def extrair_dados(html_content, membro_id):
    """Extração máxima performance - verificações mínimas"""
    try:
        if not html_content or len(html_content) < 500 or 'name="nome"' not in html_content:
            return None
        
        dados = {'id': membro_id}
        
        # Nome (crítico)
        nome_match = REGEX_NOME.search(html_content)
        if not nome_match:
            return None
        dados['nome'] = nome_match.group(1).strip()
        if not dados['nome']:
            return None
        
        # Campos opcionais
        igreja_match = REGEX_IGREJA.search(html_content)
        dados['igreja_selecionada'] = igreja_match.group(1) if igreja_match else ''
        
        cargo_match = REGEX_CARGO.search(html_content)
        dados['cargo_ministerio'] = cargo_match.group(1).strip() if cargo_match else ''
        
        nivel_match = REGEX_NIVEL.search(html_content)
        dados['nivel'] = nivel_match.group(1).strip() if nivel_match else ''
        
        instrumento_match = REGEX_INSTRUMENTO.search(html_content)
        dados['instrumento'] = instrumento_match.group(1).strip() if instrumento_match else ''
        
        tonalidade_match = REGEX_TONALIDADE.search(html_content)
        dados['tonalidade'] = tonalidade_match.group(1).strip() if tonalidade_match else ''
        
        return dados
    except:
        return None

# ========================================
# COLETOR INSANO - 3 FASES DE RETRY
# ========================================
class ColetorInsano:
    def __init__(self, cookies):
        self.cookies = cookies
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1'
        }
        
        self.lock = asyncio.Lock()
        self.stats = {
            'coletados': 0,
            'vazios': 0,
            'erros_fase1': 0,
            'erros_fase2': 0,
            'erros_fase3': 0,
            'retry_fase2': 0,
            'retry_fase3': 0
        }
        
        self.retry_fase2 = deque()
        self.retry_fase3 = deque()
        self.membros = []
    
    async def coletar_id(self, client, membro_id, timeout, semaphore, fase=1):
        """Coleta um ID - ultra otimizado"""
        
        if membro_id in CACHE_IDS_VAZIOS:
            async with self.lock:
                self.stats['vazios'] += 1
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
                            async with self.lock:
                                self.stats['coletados'] += 1
                                self.membros.append(dados)
                            return dados
                        else:
                            CACHE_IDS_VAZIOS.add(membro_id)
                            async with self.lock:
                                self.stats['vazios'] += 1
                            return None
                    else:
                        CACHE_IDS_VAZIOS.add(membro_id)
                        async with self.lock:
                            self.stats['vazios'] += 1
                        return None
                else:
                    return ('retry', membro_id)
                    
            except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError):
                return ('retry', membro_id)
            except Exception:
                return ('retry', membro_id)
        
        return None
    
    async def fase1_ultra_rapida(self, ids_chunk, pbar):
        """FASE 1: Ultra agressiva - 500 concurrent, timeout 2s"""
        
        limits = httpx.Limits(
            max_keepalive_connections=200,
            max_connections=600,
            keepalive_expiry=60
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            limits=limits,
            http2=True,
            follow_redirects=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(SEMAPHORE_PHASE1)
            
            tasks = [self.coletar_id(client, mid, TIMEOUT_ULTRA_FAST, semaphore, fase=1) for mid in ids_chunk]
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            for resultado in resultados:
                if isinstance(resultado, tuple) and resultado[0] == 'retry':
                    self.retry_fase2.append(resultado[1])
                    async with self.lock:
                        self.stats['erros_fase1'] += 1
                
                if pbar:
                    pbar.update(1)
    
    async def fase2_moderada(self, pbar):
        """FASE 2: Moderada - 300 concurrent, timeout 4s"""
        
        if not self.retry_fase2:
            return
        
        ids_retry = list(self.retry_fase2)
        self.retry_fase2.clear()
        
        async with self.lock:
            self.stats['retry_fase2'] = len(ids_retry)
        
        limits = httpx.Limits(
            max_keepalive_connections=150,
            max_connections=350,
            keepalive_expiry=60
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            limits=limits,
            http2=True,
            follow_redirects=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(SEMAPHORE_PHASE2)
            
            tasks = [self.coletar_id(client, mid, TIMEOUT_FAST, semaphore, fase=2) for mid in ids_retry]
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, resultado in enumerate(resultados):
                if isinstance(resultado, tuple) and resultado[0] == 'retry':
                    self.retry_fase3.append(resultado[1])
                    async with self.lock:
                        self.stats['erros_fase2'] += 1
                elif isinstance(resultado, Exception):
                    self.retry_fase3.append(ids_retry[i])
                
                if pbar:
                    pbar.update(1)
    
    async def fase3_garantia(self, pbar):
        """FASE 3: Conservadora - 150 concurrent, timeout 8s"""
        
        if not self.retry_fase3:
            return
        
        ids_retry = list(self.retry_fase3)
        self.retry_fase3.clear()
        
        async with self.lock:
            self.stats['retry_fase3'] = len(ids_retry)
        
        limits = httpx.Limits(
            max_keepalive_connections=80,
            max_connections=180,
            keepalive_expiry=60
        )
        
        async with httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            limits=limits,
            http2=True,
            follow_redirects=True,
            timeout=None
        ) as client:
            
            semaphore = asyncio.Semaphore(SEMAPHORE_PHASE3)
            
            tasks = [self.coletar_id(client, mid, TIMEOUT_CAREFUL, semaphore, fase=3) for mid in ids_retry]
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            
            for resultado in resultados:
                if isinstance(resultado, tuple) and resultado[0] == 'retry':
                    async with self.lock:
                        self.stats['erros_fase3'] += 1
                elif isinstance(resultado, Exception):
                    async with self.lock:
                        self.stats['erros_fase3'] += 1
                
                if pbar:
                    pbar.update(1)

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
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
            context = browser.new_context(
                user_agent='Mozilla/5.0 (X11; Linux x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()
            page.goto(URL_INICIAL, timeout=30000)
            page.fill('input[name="login"]', EMAIL)
            page.fill('input[name="password"]', SENHA)
            page.click('button[type="submit"]')
            page.wait_for_selector("nav", timeout=20000)
            cookies = {cookie['name']: cookie['value'] for cookie in context.cookies()}
            browser.close()
            print("✓ Login realizado")
            return cookies
    except Exception as e:
        print(f"✗ Erro no login: {e}")
        return None

# ========================================
# EXECUÇÃO INSANA - 3 FASES
# ========================================
async def executar_coleta_insana(cookies):
    """Estratégia 3 fases: Ultra Rápida → Moderada → Garantia"""
    
    coletor = ColetorInsano(cookies)
    
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    todos_ids = list(range(RANGE_INICIO, RANGE_FIM + 1))
    
    chunks = [todos_ids[i:i + CHUNK_SIZE] for i in range(0, len(todos_ids), CHUNK_SIZE)]
    
    print(f"\n{'='*80}")
    print(f"🚀 ESTRATÉGIA 3 FASES - META: 15 MINUTOS")
    print(f"{'='*80}")
    print(f"📦 {total_ids:,} IDs → {len(chunks)} chunks de {CHUNK_SIZE:,}")
    print(f"⚡ FASE 1: {SEMAPHORE_PHASE1} concurrent | timeout {TIMEOUT_ULTRA_FAST}s")
    print(f"⚡ FASE 2: {SEMAPHORE_PHASE2} concurrent | timeout {TIMEOUT_FAST}s")
    print(f"⚡ FASE 3: {SEMAPHORE_PHASE3} concurrent | timeout {TIMEOUT_CAREFUL}s")
    print(f"{'='*80}\n")
    
    tempo_inicio = time.time()
    
    # FASE 1: ULTRA RÁPIDA
    print("🔥 FASE 1: COLETA ULTRA RÁPIDA")
    with tqdm(total=total_ids, desc="Fase 1", unit="ID", ncols=100, colour='red') as pbar:
        for chunk in chunks:
            await coletor.fase1_ultra_rapida(chunk, pbar)
            pbar.set_postfix({
                'Coletados': coletor.stats['coletados'],
                'Retry': len(coletor.retry_fase2)
            })
    
    tempo_fase1 = time.time() - tempo_inicio
    print(f"✓ Fase 1: {tempo_fase1:.1f}s | Coletados: {coletor.stats['coletados']:,} | Retry: {len(coletor.retry_fase2):,}")
    
    # FASE 2: MODERADA
    if coletor.retry_fase2:
        print(f"\n🔄 FASE 2: RETRY MODERADO ({len(coletor.retry_fase2):,} IDs)")
        with tqdm(total=len(coletor.retry_fase2), desc="Fase 2", unit="ID", ncols=100, colour='yellow') as pbar:
            await coletor.fase2_moderada(pbar)
        
        tempo_fase2 = time.time() - tempo_inicio - tempo_fase1
        print(f"✓ Fase 2: {tempo_fase2:.1f}s | Coletados: {coletor.stats['coletados']:,} | Retry: {len(coletor.retry_fase3):,}")
    
    # FASE 3: GARANTIA
    if coletor.retry_fase3:
        print(f"\n🎯 FASE 3: GARANTIA FINAL ({len(coletor.retry_fase3):,} IDs)")
        with tqdm(total=len(coletor.retry_fase3), desc="Fase 3", unit="ID", ncols=100, colour='green') as pbar:
            await coletor.fase3_garantia(pbar)
        
        tempo_fase3 = time.time() - tempo_inicio - tempo_fase1 - (tempo_fase2 if coletor.retry_fase2 else 0)
        print(f"✓ Fase 3: {tempo_fase3:.1f}s | Coletados: {coletor.stats['coletados']:,}")
    
    return coletor

# ========================================
# ENVIO DADOS - CORRIGIDO
# ========================================
def enviar_dados(membros, tempo_total, stats):
    """Envio para Google Sheets - CORRIGIDO"""
    if not membros:
        print("⚠️  Nenhum membro para enviar")
        return False
    
    print(f"\n📤 Enviando {len(membros):,} membros para Google Sheets...")
    
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
    
    # ✅ CORRIGIDO: Adiciona _lote_1 e campo "lote" na metadata
    payload = {
        "tipo": f"membros_gha_{INSTANCIA_ID}_lote_1",
        "relatorio_formatado": relatorio,
        "metadata": {
            "instancia": INSTANCIA_ID,
            "lote": 1,  # ✅ NOVO: Campo obrigatório
            "range_inicio": RANGE_INICIO,
            "range_fim": RANGE_FIM,
            "total_neste_lote": len(membros),  # ✅ NOVO: Total neste lote
            "total_coletados": len(membros),
            "total_vazios": stats['vazios'],
            "total_erros_fase3": stats['erros_fase3'],
            "tempo_execucao_min": round(tempo_total/60, 2),
            "velocidade_ids_min": round((RANGE_FIM - RANGE_INICIO + 1) / (tempo_total/60), 0),
            "velocidade_membros_min": round(len(membros) / (tempo_total/60), 0),
            "concurrent_max": CONCURRENT_REQUESTS,
            "fases_retry": f"F1:{SEMAPHORE_PHASE1}/F2:{SEMAPHORE_PHASE2}/F3:{SEMAPHORE_PHASE3}",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC")
        }
    }
    
    try:
        import requests
        print(f"🔄 Enviando para: {URL_APPS_SCRIPT}")
        print(f"📦 Payload: tipo={payload['tipo']}, linhas={len(relatorio)}")
        
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
        
        print(f"📡 Status HTTP: {response.status_code}")
        
        if response.status_code == 200:
            try:
                resultado = response.json()
                print(f"✓ Resposta: {resultado}")
                print("✅ Dados enviados com sucesso!")
                return True
            except:
                print(f"✓ Resposta (texto): {response.text[:200]}")
                print("✅ Dados enviados com sucesso!")
                return True
        else:
            print(f"✗ Erro HTTP {response.status_code}")
            print(f"✗ Resposta: {response.text[:500]}")
            return False
    except Exception as e:
        print(f"✗ Erro ao enviar: {e}")
        import traceback
        traceback.print_exc()
        return False

# ========================================
# MAIN
# ========================================
def main():
    print("=" * 80)
    print("🔥 COLETOR INSANO - META: 15 MINUTOS | 0% ERRO")
    print("=" * 80)
    print(f"📊 Range: {RANGE_INICIO:,} → {RANGE_FIM:,} ({RANGE_FIM - RANGE_INICIO + 1:,} IDs)")
    print(f"⚡ Estratégia: 3 Fases com Retry Adaptativo")
    print(f"🎯 Concorrência Máxima: {CONCURRENT_REQUESTS} requisições simultâneas")
    print("=" * 80)
    
    if not EMAIL or not SENHA:
        print("✗ Credenciais não encontradas")
        sys.exit(1)
    
    tempo_total_inicio = time.time()
    
    # Login
    cookies = login()
    if not cookies:
        sys.exit(1)
    
    # Coleta insana
    coletor = asyncio.run(executar_coleta_insana(cookies))
    
    tempo_total = time.time() - tempo_total_inicio
    
    # Estatísticas finais
    print("\n" + "=" * 80)
    print("📊 RELATÓRIO FINAL")
    print("=" * 80)
    print(f"✅ Membros coletados: {coletor.stats['coletados']:,}")
    print(f"⚪ IDs vazios/inexistentes: {coletor.stats['vazios']:,}")
    print(f"❌ Erros irrecuperáveis: {coletor.stats['erros_fase3']:,}")
    print(f"⏱️  Tempo total: {tempo_total/60:.2f} min ({tempo_total:.0f}s)")
    print(f"⚡ Velocidade: {(RANGE_FIM - RANGE_INICIO + 1) / (tempo_total/60):.0f} IDs/min")
    print(f"📈 Taxa sucesso: {(coletor.stats['coletados'] / (RANGE_FIM - RANGE_INICIO + 1) * 100):.2f}%")
    
    if tempo_total < 900:  # < 15 min
        print(f"🏆 META ALCANÇADA! {tempo_total/60:.1f} min < 15 min")
    else:
        print(f"⚠️  Meta não alcançada: {tempo_total/60:.1f} min")
    
    print("=" * 80)
    
    # Enviar
    if coletor.membros:
        enviar_dados(coletor.membros, tempo_total, coletor.stats)
        
        print("\n📋 AMOSTRAS (5 primeiros):")
        for i, m in enumerate(coletor.membros[:5], 1):
            print(f"  {i}. [{m['id']:>6}] {m['nome'][:45]:<45} | {m.get('instrumento', '')[:15]}")
    
    print("\n" + "=" * 80)
    print("✅ COLETA FINALIZADA COM SUCESSO")
    print("=" * 80)

if __name__ == "__main__":
    main()
