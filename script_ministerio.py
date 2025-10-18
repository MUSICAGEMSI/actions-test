import os
import sys
import re
import asyncio
import httpx
import time
from playwright.sync_api import sync_playwright
from collections import deque
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv(dotenv_path="credencial.env")

# ========================================
# CONFIGURAÇÕES
# ========================================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxb9NPBjodXgDiax8-yV_c0YqVnUEHGv2cyeanJBnm7OsVxVjBj7M2Q_Wtc_cJZh21udw/exec'

RANGE_INICIO = 1
RANGE_FIM = 200000
INSTANCIA_ID = "ministros_batch_1"

# Configurações de performance
CONCURRENT_REQUESTS = 300
TIMEOUT_ULTRA_FAST = 3
TIMEOUT_FAST = 6
TIMEOUT_CAREFUL = 10
CHUNK_SIZE = 5000

SEMAPHORE_PHASE1 = 300
SEMAPHORE_PHASE2 = 200
SEMAPHORE_PHASE3 = 100

# ========================================
# REGEX PRÉ-COMPILADAS
# ========================================
REGEX_NOME = re.compile(r'name="nome"[^>]*value="([^"]*)"', re.IGNORECASE)
REGEX_EMAIL = re.compile(r'name="email"[^>]*value="([^"]*)"', re.IGNORECASE)
REGEX_ID_IGREJA = re.compile(r'<option[^>]*value="(\d+)"[^>]*selected[^>]*>([^<]+)</option>', re.IGNORECASE | re.DOTALL)
REGEX_MINISTERIO = re.compile(r'id="id_cargo"[^>]*>.*?<option[^>]*value="\d+"[^>]*selected[^>]*>\s*([^<\n]+)', re.DOTALL | re.IGNORECASE)
REGEX_TELEFONE = re.compile(r'id="telefone"[^>]*value="([^"]*)"', re.IGNORECASE)
REGEX_TELEFONE2 = re.compile(r'id="telefone2"[^>]*value="([^"]*)"', re.IGNORECASE)
REGEX_CADASTRADO = re.compile(r'Cadastrado em:\s*([^<]+)<.*?por:\s*([^<]+)', re.DOTALL | re.IGNORECASE)
REGEX_ATUALIZADO = re.compile(r'Atualizado em:\s*([^<]+)<.*?por:\s*([^<]+)', re.DOTALL | re.IGNORECASE)

CACHE_IDS_VAZIOS = set()

# ========================================
# EXTRAÇÃO DE DADOS
# ========================================
def extrair_dados_ministro(html_content, ministro_id):
    """Extrai dados do ministro do HTML"""
    try:
        if not html_content or len(html_content) < 500:
            return None
        
        # Verificação rápida se tem dados
        if 'name="nome"' not in html_content:
            return None
        
        dados = {
            'ID_Ministro': ministro_id,
            'Nome': '',
            'Email': '',
            'ID_Localidade': '',
            'Comum': '',
            'Ministerio': '',
            'Telefone_Celular': '',
            'Telefone_Fixo': '',
            'Cadastrado_em': '',
            'Cadastrado_por': '',
            'Atualizado_em': '',
            'Atualizado_por': ''
        }
        
        # Nome (obrigatório)
        nome_match = REGEX_NOME.search(html_content)
        if nome_match:
            dados['Nome'] = nome_match.group(1).strip()
        
        if not dados['Nome']:
            return None
        
        # Email
        email_match = REGEX_EMAIL.search(html_content)
        if email_match:
            dados['Email'] = email_match.group(1).strip()
        
        # Igreja e ID
        igreja_match = REGEX_ID_IGREJA.search(html_content)
        if igreja_match:
            dados['ID_Localidade'] = igreja_match.group(1).strip()
            dados['Comum'] = igreja_match.group(2).strip()
        
        # Ministério
        ministerio_match = REGEX_MINISTERIO.search(html_content)
        if ministerio_match:
            dados['Ministerio'] = ministerio_match.group(1).strip()
        
        # Telefones
        tel_match = REGEX_TELEFONE.search(html_content)
        if tel_match:
            dados['Telefone_Celular'] = tel_match.group(1).strip()
        
        tel2_match = REGEX_TELEFONE2.search(html_content)
        if tel2_match:
            dados['Telefone_Fixo'] = tel2_match.group(1).strip()
        
        # Histórico - Cadastrado
        cad_match = REGEX_CADASTRADO.search(html_content)
        if cad_match:
            dados['Cadastrado_em'] = cad_match.group(1).strip()
            dados['Cadastrado_por'] = cad_match.group(2).strip()
        
        # Histórico - Atualizado
        atu_match = REGEX_ATUALIZADO.search(html_content)
        if atu_match:
            dados['Atualizado_em'] = atu_match.group(1).strip()
            dados['Atualizado_por'] = atu_match.group(2).strip()
        
        return dados
        
    except Exception as e:
        return None

# ========================================
# COLETOR COM 3 FASES
# ========================================
class ColetorMinistros:
    def __init__(self, cookies):
        self.cookies = cookies
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9',
            'Connection': 'keep-alive'
        }
        
        self.lock = asyncio.Lock()
        self.stats = {
            'coletados': 0,
            'vazios': 0,
            'erros_fase1': 0,
            'erros_fase2': 0,
            'erros_fase3': 0
        }
        
        self.retry_fase2 = deque()
        self.retry_fase3 = deque()
        self.ministros = []
    
    async def coletar_id(self, client, ministro_id, timeout, semaphore, fase=1):
        """Coleta dados de um ministro"""
        
        if ministro_id in CACHE_IDS_VAZIOS:
            async with self.lock:
                self.stats['vazios'] += 1
            return None
        
        async with semaphore:
            try:
                url = f"https://musical.congregacao.org.br/ministros/editar/{ministro_id}"
                response = await client.get(url, timeout=timeout)
                
                if response.status_code == 200:
                    html = response.text
                    
                    if 'name="nome"' in html:
                        dados = extrair_dados_ministro(html, ministro_id)
                        if dados:
                            async with self.lock:
                                self.stats['coletados'] += 1
                                self.ministros.append(dados)
                            return dados
                        else:
                            CACHE_IDS_VAZIOS.add(ministro_id)
                            async with self.lock:
                                self.stats['vazios'] += 1
                            return None
                    else:
                        CACHE_IDS_VAZIOS.add(ministro_id)
                        async with self.lock:
                            self.stats['vazios'] += 1
                        return None
                else:
                    return ('retry', ministro_id)
                    
            except:
                return ('retry', ministro_id)
        
        return None
    
    async def fase1_ultra_rapida(self, ids_chunk, pbar):
        """FASE 1: Ultra agressiva"""
        
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
        """FASE 2: Retry moderado"""
        
        if not self.retry_fase2:
            return
        
        ids_retry = list(self.retry_fase2)
        self.retry_fase2.clear()
        
        limits = httpx.Limits(
            max_keepalive_connections=100,
            max_connections=250,
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
        """FASE 3: Garantia final"""
        
        if not self.retry_fase3:
            return
        
        ids_retry = list(self.retry_fase3)
        self.retry_fase3.clear()
        
        limits = httpx.Limits(
            max_keepalive_connections=50,
            max_connections=120,
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
# LOGIN
# ========================================
def login():
    """Login com Playwright"""
    print("🔐 Realizando login...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
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
# EXECUÇÃO
# ========================================
async def executar_coleta(cookies):
    """Executa coleta em 3 fases"""
    
    coletor = ColetorMinistros(cookies)
    
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    todos_ids = list(range(RANGE_INICIO, RANGE_FIM + 1))
    
    chunks = [todos_ids[i:i + CHUNK_SIZE] for i in range(0, len(todos_ids), CHUNK_SIZE)]
    
    print(f"\n{'='*80}")
    print(f"🚀 COLETA DE MINISTROS - ESTRATÉGIA 3 FASES")
    print(f"{'='*80}")
    print(f"📦 {total_ids:,} IDs → {len(chunks)} chunks de {CHUNK_SIZE:,}")
    print(f"⚡ FASE 1: {SEMAPHORE_PHASE1} concurrent | timeout {TIMEOUT_ULTRA_FAST}s")
    print(f"⚡ FASE 2: {SEMAPHORE_PHASE2} concurrent | timeout {TIMEOUT_FAST}s")
    print(f"⚡ FASE 3: {SEMAPHORE_PHASE3} concurrent | timeout {TIMEOUT_CAREFUL}s")
    print(f"{'='*80}\n")
    
    tempo_inicio = time.time()
    
    # FASE 1
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
    
    # FASE 2
    if coletor.retry_fase2:
        print(f"\n🔄 FASE 2: RETRY MODERADO ({len(coletor.retry_fase2):,} IDs)")
        with tqdm(total=len(coletor.retry_fase2), desc="Fase 2", unit="ID", ncols=100, colour='yellow') as pbar:
            await coletor.fase2_moderada(pbar)
        
        tempo_fase2 = time.time() - tempo_inicio - tempo_fase1
        print(f"✓ Fase 2: {tempo_fase2:.1f}s | Coletados: {coletor.stats['coletados']:,} | Retry: {len(coletor.retry_fase3):,}")
    
    # FASE 3
    if coletor.retry_fase3:
        print(f"\n🎯 FASE 3: GARANTIA FINAL ({len(coletor.retry_fase3):,} IDs)")
        with tqdm(total=len(coletor.retry_fase3), desc="Fase 3", unit="ID", ncols=100, colour='green') as pbar:
            await coletor.fase3_garantia(pbar)
        
        tempo_fase3 = time.time() - tempo_inicio - tempo_fase1
        print(f"✓ Fase 3: {tempo_fase3:.1f}s | Coletados: {coletor.stats['coletados']:,}")
    
    return coletor

# ========================================
# ENVIO PARA SHEETS
# ========================================
def enviar_dados(ministros, tempo_total, stats):
    """Envia dados para Google Sheets"""
    import requests
    from tqdm import tqdm
    
    if not ministros:
        print("⚠️  Nenhum ministro para enviar")
        return False
    
    TAMANHO_LOTE = 5000
    TIMEOUT_ENVIO = 180
    
    total_ministros = len(ministros)
    total_lotes = (total_ministros + TAMANHO_LOTE - 1) // TAMANHO_LOTE
    
    print(f"\n{'='*80}")
    print(f"📤 ENVIANDO {total_ministros:,} MINISTROS EM {total_lotes} LOTES")
    print(f"{'='*80}\n")
    
    cabecalho = ["ID_Ministro", "Nome", "Email", "ID_Localidade", "Comum", "Ministerio", 
                 "Telefone_Celular", "Telefone_Fixo", "Cadastrado_em", "Cadastrado_por",
                 "Atualizado_em", "Atualizado_por"]
    
    lotes_sucesso = 0
    
    with tqdm(total=total_lotes, desc="Enviando lotes", unit="lote", ncols=100) as pbar:
        for i in range(0, total_ministros, TAMANHO_LOTE):
            lote_numero = (i // TAMANHO_LOTE) + 1
            fim_lote = min(i + TAMANHO_LOTE, total_ministros)
            ministros_lote = ministros[i:fim_lote]
            
            relatorio = []
            
            if lote_numero == 1:
                relatorio.append(cabecalho)
            
            for m in ministros_lote:
                relatorio.append([
                    str(m.get('ID_Ministro', '')),
                    m.get('Nome', ''),
                    m.get('Email', ''),
                    m.get('ID_Localidade', ''),
                    m.get('Comum', ''),
                    m.get('Ministerio', ''),
                    m.get('Telefone_Celular', ''),
                    m.get('Telefone_Fixo', ''),
                    m.get('Cadastrado_em', ''),
                    m.get('Cadastrado_por', ''),
                    m.get('Atualizado_em', ''),
                    m.get('Atualizado_por', '')
                ])
            
            payload = {
                "tipo": "ministerio",
                "relatorio_formatado": relatorio,
                "metadata": {
                    "instancia": INSTANCIA_ID,
                    "lote": lote_numero,
                    "total_lotes": total_lotes,
                    "total_neste_lote": len(ministros_lote),
                    "total_processados": total_ministros,
                    "sucesso": stats['coletados'],
                    "erros": stats['erros_fase3'],
                    "tempo_minutos": round(tempo_total/60, 2),
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                }
            }
            
            try:
                response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=TIMEOUT_ENVIO)
                if response.status_code == 200:
                    lotes_sucesso += 1
            except:
                pass
            
            pbar.update(1)
    
    print(f"\n✅ Lotes enviados: {lotes_sucesso}/{total_lotes}")
    return lotes_sucesso == total_lotes

# ========================================
# MAIN
# ========================================
def main():
    print("=" * 80)
    print("🎵 COLETOR DE MINISTROS - ESTRATÉGIA 3 FASES")
    print("=" * 80)
    print(f"📊 Range: {RANGE_INICIO:,} → {RANGE_FIM:,}")
    print("=" * 80)
    
    if not EMAIL or not SENHA:
        print("✗ Credenciais não encontradas")
        sys.exit(1)
    
    tempo_inicio = time.time()
    
    cookies = login()
    if not cookies:
        sys.exit(1)
    
    coletor = asyncio.run(executar_coleta(cookies))
    
    tempo_total = time.time() - tempo_inicio
    
    print("\n" + "=" * 80)
    print("📊 RELATÓRIO FINAL")
    print("=" * 80)
    print(f"✅ Ministros coletados: {coletor.stats['coletados']:,}")
    print(f"⚪ IDs vazios: {coletor.stats['vazios']:,}")
    print(f"❌ Erros: {coletor.stats['erros_fase3']:,}")
    print(f"⏱️  Tempo total: {tempo_total/60:.2f} min")
    print("=" * 80)
    
    if coletor.ministros:
        enviar_dados(coletor.ministros, tempo_total, coletor.stats)
        
        print("\n📋 AMOSTRAS (5 primeiros):")
        for i, m in enumerate(coletor.ministros[:5], 1):
            print(f"  {i}. [{m['ID_Ministro']:>6}] {m['Nome'][:40]:<40} | {m.get('Ministerio', '')[:20]}")
    
    print("\n✅ COLETA FINALIZADA")

if __name__ == "__main__":
    main()
