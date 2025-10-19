import os
import sys
import re
import asyncio
import httpx
import time
from playwright.sync_api import sync_playwright
from collections import deque
from tqdm import tqdm
from dataclasses import dataclass, field
from typing import Optional, Set, List
import statistics

# ========================================
# CONFIGURAÇÕES TSUNAMI 🌊
# ========================================
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzbkdOTDjGJxabnlJNDX7ZKI4_vh-t5d84MDRp-4FO4KmocRPEVs2jkHL3gjKEG-efF/exec'

RANGE_INICIO = 1
RANGE_FIM = 850000
INSTANCIA_ID = "TSUNAMI_v1"

# 🌊 PIPELINE CONTÍNUO - SEM CHUNKS!
CONCURRENT_BASE = 800          # Começamos com 800 concurrent
CONCURRENT_MIN = 200           # Mínimo em caso de problemas
CONCURRENT_MAX = 1500          # Máximo absoluto
AJUSTE_DINAMICO = True         # Auto-ajuste baseado em performance

# Timeouts adaptativos por tentativa
TIMEOUTS = [2, 4, 8]           # 2s → 4s → 8s

# Buffer de envio contínuo
BUFFER_ENVIO = 5000            # Envia a cada 5k membros coletados
ENVIO_BACKGROUND = True        # Envio paralelo à coleta

# Performance monitoring
JANELA_ANALISE = 1000          # Analisa a cada 1000 requisições
TARGET_SUCCESS_RATE = 0.95     # 95% de sucesso mínimo

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
# DATACLASSES PARA ORGANIZAÇÃO
# ========================================
@dataclass
class Membro:
    id: int
    nome: str
    igreja_selecionada: str = ""
    cargo_ministerio: str = ""
    nivel: str = ""
    instrumento: str = ""
    tonalidade: str = ""

@dataclass
class MetricasPerformance:
    """Métricas em tempo real para ajuste dinâmico"""
    ultimos_tempos: deque = field(default_factory=lambda: deque(maxlen=100))
    ultimos_sucessos: deque = field(default_factory=lambda: deque(maxlen=1000))
    concurrent_atual: int = CONCURRENT_BASE
    
    def adicionar_tempo(self, tempo: float):
        self.ultimos_tempos.append(tempo)
    
    def adicionar_resultado(self, sucesso: bool):
        self.ultimos_sucessos.append(1 if sucesso else 0)
    
    def taxa_sucesso(self) -> float:
        if not self.ultimos_sucessos:
            return 1.0
        return sum(self.ultimos_sucessos) / len(self.ultimos_sucessos)
    
    def tempo_medio(self) -> float:
        if not self.ultimos_tempos:
            return 0.0
        return statistics.mean(self.ultimos_tempos)
    
    def sugerir_ajuste(self) -> int:
        """Sugere ajuste de concorrência baseado em performance"""
        taxa = self.taxa_sucesso()
        tempo = self.tempo_medio()
        
        if taxa > 0.98 and tempo < 1.5:
            # Performance excelente - aumenta agressivamente
            return min(self.concurrent_atual + 100, CONCURRENT_MAX)
        elif taxa > 0.95 and tempo < 2.5:
            # Boa performance - aumenta moderadamente
            return min(self.concurrent_atual + 50, CONCURRENT_MAX)
        elif taxa < 0.90:
            # Performance ruim - reduz agressivamente
            return max(self.concurrent_atual - 100, CONCURRENT_MIN)
        elif taxa < 0.93:
            # Performance mediana - reduz moderadamente
            return max(self.concurrent_atual - 50, CONCURRENT_MIN)
        
        return self.concurrent_atual

# ========================================
# EXTRAÇÃO OTIMIZADA
# ========================================
def extrair_dados(html_content: str, membro_id: int) -> Optional[Membro]:
    """Extração rápida e retorna dataclass"""
    try:
        if not html_content or len(html_content) < 500 or 'name="nome"' not in html_content:
            return None
        
        nome_match = REGEX_NOME.search(html_content)
        if not nome_match:
            return None
        
        nome = nome_match.group(1).strip()
        if not nome:
            return None
        
        # Campos opcionais
        igreja_match = REGEX_IGREJA.search(html_content)
        cargo_match = REGEX_CARGO.search(html_content)
        nivel_match = REGEX_NIVEL.search(html_content)
        instrumento_match = REGEX_INSTRUMENTO.search(html_content)
        tonalidade_match = REGEX_TONALIDADE.search(html_content)
        
        return Membro(
            id=membro_id,
            nome=nome,
            igreja_selecionada=igreja_match.group(1) if igreja_match else '',
            cargo_ministerio=cargo_match.group(1).strip() if cargo_match else '',
            nivel=nivel_match.group(1).strip() if nivel_match else '',
            instrumento=instrumento_match.group(1).strip() if instrumento_match else '',
            tonalidade=tonalidade_match.group(1).strip() if tonalidade_match else ''
        )
    except:
        return None

# ========================================
# TSUNAMI COLLECTOR - PIPELINE CONTÍNUO
# ========================================
class TsunamiCollector:
    def __init__(self, cookies: dict):
        self.cookies = cookies
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0'
        }
        
        # Estruturas thread-safe
        self.lock = asyncio.Lock()
        self.membros: List[Membro] = []
        self.cache_vazios: Set[int] = set()
        self.fila_retry: deque = deque()
        
        # Métricas em tempo real
        self.metricas = MetricasPerformance()
        
        # Estatísticas
        self.stats = {
            'coletados': 0,
            'vazios': 0,
            'erros_finais': 0,
            'tentativas_total': 0,
            'ajustes_concurrent': 0,
            'envios_parciais': 0
        }
        
        # Controle de envio em background
        self.fila_envio: deque = deque()
        self.enviando = False
        
        # Cliente HTTP reutilizável
        self.client: Optional[httpx.AsyncClient] = None
    
    async def inicializar_cliente(self):
        """Inicializa cliente HTTP otimizado"""
        limits = httpx.Limits(
            max_keepalive_connections=300,
            max_connections=2000,
            keepalive_expiry=120
        )
        
        self.client = httpx.AsyncClient(
            cookies=self.cookies,
            headers=self.headers,
            limits=limits,
            http2=True,
            follow_redirects=True,
            timeout=None
        )
    
    async def finalizar_cliente(self):
        """Fecha cliente HTTP"""
        if self.client:
            await self.client.aclose()
    
    async def coletar_id_tentativa(self, membro_id: int, timeout: float, tentativa: int) -> tuple:
        """Coleta um ID com timeout específico"""
        inicio = time.time()
        
        try:
            url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
            response = await self.client.get(url, timeout=timeout)
            
            tempo_decorrido = time.time() - inicio
            self.metricas.adicionar_tempo(tempo_decorrido)
            
            if response.status_code == 200:
                html = response.text
                
                if 'name="nome"' in html:
                    dados = extrair_dados(html, membro_id)
                    if dados:
                        self.metricas.adicionar_resultado(True)
                        return ('sucesso', dados)
                    else:
                        self.cache_vazios.add(membro_id)
                        self.metricas.adicionar_resultado(True)
                        return ('vazio', None)
                else:
                    self.cache_vazios.add(membro_id)
                    self.metricas.adicionar_resultado(True)
                    return ('vazio', None)
            else:
                self.metricas.adicionar_resultado(False)
                return ('retry', membro_id)
                
        except:
            self.metricas.adicionar_resultado(False)
            return ('retry', membro_id)
    
    async def coletar_id(self, membro_id: int, semaphore: asyncio.Semaphore) -> Optional[Membro]:
        """Coleta um ID com retry adaptativo"""
        
        # Skip se já está no cache de vazios
        if membro_id in self.cache_vazios:
            async with self.lock:
                self.stats['vazios'] += 1
            return None
        
        async with semaphore:
            async with self.lock:
                self.stats['tentativas_total'] += 1
            
            # Tenta com timeouts progressivos
            for tentativa, timeout in enumerate(TIMEOUTS, 1):
                tipo, resultado = await self.coletar_id_tentativa(membro_id, timeout, tentativa)
                
                if tipo == 'sucesso':
                    async with self.lock:
                        self.stats['coletados'] += 1
                        self.membros.append(resultado)
                        
                        # Verifica se deve enviar lote parcial
                        if ENVIO_BACKGROUND and len(self.membros) % BUFFER_ENVIO == 0:
                            self.fila_envio.append(list(self.membros[-BUFFER_ENVIO:]))
                            self.stats['envios_parciais'] += 1
                    
                    return resultado
                
                elif tipo == 'vazio':
                    async with self.lock:
                        self.stats['vazios'] += 1
                    return None
                
                # Se 'retry', continua para próxima tentativa
            
            # Todas as tentativas falharam
            async with self.lock:
                self.stats['erros_finais'] += 1
            return None
    
    async def worker_envio_background(self):
        """Worker que envia lotes em background"""
        import requests
        
        while True:
            try:
                if self.fila_envio:
                    lote = self.fila_envio.popleft()
                    
                    # Envia de forma assíncrona (não bloqueia coleta)
                    asyncio.create_task(self.enviar_lote_async(lote))
                
                await asyncio.sleep(1)  # Check a cada 1s
            except:
                await asyncio.sleep(1)
    
    async def enviar_lote_async(self, lote: List[Membro]):
        """Envia um lote de forma assíncrona"""
        # Implementação simplificada - você pode expandir
        pass
    
    async def ajustar_concorrencia(self, semaphore_atual: asyncio.Semaphore) -> asyncio.Semaphore:
        """Ajusta concorrência dinamicamente baseado em métricas"""
        if not AJUSTE_DINAMICO:
            return semaphore_atual
        
        novo_concurrent = self.metricas.sugerir_ajuste()
        
        if novo_concurrent != self.metricas.concurrent_atual:
            async with self.lock:
                self.stats['ajustes_concurrent'] += 1
            
            self.metricas.concurrent_atual = novo_concurrent
            return asyncio.Semaphore(novo_concurrent)
        
        return semaphore_atual
    
    async def pipeline_continuo(self, ids: List[int], pbar):
        """Pipeline de coleta contínuo - CORAÇÃO DO TSUNAMI"""
        
        await self.inicializar_cliente()
        
        # Inicia worker de envio em background
        if ENVIO_BACKGROUND:
            asyncio.create_task(self.worker_envio_background())
        
        semaphore = asyncio.Semaphore(self.metricas.concurrent_atual)
        
        # Cria todas as tasks de uma vez - TSUNAMI TOTAL!
        tasks = []
        for membro_id in ids:
            task = asyncio.create_task(self.coletar_id(membro_id, semaphore))
            tasks.append(task)
        
        # Processa resultados conforme ficam prontos
        contador = 0
        for coro in asyncio.as_completed(tasks):
            await coro
            contador += 1
            
            # Atualiza progresso
            if pbar:
                pbar.update(1)
                
                # Atualiza stats no pbar
                if contador % 100 == 0:
                    pbar.set_postfix({
                        'Coletados': self.stats['coletados'],
                        'Taxa': f"{self.metricas.taxa_sucesso()*100:.1f}%",
                        'Concurrent': self.metricas.concurrent_atual,
                        'Tempo Médio': f"{self.metricas.tempo_medio():.2f}s"
                    })
            
            # Ajuste dinâmico de concorrência
            if AJUSTE_DINAMICO and contador % JANELA_ANALISE == 0:
                semaphore = await self.ajustar_concorrencia(semaphore)
        
        await self.finalizar_cliente()

# ========================================
# LOGIN
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
            cookies = {cookie['name']: cookie['value'] for cookie in context.cookies()}
            browser.close()
            print("✓ Login realizado")
            return cookies
    except Exception as e:
        print(f"✗ Erro no login: {e}")
        return None

# ========================================
# EXECUÇÃO TSUNAMI
# ========================================
async def executar_tsunami(cookies):
    """Execução em pipeline contínuo - SEM CHUNKS!"""
    
    coletor = TsunamiCollector(cookies)
    
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    todos_ids = list(range(RANGE_INICIO, RANGE_FIM + 1))
    
    print(f"\n{'='*80}")
    print(f"🌊 TSUNAMI COLLECTOR - PIPELINE CONTÍNUO")
    print(f"{'='*80}")
    print(f"📊 {total_ids:,} IDs processados SIMULTANEAMENTE")
    print(f"⚡ Concorrência inicial: {CONCURRENT_BASE} → Máx: {CONCURRENT_MAX}")
    print(f"🎯 Ajuste dinâmico: {'ATIVADO' if AJUSTE_DINAMICO else 'DESATIVADO'}")
    print(f"📤 Envio background: {'ATIVADO' if ENVIO_BACKGROUND else 'DESATIVADO'}")
    print(f"{'='*80}\n")
    
    tempo_inicio = time.time()
    
    # PIPELINE CONTÍNUO - TUDO DE UMA VEZ!
    with tqdm(total=total_ids, desc="🌊 Tsunami", unit="ID", ncols=120, colour='cyan') as pbar:
        await coletor.pipeline_continuo(todos_ids, pbar)
    
    tempo_total = time.time() - tempo_inicio
    
    return coletor, tempo_total

# ========================================
# ENVIO FINAL
# ========================================
def enviar_dados_final(membros: List[Membro], tempo_total: float, stats: dict):
    """Envio final dos dados"""
    import requests
    
    if not membros:
        print("⚠️  Nenhum membro para enviar")
        return False
    
    print(f"\n{'='*80}")
    print(f"📤 ENVIANDO {len(membros):,} MEMBROS")
    print(f"{'='*80}")
    
    # Converte Membro para dict
    membros_dict = [
        {
            'id': m.id,
            'nome': m.nome,
            'igreja_selecionada': m.igreja_selecionada,
            'cargo_ministerio': m.cargo_ministerio,
            'nivel': m.nivel,
            'instrumento': m.instrumento,
            'tonalidade': m.tonalidade
        }
        for m in membros
    ]
    
    cabecalho = ["ID", "NOME", "IGREJA_SELECIONADA", "CARGO/MINISTERIO", "NÍVEL", "INSTRUMENTO", "TONALIDADE"]
    
    relatorio = [cabecalho]
    for m in membros_dict:
        relatorio.append([
            str(m['id']),
            m['nome'],
            m['igreja_selecionada'],
            m['cargo_ministerio'],
            m['nivel'],
            m['instrumento'],
            m['tonalidade']
        ])
    
    payload = {
        "tipo": f"tsunami_{INSTANCIA_ID}",
        "relatorio_formatado": relatorio,
        "metadata": {
            "instancia": INSTANCIA_ID,
            "total": len(membros),
            "tempo_min": round(tempo_total/60, 2),
            "velocidade_ids_min": round((RANGE_FIM - RANGE_INICIO + 1) / (tempo_total/60), 0),
            "stats": stats
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=180)
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Erro no envio: {e}")
        return False

# ========================================
# MAIN
# ========================================
def main():
    print("=" * 80)
    print("🌊 TSUNAMI COLLECTOR - PIPELINE CONTÍNUO ADAPTATIVO")
    print("=" * 80)
    print(f"📊 Range: {RANGE_INICIO:,} → {RANGE_FIM:,}")
    print(f"🎯 Meta: < 5 minutos | 0% erro")
    print("=" * 80)
    
    if not EMAIL or not SENHA:
        print("✗ Credenciais não encontradas")
        sys.exit(1)
    
    tempo_total_inicio = time.time()
    
    # Login
    cookies = login()
    if not cookies:
        sys.exit(1)
    
    # Tsunami!
    coletor, tempo_coleta = asyncio.run(executar_tsunami(cookies))
    
    # Estatísticas
    print("\n" + "=" * 80)
    print("📊 RELATÓRIO TSUNAMI")
    print("=" * 80)
    print(f"✅ Membros coletados: {coletor.stats['coletados']:,}")
    print(f"⚪ IDs vazios: {coletor.stats['vazios']:,}")
    print(f"❌ Erros finais: {coletor.stats['erros_finais']:,}")
    print(f"🔄 Ajustes de concurrent: {coletor.stats['ajustes_concurrent']}")
    print(f"⏱️  Tempo: {tempo_coleta/60:.2f} min")
    print(f"⚡ Velocidade: {(RANGE_FIM - RANGE_INICIO + 1) / tempo_coleta:.0f} IDs/s")
    print(f"📈 Taxa sucesso: {(coletor.stats['coletados'] / (RANGE_FIM - RANGE_INICIO + 1) * 100):.2f}%")
    print("=" * 80)
    
    # Envio final
    if coletor.membros:
        enviar_dados_final(coletor.membros, tempo_coleta, coletor.stats)
        
        print("\n📋 AMOSTRAS:")
        for i, m in enumerate(coletor.membros[:5], 1):
            print(f"  {i}. [{m.id:>6}] {m.nome[:45]:<45} | {m.instrumento[:15]}")
    
    print("\n✅ TSUNAMI FINALIZADO!")

if __name__ == "__main__":
    main()
