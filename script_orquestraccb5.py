import os
import sys
import re
import requests
import time
import json
import concurrent.futures
from playwright.sync_api import sync_playwright
import threading

# Configuração para GitHub Actions
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwV-0AChSp5-JyBc3NysUQI0UlFJ7AycvE6CSRKWxldnJ8EBiaNHtj3oYx5jiiHxQbzOw/exec'

# Configuração de range para GitHub Actions (otimizado para 6 horas)
RANGE_INICIO = 400001
RANGE_FIM = 401000  # Range menor para caber no limite de tempo
INSTANCIA_ID = "GHA_optimized"

# Configurações otimizadas para GitHub Actions
NUM_THREADS = 12
BATCH_SIZE = 100
TIMEOUT_REQUEST = 8
PAUSA_MINIMA = 0.03

def verificar_ambiente():
    """Verifica se está no ambiente GitHub Actions"""
    if not EMAIL or not SENHA:
        print("❌ ERRO: Credenciais não definidas!")
        print("Verifique se as secrets LOGIN_MUSICAL e SENHA_MUSICAL estão configuradas")
        return False
    
    print(f"🔐 Credenciais encontradas para: {EMAIL}")
    print(f"🚀 GitHub Actions - Instância: {INSTANCIA_ID}")
    print(f"📊 Range: {RANGE_INICIO:,} - {RANGE_FIM:,}")
    print(f"⚙️ Configuração: {NUM_THREADS} threads, timeout {TIMEOUT_REQUEST}s")
    
    return True

def extrair_dados_ultra_rapido(html_content, membro_id):
    """Extração ultrarrápida com regex otimizadas"""
    try:
        if 'name="nome"' not in html_content or len(html_content) < 500:
            return None
            
        dados = {'id': membro_id}
        
        # Nome
        nome_match = re.search(r'name="nome"[^>]*value="([^"]*)"', html_content)
        if nome_match:
            dados['nome'] = nome_match.group(1).strip()
            if not dados['nome']:
                return None
        else:
            return None
        
        # Igreja
        igreja_match = re.search(r'igreja_selecionada\s*\(\s*(\d+)\s*\)', html_content)
        dados['igreja_selecionada'] = igreja_match.group(1) if igreja_match else ''
        
        # Outros campos
        patterns = {
            'cargo_ministerio': r'id_cargo"[^>]*>.*?selected[^>]*>([^<]*)',
            'nivel': r'id_nivel"[^>]*>.*?selected[^>]*>([^<]*)',
            'instrumento': r'id_instrumento"[^>]*>.*?selected[^>]*>([^<]*)',
            'tonalidade': r'id_tonalidade"[^>]*>.*?selected[^>]*>([^<]*)'
        }
        
        for campo, pattern in patterns.items():
            match = re.search(pattern, html_content, re.DOTALL)
            dados[campo] = match.group(1).strip() if match else ''
        
        return dados
        
    except Exception:
        return None

class ColetorGitHubActions:
    def __init__(self, session, thread_id=0):
        self.session = session
        self.thread_id = thread_id
        self.sucessos = 0
        self.falhas = 0
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        }
    
    def coletar_batch(self, ids_batch):
        """Coleta um batch de IDs com tratamento de erro melhorado"""
        membros = []
        
        for membro_id in ids_batch:
            try:
                url = f"https://musical.congregacao.org.br/licoes/index/{membro_id}"
                
                resp = self.session.get(url, headers=self.headers, timeout=TIMEOUT_REQUEST)
                
                if resp.status_code == 200:
                    dados = extrair_dados_ultra_rapido(resp.text, membro_id)
                    if dados:
                        membros.append(dados)
                        self.sucessos += 1
                        
                        # Log progress
                        if self.sucessos % 50 == 0:
                            print(f"✅ T{self.thread_id}: {self.sucessos} coletados")
                    else:
                        self.falhas += 1
                else:
                    self.falhas += 1
                
                time.sleep(PAUSA_MINIMA)
                
            except requests.exceptions.Timeout:
                self.falhas += 1
                if self.falhas % 100 == 0:
                    print(f"⏱️ T{self.thread_id}: {self.falhas} timeouts")
            except Exception as e:
                self.falhas += 1
                continue
        
        return membros

def fazer_login_playwright():
    """Login otimizado para GitHub Actions"""
    print("🔐 Iniciando login...")
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--memory-pressure-off',
                    '--max_old_space_size=4096'
                ]
            )
            
            page = browser.new_page()
            page.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
            })
            
            # Login
            page.goto(URL_INICIAL, timeout=30000)
            page.fill('input[name="login"]', EMAIL)
            page.fill('input[name="password"]', SENHA)
            page.click('button[type="submit"]')
            
            # Aguardar login
            page.wait_for_selector("nav", timeout=20000)
            print("✅ Login realizado com sucesso!")
            
            # Extrair cookies
            cookies = {cookie['name']: cookie['value'] for cookie in page.context.cookies()}
            browser.close()
            
            return cookies
            
    except Exception as e:
        print(f"❌ Erro no login: {e}")
        return None

def executar_coleta_paralela(session):
    """Executa coleta paralela otimizada"""
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    ids_per_thread = total_ids // NUM_THREADS
    
    print(f"📈 Dividindo {total_ids:,} IDs em {NUM_THREADS} threads")
    
    # Criar ranges por thread
    thread_ranges = []
    for i in range(NUM_THREADS):
        inicio = RANGE_INICIO + (i * ids_per_thread)
        fim = inicio + ids_per_thread - 1
        
        if i == NUM_THREADS - 1:
            fim = RANGE_FIM
            
        thread_ranges.append(list(range(inicio, fim + 1)))
    
    todos_membros = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        coletores = [ColetorGitHubActions(session, i) for i in range(NUM_THREADS)]
        
        futures = []
        for i, ids_thread in enumerate(thread_ranges):
            future = executor.submit(coletores[i].coletar_batch, ids_thread)
            futures.append((future, i))
        
        # Aguardar conclusão
        for future, thread_id in futures:
            try:
                membros_thread = future.result(timeout=1800)  # 30 min timeout
                todos_membros.extend(membros_thread)
                coletor = coletores[thread_id]
                print(f"✅ Thread {thread_id}: {len(membros_thread)} membros | Sucessos: {coletor.sucessos}")
            except concurrent.futures.TimeoutError:
                print(f"⏱️ Thread {thread_id}: Timeout")
            except Exception as e:
                print(f"❌ Thread {thread_id}: Erro - {str(e)[:50]}")
    
    return todos_membros

def criar_relatorio(membros):
    """Cria relatório formatado"""
    relatorio = [
        ["ID", "NOME", "IGREJA_SELECIONADA", "CARGO/MINISTERIO", "NÍVEL", "INSTRUMENTO", "TONALIDADE"]
    ]
    
    for membro in membros:
        linha = [
            str(membro.get('id', '')),
            membro.get('nome', ''),
            membro.get('igreja_selecionada', ''),
            membro.get('cargo_ministerio', ''),
            membro.get('nivel', ''),
            membro.get('instrumento', ''),
            membro.get('tonalidade', '')
        ]
        relatorio.append(linha)
    
    return relatorio

def enviar_para_sheets(membros, tempo_total):
    """Envia dados para Google Sheets"""
    print("📤 Enviando para Google Sheets...")
    
    relatorio = criar_relatorio(membros)
    
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
            "ambiente": "GitHub Actions"
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
        if response.status_code == 200:
            print("✅ Dados enviados com sucesso!")
            result = response.json() if response.text else {}
            if result:
                print(f"📊 Resposta: {result}")
        else:
            print(f"⚠️ Status HTTP: {response.status_code}")
            print(f"📄 Resposta: {response.text[:200]}")
    except Exception as e:
        print(f"❌ Erro no envio: {e}")

def main():
    """Função principal"""
    print("🎬 INICIANDO COLETA DE DADOS MUSICAIS")
    print("=" * 50)
    
    # Verificar ambiente
    if not verificar_ambiente():
        sys.exit(1)
    
    tempo_inicio = time.time()
    
    # Login
    cookies = fazer_login_playwright()
    if not cookies:
        sys.exit(1)
    
    # Configurar sessão
    session = requests.Session()
    session.cookies.update(cookies)
    
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=20,
        pool_maxsize=20,
        max_retries=1
    )
    session.mount('https://', adapter)
    
    print("🚀 Iniciando coleta paralela...")
    
    # Executar coleta
    membros_coletados = executar_coleta_paralela(session)
    
    tempo_total = time.time() - tempo_inicio
    
    # Estatísticas finais
    print("\n" + "="*50)
    print("🏁 COLETA FINALIZADA!")
    print(f"📊 Membros coletados: {len(membros_coletados):,}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    if membros_coletados:
        print(f"⚡ Velocidade: {len(membros_coletados)/tempo_total:.2f} membros/segundo")
        
        # Enviar para Google Sheets
        enviar_para_sheets(membros_coletados, tempo_total)
        
        # Amostras dos dados
        print("\n📋 PRIMEIROS 3 REGISTROS:")
        for i, membro in enumerate(membros_coletados[:3], 1):
            print(f"{i}. {membro.get('nome', 'N/A')} - Igreja: {membro.get('igreja_selecionada', 'N/A')}")
    else:
        print("⚠️ Nenhum membro foi coletado neste range")
    
    print(f"\n🎯 Processo finalizado - Instância {INSTANCIA_ID}")

if __name__ == "__main__":
    main()
