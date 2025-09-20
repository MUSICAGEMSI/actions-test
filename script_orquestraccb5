import os
import sys
import re
import requests
import time
import json
import concurrent.futures
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import threading

# Configuração para GitHub Actions
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwV-0AChSp5-JyBc3NysUQI0UlFJ7AycvE6CSRKWxldnJ8EBiaNHtj3oYx5jiiHxQbzOw/exec'

# Parâmetros do range (passados via argumentos)
if len(sys.argv) >= 3:
    RANGE_INICIO = int(sys.argv[1])
    RANGE_FIM = int(sys.argv[2])
    INSTANCIA_ID = f"GHA_{RANGE_INICIO}_{RANGE_FIM}"
else:
    RANGE_INICIO = 800001
    RANGE_FIM = 1000000
    INSTANCIA_ID = "GHA_default"

# Configurações otimizadas para GitHub Actions
NUM_THREADS = 15  # Reduzido para evitar rate limiting
BATCH_SIZE = 50   # Batches menores
TIMEOUT_REQUEST = 6  # Timeout mais agressivo
PAUSA_MINIMA = 0.05  # Pausa muito pequena

print(f"🚀 GitHub Actions - Instância {INSTANCIA_ID}")
print(f"📊 Range: {RANGE_INICIO:,} - {RANGE_FIM:,}")
print(f"⚙️ Threads: {NUM_THREADS} | Timeout: {TIMEOUT_REQUEST}s")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas nas secrets")
    sys.exit(1)

def extrair_dados_ultra_rapido(html_content, membro_id):
    """
    Extração ultrarrápida com regex otimizadas
    """
    try:
        # Verificação rápida se tem dados válidos
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
        
        # Igreja (JavaScript)
        igreja_match = re.search(r'igreja_selecionada\s*\(\s*(\d+)\s*\)', html_content)
        dados['igreja_selecionada'] = igreja_match.group(1) if igreja_match else ''
        
        # Campos com regex simplificada
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
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.8,en;q=0.6',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    def coletar_batch(self, ids_batch):
        """
        Coleta um batch de IDs sequencialmente
        """
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
                        # Log apenas cada 10 sucessos para não poluir
                        if self.sucessos % 10 == 0:
                            print(f"✅ T{self.thread_id}: {self.sucessos} coletados - Último: {dados['nome'][:25]}")
                    else:
                        self.falhas += 1
                else:
                    self.falhas += 1
                
                # Pausa mínima
                time.sleep(PAUSA_MINIMA)
                
            except Exception as e:
                self.falhas += 1
                # Log apenas erros críticos
                if "timeout" in str(e).lower():
                    print(f"⏱️ T{self.thread_id}: Timeout no ID {membro_id}")
                continue
        
        return membros

def executar_coleta_paralela(session, range_inicio, range_fim, num_threads):
    """
    Executa coleta com threads otimizada para GitHub Actions
    """
    total_ids = range_fim - range_inicio + 1
    ids_per_thread = total_ids // num_threads
    
    print(f"📈 Dividindo {total_ids:,} IDs em {num_threads} threads ({ids_per_thread:,} IDs/thread)")
    
    # Criar ranges por thread
    thread_ranges = []
    for i in range(num_threads):
        inicio = range_inicio + (i * ids_per_thread)
        fim = inicio + ids_per_thread - 1
        
        if i == num_threads - 1:
            fim = range_fim
            
        thread_ranges.append(list(range(inicio, fim + 1)))
    
    todos_membros = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        coletores = [ColetorGitHubActions(session, i) for i in range(num_threads)]
        
        futures = []
        for i, ids_thread in enumerate(thread_ranges):
            future = executor.submit(coletores[i].coletar_batch, ids_thread)
            futures.append((future, i))
        
        # Aguardar conclusão
        for future, thread_id in futures:
            try:
                membros_thread = future.result(timeout=2400)  # 40 min timeout
                todos_membros.extend(membros_thread)
                coletor = coletores[thread_id]
                print(f"✅ Thread {thread_id}: {len(membros_thread)} membros | {coletor.sucessos} sucessos | {coletor.falhas} falhas")
            except concurrent.futures.TimeoutError:
                print(f"⏱️ Thread {thread_id}: Timeout")
            except Exception as e:
                print(f"❌ Thread {thread_id}: Erro - {e}")
    
    return todos_membros

def criar_relatorio(membros):
    """
    Cria relatório otimizado
    """
    relatorio = [
        ["ID", "NOME", "IGREJA_SELECIONADA", "CARGO/MINISTERIO", "NÍVEL", "INSTRUMENTO", "TONALIDADE"]
    ]
    
    for membro in membros:
        linha = [
            membro.get('id', ''),
            membro.get('nome', ''),
            membro.get('igreja_selecionada', ''),
            membro.get('cargo_ministerio', ''),
            membro.get('nivel', ''),
            membro.get('instrumento', ''),
            membro.get('tonalidade', '')
        ]
        relatorio.append(linha)
    
    return relatorio

def main():
    tempo_inicio = time.time()
    
    print("🔐 Iniciando login...")
    
    with sync_playwright() as p:
        # Configuração otimizada para GitHub Actions
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        
        page = browser.new_page()
        page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        })
        
        try:
            page.goto(URL_INICIAL, timeout=30000)
            page.fill('input[name="login"]', EMAIL)
            page.fill('input[name="password"]', SENHA)
            page.click('button[type="submit"]')
            page.wait_for_selector("nav", timeout=20000)
            print("✅ Login realizado com sucesso!")
            
        except Exception as e:
            print(f"❌ Erro no login: {e}")
            browser.close()
            sys.exit(1)
        
        # Extrair cookies
        cookies = {cookie['name']: cookie['value'] for cookie in page.context.cookies()}
        browser.close()
    
    # Sessão otimizada
    session = requests.Session()
    session.cookies.update(cookies)
    
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=25,
        pool_maxsize=25,
        max_retries=1
    )
    session.mount('https://', adapter)
    
    print("🚀 Iniciando coleta paralela...")
    
    # Executar coleta
    membros_coletados = executar_coleta_paralela(session, RANGE_INICIO, RANGE_FIM, NUM_THREADS)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"🏁 COLETA FINALIZADA!")
    print(f"📊 Membros coletados: {len(membros_coletados)}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    if membros_coletados:
        print(f"⚡ Velocidade: {len(membros_coletados)/tempo_total:.2f} membros/segundo")
    
    # Enviar dados se coletou algo
    if membros_coletados:
        print("📤 Enviando para Google Sheets...")
        
        relatorio = criar_relatorio(membros_coletados)
        
        payload = {
            "tipo": f"membros_gha_{INSTANCIA_ID}",
            "relatorio_formatado": relatorio,
            "metadata": {
                "instancia": INSTANCIA_ID,
                "range_inicio": RANGE_INICIO,
                "range_fim": RANGE_FIM,
                "total_coletados": len(membros_coletados),
                "tempo_execucao_min": round(tempo_total/60, 2),
                "threads_utilizadas": NUM_THREADS,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC")
            }
        }
        
        try:
            response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
            if response.status_code == 200:
                print("✅ Dados enviados com sucesso!")
                print(f"📄 Resposta: {response.text[:100]}")
            else:
                print(f"⚠️ Status HTTP: {response.status_code}")
                print(f"📄 Resposta: {response.text}")
        except Exception as e:
            print(f"❌ Erro no envio: {e}")
    
    else:
        print("⚠️ Nenhum membro foi coletado neste range")
    
    print(f"🎯 Processo finalizado - Instância {INSTANCIA_ID}")

if __name__ == "__main__":
    main()
