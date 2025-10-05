import os
import sys
import re
import requests
import time
import concurrent.futures
from playwright.sync_api import sync_playwright
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============= CONFIGURAÇÕES =============
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwHlf2VUjfwX7KcHGKgvf0v2FlXZ7Y53ubkfcIPxihSb3VVUzbyzlBr5Fyx0OHrxwBx/exec'

# RANGE DE IDS A COLETAR
RANGE_INICIO = 1
RANGE_FIM = 50000  # Ajuste conforme necessário
INSTANCIA_ID = "usuarios_batch_1"
NUM_THREADS = 12
TIMEOUT_REQUEST = 10
PAUSA_MINIMA = 0.05

# ============= EXTRAÇÃO DE DADOS =============
def extrair_dados(html_content, usuario_id):
    """
    Extrai todos os dados do usuário do HTML
    """
    try:
        if not html_content or len(html_content) < 500:
            return None
        
        # Verifica se a página tem conteúdo válido
        if 'Sistema de Administração Musical' not in html_content:
            return None
        
        dados = {'id': usuario_id}
        
        # === NOME ===
        nome_match = re.search(r'<td>Nome</td>\s*<td>([^<]+)</td>', html_content, re.IGNORECASE)
        if not nome_match:
            return None
        dados['nome'] = nome_match.group(1).strip()
        if not dados['nome'] or dados['nome'] == '':
            return None
        
        # === LOGIN ===
        login_match = re.search(r'<td>Login</td>\s*<td>([^<]+)</td>', html_content, re.IGNORECASE)
        dados['login'] = login_match.group(1).strip() if login_match else ''
        
        # === EMAIL ===
        email_match = re.search(r'<a href="mailto:([^"]+)">', html_content, re.IGNORECASE)
        dados['email'] = email_match.group(1).strip() if email_match else ''
        
        # === GRUPO/PERMISSÃO ===
        grupo_match = re.search(r'<td>Grupo</td>\s*<td>([^<]+)</td>', html_content, re.IGNORECASE)
        dados['grupo'] = grupo_match.group(1).strip() if grupo_match else ''
        
        # === STATUS (ativo/inativo) ===
        status_match = re.search(r'<td>Status</td>\s*<td><i class="[^"]*text-(success|danger)', html_content, re.IGNORECASE)
        if status_match:
            dados['status'] = 'Ativo' if status_match.group(1) == 'success' else 'Inativo'
        else:
            dados['status'] = 'Desconhecido'
        
        # === ÚLTIMO LOGIN ===
        ultimo_login_match = re.search(r'<td>Último login</td>\s*<td>\s*([^<\n]+)', html_content, re.IGNORECASE)
        dados['ultimo_login'] = ultimo_login_match.group(1).strip() if ultimo_login_match else ''
        
        # === NÚMERO DE ACESSOS ===
        acessos_match = re.search(r'<td>Acessos</td>\s*<td>.*?<label[^>]*>\s*(\d+)\s*</label>', html_content, re.DOTALL | re.IGNORECASE)
        dados['acessos'] = acessos_match.group(1).strip() if acessos_match else '0'
        
        # === URL DA FOTO ===
        foto_match = re.search(r'<img src="(https://musical\.congregacao\.org\.br/[^"]+)"', html_content, re.IGNORECASE)
        dados['foto_url'] = foto_match.group(1).strip() if foto_match else ''
        
        return dados
        
    except Exception as e:
        return None


# ============= CLASSE COLETOR =============
class Coletor:
    def __init__(self, cookies, thread_id):
        self.thread_id = thread_id
        self.sucessos = 0
        self.falhas = 0
        self.vazios = 0
        
        self.session = requests.Session()
        self.session.cookies.update(cookies)
        
        # Configuração de retry
        retry_strategy = Retry(
            total=2,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
            'Connection': 'keep-alive',
            'Accept-Language': 'pt-BR,pt;q=0.9'
        }
    
    def coletar_batch(self, ids_batch):
        """
        Coleta um lote de IDs de usuários
        """
        usuarios = []
        
        for usuario_id in ids_batch:
            try:
                url = f"https://musical.congregacao.org.br/usuarios/visualizar/{usuario_id}"
                resp = self.session.get(url, headers=self.headers, timeout=TIMEOUT_REQUEST)
                
                if resp.status_code == 200:
                    dados = extrair_dados(resp.text, usuario_id)
                    
                    if dados:
                        usuarios.append(dados)
                        self.sucessos += 1
                        
                        # Log a cada 25 usuários coletados
                        if self.sucessos % 25 == 0:
                            print(f"[Thread {self.thread_id}] ✓ {self.sucessos} usuários coletados")
                    else:
                        self.vazios += 1
                else:
                    self.falhas += 1
                
                time.sleep(PAUSA_MINIMA)
                
            except requests.exceptions.Timeout:
                self.falhas += 1
            except Exception as e:
                self.falhas += 1
        
        return usuarios
    
    def get_stats(self):
        return {
            'thread_id': self.thread_id,
            'sucessos': self.sucessos,
            'falhas': self.falhas,
            'vazios': self.vazios
        }


# ============= FUNÇÃO DE LOGIN =============
def login():
    """
    Faz login no sistema usando Playwright e retorna os cookies
    """
    print("🔐 Iniciando login...")
    
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
            
            # Aguarda redirecionamento após login
            page.wait_for_selector("nav", timeout=20000)
            
            cookies = {cookie['name']: cookie['value'] for cookie in page.context.cookies()}
            browser.close()
            
            print("✓ Login realizado com sucesso!")
            return cookies
            
    except Exception as e:
        print(f"✗ Erro no login: {e}")
        return None


# ============= EXECUÇÃO DA COLETA =============
def executar_coleta(cookies):
    """
    Distribui a coleta entre múltiplas threads
    """
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    ids_per_thread = total_ids // NUM_THREADS
    
    # Divide os IDs entre as threads
    thread_ranges = []
    for i in range(NUM_THREADS):
        inicio = RANGE_INICIO + (i * ids_per_thread)
        fim = inicio + ids_per_thread - 1
        if i == NUM_THREADS - 1:
            fim = RANGE_FIM
        thread_ranges.append(list(range(inicio, fim + 1)))
    
    print(f"📊 Distribuindo {total_ids:,} IDs entre {NUM_THREADS} threads")
    print(f"📦 ~{ids_per_thread:,} IDs por thread\n")
    
    todos_usuarios = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        coletores = [Coletor(cookies, i) for i in range(NUM_THREADS)]
        futures = [executor.submit(coletores[i].coletar_batch, thread_ranges[i]) 
                  for i in range(NUM_THREADS)]
        
        # Aguarda conclusão de todas as threads
        for i, future in enumerate(futures):
            try:
                usuarios = future.result(timeout=7200)  # 2h de timeout
                todos_usuarios.extend(usuarios)
                stats = coletores[i].get_stats()
                print(f"\n[Thread {i}] Finalizada:")
                print(f"  ✓ Sucessos: {stats['sucessos']}")
                print(f"  ○ Vazios: {stats['vazios']}")
                print(f"  ✗ Falhas: {stats['falhas']}")
            except Exception as e:
                print(f"\n[Thread {i}] ✗ Erro/Timeout: {e}")
    
    return todos_usuarios


# ============= ENVIO DOS DADOS =============
def enviar_dados(usuarios, tempo_total):
    """
    Envia os dados coletados para o Google Apps Script
    """
    if not usuarios:
        print("⚠️  Nenhum usuário para enviar")
        return
    
    print(f"\n📤 Preparando envio de {len(usuarios):,} usuários...")
    
    # Formata dados para planilha
    relatorio = [[
        "ID", "NOME", "LOGIN", "EMAIL", "GRUPO", 
        "STATUS", "ÚLTIMO LOGIN", "ACESSOS", "FOTO_URL"
    ]]
    
    for usuario in usuarios:
        relatorio.append([
            str(usuario.get('id', '')),
            usuario.get('nome', ''),
            usuario.get('login', ''),
            usuario.get('email', ''),
            usuario.get('grupo', ''),
            usuario.get('status', ''),
            usuario.get('ultimo_login', ''),
            usuario.get('acessos', ''),
            usuario.get('foto_url', '')
        ])
    
    payload = {
        "tipo": f"usuarios_{INSTANCIA_ID}",
        "relatorio_formatado": relatorio,
        "metadata": {
            "instancia": INSTANCIA_ID,
            "range_inicio": RANGE_INICIO,
            "range_fim": RANGE_FIM,
            "total_coletados": len(usuarios),
            "tempo_execucao_min": round(tempo_total/60, 2),
            "threads_utilizadas": NUM_THREADS,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC")
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
        
        if response.status_code == 200:
            print("✓ Dados enviados com sucesso para o Google Sheets!")
        else:
            print(f"✗ Erro no envio: Status {response.status_code}")
            print(f"Resposta: {response.text[:200]}")
            
    except Exception as e:
        print(f"✗ Erro ao enviar dados: {e}")


# ============= FUNÇÃO PRINCIPAL =============
def main():
    print("=" * 60)
    print("  COLETOR DE USUÁRIOS - SISTEMA MUSICAL")
    print("=" * 60)
    print(f"Range: IDs {RANGE_INICIO:,} até {RANGE_FIM:,}")
    print(f"Threads: {NUM_THREADS}")
    print(f"Timeout: {TIMEOUT_REQUEST}s")
    print("=" * 60 + "\n")
    
    # Valida credenciais
    if not EMAIL or not SENHA:
        print("✗ Erro: Credenciais não encontradas nas variáveis de ambiente")
        print("  Configure: LOGIN_MUSICAL e SENHA_MUSICAL")
        sys.exit(1)
    
    tempo_inicio = time.time()
    
    # Login
    cookies = login()
    if not cookies:
        print("✗ Falha no login")
        sys.exit(1)
    
    print("\n🚀 Iniciando coleta...\n")
    
    # Executa coleta
    usuarios = executar_coleta(cookies)
    
    tempo_total = time.time() - tempo_inicio
    
    # Estatísticas finais
    print("\n" + "=" * 60)
    print("  COLETA FINALIZADA")
    print("=" * 60)
    print(f"✓ Usuários coletados: {len(usuarios):,}")
    print(f"⏱️  Tempo total: {tempo_total/60:.1f} minutos")
    print(f"⚡ Velocidade: {len(usuarios)/(tempo_total/60):.1f} usuários/min")
    print("=" * 60 + "\n")
    
    # Envia dados
    if usuarios:
        enviar_dados(usuarios, tempo_total)
        
        # Mostra amostras
        print("\n📋 Amostras dos primeiros usuários coletados:")
        for i, u in enumerate(usuarios[:5], 1):
            print(f"  {i}. {u.get('nome', '')[:40]:40} | {u.get('grupo', '')[:25]:25} | {u.get('status', '')}")
    else:
        print("⚠️  Nenhum usuário coletado")
    
    print(f"\n✓ Batch {INSTANCIA_ID} finalizado!")


if __name__ == "__main__":
    main()
