import os
import sys
import re
import requests
import time
import json
import concurrent.futures
from playwright.sync_api import sync_playwright
import threading
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# Configuração para GitHub Actions
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwV-0AChSp5-JyBc3NysUQI0UlFJ7AycvE6CSRKWxldnJ8EBiaNHtj3oYx5jiiHxQbzOw/exec'

# Configuração de range menor para teste
RANGE_INICIO = 400001
RANGE_FIM = 400200  # Apenas 200 IDs para teste
INSTANCIA_ID = "GHA_test_v2"

# Configurações mais conservadoras
NUM_THREADS = 4  # Reduzido drasticamente
TIMEOUT_REQUEST = 15  # Timeout maior
PAUSA_ENTRE_REQUESTS = 0.2  # Pausa maior
MAX_RETRIES = 2

def verificar_ambiente():
    """Verifica se está no ambiente GitHub Actions"""
    if not EMAIL or not SENHA:
        print("❌ ERRO: Credenciais não definidas!")
        return False
    
    print(f"🔐 Credenciais encontradas para: {EMAIL}")
    print(f"🚀 GitHub Actions - Instância: {INSTANCIA_ID}")
    print(f"📊 Range: {RANGE_INICIO:,} - {RANGE_FIM:,}")
    print(f"⚙️ Configuração: {NUM_THREADS} threads, timeout {TIMEOUT_REQUEST}s")
    
    return True

def extrair_dados_melhorado(html_content, membro_id):
    """Extração com validações mais robustas"""
    try:
        # Validações básicas
        if not html_content or len(html_content) < 100:
            return None
            
        # Verificar se a página tem conteúdo válido
        if 'name="nome"' not in html_content:
            return None
            
        # Verificar se não é página de erro
        if 'erro' in html_content.lower() or 'error' in html_content.lower():
            return None
            
        dados = {'id': membro_id}
        
        # Nome - mais robusto
        nome_patterns = [
            r'name="nome"[^>]*value="([^"]*)"',
            r'id="nome"[^>]*value="([^"]*)"',
        ]
        
        nome_encontrado = False
        for pattern in nome_patterns:
            nome_match = re.search(pattern, html_content)
            if nome_match:
                nome = nome_match.group(1).strip()
                if nome and nome != "":
                    dados['nome'] = nome
                    nome_encontrado = True
                    break
        
        if not nome_encontrado:
            return None
        
        # Igreja
        igreja_match = re.search(r'igreja_selecionada\s*\(\s*(\d+)\s*\)', html_content)
        dados['igreja_selecionada'] = igreja_match.group(1) if igreja_match else ''
        
        # Outros campos com fallbacks
        campos_patterns = {
            'cargo_ministerio': [
                r'id_cargo"[^>]*>.*?selected[^>]*>([^<]*)',
                r'name="cargo"[^>]*>.*?selected[^>]*>([^<]*)'
            ],
            'nivel': [
                r'id_nivel"[^>]*>.*?selected[^>]*>([^<]*)',
                r'name="nivel"[^>]*>.*?selected[^>]*>([^<]*)'
            ],
            'instrumento': [
                r'id_instrumento"[^>]*>.*?selected[^>]*>([^<]*)',
                r'name="instrumento"[^>]*>.*?selected[^>]*>([^<]*)'
            ],
            'tonalidade': [
                r'id_tonalidade"[^>]*>.*?selected[^>]*>([^<]*)',
                r'name="tonalidade"[^>]*>.*?selected[^>]*>([^<]*)'
            ]
        }
        
        for campo, patterns in campos_patterns.items():
            valor_encontrado = ''
            for pattern in patterns:
                match = re.search(pattern, html_content, re.DOTALL)
                if match:
                    valor_encontrado = match.group(1).strip()
                    break
            dados[campo] = valor_encontrado
        
        return dados
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair dados do ID {membro_id}: {str(e)[:50]}")
        return None

class ColetorMelhorado:
    def __init__(self, cookies, thread_id=0):
        self.thread_id = thread_id
        self.sucessos = 0
        self.falhas = 0
        self.timeouts = 0
        self.erros_http = 0
        
        # Criar sessão própria para cada thread
        self.session = requests.Session()
        self.session.cookies.update(cookies)
        
        # Configurar retry strategy
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=5,
            pool_maxsize=5
        )
        
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Cache-Control': 'max-age=0'
        }
    
    def testar_conectividade(self):
        """Testa se a sessão está funcionando"""
        try:
            # Fazer uma requisição simples para testar
            resp = self.session.get(
                "https://musical.congregacao.org.br/licoes/index/400001", 
                headers=self.headers, 
                timeout=10
            )
            
            if resp.status_code == 200:
                print(f"✅ T{self.thread_id}: Conectividade OK")
                return True
            else:
                print(f"⚠️ T{self.thread_id}: Status {resp.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ T{self.thread_id}: Erro conectividade - {str(e)[:50]}")
            return False
    
    def coletar_sequencial(self, ids_lista):
        """Coleta sequencial com melhor tratamento de erro"""
        membros = []
        
        # Testar conectividade primeiro
        if not self.testar_conectividade():
            print(f"❌ T{self.thread_id}: Falha na conectividade inicial")
            return membros
        
        for i, membro_id in enumerate(ids_lista):
            try:
                url = f"https://musical.congregacao.org.br/licoes/index/{membro_id}"
                
                # Log de progresso a cada 10 tentativas
                if (i + 1) % 10 == 0:
                    print(f"🔄 T{self.thread_id}: Processando {i+1}/{len(ids_lista)} - ID {membro_id}")
                
                resp = self.session.get(url, headers=self.headers, timeout=TIMEOUT_REQUEST)
                
                if resp.status_code == 200:
                    if len(resp.text) > 500:  # Verificar se tem conteúdo
                        dados = extrair_dados_melhorado(resp.text, membro_id)
                        if dados:
                            membros.append(dados)
                            self.sucessos += 1
                            print(f"✅ T{self.thread_id}: Coletado - {dados['nome'][:20]} (Total: {self.sucessos})")
                        else:
                            self.falhas += 1
                    else:
                        self.falhas += 1
                        print(f"⚠️ T{self.thread_id}: Resposta vazia para ID {membro_id}")
                
                elif resp.status_code == 404:
                    self.falhas += 1  # ID não existe, normal
                    
                else:
                    self.erros_http += 1
                    print(f"⚠️ T{self.thread_id}: HTTP {resp.status_code} para ID {membro_id}")
                
                # Pausa entre requisições
                time.sleep(PAUSA_ENTRE_REQUESTS)
                
            except requests.exceptions.Timeout:
                self.timeouts += 1
                print(f"⏱️ T{self.thread_id}: Timeout no ID {membro_id} (Total timeouts: {self.timeouts})")
                
            except requests.exceptions.RequestException as e:
                self.falhas += 1
                print(f"🌐 T{self.thread_id}: Erro de rede no ID {membro_id} - {str(e)[:30]}")
                
            except Exception as e:
                self.falhas += 1
                print(f"❌ T{self.thread_id}: Erro geral no ID {membro_id} - {str(e)[:30]}")
                continue
        
        print(f"🏁 T{self.thread_id}: Finalizado - {self.sucessos} sucessos, {self.falhas} falhas, {self.timeouts} timeouts")
        return membros

def fazer_login_robusto():
    """Login com melhor tratamento de erro"""
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
                    '--max_old_space_size=4096',
                    '--disable-background-timer-throttling',
                    '--disable-renderer-backgrounding',
                    '--disable-backgrounding-occluded-windows'
                ]
            )
            
            context = browser.new_context(
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            page = context.new_page()
            
            # Ir para página inicial
            print("🌐 Acessando página de login...")
            page.goto(URL_INICIAL, timeout=30000)
            
            # Aguardar elementos de login
            page.wait_for_selector('input[name="login"]', timeout=10000)
            
            # Preencher credenciais
            print("📝 Preenchendo credenciais...")
            page.fill('input[name="login"]', EMAIL)
            page.fill('input[name="password"]', SENHA)
            
            # Submeter formulário
            print("🚀 Submetendo login...")
            page.click('button[type="submit"]')
            
            # Aguardar redirecionamento
            page.wait_for_selector("nav", timeout=20000)
            print("✅ Login realizado com sucesso!")
            
            # Testar acesso a uma página específica
            print("🧪 Testando acesso...")
            page.goto("https://musical.congregacao.org.br/licoes/index/400001", timeout=15000)
            
            # Extrair cookies
            cookies = {cookie['name']: cookie['value'] for cookie in context.cookies()}
            
            browser.close()
            
            print(f"🍪 Cookies extraídos: {len(cookies)} cookies")
            return cookies
            
    except Exception as e:
        print(f"❌ Erro no login: {str(e)}")
        return None

def executar_coleta_robusta(cookies):
    """Executa coleta com abordagem mais robusta"""
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    ids_per_thread = total_ids // NUM_THREADS
    
    print(f"📈 Dividindo {total_ids:,} IDs em {NUM_THREADS} threads ({ids_per_thread} IDs/thread)")
    
    # Criar listas de IDs por thread
    thread_ids = []
    for i in range(NUM_THREADS):
        inicio = RANGE_INICIO + (i * ids_per_thread)
        fim = inicio + ids_per_thread - 1
        
        if i == NUM_THREADS - 1:
            fim = RANGE_FIM
            
        ids_thread = list(range(inicio, fim + 1))
        thread_ids.append(ids_thread)
        print(f"📋 Thread {i}: IDs {inicio}-{fim} ({len(ids_thread)} IDs)")
    
    todos_membros = []
    
    # Executar threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        # Criar coletores
        coletores = [ColetorMelhorado(cookies, i) for i in range(NUM_THREADS)]
        
        # Submeter tarefas
        futures = []
        for i, ids_lista in enumerate(thread_ids):
            future = executor.submit(coletores[i].coletar_sequencial, ids_lista)
            futures.append((future, i))
        
        # Coletar resultados
        for future, thread_id in futures:
            try:
                membros_thread = future.result(timeout=3600)  # 60 min timeout
                todos_membros.extend(membros_thread)
                
                coletor = coletores[thread_id]
                print(f"✅ Thread {thread_id} finalizada:")
                print(f"   📊 Membros: {len(membros_thread)}")
                print(f"   ✅ Sucessos: {coletor.sucessos}")
                print(f"   ❌ Falhas: {coletor.falhas}")
                print(f"   ⏱️ Timeouts: {coletor.timeouts}")
                print(f"   🌐 Erros HTTP: {coletor.erros_http}")
                
            except concurrent.futures.TimeoutError:
                print(f"⏱️ Thread {thread_id}: Timeout geral")
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
    if not membros:
        print("⚠️ Nenhum dado para enviar")
        return
        
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
            "ambiente": "GitHub Actions v2"
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
        if response.status_code == 200:
            print("✅ Dados enviados com sucesso!")
            try:
                result = response.json()
                print(f"📊 Resposta: {result}")
            except:
                print(f"📄 Resposta texto: {response.text[:100]}")
        else:
            print(f"⚠️ Status HTTP: {response.status_code}")
            print(f"📄 Resposta: {response.text[:200]}")
    except Exception as e:
        print(f"❌ Erro no envio: {e}")

def main():
    """Função principal melhorada"""
    print("🎬 COLETA DE DADOS MUSICAIS - VERSÃO MELHORADA")
    print("=" * 60)
    
    # Verificar ambiente
    if not verificar_ambiente():
        sys.exit(1)
    
    tempo_inicio = time.time()
    
    # Login
    cookies = fazer_login_robusto()
    if not cookies:
        print("❌ Falha no login. Abortando.")
        sys.exit(1)
    
    print("\n🚀 Iniciando coleta robusta...")
    print("-" * 40)
    
    # Executar coleta
    membros_coletados = executar_coleta_robusta(cookies)
    
    tempo_total = time.time() - tempo_inicio
    
    # Estatísticas finais
    print("\n" + "="*60)
    print("🏁 COLETA FINALIZADA!")
    print(f"📊 Membros coletados: {len(membros_coletados):,}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    
    if membros_coletados:
        print(f"⚡ Velocidade: {len(membros_coletados)/(tempo_total/60):.1f} membros/min")
        
        # Mostrar amostras
        print("\n📋 AMOSTRAS DOS DADOS COLETADOS:")
        for i, membro in enumerate(membros_coletados[:5], 1):
            nome = membro.get('nome', 'N/A')[:30]
            igreja = membro.get('igreja_selecionada', 'N/A')
            instrumento = membro.get('instrumento', 'N/A')
            print(f"  {i}. {nome} | Igreja: {igreja} | Instrumento: {instrumento}")
        
        # Enviar para Google Sheets
        enviar_para_sheets(membros_coletados, tempo_total)
        
    else:
        print("⚠️ Nenhum membro foi coletado neste range")
        print("🔍 Possíveis causas:")
        print("   - Range de IDs inválido")
        print("   - Problemas de conectividade")
        print("   - Sessão expirou")
        print("   - Site está bloqueando requisições")
    
    print(f"\n🎯 Processo finalizado - Instância {INSTANCIA_ID}")

if __name__ == "__main__":
    main()
