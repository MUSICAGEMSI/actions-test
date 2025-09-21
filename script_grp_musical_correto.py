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

# Configuração para GitHub Actions
RANGE_INICIO = 603080
RANGE_FIM = 603200  # 120 IDs para teste
INSTANCIA_ID = "GHA_grp_musical_v1"

# Configurações otimizadas
NUM_THREADS = 6
TIMEOUT_REQUEST = 12
PAUSA_ENTRE_REQUESTS = 0.1

def extrair_dados_grp_musical(html_content, membro_id):
    """
    Extrai dados da página grp_musical/editar
    """
    try:
        if not html_content or len(html_content) < 1000:
            return None
        
        dados = {'id': membro_id}
        
        # 1. NOME - extrair do input
        nome_patterns = [
            r'name="nome"[^>]*value="([^"]*)"',
            r'id="nome"[^>]*value="([^"]*)"',
        ]
        
        for pattern in nome_patterns:
            match = re.search(pattern, html_content)
            if match:
                nome = match.group(1).strip()
                if nome and nome != "":
                    dados['nome'] = nome
                    break
        
        if 'nome' not in dados:
            return None
        
        # 2. IGREJA - extrair do JavaScript igreja_selecionada
        igreja_patterns = [
            r'igreja_selecionada\s*\(\s*(\d+)\s*\)',
            r'id_igreja.*?(\d+)["\'].*?selected',
            r'value="(\d{5})".*?selected.*?congregação',
        ]
        
        dados['igreja_selecionada'] = ''
        for pattern in igreja_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if match:
                dados['igreja_selecionada'] = match.group(1)
                break
        
        # 3. CARGO/MINISTÉRIO - extrair option selected
        cargo_patterns = [
            r'name="id_cargo"[^>]*>.*?<option[^>]*value="[^"]*"[^>]*selected[^>]*>\s*([^<\n]+)',
            r'id="id_cargo"[^>]*>.*?<option[^>]*selected[^>]*>\s*([^<\n]+)',
            r'<select[^>]*id_cargo.*?selected[^>]*>\s*([^<\n]+)',
        ]
        
        dados['cargo_ministerio'] = ''
        for pattern in cargo_patterns:
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if match:
                cargo = match.group(1).strip()
                if cargo and cargo != "":
                    dados['cargo_ministerio'] = cargo
                    break
        
        # 4. NÍVEL - extrair option selected
        nivel_patterns = [
            r'name="id_nivel"[^>]*>.*?<option[^>]*value="[^"]*"[^>]*selected[^>]*>\s*([^<\n]+)',
            r'id="id_nivel"[^>]*>.*?<option[^>]*selected[^>]*>\s*([^<\n]+)',
        ]
        
        dados['nivel'] = ''
        for pattern in nivel_patterns:
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if match:
                nivel = match.group(1).strip()
                if nivel and nivel != "":
                    dados['nivel'] = nivel
                    break
        
        # 5. INSTRUMENTO - extrair option selected
        instrumento_patterns = [
            r'name="id_instrumento"[^>]*>.*?<option[^>]*value="[^"]*"[^>]*selected[^>]*>\s*([^<\n]+)',
            r'id="id_instrumento"[^>]*>.*?<option[^>]*selected[^>]*>\s*([^<\n]+)',
        ]
        
        dados['instrumento'] = ''
        for pattern in instrumento_patterns:
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if match:
                instrumento = match.group(1).strip()
                if instrumento and instrumento != "":
                    dados['instrumento'] = instrumento
                    break
        
        # 6. TONALIDADE - extrair option selected
        tonalidade_patterns = [
            r'name="id_tonalidade"[^>]*>.*?<option[^>]*value="[^"]*"[^>]*selected[^>]*>\s*([^<\n]+)',
            r'id="id_tonalidade"[^>]*>.*?<option[^>]*selected[^>]*>\s*([^<\n]+)',
        ]
        
        dados['tonalidade'] = ''
        for pattern in tonalidade_patterns:
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if match:
                tonalidade = match.group(1).strip()
                if tonalidade and tonalidade != "":
                    dados['tonalidade'] = tonalidade
                    break
        
        return dados
        
    except Exception as e:
        print(f"Erro ao extrair dados do ID {membro_id}: {str(e)[:50]}")
        return None

class ColetorGrpMusical:
    def __init__(self, cookies, thread_id=0):
        self.thread_id = thread_id
        self.sucessos = 0
        self.falhas = 0
        self.timeouts = 0
        self.erros_http = 0
        self.nao_encontrados = 0
        
        # Criar sessão
        self.session = requests.Session()
        self.session.cookies.update(cookies)
        
        # Configurar retry strategy
        retry_strategy = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=8,
            pool_maxsize=8
        )
        
        self.session.mount("https://", adapter)
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://musical.congregacao.org.br/',
            'Cache-Control': 'max-age=0'
        }
    
    def coletar_batch(self, ids_lista):
        """Coleta um batch de IDs usando a URL correta"""
        membros = []
        
        for i, membro_id in enumerate(ids_lista):
            try:
                # URL CORRETA: grp_musical/editar
                url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
                
                resp = self.session.get(url, headers=self.headers, timeout=TIMEOUT_REQUEST)
                
                if resp.status_code == 200:
                    # Verificar se não é página de erro/redirecionamento
                    if len(resp.text) > 5000:  # Página completa tem mais de 5k chars
                        dados = extrair_dados_grp_musical(resp.text, membro_id)
                        
                        if dados:
                            membros.append(dados)
                            self.sucessos += 1
                            
                            # Log a cada sucesso nos primeiros, depois a cada 10
                            if self.sucessos <= 5 or self.sucessos % 10 == 0:
                                nome_curto = dados['nome'][:25] if dados['nome'] else 'N/A'
                                print(f"✅ T{self.thread_id}: {self.sucessos} - {nome_curto} | {dados.get('instrumento', 'N/A')}")
                        else:
                            self.falhas += 1
                    else:
                        self.falhas += 1
                        
                elif resp.status_code == 404:
                    self.nao_encontrados += 1
                    
                else:
                    self.erros_http += 1
                    if self.erros_http <= 3:  # Log apenas primeiros erros
                        print(f"⚠️ T{self.thread_id}: HTTP {resp.status_code} no ID {membro_id}")
                
                # Pausa entre requests
                time.sleep(PAUSA_ENTRE_REQUESTS)
                
            except requests.exceptions.Timeout:
                self.timeouts += 1
                
            except requests.exceptions.RequestException:
                self.falhas += 1
                
            except Exception:
                self.falhas += 1
        
        return membros

def fazer_login():
    """Login otimizado"""
    print("🔐 Fazendo login...")
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security'
                ]
            )
            
            context = browser.new_context(
                user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
            )
            
            page = context.new_page()
            
            # Login
            page.goto(URL_INICIAL, timeout=30000)
            page.fill('input[name="login"]', EMAIL)
            page.fill('input[name="password"]', SENHA)
            page.click('button[type="submit"]')
            page.wait_for_selector("nav", timeout=20000)
            
            print("✅ Login realizado!")
            
            # Testar URL correta
            test_url = "https://musical.congregacao.org.br/grp_musical/editar/603084"
            page.goto(test_url, timeout=15000)
            
            # Extrair cookies
            cookies = {cookie['name']: cookie['value'] for cookie in context.cookies()}
            browser.close()
            
            return cookies
            
    except Exception as e:
        print(f"❌ Erro no login: {str(e)}")
        return None

def executar_coleta(cookies):
    """Executa coleta paralela"""
    total_ids = RANGE_FIM - RANGE_INICIO + 1
    ids_per_thread = total_ids // NUM_THREADS
    
    print(f"📈 Coletando {total_ids} IDs com {NUM_THREADS} threads")
    
    # Dividir IDs por thread
    thread_ids = []
    for i in range(NUM_THREADS):
        inicio = RANGE_INICIO + (i * ids_per_thread)
        fim = inicio + ids_per_thread - 1
        
        if i == NUM_THREADS - 1:
            fim = RANGE_FIM
            
        thread_ids.append(list(range(inicio, fim + 1)))
    
    todos_membros = []
    
    # Executar threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        coletores = [ColetorGrpMusical(cookies, i) for i in range(NUM_THREADS)]
        
        futures = []
        for i, ids_lista in enumerate(thread_ids):
            future = executor.submit(coletores[i].coletar_batch, ids_lista)
            futures.append((future, i))
        
        # Coletar resultados
        for future, thread_id in futures:
            try:
                membros_thread = future.result(timeout=1800)  # 30min timeout
                todos_membros.extend(membros_thread)
                
                coletor = coletores[thread_id]
                print(f"🏁 T{thread_id}: {len(membros_thread)} membros | "
                      f"✅{coletor.sucessos} ❌{coletor.falhas} ⏱️{coletor.timeouts} 🚫{coletor.nao_encontrados}")
                
            except concurrent.futures.TimeoutError:
                print(f"⏱️ T{thread_id}: Timeout")
            except Exception as e:
                print(f"❌ T{thread_id}: {str(e)[:30]}")
    
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
            "ambiente": "GitHub Actions - URL Correta",
            "url_usada": "grp_musical/editar"
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
        if response.status_code == 200:
            print("✅ Enviado com sucesso!")
            try:
                result = response.json()
                if 'registros_inseridos' in result:
                    print(f"📊 {result['registros_inseridos']} registros inseridos")
            except:
                pass
        else:
            print(f"⚠️ Status: {response.status_code}")
    except Exception as e:
        print(f"❌ Erro no envio: {e}")

def main():
    """Função principal"""
    print("🎵 COLETA MEMBROS MUSICAIS - URL CORRETA")
    print("=" * 50)
    print(f"🎯 Instância: {INSTANCIA_ID}")
    print(f"📊 Range: {RANGE_INICIO:,} - {RANGE_FIM:,}")
    print(f"⚙️ Threads: {NUM_THREADS} | Timeout: {TIMEOUT_REQUEST}s")
    print(f"🌐 URL: grp_musical/editar/{{ID}}")
    
    # Verificar credenciais
    if not EMAIL or not SENHA:
        print("❌ Credenciais não encontradas")
        sys.exit(1)
    
    tempo_inicio = time.time()
    
    # Login
    cookies = fazer_login()
    if not cookies:
        sys.exit(1)
    
    print("\n🚀 Iniciando coleta...")
    
    # Executar coleta
    membros_coletados = executar_coleta(cookies)
    
    tempo_total = time.time() - tempo_inicio
    
    # Resultados
    print("\n" + "="*50)
    print("🏁 COLETA FINALIZADA!")
    print(f"📊 Membros coletados: {len(membros_coletados):,}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    
    if membros_coletados:
        print(f"⚡ Velocidade: {len(membros_coletados)/(tempo_total/60):.1f} membros/min")
        
        # Mostrar amostras
        print("\n📋 AMOSTRAS:")
        for i, membro in enumerate(membros_coletados[:3], 1):
            nome = membro.get('nome', 'N/A')[:30]
            igreja = membro.get('igreja_selecionada', 'N/A')
            instrumento = membro.get('instrumento', 'N/A')[:15]
            cargo = membro.get('cargo_ministerio', 'N/A')[:10]
            print(f"  {i}. {nome} | Igreja: {igreja} | {instrumento} | {cargo}")
        
        # Enviar para Sheets
        enviar_para_sheets(membros_coletados, tempo_total)
        
    else:
        print("⚠️ Nenhum membro coletado")
    
    print(f"\n🎯 Finalizado - {INSTANCIA_ID}")

if __name__ == "__main__":
    main()
