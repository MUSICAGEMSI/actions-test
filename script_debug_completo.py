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

# Configuração para DEBUG - apenas alguns IDs
RANGE_INICIO = 400001
RANGE_FIM = 400005  # APENAS 5 IDs PARA DEBUG
INSTANCIA_ID = "GHA_debug"

# Configurações para debug
NUM_THREADS = 1  # Apenas 1 thread para debug
TIMEOUT_REQUEST = 15
PAUSA_ENTRE_REQUESTS = 1.0  # Pausa maior para debug

# Flag de debug
DEBUG_MODE = True

def debug_print(mensagem):
    """Print apenas se estiver em modo debug"""
    if DEBUG_MODE:
        print(f"🐛 DEBUG: {mensagem}")

def salvar_html_debug(html_content, membro_id, motivo=""):
    """Salva HTML para debug (apenas primeiros IDs)"""
    if DEBUG_MODE and membro_id <= 400003:
        filename = f"debug_html_{membro_id}_{motivo}.html"
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
            debug_print(f"HTML salvo: {filename} ({len(html_content)} chars)")
        except Exception as e:
            debug_print(f"Erro ao salvar HTML: {e}")

def extrair_dados_com_debug(html_content, membro_id):
    """Extração com debug detalhado"""
    debug_print(f"=== INICIANDO EXTRAÇÃO ID {membro_id} ===")
    debug_print(f"Tamanho HTML: {len(html_content)} caracteres")
    
    try:
        # Validações básicas
        if not html_content:
            debug_print("❌ HTML vazio")
            return None
            
        if len(html_content) < 100:
            debug_print(f"❌ HTML muito pequeno: {len(html_content)} chars")
            salvar_html_debug(html_content, membro_id, "pequeno")
            return None
        
        # Verificar se tem conteúdo esperado
        debug_print("🔍 Verificando presença de elementos...")
        
        elementos_check = {
            'name="nome"': 'name="nome"' in html_content,
            'id="nome"': 'id="nome"' in html_content,
            'igreja_selecionada': 'igreja_selecionada' in html_content,
            'musical.congregacao': 'musical.congregacao' in html_content,
            'form': '<form' in html_content,
            'input': '<input' in html_content
        }
        
        for elemento, presente in elementos_check.items():
            status = "✅" if presente else "❌"
            debug_print(f"{status} {elemento}: {presente}")
        
        # Se não tem elementos básicos, salvar HTML e analisar
        if not elementos_check['name="nome"'] and not elementos_check['id="nome"']:
            debug_print("❌ Nenhum campo nome encontrado")
            salvar_html_debug(html_content, membro_id, "sem_nome")
            
            # Verificar se é página de erro ou redirecionamento
            if 'erro' in html_content.lower():
                debug_print("🚨 Página contém 'erro'")
            if 'login' in html_content.lower():
                debug_print("🚨 Página contém 'login' - possível redirecionamento")
            if 'forbidden' in html_content.lower():
                debug_print("🚨 Página contém 'forbidden'")
            if len(html_content) < 1000:
                debug_print(f"📝 HTML completo (pequeno): {html_content[:500]}...")
            
            return None
        
        dados = {'id': membro_id}
        
        # Tentar extrair nome com vários patterns
        debug_print("🔍 Tentando extrair NOME...")
        nome_patterns = [
            r'name="nome"[^>]*value="([^"]*)"',
            r'id="nome"[^>]*value="([^"]*)"',
            r'<input[^>]*nome[^>]*value="([^"]*)"',
        ]
        
        nome_encontrado = False
        for i, pattern in enumerate(nome_patterns):
            debug_print(f"   Tentando pattern {i+1}: {pattern}")
            nome_match = re.search(pattern, html_content, re.IGNORECASE)
            if nome_match:
                nome = nome_match.group(1).strip()
                debug_print(f"   ✅ NOME encontrado: '{nome}'")
                if nome and nome != "":
                    dados['nome'] = nome
                    nome_encontrado = True
                    break
                else:
                    debug_print(f"   ⚠️ NOME vazio")
            else:
                debug_print(f"   ❌ Pattern {i+1} não encontrou")
        
        if not nome_encontrado:
            debug_print("❌ NENHUM NOME ENCONTRADO")
            salvar_html_debug(html_content, membro_id, "nome_nao_encontrado")
            return None
        
        # Tentar extrair igreja
        debug_print("🔍 Tentando extrair IGREJA...")
        igreja_patterns = [
            r'igreja_selecionada\s*\(\s*(\d+)\s*\)',
            r'igreja[^>]*value="(\d+)"',
            r'id_igreja[^>]*>.*?selected[^>]*value="(\d+)"'
        ]
        
        igreja_encontrada = False
        for i, pattern in enumerate(igreja_patterns):
            debug_print(f"   Tentando pattern igreja {i+1}: {pattern}")
            igreja_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if igreja_match:
                igreja = igreja_match.group(1).strip()
                debug_print(f"   ✅ IGREJA encontrada: '{igreja}'")
                dados['igreja_selecionada'] = igreja
                igreja_encontrada = True
                break
            else:
                debug_print(f"   ❌ Pattern igreja {i+1} não encontrou")
        
        if not igreja_encontrada:
            dados['igreja_selecionada'] = ''
            debug_print("⚠️ Igreja não encontrada")
        
        # Outros campos
        debug_print("🔍 Tentando extrair outros campos...")
        campos_patterns = {
            'cargo_ministerio': [
                r'id_cargo[^>]*>.*?selected[^>]*>([^<]*)',
                r'name="cargo"[^>]*>.*?selected[^>]*>([^<]*)',
                r'cargo[^>]*value="([^"]*)"'
            ],
            'nivel': [
                r'id_nivel[^>]*>.*?selected[^>]*>([^<]*)',
                r'name="nivel"[^>]*>.*?selected[^>]*>([^<]*)',
                r'nivel[^>]*value="([^"]*)"'
            ],
            'instrumento': [
                r'id_instrumento[^>]*>.*?selected[^>]*>([^<]*)',
                r'name="instrumento"[^>]*>.*?selected[^>]*>([^<]*)',
                r'instrumento[^>]*value="([^"]*)"'
            ],
            'tonalidade': [
                r'id_tonalidade[^>]*>.*?selected[^>]*>([^<]*)',
                r'name="tonalidade"[^>]*>.*?selected[^>]*>([^<]*)',
                r'tonalidade[^>]*value="([^"]*)"'
            ]
        }
        
        for campo, patterns in campos_patterns.items():
            debug_print(f"   Extraindo {campo}...")
            valor_encontrado = ''
            for j, pattern in enumerate(patterns):
                match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
                if match:
                    valor_encontrado = match.group(1).strip()
                    debug_print(f"      ✅ {campo}: '{valor_encontrado}' (pattern {j+1})")
                    break
            
            dados[campo] = valor_encontrado
            if not valor_encontrado:
                debug_print(f"      ❌ {campo}: não encontrado")
        
        debug_print(f"✅ DADOS EXTRAÍDOS: {dados}")
        
        # Para debug, salvar HTML bem-sucedido também
        if membro_id <= 400003:
            salvar_html_debug(html_content, membro_id, "sucesso")
        
        return dados
        
    except Exception as e:
        debug_print(f"❌ ERRO na extração: {str(e)}")
        salvar_html_debug(html_content, membro_id, "erro")
        return None

class ColetorDebug:
    def __init__(self, cookies, thread_id=0):
        self.thread_id = thread_id
        self.sucessos = 0
        self.falhas = 0
        self.timeouts = 0
        self.erros_http = 0
        
        # Criar sessão
        self.session = requests.Session()
        self.session.cookies.update(cookies)
        
        # Headers mais completos
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
    
    def coletar_com_debug(self, ids_lista):
        """Coleta com debug detalhado"""
        membros = []
        
        debug_print(f"=== INICIANDO COLETA THREAD {self.thread_id} ===")
        debug_print(f"IDs para processar: {ids_lista}")
        
        for i, membro_id in enumerate(ids_lista):
            debug_print(f"\n--- PROCESSANDO ID {membro_id} ({i+1}/{len(ids_lista)}) ---")
            
            try:
                url = f"https://musical.congregacao.org.br/licoes/index/{membro_id}"
                debug_print(f"🌐 URL: {url}")
                
                # Fazer requisição
                debug_print("📡 Fazendo requisição...")
                resp = self.session.get(url, headers=self.headers, timeout=TIMEOUT_REQUEST)
                
                debug_print(f"📊 Status: {resp.status_code}")
                debug_print(f"📊 Headers resposta: {dict(list(resp.headers.items())[:5])}")
                debug_print(f"📊 Tamanho resposta: {len(resp.text)} chars")
                
                if resp.status_code == 200:
                    # Mostrar início do HTML
                    html_inicio = resp.text[:300].replace('\n', ' ').replace('\r', ' ')
                    debug_print(f"📄 Início HTML: {html_inicio}...")
                    
                    # Verificar se tem indicadores de redirecionamento
                    if 'location.href' in resp.text or 'window.location' in resp.text:
                        debug_print("🚨 Possível redirecionamento JavaScript detectado")
                    
                    # Tentar extrair dados
                    dados = extrair_dados_com_debug(resp.text, membro_id)
                    
                    if dados:
                        membros.append(dados)
                        self.sucessos += 1
                        debug_print(f"✅ SUCESSO: Dados coletados para {dados['nome']}")
                    else:
                        self.falhas += 1
                        debug_print(f"❌ FALHA: Não foi possível extrair dados")
                
                elif resp.status_code == 404:
                    debug_print("ℹ️ ID não existe (404) - normal")
                    self.falhas += 1
                    
                else:
                    debug_print(f"⚠️ Status inesperado: {resp.status_code}")
                    debug_print(f"📄 Resposta: {resp.text[:200]}...")
                    self.erros_http += 1
                
                # Pausa entre requisições
                debug_print(f"⏸️ Pausando {PAUSA_ENTRE_REQUESTS}s...")
                time.sleep(PAUSA_ENTRE_REQUESTS)
                
            except requests.exceptions.Timeout:
                self.timeouts += 1
                debug_print(f"⏱️ TIMEOUT no ID {membro_id}")
                
            except Exception as e:
                self.falhas += 1
                debug_print(f"❌ ERRO GERAL no ID {membro_id}: {str(e)}")
        
        debug_print(f"\n=== THREAD {self.thread_id} FINALIZADA ===")
        debug_print(f"Sucessos: {self.sucessos}, Falhas: {self.falhas}, Timeouts: {self.timeouts}")
        
        return membros

def fazer_login_debug():
    """Login com debug"""
    debug_print("=== INICIANDO LOGIN ===")
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            
            debug_print(f"🌐 Acessando: {URL_INICIAL}")
            page.goto(URL_INICIAL, timeout=30000)
            
            debug_print("📝 Preenchendo login...")
            page.fill('input[name="login"]', EMAIL)
            page.fill('input[name="password"]', SENHA)
            
            debug_print("🚀 Submetendo...")
            page.click('button[type="submit"]')
            
            debug_print("⏱️ Aguardando redirecionamento...")
            page.wait_for_selector("nav", timeout=20000)
            
            debug_print("🧪 Testando URL específica...")
            test_url = "https://musical.congregacao.org.br/licoes/index/400001"
            page.goto(test_url, timeout=15000)
            
            # Capturar conteúdo da página de teste
            test_content = page.content()
            debug_print(f"📄 Página teste: {len(test_content)} chars")
            
            if DEBUG_MODE:
                with open("debug_test_page.html", "w", encoding="utf-8") as f:
                    f.write(test_content)
                debug_print("💾 Página de teste salva: debug_test_page.html")
            
            # Extrair cookies
            cookies = {cookie['name']: cookie['value'] for cookie in context.cookies()}
            debug_print(f"🍪 Cookies: {list(cookies.keys())}")
            
            browser.close()
            return cookies
            
    except Exception as e:
        debug_print(f"❌ ERRO no login: {str(e)}")
        return None

def main():
    """Função principal para debug"""
    print("🐛 MODO DEBUG - COLETA DE DADOS MUSICAIS")
    print("=" * 60)
    
    debug_print(f"Range: {RANGE_INICIO} - {RANGE_FIM}")
    debug_print(f"Total IDs: {RANGE_FIM - RANGE_INICIO + 1}")
    
    # Login
    cookies = fazer_login_debug()
    if not cookies:
        print("❌ Falha no login")
        sys.exit(1)
    
    # Coletar dados
    debug_print("\n=== INICIANDO COLETA ===")
    ids_lista = list(range(RANGE_INICIO, RANGE_FIM + 1))
    
    coletor = ColetorDebug(cookies, 0)
    membros_coletados = coletor.coletar_com_debug(ids_lista)
    
    # Resultados
    print("\n" + "="*60)
    print("🏁 DEBUG FINALIZADO!")
    print(f"📊 Membros coletados: {len(membros_coletados)}")
    print(f"✅ Sucessos: {coletor.sucessos}")
    print(f"❌ Falhas: {coletor.falhas}")
    print(f"⏱️ Timeouts: {coletor.timeouts}")
    print(f"🌐 Erros HTTP: {coletor.erros_http}")
    
    if membros_coletados:
        print("\n📋 DADOS COLETADOS:")
        for membro in membros_coletados:
            print(f"  ID {membro['id']}: {membro.get('nome', 'N/A')}")
    
    print("\n📁 Arquivos de debug gerados:")
    print("  - debug_test_page.html (página após login)")
    print("  - debug_html_*.html (páginas específicas)")

if __name__ == "__main__":
    main()
