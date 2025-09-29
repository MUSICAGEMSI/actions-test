from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os
import requests
import json
import time
from urllib.parse import urljoin

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
BASE_URL = "https://musical.congregacao.org.br"

# IDs de teste de Hortolândia (você vai substituir pelos reais)
TURMAS_HORTOLANDIA = [27292, 42233]  # SUBSTITUIR
CONGREGACOES_HORTOLANDIA = [20391, 20964]  # SUBSTITUIR

def fazer_login_e_obter_session():
    """Faz login e retorna session com cookies"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        print("🔐 Fazendo login...")
        page.goto(BASE_URL)
        page.fill('input[name="login"]', EMAIL)
        page.fill('input[name="password"]', SENHA)
        page.click('button[type="submit"]')
        
        page.wait_for_selector("nav", timeout=15000)
        print("✅ Login realizado!")
        
        # Extrair cookies
        cookies = page.context.cookies()
        session = requests.Session()
        for cookie in cookies:
            session.cookies.set(cookie['name'], cookie['value'])
        
        browser.close()
        return session

def testar_endpoint(session, endpoint, metodo="GET", data=None):
    """Testa um endpoint e retorna resultado"""
    url = urljoin(BASE_URL, endpoint)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'X-Requested-With': 'XMLHttpRequest',  # Importante para APIs AJAX
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': f'{BASE_URL}/aulas_abertas/listagem',
    }
    
    try:
        if metodo == "GET":
            response = session.get(url, headers=headers, timeout=5)
        else:
            response = session.post(url, headers=headers, json=data, timeout=5)
        
        return {
            'url': url,
            'status': response.status_code,
            'content_type': response.headers.get('Content-Type', ''),
            'size': len(response.content),
            'preview': response.text[:200] if response.status_code == 200 else None
        }
    except Exception as e:
        return {
            'url': url,
            'status': 'ERROR',
            'error': str(e)
        }

def explorar_apis_por_turma(session):
    """Testa diferentes padrões de API para buscar aulas por turma"""
    
    print("\n" + "="*60)
    print("🔍 FASE 1: EXPLORANDO APIs POR TURMA_ID")
    print("="*60)
    
    padroes_api = [
        # Padrões REST clássicos
        "/api/aulas/turma/{turma_id}",
        "/api/turmas/{turma_id}/aulas",
        "/aulas/turma/{turma_id}",
        
        # Padrões do sistema atual
        "/aulas_abertas/por_turma/{turma_id}",
        "/aulas_abertas/listar/{turma_id}",
        "/aulas_abertas/buscar?turma_id={turma_id}",
        
        # Padrões de frequência
        "/frequencias/turma/{turma_id}",
        "/frequencias/listar?turma={turma_id}",
        
        # Padrões com query string
        "/aulas_abertas/listagem?turma_id={turma_id}",
        "/aulas_abertas/listagem?id_turma={turma_id}",
        "/aulas_abertas?turma={turma_id}",
        
        # Padrões JSON-RPC ou DataTables
        "/aulas_abertas/listar_json",
        "/aulas_abertas/ajax_list",
        "/aulas_abertas/get_aulas",
    ]
    
    resultados = []
    
    for turma_id in TURMAS_HORTOLANDIA[:1]:  # Testar com primeira turma
        print(f"\n📋 Testando com turma_id = {turma_id}")
        
        for padrao in padroes_api:
            endpoint = padrao.format(turma_id=turma_id)
            resultado = testar_endpoint(session, endpoint)
            
            if resultado['status'] == 200:
                print(f"   ✅ {endpoint}")
                print(f"      📄 Content-Type: {resultado['content_type']}")
                print(f"      📦 Size: {resultado['size']} bytes")
                if resultado['preview']:
                    print(f"      👀 Preview: {resultado['preview']}")
                resultados.append(resultado)
            elif resultado['status'] in [403, 401]:
                print(f"   🔒 {endpoint} - Acesso negado")
            elif resultado['status'] == 404:
                print(f"   ❌ {endpoint} - Not found")
            else:
                print(f"   ⚠️ {endpoint} - Status {resultado['status']}")
    
    return resultados

def explorar_apis_por_congregacao(session):
    """Testa diferentes padrões de API para buscar aulas por congregação"""
    
    print("\n" + "="*60)
    print("🔍 FASE 2: EXPLORANDO APIs POR CONGREGACAO_ID")
    print("="*60)
    
    padroes_api = [
        "/api/aulas/congregacao/{congregacao_id}",
        "/aulas_abertas/congregacao/{congregacao_id}",
        "/aulas_abertas/listagem?congregacao_id={congregacao_id}",
        "/aulas_abertas?congregacao={congregacao_id}",
    ]
    
    resultados = []
    
    for congregacao_id in CONGREGACOES_HORTOLANDIA[:1]:
        print(f"\n🏛️ Testando com congregacao_id = {congregacao_id}")
        
        for padrao in padroes_api:
            endpoint = padrao.format(congregacao_id=congregacao_id)
            resultado = testar_endpoint(session, endpoint)
            
            if resultado['status'] == 200:
                print(f"   ✅ {endpoint}")
                print(f"      📄 Content-Type: {resultado['content_type']}")
                print(f"      📦 Size: {resultado['size']} bytes")
                resultados.append(resultado)
            elif resultado['status'] == 404:
                print(f"   ❌ {endpoint}")
    
    return resultados

def testar_datatables_api(session):
    """Testa se a listagem usa DataTables e aceita filtros"""
    
    print("\n" + "="*60)
    print("🔍 FASE 3: TESTANDO DataTables API")
    print("="*60)
    
    # DataTables envia requisições AJAX neste formato
    datatables_params = {
        'draw': 1,
        'start': 0,
        'length': 100,
        'search[value]': '',
        'search[regex]': 'false',
    }
    
    # Tentar diferentes combinações de filtro
    testes = [
        {'turma_id': TURMAS_HORTOLANDIA[0]},
        {'id_turma': TURMAS_HORTOLANDIA[0]},
        {'congregacao_id': CONGREGACOES_HORTOLANDIA[0]},
        {'search[value]': 'HORTOLÂNDIA'},
        {'columns[0][search][value]': 'HORTOLÂNDIA'},  # Filtro na coluna 0
    ]
    
    for teste in testes:
        params = {**datatables_params, **teste}
        
        url = f"{BASE_URL}/aulas_abertas/listagem"
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'X-Requested-With': 'XMLHttpRequest',
            'Accept': 'application/json',
        }
        
        try:
            response = session.get(url, params=params, headers=headers, timeout=5)
            
            if response.status_code == 200:
                print(f"   ✅ Params: {teste}")
                print(f"      📦 Size: {len(response.content)} bytes")
                
                # Tentar parsear como JSON
                try:
                    data = response.json()
                    print(f"      📊 JSON Keys: {list(data.keys())}")
                    if 'data' in data:
                        print(f"      📈 Records: {len(data.get('data', []))}")
                except:
                    print(f"      📄 HTML response (not JSON)")
                    
        except Exception as e:
            print(f"   ⚠️ Erro: {e}")

def interceptar_requisicoes_reais():
    """Intercepta requisições reais do navegador para descobrir APIs"""
    
    print("\n" + "="*60)
    print("🔍 FASE 4: INTERCEPTANDO REQUISIÇÕES DO NAVEGADOR")
    print("="*60)
    
    requisicoes_ajax = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        def capturar_request(request):
            # Capturar apenas requisições AJAX/Fetch
            if any(keyword in request.url for keyword in ['aulas', 'listagem', 'frequencia', 'api', 'ajax']):
                requisicoes_ajax.append({
                    'method': request.method,
                    'url': request.url,
                    'resource_type': request.resource_type,
                    'headers': dict(request.headers),
                    'post_data': request.post_data
                })
                print(f"   📡 {request.method} {request.url}")
        
        page.on('request', capturar_request)
        
        # Login
        print("\n🔐 Fazendo login...")
        page.goto(BASE_URL)
        page.fill('input[name="login"]', EMAIL)
        page.fill('input[name="password"]', SENHA)
        page.click('button[type="submit"]')
        page.wait_for_selector("nav", timeout=15000)
        
        # Navegar para histórico
        print("🔍 Navegando para histórico de aulas...")
        page.goto(f"{BASE_URL}/aulas_abertas/listagem")
        time.sleep(3)
        
        # Mudar quantidade de registros (dispara AJAX)
        print("⚙️ Alterando quantidade de registros...")
        page.select_option('select[name="listagem_length"]', "100")
        time.sleep(3)
        
        # Tentar usar busca (se houver)
        try:
            search_input = page.query_selector('input[type="search"]')
            if search_input:
                print("🔍 Testando busca...")
                search_input.fill("HORTOLÂNDIA")
                time.sleep(2)
        except:
            pass
        
        print("\n📋 Pressione ENTER para finalizar e ver requisições capturadas...")
        input()
        
        browser.close()
    
    return requisicoes_ajax

def main():
    print("🚀 INICIANDO DESCOBERTA DE APIs OCULTAS")
    print("="*60)
    
    # Fazer login
    session = fazer_login_e_obter_session()
    
    # Fase 1: Testar endpoints por turma
    resultados_turma = explorar_apis_por_turma(session)
    
    # Fase 2: Testar endpoints por congregação
    resultados_congregacao = explorar_apis_por_congregacao(session)
    
    # Fase 3: Testar DataTables
    testar_datatables_api(session)
    
    # Fase 4: Interceptar requisições reais
    print("\n" + "="*60)
    print("⚠️ FASE 4 abrirá o navegador para interceptar requisições reais")
    print("   Você precisará interagir com a página")
    print("="*60)
    resposta = input("Executar Fase 4? (s/n): ")
    
    if resposta.lower() == 's':
        requisicoes = interceptar_requisicoes_reais()
        
        print("\n" + "="*60)
        print("📊 REQUISIÇÕES AJAX CAPTURADAS")
        print("="*60)
        
        for req in requisicoes:
            print(f"\n{req['method']} {req['url']}")
            if req['post_data']:
                print(f"   📦 POST Data: {req['post_data']}")
    
    print("\n" + "="*60)
    print("✅ DESCOBERTA FINALIZADA!")
    print("="*60)
    
    # Salvar resultados
    with open('api_discovery_results.json', 'w', encoding='utf-8') as f:
        json.dump({
            'turma_apis': resultados_turma,
            'congregacao_apis': resultados_congregacao,
        }, f, indent=2, ensure_ascii=False)
    
    print("\n💾 Resultados salvos em: api_discovery_results.json")

if __name__ == "__main__":
    main()
