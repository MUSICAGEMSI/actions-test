from playwright.sync_api import sync_playwright
import json
import time
from urllib.parse import urlparse, parse_qs

class NetworkAPIDiscovery:
    def __init__(self, email, senha):
        self.email = email
        self.senha = senha
        self.captured_requests = []
        self.captured_responses = []
    
    def capture_network_traffic(self):
        """Captura todo tráfego de rede para descobrir APIs ocultas"""
        with sync_playwright() as p:
            navegador = p.chromium.launch(headless=False)  # Visível para debug
            context = navegador.new_context()
            pagina = context.new_page()
            
            # Interceptar todas as requisições
            def handle_request(request):
                if any(keyword in request.url.lower() for keyword in 
                      ['api', 'ajax', 'json', 'data', 'licoes', 'historico', 'alunos']):
                    
                    request_info = {
                        'url': request.url,
                        'method': request.method,
                        'headers': dict(request.headers),
                        'post_data': request.post_data,
                        'timestamp': time.time()
                    }
                    self.captured_requests.append(request_info)
                    print(f"📡 Request: {request.method} {request.url}")
            
            def handle_response(response):
                if any(keyword in response.url.lower() for keyword in 
                      ['api', 'ajax', 'json', 'data', 'licoes', 'historico', 'alunos']):
                    
                    try:
                        # Tentar capturar o body da resposta
                        body = response.body() if response.status == 200 else None
                        content_type = response.headers.get('content-type', '')
                        
                        response_info = {
                            'url': response.url,
                            'status': response.status,
                            'headers': dict(response.headers),
                            'content_type': content_type,
                            'body_size': len(body) if body else 0,
                            'timestamp': time.time()
                        }
                        
                        # Se for JSON, tentar parsear
                        if 'json' in content_type and body:
                            try:
                                json_data = json.loads(body.decode('utf-8'))
                                response_info['json_structure'] = self.analyze_json_structure(json_data)
                            except:
                                pass
                        
                        self.captured_responses.append(response_info)
                        print(f"📥 Response: {response.status} {response.url} ({response_info['body_size']} bytes)")
                        
                    except Exception as e:
                        print(f"⚠️ Erro ao capturar resposta: {e}")
            
            # Configurar interceptadores
            pagina.on('request', handle_request)
            pagina.on('response', handle_response)
            
            print("🌐 Iniciando captura de tráfego de rede...")
            
            # 1. Login
            pagina.goto("https://musical.congregacao.org.br/")
            pagina.fill('input[name="login"]', self.email)
            pagina.fill('input[name="password"]', self.senha)
            pagina.click('button[type="submit"]')
            
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado!")
            
            # 2. Navegar por diferentes seções para capturar APIs
            secoes_para_testar = [
                ("/alunos/listagem", "Lista de alunos"),
                ("/licoes", "Lições"),
                ("/dashboard", "Dashboard"),
                ("/ministerios", "Ministérios"),
                ("/instrumentos", "Instrumentos")
            ]
            
            for url_secao, nome_secao in secoes_para_testar:
                try:
                    print(f"📂 Navegando para: {nome_secao}")
                    pagina.goto(f"https://musical.congregacao.org.br{url_secao}")
                    
                    # Aguardar carregamento e possíveis chamadas AJAX
                    time.sleep(3)
                    
                    # Tentar interagir com elementos que podem fazer chamadas API
                    # Buscar por datatables, dropdowns, etc.
                    datatables = pagina.query_selector_all('table[id*="datatable"]')
                    for dt in datatables:
                        try:
                            # Tentar filtrar/ordenar para gerar mais requisições
                            search_input = pagina.query_selector('input[type="search"]')
                            if search_input:
                                search_input.fill("teste")
                                time.sleep(1)
                                search_input.fill("")
                                time.sleep(1)
                        except:
                            pass
                    
                except Exception as e:
                    print(f"⚠️ Erro ao navegar para {nome_secao}: {e}")
            
            # 3. Tentar acessar histórico de um aluno específico
            if self.captured_requests:
                # Procurar por requests que retornaram dados de alunos
                for req in self.captured_requests:
                    if 'alunos' in req['url'] and 'listagem' in req['url']:
                        print("🎯 Tentando acessar histórico de aluno...")
                        pagina.goto("https://musical.congregacao.org.br/licoes/index/1")
                        time.sleep(3)
                        break
            
            navegador.close()
            
        return self.captured_requests, self.captured_responses
    
    def analyze_json_structure(self, json_data):
        """Analisa a estrutura de dados JSON"""
        if isinstance(json_data, dict):
            return {
                'type': 'dict',
                'keys': list(json_data.keys()),
                'key_count': len(json_data.keys())
            }
        elif isinstance(json_data, list):
            return {
                'type': 'list',
                'length': len(json_data),
                'first_item_type': type(json_data[0]).__name__ if json_data else None,
                'first_item_keys': list(json_data[0].keys()) if json_data and isinstance(json_data[0], dict) else None
            }
        else:
            return {
                'type': type(json_data).__name__,
                'value_sample': str(json_data)[:100]
            }
    
    def generate_api_map(self, requests, responses):
        """Gera um mapa das APIs descobertas"""
        api_map = {}
        
        # Agrupar por endpoint base
        for req in requests:
            parsed_url = urlparse(req['url'])
            base_path = parsed_url.path
            
            if base_path not in api_map:
                api_map[base_path] = {
                    'methods': set(),
                    'parameters': set(),
                    'call_count': 0,
                    'responses': []
                }
            
            api_map[base_path]['methods'].add(req['method'])
            api_map[base_path]['call_count'] += 1
            
            # Analisar parâmetros
            if req['post_data']:
                try:
                    params = parse_qs(req['post_data'])
                    for param in params.keys():
                        api_map[base_path]['parameters'].add(param)
                except:
                    pass
            
            if parsed_url.query:
                params = parse_qs(parsed_url.query)
                for param in params.keys():
                    api_map[base_path]['parameters'].add(param)
        
        # Adicionar informações de resposta
        for resp in responses:
            parsed_url = urlparse(resp['url'])
            base_path = parsed_url.path
            
            if base_path in api_map:
                api_map[base_path]['responses'].append({
                    'status': resp['status'],
                    'content_type': resp['content_type'],
                    'size': resp['body_size'],
                    'json_structure': resp.get('json_structure')
                })
        
        # Converter sets para lists para serialização JSON
        for path in api_map:
            api_map[path]['methods'] = list(api_map[path]['methods'])
            api_map[path]['parameters'] = list(api_map[path]['parameters'])
        
        return api_map
    
    def discover_all_apis(self):
        """Método principal para descobrir todas as APIs"""
        print("🚀 Iniciando descoberta completa de APIs...")
        
        requests, responses = self.capture_network_traffic()
        
        print(f"\n📊 Estatísticas de captura:")
        print(f"   📡 Requisições capturadas: {len(requests)}")
        print(f"   📥 Respostas capturadas: {len(responses)}")
        
        # Gerar mapa de APIs
        api_map = self.generate_api_map(requests, responses)
        
        print(f"\n🗺️ APIs descobertas: {len(api_map)}")
        for path, info in api_map.items():
            print(f"   {path}")
            print(f"      Métodos: {info['methods']}")
            print(f"      Parâmetros: {info['parameters']}")
            print(f"      Chamadas: {info['call_count']}")
            if info['responses']:
                unique_statuses = set(r['status'] for r in info['responses'])
                print(f"      Status codes: {list(unique_statuses)}")
        
        # Salvar descobertas
        discovery_data = {
            'timestamp': time.time(),
            'requests': requests,
            'responses': responses,
            'api_map': api_map,
            'summary': {
                'total_requests': len(requests),
                'total_responses': len(responses),
                'unique_endpoints': len(api_map),
                'successful_responses': len([r for r in responses if r['status'] == 200])
            }
        }
        
        with open('network_discovery.json', 'w', encoding='utf-8') as f:
            json.dump(discovery_data, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n💾 Descobertas salvas em 'network_discovery.json'")
        
        return discovery_data

# Uso
if __name__ == "__main__":
    from dotenv import load_dotenv
    import os
    
    load_dotenv(dotenv_path="credencial.env")
    
    EMAIL = os.environ.get("LOGIN_MUSICAL")
    SENHA = os.environ.get("SENHA_MUSICAL")
    
    if not EMAIL or not SENHA:
        print("❌ Credenciais não encontradas")
        exit(1)
    
    discoverer = NetworkAPIDiscovery(EMAIL, SENHA)
    resultado = discoverer.discover_all_apis()
    
    # Mostrar os endpoints mais promissores
    print("\n🎯 ENDPOINTS MAIS PROMISSORES:")
    api_map = resultado['api_map']
    
    for path, info in sorted(api_map.items(), key=lambda x: x[1]['call_count'], reverse=True):
        if any(keyword in path.lower() for keyword in ['api', 'ajax', 'data', 'json']):
            print(f"⭐ {path} - {info['call_count']} calls - {info['methods']}")
