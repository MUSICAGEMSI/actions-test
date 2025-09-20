import requests
from playwright.sync_api import sync_playwright
import json
import time
from datetime import datetime

class MusicalAPIExtractor:
    def __init__(self, email, senha):
        self.email = email
        self.senha = senha
        self.session = requests.Session()
        self.base_url = "https://musical.congregacao.org.br"
        
    def fazer_login_e_obter_cookies(self):
        """Faz login via Playwright e obtém cookies para requests"""
        with sync_playwright() as p:
            navegador = p.chromium.launch(headless=True)
            pagina = navegador.new_page()
            
            # Interceptar requisições para descobrir APIs
            requisicoes = []
            def interceptar_request(request):
                if 'api' in request.url or '.json' in request.url or 'ajax' in request.url:
                    requisicoes.append({
                        'url': request.url,
                        'method': request.method,
                        'headers': dict(request.headers),
                        'post_data': request.post_data
                    })
            
            pagina.on('request', interceptar_request)
            
            # Login
            pagina.goto(f"{self.base_url}/")
            pagina.fill('input[name="login"]', self.email)
            pagina.fill('input[name="password"]', self.senha)
            pagina.click('button[type="submit"]')
            
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado!")
            
            # Navegar para área de alunos para interceptar APIs
            pagina.goto(f"{self.base_url}/alunos/listagem")
            time.sleep(3)  # Aguardar carregamento das APIs
            
            # Extrair cookies
            cookies = pagina.context.cookies()
            cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}
            self.session.cookies.update(cookies_dict)
            
            navegador.close()
            
            # Mostrar APIs descobertas
            print(f"🔍 APIs descobertas: {len(requisicoes)}")
            for req in requisicoes:
                print(f"   {req['method']} - {req['url']}")
            
            return cookies_dict, requisicoes
    
    def descobrir_endpoints_api(self):
        """Tenta descobrir todos os endpoints da API"""
        endpoints_conhecidos = [
            "/api/alunos",
            "/api/alunos/listagem", 
            "/api/historico",
            "/alunos/api",
            "/licoes/api",
            "/ajax/alunos",
            "/data/alunos",
            # Baseado no seu código atual
            "/alunos/listagem",  # DataTable endpoint
        ]
        
        endpoints_funcionais = []
        
        for endpoint in endpoints_conhecidos:
            try:
                url = f"{self.base_url}{endpoint}"
                
                # Tentar GET primeiro
                resp_get = self.session.get(url, timeout=10)
                if resp_get.status_code == 200:
                    content_type = resp_get.headers.get('content-type', '')
                    if 'json' in content_type:
                        endpoints_funcionais.append({
                            'endpoint': endpoint,
                            'method': 'GET',
                            'status': resp_get.status_code,
                            'sample_data': str(resp_get.text)[:200]
                        })
                        continue
                
                # Tentar POST (como DataTables)
                data = {
                    'draw': '1',
                    'start': '0', 
                    'length': '10000',
                    'search[value]': '',
                    'search[regex]': 'false'
                }
                headers = {
                    'X-Requested-With': 'XMLHttpRequest',
                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
                }
                
                resp_post = self.session.post(url, data=data, headers=headers, timeout=10)
                if resp_post.status_code == 200:
                    endpoints_funcionais.append({
                        'endpoint': endpoint,
                        'method': 'POST',
                        'status': resp_post.status_code,
                        'sample_data': str(resp_post.text)[:200]
                    })
                
            except Exception as e:
                continue
        
        return endpoints_funcionais
    
    def obter_todos_alunos_direto(self):
        """Obtém todos os alunos direto da API sem scraping HTML"""
        try:
            url = f"{self.base_url}/alunos/listagem"
            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': f'{self.base_url}/alunos/listagem',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
            }
            
            # Buscar TODOS os alunos de uma vez
            data = {
                'draw': '1',
                'start': '0',
                'length': '50000',  # Número bem alto
                'search[value]': '',
                'search[regex]': 'false',
                # Tentar adicionar mais parâmetros se necessário
                'order[0][column]': '1',
                'order[0][dir]': 'asc'
            }
            
            resp = self.session.post(url, data=data, headers=headers, timeout=30)
            
            if resp.status_code == 200:
                dados_json = resp.json()
                print(f"✅ API retornou {dados_json.get('recordsTotal', 0)} registros")
                return dados_json
            else:
                print(f"❌ Erro na API: {resp.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Erro ao acessar API: {e}")
            return None
    
    def obter_historico_api_direto(self, aluno_id):
        """Tenta obter histórico direto da API em vez de scraping"""
        endpoints_historico = [
            f"/api/alunos/{aluno_id}/historico",
            f"/licoes/api/{aluno_id}",
            f"/ajax/historico/{aluno_id}",
            f"/alunos/{aluno_id}/historico.json",
        ]
        
        for endpoint in endpoints_historico:
            try:
                url = f"{self.base_url}{endpoint}"
                resp = self.session.get(url, timeout=10)
                
                if resp.status_code == 200:
                    content_type = resp.headers.get('content-type', '')
                    if 'json' in content_type:
                        return resp.json()
                        
            except Exception:
                continue
        
        return None
    
    def explorar_estrutura_dados(self):
        """Explora a estrutura completa de dados disponível"""
        print("🔍 Explorando estrutura de dados...")
        
        # 1. Fazer login e obter cookies
        cookies, requisicoes_interceptadas = self.fazer_login_e_obter_cookies()
        
        # 2. Descobrir endpoints
        endpoints = self.descobrir_endpoints_api()
        print(f"\n📊 Endpoints funcionais encontrados: {len(endpoints)}")
        for ep in endpoints:
            print(f"   {ep['method']} {ep['endpoint']} - Status: {ep['status']}")
            print(f"      Sample: {ep['sample_data'][:100]}...")
        
        # 3. Testar API de alunos
        dados_alunos = self.obter_todos_alunos_direto()
        if dados_alunos:
            print(f"\n📋 Estrutura dos dados de alunos:")
            print(f"   Total records: {dados_alunos.get('recordsTotal')}")
            print(f"   Filtered: {dados_alunos.get('recordsFiltered')}")
            
            if dados_alunos.get('data'):
                print(f"   Campos por aluno: {len(dados_alunos['data'][0])}")
                print(f"   Exemplo primeiro aluno: {dados_alunos['data'][0]}")
        
        # 4. Testar API de histórico em alguns alunos
        if dados_alunos and dados_alunos.get('data'):
            print(f"\n🔍 Testando APIs de histórico...")
            for i, aluno in enumerate(dados_alunos['data'][:3]):  # Testar 3 primeiros
                aluno_id = aluno[0]  # ID geralmente é o primeiro campo
                historico = self.obter_historico_api_direto(aluno_id)
                if historico:
                    print(f"   ✅ Aluno {aluno_id}: Histórico via API encontrado!")
                    print(f"      Estrutura: {type(historico)} com {len(historico) if isinstance(historico, (list, dict)) else 'N/A'} elementos")
                else:
                    print(f"   ❌ Aluno {aluno_id}: Histórico via API não encontrado")
        
        return {
            'cookies': cookies,
            'requisicoes_interceptadas': requisicoes_interceptadas,
            'endpoints': endpoints,
            'dados_alunos': dados_alunos
        }

# Exemplo de uso
if __name__ == "__main__":
    from dotenv import load_dotenv
    import os
    
    load_dotenv(dotenv_path="credencial.env")
    
    EMAIL = os.environ.get("LOGIN_MUSICAL")
    SENHA = os.environ.get("SENHA_MUSICAL")
    
    if not EMAIL or not SENHA:
        print("❌ Credenciais não encontradas")
        exit(1)
    
    extractor = MusicalAPIExtractor(EMAIL, SENHA)
    
    # Explorar toda a estrutura
    resultado = extractor.explorar_estrutura_dados()
    
    # Salvar descobertas em arquivo
    with open('descobertas_api.json', 'w', encoding='utf-8') as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2, default=str)
    
    print("\n💾 Descobertas salvas em 'descobertas_api.json'")
