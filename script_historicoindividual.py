from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

import requests
import json
import os
import time
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"

class SimpleAPIDiscovery:
    def __init__(self):
        self.session = requests.Session()
        self.discovered_apis = []
        
    def extrair_cookies_playwright(self, pagina):
        """Extrai cookies do Playwright para usar em requests"""
        cookies = pagina.context.cookies()
        return {cookie['name']: cookie['value'] for cookie in cookies}
    
    def fazer_login_obter_session(self):
        """Faz login e retorna session com cookies"""
        with sync_playwright() as p:
            navegador = p.chromium.launch(headless=True)
            pagina = navegador.new_page()
            
            pagina.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            
            pagina.goto(URL_INICIAL)
            
            # Login
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            
            try:
                pagina.wait_for_selector("nav", timeout=15000)
                print("✅ Login realizado com sucesso!")
            except PlaywrightTimeoutError:
                print("❌ Falha no login. Verifique suas credenciais.")
                navegador.close()
                return None
            
            # Extrair cookies
            cookies_dict = self.extrair_cookies_playwright(pagina)
            self.session.cookies.update(cookies_dict)
            
            navegador.close()
            return True
    
    def testar_endpoint_alunos_avancado(self):
        """Testa diferentes variações do endpoint de alunos"""
        print("🔍 Testando endpoint de alunos com diferentes parâmetros...")
        
        url = "https://musical.congregacao.org.br/alunos/listagem"
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/alunos/listagem',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }
        
        # Teste 1: Busca básica (seu código atual)
        data_basico = {
            'draw': '1',
            'start': '0',
            'length': '10000',
            'search[value]': '',
            'search[regex]': 'false'
        }
        
        try:
            resp = self.session.post(url, data=data_basico, headers=headers, timeout=30)
            if resp.status_code == 200:
                dados = resp.json()
                print(f"✅ Endpoint básico funcionou!")
                print(f"   Total de registros: {dados.get('recordsTotal', 'N/A')}")
                print(f"   Registros filtrados: {dados.get('recordsFiltered', 'N/A')}")
                print(f"   Dados disponíveis: {len(dados.get('data', []))}")
                
                if dados.get('data'):
                    primeiro_aluno = dados['data'][0]
                    print(f"   Campos por aluno: {len(primeiro_aluno)}")
                    print(f"   Estrutura do primeiro aluno: {primeiro_aluno}")
                
                return dados
        except Exception as e:
            print(f"❌ Erro no teste básico: {e}")
        
        return None
    
    def descobrir_endpoints_historico(self, sample_aluno_id):
        """Descobre possíveis endpoints para histórico individual"""
        print(f"🔍 Testando endpoints de histórico para aluno ID: {sample_aluno_id}")
        
        # Lista de possíveis endpoints baseados em padrões comuns
        endpoints_teste = [
            f"https://musical.congregacao.org.br/api/alunos/{sample_aluno_id}",
            f"https://musical.congregacao.org.br/api/alunos/{sample_aluno_id}/historico",
            f"https://musical.congregacao.org.br/ajax/aluno/{sample_aluno_id}",
            f"https://musical.congregacao.org.br/alunos/{sample_aluno_id}/dados",
            f"https://musical.congregacao.org.br/licoes/api/{sample_aluno_id}",
            f"https://musical.congregacao.org.br/licoes/dados/{sample_aluno_id}",
            f"https://musical.congregacao.org.br/historico/{sample_aluno_id}",
            f"https://musical.congregacao.org.br/alunos/{sample_aluno_id}/historico.json",
        ]
        
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        }
        
        endpoints_funcionais = []
        
        for endpoint in endpoints_teste:
            try:
                # Testar GET
                resp_get = self.session.get(endpoint, headers=headers, timeout=10)
                
                if resp_get.status_code == 200:
                    content_type = resp_get.headers.get('content-type', '')
                    
                    result = {
                        'endpoint': endpoint,
                        'method': 'GET',
                        'status': resp_get.status_code,
                        'content_type': content_type,
                        'size': len(resp_get.content)
                    }
                    
                    # Se retornar JSON, tentar parsear
                    if 'json' in content_type:
                        try:
                            json_data = resp_get.json()
                            result['is_json'] = True
                            result['json_keys'] = list(json_data.keys()) if isinstance(json_data, dict) else None
                            result['json_type'] = type(json_data).__name__
                        except:
                            result['is_json'] = False
                    else:
                        # Se não for JSON, verificar se tem dados úteis
                        text_sample = resp_get.text[:200]
                        result['text_sample'] = text_sample
                        result['has_table_data'] = '<table' in resp_get.text.lower()
                    
                    endpoints_funcionais.append(result)
                    print(f"   ✅ {endpoint} - {resp_get.status_code} - {content_type}")
                
                elif resp_get.status_code == 404:
                    print(f"   ❌ {endpoint} - 404 (não existe)")
                else:
                    print(f"   ⚠️ {endpoint} - {resp_get.status_code}")
                    
            except Exception as e:
                print(f"   💥 {endpoint} - Erro: {str(e)[:50]}")
                continue
        
        return endpoints_funcionais
    
    def analisar_endpoint_licoes_atual(self, sample_aluno_id):
        """Analisa o endpoint atual que você já usa para ver se há dados estruturados"""
        print(f"🔍 Analisando endpoint atual de lições...")
        
        url_atual = f"https://musical.congregacao.org.br/licoes/index/{sample_aluno_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://musical.congregacao.org.br/alunos/listagem',
        }
        
        try:
            resp = self.session.get(url_atual, headers=headers, timeout=15)
            
            if resp.status_code == 200:
                print(f"✅ Endpoint atual funcionou - {len(resp.text)} caracteres")
                
                # Analisar se há dados JSON embutidos
                import re
                
                # Procurar por padrões JSON no HTML
                json_patterns = [
                    r'var\s+\w+\s*=\s*(\{.*?\});',
                    r'data-json=[\'"](.*?)[\'"]',
                    r'<script[^>]*>.*?(\{.*?"data".*?\}).*?</script>',
                ]
                
                for pattern in json_patterns:
                    matches = re.findall(pattern, resp.text, re.DOTALL)
                    if matches:
                        print(f"   📄 Encontrado possível JSON embutido: {len(matches)} matches")
                        for i, match in enumerate(matches[:3]):  # Mostrar apenas os 3 primeiros
                            print(f"      Match {i+1}: {match[:100]}...")
                
                # Verificar se há tabelas com IDs específicos
                table_ids = re.findall(r'<table[^>]*id=[\'"]([^\'"]*datatable[^\'"]*)[\'"]', resp.text)
                if table_ids:
                    print(f"   📊 Tables encontradas: {table_ids}")
                
                # Verificar se há divs com dados estruturados
                tab_panes = re.findall(r'<div[^>]*class=[\'"][^\'"]* tab-pane[^\'"]* id=[\'"]([^\'"]*)[\'"]', resp.text)
                if tab_panes:
                    print(f"   📂 Abas encontradas: {tab_panes}")
                
                return {
                    'success': True,
                    'size': len(resp.text),
                    'has_datatables': len(table_ids) > 0,
                    'table_ids': table_ids,
                    'tab_panes': tab_panes
                }
        
        except Exception as e:
            print(f"❌ Erro ao analisar endpoint atual: {e}")
            return None
    
    def executar_descoberta_completa(self):
        """Executa descoberta completa de APIs"""
        print("🚀 Iniciando descoberta de APIs...")
        
        # 1. Fazer login
        if not self.fazer_login_obter_session():
            print("❌ Não foi possível fazer login")
            return
        
        # 2. Testar endpoint de alunos
        dados_alunos = self.testar_endpoint_alunos_avancado()
        
        if not dados_alunos or not dados_alunos.get('data'):
            print("❌ Não foi possível obter dados de alunos")
            return
        
        # 3. Pegar alguns IDs de alunos para testar
        sample_ids = [aluno[0] for aluno in dados_alunos['data'][:5]]  # 5 primeiros IDs
        print(f"🎯 Testando com IDs de amostra: {sample_ids}")
        
        # 4. Para cada ID, testar endpoints de histórico
        resultados_historico = {}
        
        for aluno_id in sample_ids:
            print(f"\n--- Testando aluno ID: {aluno_id} ---")
            
            # Testar novos endpoints
            endpoints_funcionais = self.descobrir_endpoints_historico(aluno_id)
            
            # Analisar endpoint atual
            analise_atual = self.analisar_endpoint_licoes_atual(aluno_id)
            
            resultados_historico[aluno_id] = {
                'endpoints_funcionais': endpoints_funcionais,
                'analise_atual': analise_atual
            }
        
        # 5. Resumo final
        print(f"\n" + "="*50)
        print("📊 RESUMO DA DESCOBERTA")
        print("="*50)
        
        print(f"✅ Dados de alunos: {len(dados_alunos.get('data', []))} registros disponíveis")
        
        # Contar endpoints funcionais encontrados
        total_endpoints_funcionais = 0
        endpoints_json = 0
        
        for aluno_id, resultado in resultados_historico.items():
            funcionais = resultado['endpoints_funcionais']
            total_endpoints_funcionais += len(funcionais)
            endpoints_json += len([ep for ep in funcionais if ep.get('is_json')])
        
        print(f"🔍 Endpoints alternativos testados: {len(endpoints_teste) * len(sample_ids)}")
        print(f"✅ Endpoints funcionais encontrados: {total_endpoints_funcionais}")
        print(f"📄 Endpoints que retornam JSON: {endpoints_json}")
        
        if endpoints_json > 0:
            print(f"\n🎉 ÓTIMA NOTÍCIA! Encontramos {endpoints_json} endpoints JSON!")
            print("   Isso significa que podemos otimizar drasticamente seu processo!")
        
        # Salvar descobertas
        discovery_data = {
            'timestamp': time.time(),
            'dados_alunos': {
                'total': len(dados_alunos.get('data', [])),
                'sample_structure': dados_alunos['data'][0] if dados_alunos.get('data') else None
            },
            'resultados_historico': resultados_historico,
            'resumo': {
                'total_endpoints_testados': len(endpoints_teste) * len(sample_ids),
                'endpoints_funcionais': total_endpoints_funcionais,
                'endpoints_json': endpoints_json
            }
        }
        
        with open('api_discovery_results.json', 'w', encoding='utf-8') as f:
            json.dump(discovery_data, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n💾 Resultados salvos em 'api_discovery_results.json'")
        
        return discovery_data

# Executar descoberta
if __name__ == "__main__":
    if not EMAIL or not SENHA:
        print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
        exit(1)
    
    discoverer = SimpleAPIDiscovery()
    resultado = discoverer.executar_descoberta_completa()
