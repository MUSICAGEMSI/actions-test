# script_integrado_musical.py
from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
import json
import asyncio
import threading
from bs4 import BeautifulSoup
from collections import defaultdict

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_LISTAGEM_ALUNOS = "https://musical.congregacao.org.br/alunos/listagem"
URL_LISTAGEM_MUSICOS = "https://musical.congregacao.org.br/grp_musical/listagem"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzRSGdID5WjLuukBUt-5TbQjCqSvCKjr0vOWHFfFr0rChW1vINwgQE5VJDQCKM5mc693Q/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

class ColetorMusical:
    def __init__(self):
        self.dados_turmas = []
        self.dados_candidatos = {}
        self.dados_oficializados = {}
        self.navegador = None
        self.context = None
        
    def extrair_cookies_playwright(self, pagina):
        """Extrai cookies do Playwright para usar em requests"""
        cookies = pagina.context.cookies()
        return {cookie['name']: cookie['value'] for cookie in cookies}

    def extrair_localidade_limpa(self, localidade_texto):
        """Extrai apenas o nome da localidade, removendo HTML e informações extras"""
        # Remove tags HTML e span
        localidade_texto = re.sub(r'<[^>]+>', '', localidade_texto)
        localidade_texto = localidade_texto.replace('<\\/span>', '').replace('</span>', '')
        
        # Remove informações de compartilhado
        if 'compartilhado' in localidade_texto.lower():
            return None  # Ignora compartilhados
        
        # Pega apenas a parte antes do " | " ou remove espaços extras
        if ' | ' in localidade_texto:
            localidade = localidade_texto.split(' | ')[0].strip()
        else:
            localidade = localidade_texto.strip()
        
        return localidade

    def obter_matriculados_reais(self, session, turma_id):
        """Obtém o número real de matriculados contando as linhas da tabela"""
        try:
            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': 'https://musical.congregacao.org.br/painel',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
            }
            
            url = f"https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/{turma_id}"
            resp = session.get(url, headers=headers, timeout=15)
            
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                # Primeiro: tentar encontrar o texto "de um total de X registros"
                info_div = soup.find('div', {'class': 'dataTables_info'})
                if info_div and info_div.text:
                    match = re.search(r'de um total de (\d+) registros', info_div.text)
                    if match:
                        return int(match.group(1))
                        
                    # Fallback: tentar "Mostrando de X até Y"
                    match2 = re.search(r'Mostrando de \d+ até (\d+)', info_div.text)
                    if match2:
                        return int(match2.group(1))
                
                # Segundo: contar linhas da tabela tbody
                tbody = soup.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr')
                    valid_rows = [row for row in rows if len(row.find_all('td')) >= 4]
                    return len(valid_rows)
                
                # Terceiro: contar botões "Desmatricular"
                desmatricular_count = resp.text.count('Desmatricular')
                if desmatricular_count > 0:
                    return desmatricular_count
                    
            return 0
            
        except Exception as e:
            print(f"⚠️ Erro ao obter matriculados para turma {turma_id}: {e}")
            return -1

    def coletar_turmas(self, pagina_turmas, session):
        """Coleta dados de turmas em uma aba separada"""
        print("📊 Iniciando coleta de turmas...")
        
        try:
            # Navegar para G.E.M
            gem_selector = 'span:has-text("G.E.M")'
            pagina_turmas.wait_for_selector(gem_selector, timeout=15000)
            gem_element = pagina_turmas.locator(gem_selector).first
            gem_element.hover()
            pagina_turmas.wait_for_timeout(1000)
            gem_element.click()
            
            # Navegar para Turmas
            pagina_turmas.wait_for_selector('a[href="turmas"]', timeout=10000)
            pagina_turmas.click('a[href="turmas"]')
            
            # Aguardar carregamento da tabela
            pagina_turmas.wait_for_selector('table#tabela-turmas', timeout=15000)
            pagina_turmas.wait_for_function(
                """() => {
                    const tbody = document.querySelector('table#tabela-turmas tbody');
                    return tbody && tbody.querySelectorAll('tr').length > 0;
                }""", timeout=15000
            )
            
            # Configurar para mostrar 100 itens
            try:
                select_length = pagina_turmas.query_selector('select[name="tabela-turmas_length"]')
                if select_length:
                    pagina_turmas.select_option('select[name="tabela-turmas_length"]', '100')
                    pagina_turmas.wait_for_timeout(2000)
            except Exception:
                pass
            
            # Processar turmas
            dados_consolidados = defaultdict(lambda: {'qtd_turmas': 0, 'matric_soma': 0, 'matric_reais': 0})
            pagina_atual = 1
            
            while True:
                print(f"📄 Processando página {pagina_atual}...")
                
                linhas = pagina_turmas.query_selector_all('table#tabela-turmas tbody tr')
                
                for linha in linhas:
                    try:
                        # Extrair dados das colunas
                        colunas_td = linha.query_selector_all('td')
                        if len(colunas_td) < 4:
                            continue
                        
                        # Igreja (primeira coluna após radio)
                        igreja = colunas_td[1].inner_text().strip()
                        igreja_limpa = self.extrair_localidade_limpa(igreja)
                        
                        if not igreja_limpa:  # Ignora compartilhados
                            continue
                        
                        # Matriculados badge (coluna 4)
                        badge = colunas_td[4].query_selector('span.badge')
                        if badge:
                            matriculados_badge = int(badge.inner_text().strip())
                        else:
                            matriculados_badge = 0
                        
                        # ID da turma
                        radio_input = linha.query_selector('input[type="radio"][name="item[]"]')
                        if not radio_input:
                            continue
                        
                        turma_id = radio_input.get_attribute('value')
                        if not turma_id:
                            continue
                        
                        # Obter número real de matriculados
                        matriculados_reais = self.obter_matriculados_reais(session, turma_id)
                        if matriculados_reais < 0:
                            matriculados_reais = matriculados_badge
                        
                        # Consolidar por localidade
                        dados_consolidados[igreja_limpa]['qtd_turmas'] += 1
                        dados_consolidados[igreja_limpa]['matric_soma'] += matriculados_badge
                        dados_consolidados[igreja_limpa]['matric_reais'] += matriculados_reais
                        
                        print(f"   📊 {igreja_limpa} - Turma {turma_id}: Badge={matriculados_badge}, Real={matriculados_reais}")
                        
                        time.sleep(0.3)  # Evita sobrecarga
                        
                    except Exception as e:
                        print(f"⚠️ Erro ao processar linha: {e}")
                        continue
                
                # Verificar próxima página
                try:
                    btn_next = pagina_turmas.query_selector('a.paginate_button.next:not(.disabled)')
                    if btn_next and btn_next.is_enabled():
                        print(f"➡️ Avançando para página {pagina_atual + 1}...")
                        btn_next.click()
                        pagina_turmas.wait_for_function(
                            """() => {
                                const tbody = document.querySelector('table#tabela-turmas tbody');
                                return tbody && tbody.querySelectorAll('tr').length > 0;
                            }""", timeout=15000
                        )
                        pagina_turmas.wait_for_timeout(3000)
                        pagina_atual += 1
                    else:
                        break
                except Exception as e:
                    print(f"⚠️ Erro na paginação: {e}")
                    break
            
            # Converter para lista ordenada
            self.dados_turmas = []
            for localidade in sorted(dados_consolidados.keys()):
                dados = dados_consolidados[localidade]
                self.dados_turmas.append([
                    localidade,
                    dados['qtd_turmas'],
                    dados['matric_soma'],
                    dados['matric_reais']
                ])
            
            print(f"✅ Coleta de turmas concluída: {len(self.dados_turmas)} localidades")
            
        except Exception as e:
            print(f"❌ Erro na coleta de turmas: {e}")
            import traceback
            traceback.print_exc()

    def coletar_candidatos_niveis(self, pagina_candidatos, session):
        """Coleta candidatos por nível usando a listagem de alunos"""
        print("📊 Iniciando coleta de candidatos por nível...")
        
        try:
            # Navegar para listagem de alunos
            pagina_candidatos.goto("https://musical.congregacao.org.br/alunos")
            pagina_candidatos.wait_for_timeout(2000)
            
            # Atualizar cookies após navegação
            cookies_dict = self.extrair_cookies_playwright(pagina_candidatos)
            session.cookies.update(cookies_dict)
            
            # Preparar requisição para API de listagem
            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': 'https://musical.congregacao.org.br/painel',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
            }
            
            form_data = {
                'draw': '1',
                'start': '0',
                'length': '10000',
                'search[value]': '',
                'search[regex]': 'false'
            }
            
            # Adicionar colunas da tabela
            for i in range(7):  # 7 colunas
                form_data.update({
                    f'columns[{i}][data]': str(i),
                    f'columns[{i}][name]': '',
                    f'columns[{i}][searchable]': 'true',
                    f'columns[{i}][orderable]': 'true',
                    f'columns[{i}][search][value]': '',
                    f'columns[{i}][search][regex]': 'false'
                })
            
            form_data.update({
                'order[0][column]': '0',
                'order[0][dir]': 'asc'
            })
            
            print("📊 Obtendo candidatos da listagem de alunos...")
            resp = session.post(URL_LISTAGEM_ALUNOS, headers=headers, data=form_data, timeout=60)
            
            niveis_candidatos = {
                'CANDIDATO(A)': 0,
                'ENSAIO': 0,
                'RJM / ENSAIO': 0,
                'RJM': 0,
                'RJM / CULTO OFICIAL': 0,
                'CULTO OFICIAL': 0
            }
            
            dados_por_localidade = defaultdict(lambda: niveis_candidatos.copy())
            
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    print(f"📊 JSON recebido com {len(data.get('data', []))} registros de candidatos")
                    
                    if 'data' in data and isinstance(data['data'], list):
                        for record in data['data']:
                            if isinstance(record, list) and len(record) >= 6:
                                localidade_completa = record[2]
                                nivel = record[5]
                                
                                # Extrair localidade limpa
                                localidade = self.extrair_localidade_limpa(localidade_completa)
                                if not localidade:  # Ignora compartilhados
                                    continue
                                
                                # Lista de termos que devem ser ignorados
                                termos_ignorados = [
                                    'ORGANISTA', 'OFICIALIZADO(A)', 'RJM / OFICIALIZADO(A)', 
                                    'RJM/OFICIALIZADO(A)', 'COMPARTILHADO', 'COMPARTILHADA'
                                ]
                                
                                # Verificar se deve ser ignorado
                                if any(termo in nivel.upper() for termo in termos_ignorados):
                                    continue
                                
                                # Contar apenas os níveis válidos
                                if nivel in niveis_candidatos:
                                    dados_por_localidade[localidade][nivel] += 1
                
                except json.JSONDecodeError as e:
                    print(f"❌ Erro ao decodificar JSON de candidatos: {e}")
                    dados_por_localidade = {}
            
            self.dados_candidatos = dict(dados_por_localidade)
            print(f"✅ Coleta de candidatos concluída: {len(self.dados_candidatos)} localidades")
            
        except Exception as e:
            print(f"❌ Erro na coleta de candidatos: {e}")
            import traceback
            traceback.print_exc()

    def coletar_oficializados(self, pagina_oficializados, session):
        """Coleta músicos oficializados usando a listagem de grupo musical"""
        print("📊 Iniciando coleta de oficializados...")
        
        try:
            # Navegar para listagem de músicos
            pagina_oficializados.goto("https://musical.congregacao.org.br/grp_musical")
            pagina_oficializados.wait_for_timeout(2000)
            
            # Atualizar cookies após navegação
            cookies_dict = self.extrair_cookies_playwright(pagina_oficializados)
            session.cookies.update(cookies_dict)
            
            # Preparar requisição para API de listagem de músicos
            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': 'https://musical.congregacao.org.br/painel',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
            }
            
            form_data = {
                'draw': '1',
                'start': '0',
                'length': '10000',
                'search[value]': '',
                'search[regex]': 'false'
            }
            
            # Adicionar colunas da tabela (7 colunas para músicos)
            for i in range(7):
                form_data.update({
                    f'columns[{i}][data]': str(i),
                    f'columns[{i}][name]': '',
                    f'columns[{i}][searchable]': 'true',
                    f'columns[{i}][orderable]': 'true',
                    f'columns[{i}][search][value]': '',
                    f'columns[{i}][search][regex]': 'false'
                })
            
            form_data.update({
                'order[0][column]': '0',
                'order[0][dir]': 'asc'
            })
            
            print("📊 Obtendo músicos oficializados...")
            resp = session.post(URL_LISTAGEM_MUSICOS, headers=headers, data=form_data, timeout=60)
            
            niveis_oficializados = {
                'RJM / OFICIALIZADO(A)': 0,
                'OFICIALIZADO(A)': 0
            }
            
            dados_por_localidade = defaultdict(lambda: niveis_oficializados.copy())
            
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    print(f"📊 JSON recebido com {len(data.get('data', []))} registros de músicos")
                    
                    if 'data' in data and isinstance(data['data'], list):
                        for record in data['data']:
                            if isinstance(record, list) and len(record) >= 5:
                                # Estrutura: [id, nome, localidade, ministério, nivel, ...]
                                localidade_completa = record[2]
                                nivel = record[4]
                                
                                # Extrair localidade limpa
                                localidade = self.extrair_localidade_limpa(localidade_completa)
                                if not localidade:  # Ignora compartilhados
                                    continue
                                
                                # Normalizar nível para corresponder aos oficializados
                                if 'RJM / OFICIALIZADO' in nivel or 'RJM/OFICIALIZADO' in nivel:
                                    nivel_normalizado = 'RJM / OFICIALIZADO(A)'
                                elif 'OFICIALIZADO(A)' == nivel:
                                    nivel_normalizado = 'OFICIALIZADO(A)'
                                else:
                                    continue  # Ignora outros níveis
                                
                                dados_por_localidade[localidade][nivel_normalizado] += 1
                
                except json.JSONDecodeError as e:
                    print(f"❌ Erro ao decodificar JSON de oficializados: {e}")
                    dados_por_localidade = {}
            
            self.dados_oficializados = dict(dados_por_localidade)
            print(f"✅ Coleta de oficializados concluída: {len(self.dados_oficializados)} localidades")
            
        except Exception as e:
            print(f"❌ Erro na coleta de oficializados: {e}")
            import traceback
            traceback.print_exc()

    def executar_coletas_paralelas(self):
        """Executa todas as coletas em paralelo usando múltiplas abas"""
        tempo_inicio = time.time()
        
        with sync_playwright() as p:
            self.navegador = p.chromium.launch(headless=True)
            self.context = self.navegador.new_context()
            
            # Página principal para login
            pagina_principal = self.context.new_page()
            pagina_principal.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            
            # Login
            print("🔐 Fazendo login...")
            pagina_principal.goto(URL_INICIAL)
            pagina_principal.fill('input[name="login"]', EMAIL)
            pagina_principal.fill('input[name="password"]', SENHA)
            pagina_principal.click('button[type="submit"]')
            
            try:
                pagina_principal.wait_for_selector("nav", timeout=15000)
                print("✅ Login realizado com sucesso")
            except PlaywrightTimeoutError:
                print("❌ Falha no login")
                self.navegador.close()
                return
            
            # Criar sessão requests com cookies do login
            cookies_dict = self.extrair_cookies_playwright(pagina_principal)
            session = requests.Session()
            session.cookies.update(cookies_dict)
            
            # Criar abas separadas para cada coleta
            pagina_turmas = self.context.new_page()
            pagina_candidatos = self.context.new_page()
            pagina_oficializados = self.context.new_page()
            
            # Configurar headers para todas as abas
            for pagina in [pagina_turmas, pagina_candidatos, pagina_oficializados]:
                pagina.set_extra_http_headers({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
            
            # Executar coletas em threads separadas
            threads = []
            
            # Thread para coleta de turmas
            thread_turmas = threading.Thread(
                target=self.coletar_turmas,
                args=(pagina_turmas, session)
            )
            threads.append(thread_turmas)
            
            # Thread para coleta de candidatos
            thread_candidatos = threading.Thread(
                target=self.coletar_candidatos_niveis,
                args=(pagina_candidatos, session)
            )
            threads.append(thread_candidatos)
            
            # Thread para coleta de oficializados
            thread_oficializados = threading.Thread(
                target=self.coletar_oficializados,
                args=(pagina_oficializados, session)
            )
            threads.append(thread_oficializados)
            
            # Iniciar todas as threads
            print("🚀 Iniciando coletas paralelas...")
            for thread in threads:
                thread.start()
            
            # Aguardar conclusão de todas as threads
            for thread in threads:
                thread.join()
            
            self.navegador.close()
            
            tempo_total = time.time() - tempo_inicio
            print(f"⏱️ Todas as coletas concluídas em {tempo_total:.1f} segundos")
            
            # Consolidar e enviar dados
            self.consolidar_e_enviar_dados()

    def consolidar_e_enviar_dados(self):
        """Consolida todos os dados coletados e envia para o Google Sheets"""
        print("📊 Consolidando dados...")
        
        # PRIMEIRA GUIA - Dados de Turmas (colunas A-D)
        guia1_dados = [["LOCALIDADE", "QTD TURMAS", "MATRIC. SOMA", "MATRIC. REAIS"]]
        guia1_dados.extend(self.dados_turmas)
        
        # SEGUNDA GUIA - Consolidação completa
        # Obter todas as localidades únicas
        todas_localidades = set()
        
        # Das turmas
        for linha in self.dados_turmas:
            todas_localidades.add(linha[0])
        
        # Dos candidatos
        todas_localidades.update(self.dados_candidatos.keys())
        
        # Dos oficializados
        todas_localidades.update(self.dados_oficializados.keys())
        
        # Preparar dados da segunda guia
        guia2_headers = [
            "LOCALIDADE", "QTD TURMAS", "MATRIC. SOMA", "MATRIC. REAIS",
            "CANDIDATO(A)", "ENSAIO", "RJM / ENSAIO", "RJM", "RJM / CULTO OFICIAL", "CULTO OFICIAL",
            "RJM / OFICIALIZADO(A)", "OFICIALIZADO(A)"
        ]
        
        guia2_dados = [guia2_headers]
        
        # Criar dicionário de turmas para facilitar busca
        turmas_dict = {linha[0]: linha[1:] for linha in self.dados_turmas}
        
        for localidade in sorted(todas_localidades):
            linha = [localidade]
            
            # Dados de turmas (colunas B-D)
            if localidade in turmas_dict:
                linha.extend(turmas_dict[localidade])  # QTD TURMAS, MATRIC. SOMA, MATRIC. REAIS
            else:
                linha.extend([0, 0, 0])
            
            # Dados de candidatos (colunas E-J)
            if localidade in self.dados_candidatos:
                candidatos = self.dados_candidatos[localidade]
                linha.extend([
                    candidatos.get('CANDIDATO(A)', 0),
                    candidatos.get('ENSAIO', 0),
                    candidatos.get('RJM / ENSAIO', 0),
                    candidatos.get('RJM', 0),
                    candidatos.get('RJM / CULTO OFICIAL', 0),
                    candidatos.get('CULTO OFICIAL', 0)
                ])
            else:
                linha.extend([0, 0, 0, 0, 0, 0])
            
            # Dados de oficializados (colunas K-L)
            if localidade in self.dados_oficializados:
                oficializados = self.dados_oficializados[localidade]
                linha.extend([
                    oficializados.get('RJM / OFICIALIZADO(A)', 0),
                    oficializados.get('OFICIALIZADO(A)', 0)
                ])
            else:
                linha.extend([0, 0])
            
            guia2_dados.append(linha)
        
        # Preparar body para envio
        body = {
            "tipo": "consolidacao_completa",
            "guia1": {
                "nome": "Resumo Turmas",
                "dados": guia1_dados
            },
            "guia2": {
                "nome": "Dados Completos", 
                "dados": guia2_dados
            },
            "resumo": {
                "total_localidades": len(todas_localidades),
                "total_turmas": len(self.dados_turmas),
                "total_candidatos_localidades": len(self.dados_candidatos),
                "total_oficializados_localidades": len(self.dados_oficializados)
            }
        }
        
        # Enviar para Google Sheets
        try:
            print(f"📤 Enviando dados consolidados para Google Sheets...")
            print(f"   📊 Guia 1: {len(guia1_dados)-1} localidades com turmas")
            print(f"   📊 Guia 2: {len(guia2_dados)-1} localidades consolidadas")
            
            resposta = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
            print(f"✅ Status do envio: {resposta.status_code}")
            print(f"📝 Resposta: {resposta.text}")
        except Exception as e:
            print(f"❌ Erro no envio: {e}")
        
        # Mostrar resumo final
        print("\n" + "="*80)
        print("📈 RESUMO FINAL DA COLETA")
        print("="*80)
        print(f"🎯 Total de localidades únicas: {len(todas_localidades)}")
        print(f"🏫 Localidades com turmas: {len(self.dados_turmas)}")
        print(f"👥 Localidades com candidatos: {len(self.dados_candidatos)}")  
        print(f"🎼 Localidades com oficializados: {len(self.dados_oficializados)}")
        
        # Calcular totais
        total_turmas = sum(linha[1] for linha in self.dados_turmas)
        total_matriculas_soma = sum(linha[2] for linha in self.dados_turmas)
        total_matriculas_reais = sum(linha[3] for linha in self.dados_turmas)
        
        print(f"\n📊 TOTAIS GERAIS:")
        print(f"   🎯 Total de turmas: {total_turmas}")
        print(f"   📋 Total matrículas (soma): {total_matriculas_soma}")
        print(f"   ✅ Total matrículas (reais): {total_matriculas_reais}")
        
        # Totais por nível de candidatos
        if self.dados_candidatos:
            total_candidatos = sum(sum(dados.values()) for dados in self.dados_candidatos.values())
            print(f"   👥 Total candidatos: {total_candidatos}")
        
        # Totais de oficializados  
        if self.dados_oficializados:
            total_oficializados = sum(sum(dados.values()) for dados in self.dados_oficializados.values())
            print(f"   🎼 Total oficializados: {total_oficializados}")


def main():
    print("🎵 INICIANDO COLETA INTEGRADA - SISTEMA MUSICAL")
    print("="*60)
    print("📋 Funcionalidades:")
    print("   🏫 Coleta de turmas (QTD, Matrículas)")
    print("   👥 Coleta de candidatos por nível") 
    print("   🎼 Coleta de músicos oficializados")
    print("   📊 Consolidação em duas guias")
    print("="*60)
    
    coletor = ColetorMusical()
    coletor.executar_coletas_paralelas()
    
    print("\n🎉 PROCESSO CONCLUÍDO COM SUCESSO!")

if __name__ == "__main__":
    main()
