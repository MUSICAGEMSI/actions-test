# script_unificado_musical.py
from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
import json
from collections import defaultdict
from bs4 import BeautifulSoup
import threading
from queue import Queue

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_LISTAGEM_ALUNOS = "https://musical.congregacao.org.br/alunos/listagem"
URL_GRP_MUSICAL = "https://musical.congregacao.org.br/grp_musical/listagem"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzHgvDkG-aGKH12y3mKHX1kNZ96Ucjp3mr0RXNt6PFP23zfLwK3KLVj1IlYVUEzMZR0Fg/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

class MusicalScraper:
    def __init__(self):
        self.navegador = None
        self.pagina_principal = None
        self.session = None
        self.resultados = {}
        
    def fazer_login(self):
        """Realiza login na plataforma"""
        print("🔐 Fazendo login...")
        self.pagina_principal.goto(URL_INICIAL)
        self.pagina_principal.fill('input[name="login"]', EMAIL)
        self.pagina_principal.fill('input[name="password"]', SENHA)
        self.pagina_principal.click('button[type="submit"]')

        try:
            self.pagina_principal.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado com sucesso!")
            return True
        except PlaywrightTimeoutError:
            print("❌ Falha no login. Verifique suas credenciais.")
            return False

    def extrair_cookies_playwright(self, pagina):
        """Extrai cookies do Playwright para usar em requests"""
        cookies = pagina.context.cookies()
        return {cookie['name']: cookie['value'] for cookie in cookies}

    def extrair_localidade_limpa(self, localidade_texto):
        """Extrai apenas o nome da localidade, removendo HTML e informações extras"""
        # Remove tags HTML
        localidade_texto = localidade_texto.replace('<\\/span>', '').replace('<span>', '').replace('</span>', '')
        localidade_texto = re.sub(r'<span[^>]*>', '', localidade_texto)
        localidade_texto = re.sub(r'</span>', '', localidade_texto)
        
        # Remove (Compartilhado) e variações
        localidade_texto = re.sub(r'\s*\(compartilhad[ao]\)', '', localidade_texto, flags=re.IGNORECASE)
        
        # Pega apenas a parte antes do " | "
        if ' | ' in localidade_texto:
            localidade = localidade_texto.split(' | ')[0].strip()
        else:
            localidade = localidade_texto.strip()
        
        return localidade

    def coletar_turmas_resumo(self):
        """Coleta resumo de turmas para ABA 1 - Colunas A-D"""
        print("\n📊 Iniciando coleta de RESUMO DE TURMAS (Aba 1)...")
        
        # Criar nova página para turmas
        pagina_turmas = self.navegador.new_page()
        
        try:
            # Navegar para G.E.M > Turmas
            pagina_turmas.goto("https://musical.congregacao.org.br/painel")
            
            gem_selector = 'span:has-text("G.E.M")'
            pagina_turmas.wait_for_selector(gem_selector, timeout=15000)
            gem_element = pagina_turmas.locator(gem_selector).first
            gem_element.hover()
            time.sleep(1)
            gem_element.click()
            
            pagina_turmas.wait_for_selector('a[href="turmas"]', timeout=10000)
            pagina_turmas.click('a[href="turmas"]')
            
            # Aguardar carregamento da tabela
            pagina_turmas.wait_for_selector('table#tabela-turmas', timeout=15000)
            
            # Configurar para mostrar 100 itens
            try:
                select_length = pagina_turmas.query_selector('select[name="tabela-turmas_length"]')
                if select_length:
                    pagina_turmas.select_option('select[name="tabela-turmas_length"]', '100')
                    time.sleep(2)
            except:
                pass

            # Coletar dados agrupados por localidade
            dados_por_localidade = defaultdict(lambda: {'qtd_turmas': 0, 'matric_soma': 0, 'matric_reais': 0})
            
            parar = False
            pagina_atual = 1
            
            while not parar:
                print(f"📄 Processando página {pagina_atual} de turmas...")
                
                linhas = pagina_turmas.query_selector_all('table#tabela-turmas tbody tr')
                
                for linha in linhas:
                    try:
                        colunas_td = linha.query_selector_all('td')
                        
                        # Extrair localidade (coluna 1, índice 1)
                        localidade_bruta = colunas_td[1].inner_text().strip() if len(colunas_td) > 1 else ""
                        localidade = self.extrair_localidade_limpa(localidade_bruta)
                        
                        # Pular se for compartilhado
                        if 'compartilhad' in localidade.lower():
                            continue
                        
                        # Extrair matriculados do badge (coluna 4, índice 4)
                        badge = colunas_td[4].query_selector('span.badge') if len(colunas_td) > 4 else None
                        matriculados_badge = int(badge.inner_text().strip()) if badge else 0
                        
                        # Extrair ID da turma
                        radio_input = linha.query_selector('input[type="radio"][name="item[]"]')
                        turma_id = radio_input.get_attribute('value') if radio_input else None
                        
                        if localidade and turma_id:
                            # Obter matriculados reais
                            matriculados_reais = self.obter_matriculados_reais(turma_id)
                            
                            # Somar aos dados da localidade
                            dados_por_localidade[localidade]['qtd_turmas'] += 1
                            dados_por_localidade[localidade]['matric_soma'] += matriculados_badge
                            dados_por_localidade[localidade]['matric_reais'] += matriculados_reais if matriculados_reais >= 0 else 0
                            
                    except Exception as e:
                        print(f"⚠️ Erro ao processar linha de turma: {e}")
                        continue
                
                # Verificar próxima página
                try:
                    btn_next = pagina_turmas.query_selector('a.paginate_button.next:not(.disabled)')
                    if btn_next and btn_next.is_enabled():
                        btn_next.click()
                        time.sleep(3)
                        pagina_atual += 1
                    else:
                        break
                except:
                    break
            
            # Converter para formato de lista
            resultado_resumo = []
            for localidade in sorted(dados_por_localidade.keys()):
                dados = dados_por_localidade[localidade]
                resultado_resumo.append([
                    localidade,
                    dados['qtd_turmas'],
                    dados['matric_soma'],
                    dados['matric_reais']
                ])
            
            self.resultados['resumo_turmas'] = resultado_resumo
            print(f"✅ Resumo de turmas coletado: {len(resultado_resumo)} localidades")
            
        except Exception as e:
            print(f"❌ Erro na coleta de resumo de turmas: {e}")
        finally:
            pagina_turmas.close()

    def coletar_candidatos_niveis(self):
        """Coleta candidatos por nível (colunas E-L da Aba 1)"""
        print("\n📊 Iniciando coleta de CANDIDATOS POR NÍVEL...")
        
        # Criar nova página para alunos
        pagina_alunos = self.navegador.new_page()
        
        try:
            # Navegar para listagem de alunos
            pagina_alunos.goto("https://musical.congregacao.org.br/alunos")
            time.sleep(2)
            
            # Atualizar cookies
            cookies_dict = self.extrair_cookies_playwright(pagina_alunos)
            self.session.cookies.update(cookies_dict)
            
            # Fazer requisição POST para obter dados
            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': 'https://musical.congregacao.org.br/painel',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Accept': 'application/json, text/javascript, */*; q=0.01'
            }
            
            form_data = {
                'draw': '1',
                'start': '0',
                'length': '10000',
                'search[value]': '',
                'order[0][column]': '0',
                'order[0][dir]': 'asc'
            }
            
            # Adicionar colunas
            for i in range(7):
                form_data[f'columns[{i}][data]'] = str(i)
                form_data[f'columns[{i}][searchable]'] = 'true'
                form_data[f'columns[{i}][orderable]'] = 'true'
                form_data[f'columns[{i}][search][value]'] = ''
                form_data[f'columns[{i}][search][regex]'] = 'false'
            
            resp = self.session.post(URL_LISTAGEM_ALUNOS, headers=headers, data=form_data, timeout=60)
            
            # Processar dados dos alunos
            niveis_validos = {
                'CANDIDATO(A)': 0,
                'RJM/ENSAIO': 0,
                'ENSAIO': 0,
                'RJM': 0,
                'RJM/CULTO OFICIAL': 0,
                'CULTO OFICIAL': 0
            }
            
            dados_por_localidade = defaultdict(lambda: niveis_validos.copy())
            
            if resp.status_code == 200:
                data = resp.json()
                
                for record in data.get('data', []):
                    if isinstance(record, list) and len(record) >= 6:
                        localidade_completa = record[2]
                        nivel = record[5]
                        
                        localidade = self.extrair_localidade_limpa(localidade_completa)
                        
                        # Ignorar termos específicos
                        termos_ignorados = [
                            'ORGANISTA', 'OFICIALIZADO(A)', 'RJM/OFICIALIZADO(A)', 
                            'RJM / OFICIALIZADO(A)', 'COMPARTILHADO', 'COMPARTILHADA'
                        ]
                        
                        if any(termo in nivel.upper() for termo in termos_ignorados):
                            continue
                        
                        # Normalizar níveis
                        nivel_normalizado = nivel.replace(' / ', '/').strip()
                        
                        if nivel_normalizado in niveis_validos:
                            dados_por_localidade[localidade][nivel_normalizado] += 1
            
            self.resultados['candidatos_niveis'] = dict(dados_por_localidade)
            print(f"✅ Candidatos por nível coletados: {len(dados_por_localidade)} localidades")
            
        except Exception as e:
            print(f"❌ Erro na coleta de candidatos: {e}")
        finally:
            pagina_alunos.close()

    def coletar_oficializados(self):
        """Coleta músicos oficializados (colunas K-L da Aba 1)"""
        print("\n📊 Iniciando coleta de OFICIALIZADOS...")
        
        # Criar nova página para grupo musical
        pagina_grupo = self.navegador.new_page()
        
        try:
            pagina_grupo.goto("https://musical.congregacao.org.br/grp_musical")
            time.sleep(2)
            
            # Atualizar cookies
            cookies_dict = self.extrair_cookies_playwright(pagina_grupo)
            self.session.cookies.update(cookies_dict)
            
            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': 'https://musical.congregacao.org.br/painel',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Accept': 'application/json, text/javascript, */*; q=0.01'
            }
            
            form_data = {
                'draw': '1',
                'start': '0',
                'length': '10000',
                'search[value]': '',
                'order[0][column]': '0',
                'order[0][dir]': 'asc'
            }
            
            # Adicionar colunas
            for i in range(7):
                form_data[f'columns[{i}][data]'] = str(i)
                form_data[f'columns[{i}][searchable]'] = 'true'
                form_data[f'columns[{i}][orderable]'] = 'true'
                form_data[f'columns[{i}][search][value]'] = ''
                form_data[f'columns[{i}][search][regex]'] = 'false'
            
            resp = self.session.post(URL_GRP_MUSICAL, headers=headers, data=form_data, timeout=60)
            
            dados_por_localidade = defaultdict(lambda: {'RJM/OFICIALIZADO': 0, 'OFICIALIZADO': 0})
            
            if resp.status_code == 200:
                data = resp.json()
                
                for record in data.get('data', []):
                    if isinstance(record, list) and len(record) >= 5:
                        localidade_completa = record[2]
                        nivel = record[4]
                        
                        localidade = self.extrair_localidade_limpa(localidade_completa)
                        
                        # Pular compartilhados
                        if 'compartilhad' in localidade.lower():
                            continue
                        
                        if 'RJM / OFICIALIZADO(A)' in nivel or 'RJM/OFICIALIZADO(A)' in nivel:
                            dados_por_localidade[localidade]['RJM/OFICIALIZADO'] += 1
                        elif 'OFICIALIZADO(A)' in nivel:
                            dados_por_localidade[localidade]['OFICIALIZADO'] += 1
            
            self.resultados['oficializados'] = dict(dados_por_localidade)
            print(f"✅ Oficializados coletados: {len(dados_por_localidade)} localidades")
            
        except Exception as e:
            print(f"❌ Erro na coleta de oficializados: {e}")
        finally:
            pagina_grupo.close()

    def obter_matriculados_reais(self, turma_id):
        """Obtém o número real de matriculados contando as linhas da tabela"""
        try:
            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': 'https://musical.congregacao.org.br/painel',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            url = f"https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/{turma_id}"
            resp = self.session.get(url, headers=headers, timeout=15)
            
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                # Tentar encontrar "de um total de X registros"
                info_div = soup.find('div', {'class': 'dataTables_info'})
                if info_div and info_div.text:
                    match = re.search(r'de um total de (\d+) registros', info_div.text)
                    if match:
                        return int(match.group(1))
                
                # Contar linhas da tabela
                tbody = soup.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr')
                    valid_rows = [row for row in rows if len(row.find_all('td')) >= 4]
                    return len(valid_rows)
                    
            return 0
            
        except Exception as e:
            print(f"⚠️ Erro ao obter matriculados para turma {turma_id}: {e}")
            return -1

    def extrair_dia_da_semana(self, dia_hora_texto):
        """Extrai apenas o dia da semana do texto 'QUA - 19:30 ÀS 21:00'"""
        try:
            return dia_hora_texto.split(' - ')[0].strip()
        except:
            return dia_hora_texto

    def coletar_turmas_detalhadas(self):
        """Coleta turmas detalhadas para ABA 2"""
        print("\n📊 Iniciando coleta de TURMAS DETALHADAS (Aba 2)...")
        
        # Criar nova página para turmas detalhadas
        pagina_turmas_det = self.navegador.new_page()
        
        try:
            # Navegar para turmas
            pagina_turmas_det.goto("https://musical.congregacao.org.br/painel")
            
            gem_selector = 'span:has-text("G.E.M")'
            pagina_turmas_det.wait_for_selector(gem_selector, timeout=15000)
            gem_element = pagina_turmas_det.locator(gem_selector).first
            gem_element.hover()
            time.sleep(1)
            gem_element.click()
            
            pagina_turmas_det.wait_for_selector('a[href="turmas"]', timeout=10000)
            pagina_turmas_det.click('a[href="turmas"]')
            
            pagina_turmas_det.wait_for_selector('table#tabela-turmas', timeout=15000)
            
            # Configurar para mostrar 100 itens
            try:
                select_length = pagina_turmas_det.query_selector('select[name="tabela-turmas_length"]')
                if select_length:
                    pagina_turmas_det.select_option('select[name="tabela-turmas_length"]', '100')
                    time.sleep(2)
            except:
                pass

            resultado_detalhado = []
            parar = False
            pagina_atual = 1

            while not parar:
                print(f"📄 Processando página {pagina_atual} de turmas detalhadas...")
                
                linhas = pagina_turmas_det.query_selector_all('table#tabela-turmas tbody tr')
                
                for linha in linhas:
                    try:
                        colunas_td = linha.query_selector_all('td')
                        
                        # Extrair dados das colunas
                        dados_linha = []
                        for j, td in enumerate(colunas_td[1:], 1):  # Pular primeira coluna (radio)
                            if j == len(colunas_td) - 1:  # Pular última coluna (ações)
                                continue
                            
                            badge = td.query_selector('span.badge')
                            if badge:
                                dados_linha.append(badge.inner_text().strip())
                            else:
                                texto = td.inner_text().strip().replace('\n', ' ').replace('\t', ' ')
                                texto = re.sub(r'\s+', ' ', texto).strip()
                                dados_linha.append(texto)

                        # Extrair ID da turma
                        radio_input = linha.query_selector('input[type="radio"][name="item[]"]')
                        turma_id = radio_input.get_attribute('value') if radio_input else None
                        
                        if not turma_id or len(dados_linha) < 7:
                            continue

                        # Limpar localidade
                        localidade = self.extrair_localidade_limpa(dados_linha[0])
                        
                        # Pular compartilhados
                        if 'compartilhad' in localidade.lower():
                            continue

                        matriculados_badge = dados_linha[3]
                        matriculados_reais = self.obter_matriculados_reais(turma_id)
                        
                        # Determinar status
                        if matriculados_reais >= 0:
                            if matriculados_reais == int(matriculados_badge):
                                status = "✅ OK"
                            else:
                                status = f"⚠️ Diferença (Original: {matriculados_badge}, Real: {matriculados_reais})"
                        else:
                            status = "❌ Erro ao verificar"
                        
                        # Extrair dia da semana
                        dia_semana = self.extrair_dia_da_semana(dados_linha[6])
                        
                        # Montar linha conforme especificação
                        linha_completa = [
                            localidade,                    # LOCALIDADE
                            dados_linha[1],               # CURSO
                            dados_linha[2],               # NOMENCLATURA
                            matriculados_badge,           # MATRICULADOS
                            dados_linha[4],               # INICIO
                            dados_linha[5],               # TERMINO
                            dados_linha[6],               # DIA - HORA
                            dia_semana,                   # DIA
                            turma_id,                     # ID
                            str(matriculados_reais) if matriculados_reais >= 0 else "Erro",  # REAL
                            status                        # STATUS
                        ]
                        
                        resultado_detalhado.append(linha_completa)
                        time.sleep(0.3)
                        
                    except Exception as e:
                        print(f"⚠️ Erro ao processar linha detalhada: {e}")
                        continue
                
                # Verificar próxima página
                try:
                    btn_next = pagina_turmas_det.query_selector('a.paginate_button.next:not(.disabled)')
                    if btn_next and btn_next.is_enabled():
                        btn_next.click()
                        time.sleep(3)
                        pagina_atual += 1
                    else:
                        break
                except:
                    break
            
            self.resultados['turmas_detalhadas'] = resultado_detalhado
            print(f"✅ Turmas detalhadas coletadas: {len(resultado_detalhado)} turmas")
            
        except Exception as e:
            print(f"❌ Erro na coleta de turmas detalhadas: {e}")
        finally:
            pagina_turmas_det.close()

    def processar_coleta_paralela(self):
        """Executa todas as coletas em paralelo usando threads"""
        threads = []
        
        # Thread para candidatos por nível
        thread_candidatos = threading.Thread(target=self.coletar_candidatos_niveis)
        threads.append(thread_candidatos)
        
        # Thread para oficializados
        thread_oficializados = threading.Thread(target=self.coletar_oficializados)
        threads.append(thread_oficializados)
        
        # Thread para turmas detalhadas
        thread_turmas_det = threading.Thread(target=self.coletar_turmas_detalhadas)
        threads.append(thread_turmas_det)
        
        # Iniciar todas as threads
        for thread in threads:
            thread.start()
        
        # Aguardar conclusão de todas as threads
        for thread in threads:
            thread.join()

    def consolidar_dados_aba1(self):
        """Consolida dados da ABA 1 combinando resumo de turmas com níveis"""
        print("\n🔄 Consolidando dados da ABA 1...")
        
        # Obter dados do resumo de turmas
        resumo_turmas = self.resultados.get('resumo_turmas', [])
        candidatos_niveis = self.resultados.get('candidatos_niveis', {})
        oficializados = self.resultados.get('oficializados', {})
        
        # Criar dicionário de resumo por localidade
        resumo_dict = {}
        for linha in resumo_turmas:
            resumo_dict[linha[0]] = linha[1:]  # [qtd_turmas, matric_soma, matric_reais]
        
        # Consolidar tudo
        dados_consolidados = []
        
        # Todas as localidades presentes em qualquer fonte
        todas_localidades = set(resumo_dict.keys()) | set(candidatos_niveis.keys()) | set(oficializados.keys())
        
        for localidade in sorted(todas_localidades):
            # Dados do resumo (colunas A-D)
            resumo_local = resumo_dict.get(localidade, [0, 0, 0])
            
            # Dados de níveis (colunas E-J)
            niveis_local = candidatos_niveis.get(localidade, {
                'CANDIDATO(A)': 0, 'RJM/ENSAIO': 0, 'ENSAIO': 0, 
                'RJM': 0, 'RJM/CULTO OFICIAL': 0, 'CULTO OFICIAL': 0
            })
            
            # Dados de oficializados (colunas K-L)
            oficial_local = oficializados.get(localidade, {'RJM/OFICIALIZADO': 0, 'OFICIALIZADO': 0})
            
            linha_consolidada = [
                localidade,                                    # A: LOCALIDADE
                resumo_local[0],                              # B: QTD TURMAS
                resumo_local[1],                              # C: MATRIC. SOMA
                resumo_local[2],                              # D: MATRIC. REAIS
                niveis_local['CANDIDATO(A)'],                 # E: CANDIDATO(A)
                niveis_local['RJM/ENSAIO'],                   # F: RJM/ENSAIO
                niveis_local['ENSAIO'],                       # G: ENSAIO
                niveis_local['RJM'],                          # H: RJM
                niveis_local['RJM/CULTO OFICIAL'],            # I: RJM/CULTO OFICIAL
                niveis_local['CULTO OFICIAL'],                # J: CULTO OFICIAL
                oficial_local['RJM/OFICIALIZADO'],            # K: RJM/OFICIALIZADO
                oficial_local['OFICIALIZADO']                 # L: OFICIALIZADO
            ]
            
            dados_consolidados.append(linha_consolidada)
        
        return dados_consolidados

    def enviar_para_sheets(self):
        """Envia todos os dados consolidados para Google Sheets"""
        print("\n📤 Enviando dados para Google Sheets...")
        
        # Consolidar dados da ABA 1
        dados_aba1 = self.consolidar_dados_aba1()
        
        # Headers da ABA 1
        headers_aba1 = [
            "LOCALIDADE", "QTD TURMAS", "MATRIC. SOMA", "MATRIC. REAIS",
            "CANDIDATO(A)", "RJM/ENSAIO", "ENSAIO", "RJM", "RJM/CULTO OFICIAL", "CULTO OFICIAL",
            "RJM/OFICIALIZADO", "OFICIALIZADO"
        ]
        
        # Headers da ABA 2
        headers_aba2 = [
            "LOCALIDADE", "CURSO", "NOMENCLATURA", "MATRICULADOS", "INICIO", "TERMINO", 
            "DIA - HORA", "DIA", "ID", "REAL", "STATUS"
        ]
        
        # Preparar dados para envio
        body = {
            "tipo": "dados_consolidados_musical",
            "aba1": {
                "nome": "Resumo por Localidade",
                "headers": headers_aba1,
                "dados": dados_aba1
            },
            "aba2": {
                "nome": "Turmas Detalhadas", 
                "headers": headers_aba2,
                "dados": self.resultados.get('turmas_detalhadas', [])
            },
            "resumo": {
                "total_localidades_aba1": len(dados_aba1),
                "total_turmas_aba2": len(self.resultados.get('turmas_detalhadas', [])),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        }

        try:
            resposta = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
            print(f"✅ Dados enviados! Status: {resposta.status_code}")
            print(f"📝 Resposta: {resposta.text}")
            return True
            
        except Exception as e:
            print(f"❌ Erro ao enviar para Google Sheets: {e}")
            return False

    def executar_coleta_completa(self):
        """Executa todo o processo de coleta de dados"""
        print("🚀 Iniciando coleta completa de dados musicais...")
        tempo_inicio = time.time()
        
        with sync_playwright() as p:
            self.navegador = p.chromium.launch(headless=True)
            self.pagina_principal = self.navegador.new_page()
            
            # Configurar headers
            self.pagina_principal.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
            })
            
            # Fazer login
            if not self.fazer_login():
                self.navegador.close()
                return
            
            # Configurar sessão requests
            cookies_dict = self.extrair_cookies_playwright(self.pagina_principal)
            self.session = requests.Session()
            self.session.cookies.update(cookies_dict)
            
            print("\n🔄 Executando coletas em paralelo...")
            
            # Primeiro coletar resumo de turmas (necessário para ABA 1)
            self.coletar_turmas_resumo()
            
            # Depois executar coletas paralelas
            self.processar_coleta_paralela()
            
            # Enviar dados para Google Sheets
            sucesso_envio = self.enviar_para_sheets()
            
            # Mostrar resumo final
            self.mostrar_resumo_final()
            
            self.navegador.close()
            
            tempo_total = time.time() - tempo_inicio
            print(f"\n🎯 Processo completo finalizado em {tempo_total:.1f} segundos!")
            
            if sucesso_envio:
                print("✅ Todos os dados foram enviados com sucesso para o Google Sheets!")
            else:
                print("⚠️ Houve problemas no envio dos dados.")

    def mostrar_resumo_final(self):
        """Mostra resumo final de todos os dados coletados"""
        print("\n" + "="*80)
        print("📈 RESUMO FINAL DA COLETA")
        print("="*80)
        
        # Resumo ABA 1
        dados_aba1 = self.consolidar_dados_aba1()
        print(f"📊 ABA 1 - Resumo por Localidade:")
        print(f"   🏢 Total de localidades: {len(dados_aba1)}")
        
        if dados_aba1:
            total_turmas = sum(linha[1] for linha in dados_aba1)
            total_matriculas_soma = sum(linha[2] for linha in dados_aba1)
            total_matriculas_reais = sum(linha[3] for linha in dados_aba1)
            total_candidatos = sum(linha[4] for linha in dados_aba1)
            total_oficializados = sum(linha[10] + linha[11] for linha in dados_aba1)
            
            print(f"   📚 Total de turmas: {total_turmas}")
            print(f"   👥 Total matrículas (soma): {total_matriculas_soma}")
            print(f"   ✅ Total matrículas (reais): {total_matriculas_reais}")
            print(f"   🎵 Total candidatos: {total_candidatos}")
            print(f"   🏆 Total oficializados: {total_oficializados}")
        
        # Resumo ABA 2
        turmas_detalhadas = self.resultados.get('turmas_detalhadas', [])
        print(f"\n📊 ABA 2 - Turmas Detalhadas:")
        print(f"   📚 Total de turmas: {len(turmas_detalhadas)}")
        
        if turmas_detalhadas:
            turmas_ok = len([t for t in turmas_detalhadas if "✅ OK" in t[-1]])
            turmas_diferenca = len([t for t in turmas_detalhadas if "⚠️ Diferença" in t[-1]])
            turmas_erro = len([t for t in turmas_detalhadas if "❌ Erro" in t[-1]])
            
            print(f"   ✅ Turmas OK: {turmas_ok}")
            print(f"   ⚠️ Com diferenças: {turmas_diferenca}")
            print(f"   ❌ Com erro: {turmas_erro}")
        
        print("="*80)

def main():
    """Função principal que executa todo o processo"""
    scraper = MusicalScraper()
    scraper.executar_coleta_completa()

if __name__ == "__main__":
    main()
