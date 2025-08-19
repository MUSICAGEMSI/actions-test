from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
import json
from bs4 import BeautifulSoup
from datetime import datetime
import csv

# Configurações
EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxVW_i69_DL_UQQqVjxLsAcEv5edorXSD4g-PZUu4LC9TkGd9yEfNiTL0x92ELDNm8M/exec'

# URL para acessar a lista de alunos (ajuste conforme necessário)
URL_ALUNOS = "https://musical.congregacao.org.br/alunos/"

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

class MusicLessonScraper:
    def __init__(self, page):
        self.page = page
        
    def obter_lista_alunos(self):
        """Obtém a lista completa de IDs de alunos do sistema"""
        print("🔍 Buscando lista completa de alunos...")
        
        alunos_ids = []
        
        try:
            # Tentar diferentes URLs possíveis para lista de alunos
            urls_possiveis = [
                "https://musical.congregacao.org.br/alunos/",
                "https://musical.congregacao.org.br/alunos/index",
                "https://musical.congregacao.org.br/usuarios/",
                "https://musical.congregacao.org.br/dashboard/",
                "https://musical.congregacao.org.br/"
            ]
            
            for url in urls_possiveis:
                try:
                    print(f"   🔍 Tentando: {url}")
                    self.page.goto(url, timeout=30000)
                    self.page.wait_for_load_state('networkidle')
                    
                    html_content = self.page.content()
                    
                    # Procurar por links que contenham IDs de alunos
                    # Padrões comuns: /licoes/index/123456, /aluno/123456, etc.
                    padroes_ids = [
                        r'/licoes/index/(\d+)',
                        r'/aluno[s]?/(\d+)',
                        r'/usuario[s]?/(\d+)',
                        r'aluno[_-]?id[=:](\d+)',
                        r'id[=:](\d+)',
                        r'data-id[=:][\'""](\d+)[\'""]',
                        r'value[=:][\'""](\d+)[\'""]'
                    ]
                    
                    ids_encontrados = set()
                    
                    for padrao in padroes_ids:
                        matches = re.findall(padrao, html_content, re.IGNORECASE)
                        for match in matches:
                            if len(match) >= 4:  # IDs geralmente têm pelo menos 4 dígitos
                                ids_encontrados.add(int(match))
                    
                    if ids_encontrados:
                        alunos_ids.extend(list(ids_encontrados))
                        print(f"   ✅ Encontrados {len(ids_encontrados)} IDs únicos em: {url}")
                        break
                        
                except Exception as e:
                    print(f"   ❌ Erro ao acessar {url}: {e}")
                    continue
            
            # Se não encontrou IDs automaticamente, tentar extrair de tabelas
            if not alunos_ids:
                print("🔍 Tentando extrair IDs de tabelas...")
                soup = BeautifulSoup(self.page.content(), 'html.parser')
                
                # Procurar em tabelas
                for table in soup.find_all('table'):
                    for link in table.find_all('a', href=True):
                        href = link['href']
                        match = re.search(r'(\d{4,})', href)
                        if match:
                            alunos_ids.append(int(match.group(1)))
                
                # Procurar em forms e inputs
                for form in soup.find_all('form'):
                    for input_elem in form.find_all('input'):
                        value = input_elem.get('value', '')
                        if value.isdigit() and len(value) >= 4:
                            alunos_ids.append(int(value))
            
            # Remover duplicatas e ordenar
            alunos_ids = sorted(list(set(alunos_ids)))
            
            print(f"✅ Total de {len(alunos_ids)} alunos encontrados: {alunos_ids[:10]}{'...' if len(alunos_ids) > 10 else ''}")
            
            # Se ainda não encontrou nada, usar lista padrão expandida
            if not alunos_ids:
                print("⚠️ Não foi possível encontrar IDs automaticamente. Usando lista padrão...")
                alunos_ids = [697150, 732523]  # Lista padrão como fallback
                
                # Tentar alguns IDs sequenciais próximos aos conhecidos
                base_ids = [697150, 732523]
                for base_id in base_ids:
                    for offset in range(-50, 51):
                        test_id = base_id + offset
                        if test_id > 0:
                            alunos_ids.append(test_id)
                
                alunos_ids = sorted(list(set(alunos_ids)))
                print(f"📝 Lista expandida com {len(alunos_ids)} IDs para testar")
            
            return alunos_ids
            
        except Exception as e:
            print(f"❌ Erro ao obter lista de alunos: {e}")
            # Retornar lista padrão em caso de erro
            return [697150, 732523]
    
    def verificar_aluno_existe(self, aluno_id):
        """Verifica se um aluno existe antes de tentar extrair dados"""
        try:
            url = f"https://musical.congregacao.org.br/licoes/index/{aluno_id}"
            self.page.goto(url, timeout=15000)
            
            # Aguardar um pouco para carregar
            time.sleep(2)
            
            content = self.page.content().lower()
            
            # Verificar se login foi bem-sucedido
        content = page.content().lower()
        sucesso_indicadores = ["sair", "logout", "dashboard", "painel", "alunos", "licoes", "bem-vindo"]
        
        if any(termo in content for termo in sucesso_indicadores):
            print("✅ Login realizado com sucesso!")
            return True
        else:
            print("❌ Login parece ter falhado")
            # Salvar página pós-login para debug
            with open('debug_pos_login.html', 'w', encoding='utf-8') as f:
                f.write(page.content())
            print("📄 Página pós-login salva como debug_pos_login.html")
            return False
            
    except Exception as e:
        print(f"❌ Erro durante o login: {e}")
        return False

def salvar_dados_local(dados, nome_arquivo='licoes_musicais_extraidas.csv'):
    """Salva os dados localmente em CSV"""
    try:
        with open(nome_arquivo, 'w', newline='', encoding='utf-8-sig') as arquivo:
            writer = csv.writer(arquivo, delimiter=';')
            
            # Cabeçalho
            writer.writerow([
                'ID', 'Nome do Aluno', 'MTS (Data da Lição)', 
                'MSA (Data da Lição)', 'PROVAS (Data da Prova)', 
                'MÉTODO (Data da Lição)', 'HINÁRIO (Data da Aula)', 
                'ESCALAS (Data de Cadastro)'
            ])
            
            # Dados
            for aluno in dados:
                writer.writerow([
                    aluno['id'], aluno['nome'], aluno['mts'],
                    aluno['msa'], aluno['provas'], aluno['metodo'],
                    aluno['hinario'], aluno['escalas']
                ])
        
        print(f"💾 Dados salvos localmente em: {nome_arquivo}")
        return True
    except Exception as e:
        print(f"❌ Erro ao salvar dados localmente: {e}")
        return False

def enviar_para_apps_script(dados):
    """Envia dados para Google Apps Script"""
    try:
        print("📤 Enviando dados para Google Sheets...")
        
        # Preparar dados para envio
        headers = [
            'ID', 'Nome do Aluno', 'MTS (Data da Lição)', 
            'MSA (Data da Lição)', 'PROVAS (Data da Prova)', 
            'MÉTODO (Data da Lição)', 'HINÁRIO (Data da Aula)', 
            'ESCALAS (Data de Cadastro)'
        ]
        
        rows = []
        for aluno in dados:
            rows.append([
                aluno['id'], aluno['nome'], aluno['mts'],
                aluno['msa'], aluno['provas'], aluno['metodo'],
                aluno['hinario'], aluno['escalas']
            ])
        
        payload = {
            "headers": headers,
            "data": rows
        }
        
        # Configurar timeout maior para envios grandes
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
        
        if response.status_code == 200:
            print("✅ Dados enviados com sucesso para Google Sheets!")
            try:
                resposta_json = response.json()
                if 'planilha_url' in resposta_json:
                    print(f"🔗 Link da planilha: {resposta_json['planilha_url']}")
            except:
                pass
            return True
        else:
            print(f"❌ Erro ao enviar para Apps Script: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao enviar para Apps Script: {e}")
        return False

def main():
    """Função principal"""
    tempo_inicio = time.time()
    print("🎵 Iniciando extração de lições musicais...")
    
    with sync_playwright() as p:
        # Configurar navegador
        navegador = p.chromium.launch(
            headless=True,  # Sempre headless para compatibilidade com servidores
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        contexto = navegador.new_context()
        page = contexto.new_page()
        
        try:
            # Fazer login
            if not fazer_login(page):
                print("❌ Não foi possível fazer login. Encerrando.")
                return
            
            # Inicializar scraper
            scraper = MusicLessonScraper(page)
            
            # Obter lista completa de alunos
            alunos_ids = scraper.obter_lista_alunos()
            print(f"📋 Total de alunos para verificar: {len(alunos_ids)}")
            
            # Filtrar apenas alunos válidos (existe uma página para eles)
            print("🔍 Verificando quais alunos são válidos...")
            alunos_validos = []
            
            for i, aluno_id in enumerate(alunos_ids[:50]):  # Limitar a 50 primeiros para teste
                print(f"   Verificando {i+1}/{min(50, len(alunos_ids))}: ID {aluno_id}")
                
                if scraper.verificar_aluno_existe(aluno_id):
                    alunos_validos.append(aluno_id)
                    print(f"   ✅ Aluno {aluno_id} é válido")
                else:
                    print(f"   ❌ Aluno {aluno_id} não existe ou sem acesso")
                
                # Pausa entre verificações
                time.sleep(1)
                
                # A cada 10 verificações, mostrar progresso
                if (i + 1) % 10 == 0:
                    print(f"   📊 Progresso: {i+1}/{min(50, len(alunos_ids))} verificados, {len(alunos_validos)} válidos encontrados")
            
            print(f"\n✅ {len(alunos_validos)} alunos válidos encontrados: {alunos_validos}")
            
            if not alunos_validos:
                print("❌ Nenhum aluno válido encontrado. Verifique as credenciais e URLs.")
                return
            
            # Processar dados dos alunos válidos
            dados_extraidos = []
            print(f"\n🔄 Iniciando extração de dados de {len(alunos_validos)} alunos...")
            
            for i, aluno_id in enumerate(alunos_validos, 1):
                print(f"\n📋 Processando {i}/{len(alunos_validos)}")
                
                dados_aluno = scraper.extrair_dados_aluno(aluno_id)
                if dados_aluno:
                    dados_extraidos.append(dados_aluno)
                    print(f"   ✅ Dados extraídos com sucesso para {dados_aluno['nome']}")
                else:
                    print(f"   ❌ Falha na extração para aluno {aluno_id}")
                
                # Pequena pausa entre requisições
                time.sleep(3)
                
                # A cada 5 alunos, mostrar estatísticas
                if i % 5 == 0:
                    print(f"\n📊 Progresso: {i}/{len(alunos_validos)} processados, {len(dados_extraidos)} com sucesso")
            
            # Salvar dados
            if dados_extraidos:
                print(f"\n🎉 Extração concluída! {len(dados_extraidos)} alunos processados com sucesso.")
                
                # Salvar localmente primeiro
                if salvar_dados_local(dados_extraidos):
                    print("✅ Backup local criado com sucesso")
                
                # Tentar enviar para Google Sheets
                print("\n📤 Enviando dados para Google Sheets...")
                if enviar_para_apps_script(dados_extraidos):
                    print("🎉 Dados sincronizados com Google Sheets!")
                else:
                    print("⚠️ Falha no envio para Google Sheets, mas dados foram salvos localmente.")
                
                # Mostrar estatísticas finais
                print(f"\n📊 ESTATÍSTICAS FINAIS:")
                print(f"   📈 Total de alunos processados: {len(dados_extraidos)}")
                
                # Contar registros por categoria
                stats = {
                    'MTS': sum(1 for a in dados_extraidos if a['mts']),
                    'MSA': sum(1 for a in dados_extraidos if a['msa']),
                    'Provas': sum(1 for a in dados_extraidos if a['provas']),
                    'Método': sum(1 for a in dados_extraidos if a['metodo']),
                    'Hinário': sum(1 for a in dados_extraidos if a['hinario']),
                    'Escalas': sum(1 for a in dados_extraidos if a['escalas'])
                }
                
                for categoria, count in stats.items():
                    percentual = (count / len(dados_extraidos) * 100) if dados_extraidos else 0
                    print(f"   📊 {categoria}: {count} alunos ({percentual:.1f}%)")
                
            else:
                print("❌ Nenhum dado foi extraído com sucesso.")
            
        except Exception as e:
            print(f"❌ Erro geral: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            navegador.close()
            
            # Estatísticas finais
            tempo_total = round(time.time() - tempo_inicio, 2)
            print(f"\n⏱️ Tempo total de processamento: {tempo_total} segundos")
            print(f"📈 Alunos processados com sucesso: {len(dados_extraidos) if 'dados_extraidos' in locals() else 0}")

if __name__ == "__main__":
    main() é uma página válida de aluno
            indicadores_validos = [
                "lições aprovadas",
                "módulo",
                "fase",
                "método",
                "hinário",
                "escala",
                "prova"
            ]
            
            # Verificar se não é página de erro
            indicadores_erro = [
                "erro",
                "não encontrado",
                "not found",
                "acesso negado",
                "forbidden"
            ]
            
            tem_indicadores_validos = any(ind in content for ind in indicadores_validos)
            tem_indicadores_erro = any(ind in content for ind in indicadores_erro)
            
            return tem_indicadores_validos and not tem_indicadores_erro
            
        except Exception as e:
            print(f"   ⚠️ Erro ao verificar aluno {aluno_id}: {e}")
            return False
        
    def extrair_nome_aluno(self, html_content):
        """Extrai o nome do aluno do título da página"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            title = soup.find('title')
            if title and title.text:
                # Padrão: "Lições Aprovadas / Nome - Estado/Idade / Instrumento"
                match = re.search(r'Lições Aprovadas / (.+?) - .+?/.+? / (.+)', title.text)
                if match:
                    nome = match.group(1).strip()
                    instrumento = match.group(2).strip()
                    return f"{nome} - {instrumento}"
                
                # Fallback: só o nome
                match = re.search(r'Lições Aprovadas / (.+)', title.text)
                if match:
                    return match.group(1).strip()
            
            return "Nome não encontrado"
        except Exception as e:
            print(f"⚠️ Erro ao extrair nome: {e}")
            return "Erro ao extrair nome"

    def processar_data(self, data_texto):
        """Converte texto de data para formato DD/MM/AAAA"""
        if not data_texto or data_texto.strip() == '':
            return ""
        
        try:
            data_limpa = data_texto.strip()
            
            # Remover horário se existir
            if ' ' in data_limpa:
                data_limpa = data_limpa.split()[0]
            
            # Verificar se já está no formato DD/MM/AAAA
            if re.match(r'\d{2}/\d{2}/\d{4}', data_limpa):
                return data_limpa
            
            # Tentar outros formatos
            formatos = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']
            
            for fmt in formatos:
                try:
                    data_obj = datetime.strptime(data_limpa, fmt)
                    return data_obj.strftime('%d/%m/%Y')
                except ValueError:
                    continue
            
            return data_limpa
        except:
            return data_texto

    def extrair_datas_mts(self, soup):
        """Extrai datas das lições de MTS (individual e grupo)"""
        datas = []
        
        try:
            # Procurar por todas as células que contêm datas
            for td in soup.find_all('td'):
                texto = td.get_text(strip=True)
                # Verificar se é uma data válida
                if re.match(r'\d{1,2}/\d{1,2}/\d{4}', texto):
                    data_processada = self.processar_data(texto)
                    if data_processada and data_processada not in datas:
                        # Verificar se está na seção MTS
                        linha = td.find_parent('tr')
                        if linha:
                            contexto = linha.get_text().upper()
                            # Procurar indicadores de que é MTS
                            if any(palavra in contexto for palavra in ['MÓDULO', 'FASE', 'PÁGINA']):
                                # Verificar se não é MSA (que também tem fases)
                                texto_anterior = str(soup)[:str(soup).find(str(td))]
                                if 'MTS' in texto_anterior[-1000:] and 'MSA' not in texto_anterior[-200:]:
                                    datas.append(data_processada)
                                elif 'MTS' in contexto:
                                    datas.append(data_processada)
        except Exception as e:
            print(f"⚠️ Erro ao extrair MTS: {e}")
        
        # Remover duplicatas e ordenar
        datas_unicas = sorted(list(set(datas)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
        return "; ".join(datas_unicas)

    def extrair_datas_msa(self, soup):
        """Extrai datas das lições de MSA (individual e grupo)"""
        datas = []
        
        try:
            # Método 1: Procurar por padrão "Fase(s): de X até Y" seguido de data
            texto_completo = soup.get_text()
            padrao_msa = re.findall(r'Fase\(s\):[^;]*?(\d{2}/\d{2}/\d{4})', texto_completo)
            
            for data in padrao_msa:
                data_processada = self.processar_data(data)
                if data_processada:
                    datas.append(data_processada)
            
            # Método 2: Procurar em contexto MSA
            if not datas:
                for td in soup.find_all('td'):
                    texto = td.get_text(strip=True)
                    if re.match(r'\d{1,2}/\d{1,2}/\d{4}', texto):
                        # Verificar contexto MSA
                        linha = td.find_parent('tr')
                        if linha:
                            contexto_linha = linha.get_text().upper()
                            # Verificar se está na seção MSA
                            texto_anterior = str(soup)[:str(soup).find(str(td))]
                            if 'MSA' in texto_anterior[-1000:] and 'MTS' not in texto_anterior[-200:]:
                                data_processada = self.processar_data(texto)
                                if data_processada:
                                    datas.append(data_processada)
                                    
        except Exception as e:
            print(f"⚠️ Erro ao extrair MSA: {e}")
        
        datas_unicas = sorted(list(set(datas)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
        return "; ".join(datas_unicas)

    def extrair_datas_provas(self, soup):
        """Extrai datas das provas"""
        datas = []
        
        try:
            # Procurar por colunas com "Data da Prova"
            for th in soup.find_all('th'):
                if 'Data da Prova' in th.get_text():
                    # Encontrou header da tabela de provas
                    tabela = th.find_parent('table')
                    if tabela:
                        for tr in tabela.find_all('tr')[1:]:  # Pular header
                            colunas = tr.find_all('td')
                            for td in colunas:
                                texto = td.get_text(strip=True)
                                if re.match(r'\d{1,2}/\d{1,2}/\d{4}', texto):
                                    data_processada = self.processar_data(texto)
                                    if data_processada:
                                        datas.append(data_processada)
                                    break
                    break
                    
        except Exception as e:
            print(f"⚠️ Erro ao extrair Provas: {e}")
        
        datas_unicas = sorted(list(set(datas)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
        return "; ".join(datas_unicas)

    def extrair_datas_metodo(self, soup):
        """Extrai datas das lições de método"""
        datas = []
        
        try:
            # Procurar por colunas com "Data da Lição" em contexto de método
            for th in soup.find_all('th'):
                if 'Data da Lição' in th.get_text():
                    tabela = th.find_parent('table')
                    if tabela:
                        # Verificar se é tabela de método
                        tabela_texto = tabela.get_text().upper()
                        if 'MÉTODO' in tabela_texto or 'PÁGINAS' in tabela_texto:
                            for tr in tabela.find_all('tr')[1:]:  # Pular header
                                colunas = tr.find_all('td')
                                for td in colunas:
                                    texto = td.get_text(strip=True)
                                    if re.match(r'\d{1,2}/\d{1,2}/\d{4}', texto):
                                        data_processada = self.processar_data(texto)
                                        if data_processada:
                                            datas.append(data_processada)
                                        break
                    
        except Exception as e:
            print(f"⚠️ Erro ao extrair Método: {e}")
        
        datas_unicas = sorted(list(set(datas)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
        return "; ".join(datas_unicas)

    def extrair_datas_hinario(self, soup):
        """Extrai datas das aulas de hinário (individual e grupo)"""
        datas = []
        
        try:
            # Procurar por "Data da aula" ou "Data da Lição" em contexto de hino
            for th in soup.find_all('th'):
                texto_th = th.get_text(strip=True)
                if 'Data da aula' in texto_th or 'Data da Lição' in texto_th:
                    tabela = th.find_parent('table')
                    if tabela:
                        tabela_texto = tabela.get_text().upper()
                        if 'HINO' in tabela_texto:
                            for tr in tabela.find_all('tr')[1:]:  # Pular header
                                colunas = tr.find_all('td')
                                for td in colunas:
                                    texto = td.get_text(strip=True)
                                    if re.match(r'\d{1,2}/\d{1,2}/\d{4}', texto):
                                        data_processada = self.processar_data(texto)
                                        if data_processada:
                                            datas.append(data_processada)
                                        break
            
            # Procurar em "Hinos - Aulas em grupo" 
            texto_completo = soup.get_text()
            if "Hinos - Aulas em grupo" in texto_completo:
                # Encontrar datas após esse texto
                linhas = texto_completo.split('\n')
                encontrou_secao = False
                for linha in linhas:
                    if "Hinos - Aulas em grupo" in linha:
                        encontrou_secao = True
                        continue
                    if encontrou_secao and re.search(r'\d{2}/\d{2}/\d{4}', linha):
                        matches = re.findall(r'\d{2}/\d{2}/\d{4}', linha)
                        for match in matches:
                            data_processada = self.processar_data(match)
                            if data_processada:
                                datas.append(data_processada)
                                
        except Exception as e:
            print(f"⚠️ Erro ao extrair Hinário: {e}")
        
        datas_unicas = sorted(list(set(datas)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
        return "; ".join(datas_unicas)

    def extrair_datas_escalas(self, soup):
        """Extrai datas de cadastro das escalas (individual e grupo)"""
        datas = []
        
        try:
            # Procurar por "Data de Cadastro" ou "Data" em contexto de escalas
            for th in soup.find_all('th'):
                texto_th = th.get_text(strip=True)
                if 'Data de Cadastro' in texto_th or (texto_th == 'Data' and 'ESCALA' in str(th.find_parent('table'))):
                    tabela = th.find_parent('table')
                    if tabela:
                        tabela_texto = tabela.get_text().upper()
                        if 'ESCALA' in tabela_texto:
                            for tr in tabela.find_all('tr')[1:]:  # Pular header
                                colunas = tr.find_all('td')
                                for td in colunas:
                                    texto = td.get_text(strip=True)
                                    if re.match(r'\d{1,2}/\d{1,2}/\d{4}', texto):
                                        data_processada = self.processar_data(texto)
                                        if data_processada:
                                            datas.append(data_processada)
                                        break
            
            # Procurar em "Escalas - Aulas em grupo"
            texto_completo = soup.get_text()
            if "Escalas - Aulas em grupo" in texto_completo:
                linhas = texto_completo.split('\n')
                encontrou_secao = False
                for linha in linhas:
                    if "Escalas - Aulas em grupo" in linha:
                        encontrou_secao = True
                        continue
                    if encontrou_secao and re.search(r'\d{2}/\d{2}/\d{4}', linha):
                        matches = re.findall(r'\d{2}/\d{2}/\d{4}', linha)
                        for match in matches:
                            data_processada = self.processar_data(match)
                            if data_processada:
                                datas.append(data_processada)
                                
        except Exception as e:
            print(f"⚠️ Erro ao extrair Escalas: {e}")
        
        datas_unicas = sorted(list(set(datas)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
        return "; ".join(datas_unicas)

    def extrair_dados_aluno(self, aluno_id):
        """Extrai todos os dados de um aluno específico"""
        url = f"https://musical.congregacao.org.br/licoes/index/{aluno_id}"
        
        try:
            print(f"🔍 Processando aluno ID: {aluno_id}")
            self.page.goto(url, timeout=30000)
            self.page.wait_for_load_state('networkidle')
            
            html_content = self.page.content()
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extrair nome do aluno
            nome_aluno = self.extrair_nome_aluno(html_content)
            print(f"   👤 Nome: {nome_aluno}")
            
            # Extrair dados de cada seção
            mts_datas = self.extrair_datas_mts(soup)
            msa_datas = self.extrair_datas_msa(soup)
            provas_datas = self.extrair_datas_provas(soup)
            metodo_datas = self.extrair_datas_metodo(soup)
            hinario_datas = self.extrair_datas_hinario(soup)
            escalas_datas = self.extrair_datas_escalas(soup)
            
            # Mostrar estatísticas
            print(f"   📊 MTS: {len(mts_datas.split(';')) if mts_datas else 0} datas")
            print(f"   📊 MSA: {len(msa_datas.split(';')) if msa_datas else 0} datas")
            print(f"   📊 Provas: {len(provas_datas.split(';')) if provas_datas else 0} datas")
            print(f"   📊 Método: {len(metodo_datas.split(';')) if metodo_datas else 0} datas")
            print(f"   📊 Hinário: {len(hinario_datas.split(';')) if hinario_datas else 0} datas")
            print(f"   📊 Escalas: {len(escalas_datas.split(';')) if escalas_datas else 0} datas")
            
            return {
                'id': aluno_id,
                'nome': nome_aluno,
                'mts': mts_datas,
                'msa': msa_datas,
                'provas': provas_datas,
                'metodo': metodo_datas,
                'hinario': hinario_datas,
                'escalas': escalas_datas
            }
            
        except Exception as e:
            print(f"❌ Erro ao processar aluno {aluno_id}: {e}")
            return None

def fazer_login(page):
    """Realiza login no sistema"""
    try:
        print("🔐 Fazendo login...")
        page.goto(URL_INICIAL, timeout=30000)
        
        # Aguardar página carregar completamente
        print("⏳ Aguardando página carregar...")
        page.wait_for_load_state('domcontentloaded')
        time.sleep(3)  # Aguardar elementos JavaScript carregarem
        
        # Debug: salvar página inicial para análise
        print("🔍 Analisando estrutura da página de login...")
        with open('debug_pagina_inicial.html', 'w', encoding='utf-8') as f:
            f.write(page.content())
        print("📄 Página inicial salva como debug_pagina_inicial.html")
        
        # Procurar por campos de login de forma mais ampla
        page_content = page.content().lower()
        
        # Verificar se já está logado
        if any(termo in page_content for termo in ["sair", "logout", "dashboard", "alunos", "licoes"]):
            print("✅ Já estava logado!")
            return True
        
        # Lista de seletores mais ampla para email
        email_selectors = [
            'input[type="email"]',
            'input[name*="email"]',
            'input[id*="email"]',
            'input[placeholder*="email"]',
            'input[name*="usuario"]',
            'input[name*="login"]',
            'input[name="data[Usuario][email]"]',
            'input.email',
            '#email',
            '#usuario',
            '#login'
        ]
        
        # Tentar preencher email
        email_preenchido = False
        for selector in email_selectors:
            try:
                elements = page.query_selector_all(selector)
                if elements:
                    for element in elements:
                        if element.is_visible():
                            page.fill(selector, EMAIL)
                            email_preenchido = True
                            print(f"✅ Email preenchido com: {selector}")
                            break
                if email_preenchido:
                    break
            except Exception as e:
                continue
        
        if not email_preenchido:
            print("❌ Campo de email não encontrado. Verificando estrutura...")
            # Listar todos os inputs para debug
            inputs = page.query_selector_all('input')
            print(f"🔍 Encontrados {len(inputs)} campos input na página")
            for i, inp in enumerate(inputs):
                try:
                    input_type = inp.get_attribute('type') or 'text'
                    input_name = inp.get_attribute('name') or 'sem-nome'
                    input_id = inp.get_attribute('id') or 'sem-id'
                    print(f"  Input {i+1}: type='{input_type}', name='{input_name}', id='{input_id}'")
                except:
                    continue
            return False
        
        # Lista de seletores para senha
        senha_selectors = [
            'input[type="password"]',
            'input[name*="senha"]',
            'input[name*="password"]',
            'input[id*="senha"]',
            'input[id*="password"]',
            'input[name="data[Usuario][senha]"]',
            'input.senha',
            '#senha',
            '#password'
        ]
        
        # Tentar preencher senha
        senha_preenchida = False
        for selector in senha_selectors:
            try:
                elements = page.query_selector_all(selector)
                if elements:
                    for element in elements:
                        if element.is_visible():
                            page.fill(selector, SENHA)
                            senha_preenchida = True
                            print(f"✅ Senha preenchida com: {selector}")
                            break
                if senha_preenchida:
                    break
            except Exception as e:
                continue
        
        if not senha_preenchida:
            print("❌ Campo de senha não encontrado")
            return False
        
        # Aguardar um pouco antes de submeter
        time.sleep(2)
        
        # Lista de seletores para botão de submit
        submit_selectors = [
            'input[type="submit"]',
            'button[type="submit"]',
            'button:text("Entrar")',
            'button:text("Login")',
            'input[value*="Entrar"]',
            '.btn-login',
            '#login-btn',
            'form button',
            'form input[type="submit"]'
        ]
        
        # Tentar clicar no botão de login
        submit_clicado = False
        for selector in submit_selectors:
            try:
                elements = page.query_selector_all(selector)
                if elements:
                    for element in elements:
                        if element.is_visible():
                            page.click(selector)
                            submit_clicado = True
                            print(f"✅ Botão clicado: {selector}")
                            break
                if submit_clicado:
                    break
            except Exception as e:
                continue
        
        if not submit_clicado:
            print("❌ Botão de login não encontrado")
            return False
        
        # Aguardar redirecionamento após login
        print("⏳ Aguardando resposta do login...")
        time.sleep(5)
        page.wait_for_load_state('networkidle', timeout=20000)
        
        # Verificar se
