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

# Lista de IDs dos alunos (adicione os IDs que você precisa)
ALUNOS_IDS = [
    697150,
    732523,
    # Adicione mais IDs conforme necessário
]

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

class MusicLessonScraper:
    def __init__(self, page):
        self.page = page
        
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
        
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=60)
        
        if response.status_code == 200:
            print("✅ Dados enviados com sucesso para Google Sheets!")
            return True
        else:
            print(f"❌ Erro ao enviar para Apps Script: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao enviar para Apps Script: {e}")
        return False

def main():
    """Função principal"""
    tempo_inicio = time.time()
    print("🎵 Iniciando extração de lições musicais...")
    print(f"📋 Total de alunos a processar: {len(ALUNOS_IDS)}")
    
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
            dados_extraidos = []
            
            # Processar cada aluno
            for i, aluno_id in enumerate(ALUNOS_IDS, 1):
                print(f"\n📋 Processando {i}/{len(ALUNOS_IDS)}")
                
                dados_aluno = scraper.extrair_dados_aluno(aluno_id)
                if dados_aluno:
                    dados_extraidos.append(dados_aluno)
                
                # Pequena pausa entre requisições
                time.sleep(2)
            
            # Salvar dados
            if dados_extraidos:
                print(f"\n✅ Extração concluída! {len(dados_extraidos)} alunos processados.")
                
                # Salvar localmente primeiro
                salvar_dados_local(dados_extraidos)
                
                # Tentar enviar para Google Sheets
                if not enviar_para_apps_script(dados_extraidos):
                    print("⚠️ Falha no envio para Google Sheets, mas dados foram salvos localmente.")
                
            else:
                print("❌ Nenhum dado foi extraído.")
            
        except Exception as e:
            print(f"❌ Erro geral: {e}")
        
        finally:
            navegador.close()
            
            # Estatísticas finais
            tempo_total = round(time.time() - tempo_inicio, 2)
            print(f"\n⏱️ Tempo total de processamento: {tempo_total} segundos")
            print(f"📈 Alunos processados com sucesso: {len(dados_extraidos)}")

if __name__ == "__main__":
    main()
