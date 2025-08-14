# script_licoes_musicais_otimizado.py
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

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzx5wJjPYSBEeoNQMc02fxi2j4JqROJ1HKbdM59tMHmb2TD2A2Y6IYDtTpHiZvmLFsGug/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

class MusicLessonScraper:
    def __init__(self, session, base_url="https://musical.congregacao.org.br/"):
        self.session = session
        self.base_url = base_url
        
    def extrair_nome_aluno(self, soup):
        """Extrai o nome do aluno da página"""
        try:
            # Procura no título da página
            title = soup.find('title')
            if title and title.text:
                # Padrão: "Lições Aprovadas / Nome - Estado/Idade / Instrumento"
                match = re.search(r'Lições Aprovadas / (.+)', title.text)
                if match:
                    return match.group(1).strip()
            
            # Fallback: procurar em headers h1, h2
            for header in soup.find_all(['h1', 'h2', 'h3']):
                if header.text and '-' in header.text and '/' in header.text:
                    return header.text.strip()
            
            return "Nome não encontrado"
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
            
            # Salvar dados localmente como backup
            import csv
            with open('backup_licoes_musicais_otimizado.csv', 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow(body["headers"])
                writer.writerows(resultado)
            print("💾 Dados salvos localmente como backup")
        
        print(f"\n⏱️ Tempo total de processamento: {round(time.time() - tempo_inicio, 2)} segundos")
        print(f"📈 Estatísticas finais:")
        print(f"   - Alunos processados: {len(resultado)}")
        if resultado:
            # Calcular estatísticas dos registros
            total_registros = 0
            secoes_com_dados = 0
            
            for linha in resultado:
                for i in range(1, 11):  # Colunas de seções (índices 1 a 10)
                    if linha[i]:  # Se tem dados na seção
                        num_datas = len(linha[i].split(';'))
                        total_registros += num_datas
                        secoes_com_dados += 1
            
            print(f"   - Total de registros de datas: {total_registros}")
            print(f"   - Seções com dados: {secoes_com_dados}")
            print(f"   - Média de registros por aluno: {round(total_registros/len(resultado), 1)}")
        
        navegador.close()

if __name__ == "__main__":
    main()f"⚠️ Erro ao extrair nome: {e}")
            return "Erro ao extrair nome"

    def processar_data(self, data_texto):
        """Converte texto de data para formato padronizado DD/MM/AAAA"""
        if not data_texto or data_texto.strip() == '':
            return ""
        
        try:
            # Limpar e normalizar
            data_limpa = data_texto.strip()
            
            # Tentar diferentes formatos
            formatos = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y %H:%M:%S']
            
            for fmt in formatos:
                try:
                    data_obj = datetime.strptime(data_limpa.split()[0], fmt)
                    return data_obj.strftime('%d/%m/%Y')
                except ValueError:
                    continue
            
            # Se não conseguir converter, tentar regex
            match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', data_limpa)
            if match:
                dia, mes, ano = match.groups()
                return f"{dia.zfill(2)}/{mes.zfill(2)}/{ano}"
            
            return data_limpa  # Retorna original se não conseguir converter
        except:
            return data_texto

    def extrair_datas_validas(self, dados_lista):
        """Extrai apenas as datas válidas de uma lista de dados"""
        datas_unicas = set()
        
        for entrada in dados_lista:
            if isinstance(entrada, dict) and 'data' in entrada:
                data = entrada['data']
                if data and data != "" and re.match(r'\d{2}/\d{2}/\d{4}', data):
                    datas_unicas.add(data)
        
        # Retornar datas ordenadas
        return sorted(list(datas_unicas), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))

    def extrair_mts_individual(self, soup):
        """Extrai dados de MTS Individual - OTIMIZADO"""
        dados = []
        try:
            tabelas = soup.find_all('table')
            for tabela in tabelas:
                # Verificar se é tabela de MTS Individual
                tabela_texto = tabela.get_text().upper()
                if any(termo in tabela_texto for termo in ['MÓDULO', 'MTS']) and 'GRUPO' not in tabela_texto:
                    linhas = tabela.find_all('tr')[1:]  # Pular header
                    
                    for linha in linhas:
                        colunas = linha.find_all('td')
                        if len(colunas) >= 3:
                            # Buscar data em qualquer coluna
                            data_encontrada = None
                            modulo = ""
                            licoes = ""
                            
                            for i, col in enumerate(colunas):
                                col_texto = col.get_text(strip=True)
                                
                                # Verificar se é data
                                if re.match(r'\d{1,2}/\d{1,2}/\d{4}', col_texto):
                                    data_encontrada = self.processar_data(col_texto)
                                # Capturar módulo (geralmente primeiro ou segundo)
                                elif i <= 1 and col_texto and not data_encontrada:
                                    if 'módulo' in col_texto.lower() or re.match(r'\d+', col_texto):
                                        modulo = col_texto
                                # Capturar lições
                                elif 'lição' in col_texto.lower() or re.match(r'\d+', col_texto):
                                    licoes = col_texto
                            
                            if data_encontrada:
                                dados.append({
                                    'data': data_encontrada,
                                    'modulo': modulo,
                                    'licoes': licoes
                                })
        except Exception as e:
            print(f"⚠️ Erro ao extrair MTS Individual: {e}")
        
        # Retornar apenas as datas com registro
        datas_validas = self.extrair_datas_validas(dados)
        return "; ".join(datas_validas)

    def extrair_mts_grupo(self, soup):
        """Extrai dados de MTS em Grupo - OTIMIZADO"""
        dados = []
        try:
            texto_pagina = soup.get_text()
            
            # Procurar por "MTS - Aulas em grupo" ou padrões similares
            if "MTS" in texto_pagina and ("grupo" in texto_pagina.lower() or "Aulas em grupo" in texto_pagina):
                tabelas = soup.find_all('table')
                
                for tabela in tabelas:
                    tabela_texto = tabela.get_text().upper()
                    if "GRUPO" in tabela_texto or "FASE" in tabela_texto:
                        linhas = tabela.find_all('tr')
                        
                        for linha in linhas:
                            colunas = linha.find_all('td')
                            if len(colunas) >= 3:
                                # Procurar data em qualquer coluna
                                for col in colunas:
                                    col_texto = col.get_text(strip=True)
                                    if re.match(r'\d{1,2}/\d{1,2}/\d{4}', col_texto):
                                        data_processada = self.processar_data(col_texto)
                                        if data_processada:
                                            dados.append({'data': data_processada})
                                        break
        except Exception as e:
            print(f"⚠️ Erro ao extrair MTS Grupo: {e}")
        
        datas_validas = self.extrair_datas_validas(dados)
        return "; ".join(datas_validas)

    def extrair_msa_individual(self, soup):
        """Extrai dados de MSA Individual - OTIMIZADO"""
        dados = []
        try:
            texto_pagina = soup.get_text()
            
            if "MSA" in texto_pagina and "grupo" not in texto_pagina.lower():
                tabelas = soup.find_all('table')
                
                for tabela in tabelas:
                    tabela_texto = tabela.get_text().upper()
                    if "MSA" in tabela_texto and "GRUPO" not in tabela_texto:
                        linhas = tabela.find_all('tr')
                        
                        for linha in linhas:
                            colunas = linha.find_all('td')
                            for col in colunas:
                                col_texto = col.get_text(strip=True)
                                if re.match(r'\d{1,2}/\d{1,2}/\d{4}', col_texto):
                                    data_processada = self.processar_data(col_texto)
                                    if data_processada:
                                        dados.append({'data': data_processada})
                                    break
        except Exception as e:
            print(f"⚠️ Erro ao extrair MSA Individual: {e}")
        
        datas_validas = self.extrair_datas_validas(dados)
        return "; ".join(datas_validas)

    def extrair_msa_grupo(self, soup):
        """Extrai dados de MSA em Grupo - OTIMIZADO"""
        dados = []
        try:
            texto_pagina = soup.get_text()
            
            # Método 1: Padrão específico "Fase(s): de X até Y"
            padrao_msa = re.findall(
                r'Fase\(s\): de.*?(\d{2}/\d{2}/\d{4})', 
                texto_pagina, 
                re.DOTALL
            )
            
            for match in padrao_msa:
                data_processada = self.processar_data(match)
                if data_processada:
                    dados.append({'data': data_processada})
            
            # Método 2: Procurar em tabelas com "MSA" e "grupo"
            if not dados and "MSA" in texto_pagina:
                tabelas = soup.find_all('table')
                
                for tabela in tabelas:
                    tabela_texto = tabela.get_text().upper()
                    if "MSA" in tabela_texto or "FASE" in tabela_texto:
                        linhas = tabela.find_all('tr')
                        
                        for linha in linhas:
                            colunas = linha.find_all('td')
                            for col in colunas:
                                col_texto = col.get_text(strip=True)
                                if re.match(r'\d{1,2}/\d{1,2}/\d{4}', col_texto):
                                    data_processada = self.processar_data(col_texto)
                                    if data_processada:
                                        dados.append({'data': data_processada})
                                    break
                                    
        except Exception as e:
            print(f"⚠️ Erro ao extrair MSA Grupo: {e}")
        
        datas_validas = self.extrair_datas_validas(dados)
        return "; ".join(datas_validas)

    def extrair_provas(self, soup):
        """Extrai dados de Provas - OTIMIZADO"""
        dados = []
        try:
            tabelas = soup.find_all('table')
            for tabela in tabelas:
                tabela_texto = tabela.get_text().upper()
                if any(termo in tabela_texto for termo in ['PROVA', 'NOTA', 'EXAME']):
                    linhas = tabela.find_all('tr')
                    
                    for linha in linhas:
                        colunas = linha.find_all('td')
                        for col in colunas:
                            col_texto = col.get_text(strip=True)
                            if re.match(r'\d{1,2}/\d{1,2}/\d{4}', col_texto):
                                data_processada = self.processar_data(col_texto)
                                if data_processada:
                                    dados.append({'data': data_processada})
                                break
        except Exception as e:
            print(f"⚠️ Erro ao extrair Provas: {e}")
        
        datas_validas = self.extrair_datas_validas(dados)
        return "; ".join(datas_validas)

    def extrair_metodo(self, soup):
        """Extrai dados de Método - OTIMIZADO"""
        dados = []
        try:
            tabelas = soup.find_all('table')
            for tabela in tabelas:
                tabela_texto = tabela.get_text().upper()
                if "MÉTODO" in tabela_texto:
                    linhas = tabela.find_all('tr')
                    
                    for linha in linhas:
                        colunas = linha.find_all('td')
                        for col in colunas:
                            col_texto = col.get_text(strip=True)
                            if re.match(r'\d{1,2}/\d{1,2}/\d{4}', col_texto):
                                data_processada = self.processar_data(col_texto)
                                if data_processada:
                                    dados.append({'data': data_processada})
                                break
        except Exception as e:
            print(f"⚠️ Erro ao extrair Método: {e}")
        
        datas_validas = self.extrair_datas_validas(dados)
        return "; ".join(datas_validas)

    def extrair_hinario(self, soup):
        """Extrai dados de Hinário - OTIMIZADO"""
        dados = []
        try:
            tabelas = soup.find_all('table')
            for tabela in tabelas:
                tabela_texto = tabela.get_text().upper()
                if "HINO" in tabela_texto and "GRUPO" not in tabela_texto:
                    linhas = tabela.find_all('tr')
                    
                    for linha in linhas:
                        colunas = linha.find_all('td')
                        for col in colunas:
                            col_texto = col.get_text(strip=True)
                            if re.match(r'\d{1,2}/\d{1,2}/\d{4}', col_texto):
                                data_processada = self.processar_data(col_texto)
                                if data_processada:
                                    dados.append({'data': data_processada})
                                break
        except Exception as e:
            print(f"⚠️ Erro ao extrair Hinário: {e}")
        
        datas_validas = self.extrair_datas_validas(dados)
        return "; ".join(datas_validas)

    def extrair_hinario_grupo(self, soup):
        """Extrai dados de Hinário em Grupo - OTIMIZADO"""
        dados = []
        try:
            texto_pagina = soup.get_text()
            
            if "Hinos - Aulas em grupo" in texto_pagina or ("HINO" in texto_pagina.upper() and "GRUPO" in texto_pagina.upper()):
                # Buscar padrões específicos
                padrao = re.findall(r'Hino.*?(\d{2}/\d{2}/\d{4})', texto_pagina)
                for match in padrao:
                    data_processada = self.processar_data(match)
                    if data_processada:
                        dados.append({'data': data_processada})
                
                # Buscar em tabelas também
                tabelas = soup.find_all('table')
                for tabela in tabelas:
                    tabela_texto = tabela.get_text().upper()
                    if "HINO" in tabela_texto and "GRUPO" in tabela_texto:
                        linhas = tabela.find_all('tr')
                        
                        for linha in linhas:
                            colunas = linha.find_all('td')
                            for col in colunas:
                                col_texto = col.get_text(strip=True)
                                if re.match(r'\d{1,2}/\d{1,2}/\d{4}', col_texto):
                                    data_processada = self.processar_data(col_texto)
                                    if data_processada:
                                        dados.append({'data': data_processada})
                                    break
        except Exception as e:
            print(f"⚠️ Erro ao extrair Hinário Grupo: {e}")
        
        datas_validas = self.extrair_datas_validas(dados)
        return "; ".join(datas_validas)

    def extrair_escalas(self, soup):
        """Extrai dados de Escalas - OTIMIZADO"""
        dados = []
        try:
            tabelas = soup.find_all('table')
            for tabela in tabelas:
                tabela_texto = tabela.get_text().upper()
                if "ESCALA" in tabela_texto and "GRUPO" not in tabela_texto:
                    linhas = tabela.find_all('tr')
                    
                    for linha in linhas:
                        colunas = linha.find_all('td')
                        for col in colunas:
                            col_texto = col.get_text(strip=True)
                            if re.match(r'\d{1,2}/\d{1,2}/\d{4}', col_texto):
                                data_processada = self.processar_data(col_texto)
                                if data_processada:
                                    dados.append({'data': data_processada})
                                break
        except Exception as e:
            print(f"⚠️ Erro ao extrair Escalas: {e}")
        
        datas_validas = self.extrair_datas_validas(dados)
        return "; ".join(datas_validas)

    def extrair_escalas_grupo(self, soup):
        """Extrai dados de Escalas em Grupo - OTIMIZADO"""
        dados = []
        try:
            texto_pagina = soup.get_text()
            
            if "Escalas - Aulas em grupo" in texto_pagina or ("ESCALA" in texto_pagina.upper() and "GRUPO" in texto_pagina.upper()):
                tabelas = soup.find_all('table')
                for tabela in tabelas:
                    tabela_texto = tabela.get_text().upper()
                    if "ESCALA" in tabela_texto and "GRUPO" in tabela_texto:
                        linhas = tabela.find_all('tr')
                        
                        for linha in linhas:
                            colunas = linha.find_all('td')
                            for col in colunas:
                                col_texto = col.get_text(strip=True)
                                if re.match(r'\d{1,2}/\d{1,2}/\d{4}', col_texto):
                                    data_processada = self.processar_data(col_texto)
                                    if data_processada:
                                        dados.append({'data': data_processada})
                                    break
        except Exception as e:
            print(f"⚠️ Erro ao extrair Escalas Grupo: {e}")
        
        datas_validas = self.extrair_datas_validas(dados)
        return "; ".join(datas_validas)

    def extrair_dados_aluno_otimizado(self, aluno_id):
        """
        Extração otimizada focando apenas nas datas com registro
        """
        url = f"{self.base_url}licoes/index/{aluno_id}"
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                'Referer': 'https://musical.congregacao.org.br/painel'
            }
            
            print(f"   🔗 Acessando: {url}")
            response = self.session.get(url, headers=headers, timeout=30)
            
            if response.status_code != 200:
                print(f"   ❌ HTTP {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Verificar se página carregou
            if "Lições Aprovadas" not in soup.get_text():
                print(f"   ⚠️ Página não carregou corretamente")
                return None
            
            nome_aluno = self.extrair_nome_aluno(soup)
            print(f"   📚 {nome_aluno}")
            
            # Extrair apenas as datas de cada seção
            dados = {
                'nome': nome_aluno,
                'mts_individual': self.extrair_mts_individual(soup),
                'mts_grupo': self.extrair_mts_grupo(soup),
                'msa_individual': self.extrair_msa_individual(soup),
                'msa_grupo': self.extrair_msa_grupo(soup),
                'provas': self.extrair_provas(soup),
                'metodo': self.extrair_metodo(soup),
                'hinario': self.extrair_hinario(soup),
                'hinario_grupo': self.extrair_hinario_grupo(soup),
                'escalas': self.extrair_escalas(soup),
                'escalas_grupo': self.extrair_escalas_grupo(soup),
                'id_aluno': aluno_id,
                'data_extracao': datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            }
            
            # Mostrar resumo do que foi extraído
            resumo = []
            total_registros = 0
            for campo, valor in dados.items():
                if campo not in ['nome', 'id_aluno', 'data_extracao'] and valor:
                    num_datas = len(valor.split(';')) if valor else 0
                    total_registros += num_datas
                    resumo.append(f"{campo}({num_datas})")
            
            if resumo:
                print(f"   ✅ Extraído: {', '.join(resumo)} - Total: {total_registros} registros")
            else:
                print(f"   ⚠️ Nenhum registro encontrado")
            
            return dados
            
        except Exception as e:
            print(f"   ❌ Erro: {e}")
            return None

def obter_lista_alunos(pagina, session):
    """
    Obtém lista de IDs de alunos navegando pelo sistema automaticamente
    """
    lista_ids = []
    
    try:
        print("🔍 Navegando para seção de alunos...")
        
        # Navegar para G.E.M -> Alunos (adapte conforme a estrutura do site)
        try:
            gem_selector = 'span:has-text("G.E.M")'
            pagina.wait_for_selector(gem_selector, timeout=15000)
            gem_element = pagina.locator(gem_selector).first
            gem_element.hover()
            pagina.wait_for_timeout(1000)
            gem_element.click()
            
            # Clicar em "Alunos" ou link similar
            pagina.wait_for_selector('a[href*="alunos"], a[href*="licoes"]', timeout=10000)
            alunos_link = pagina.locator('a[href*="alunos"], a[href*="licoes"]').first
            alunos_link.click()
            
            print("✅ Navegação para alunos realizada")
            
        except Exception as e:
            print(f"⚠️ Erro na navegação: {e}")
            # Fallback: usar lista manual por enquanto
            return ["635849"]  # ID de exemplo
        
        # Aguardar carregamento da lista de alunos
        pagina.wait_for_timeout(3000)
        
        # Extrair IDs dos alunos da página atual
        # Método 1: Procurar por links que contenham "/licoes/index/"
        links_licoes = pagina.locator('a[href*="/licoes/index/"]').all()
        
        for link in links_licoes:
            href = link.get_attribute('href')
            if href:
                # Extrair ID do URL: /licoes/index/123456
                match = re.search(r'/licoes/index/(\d+)', href)
                if match:
                    aluno_id = match.group(1)
                    if aluno_id not in lista_ids:
                        lista_ids.append(aluno_id)
        
        # Se não encontrou, usar IDs de exemplo
        if not lista_ids:
            print("⚠️ Não foi possível obter lista automaticamente. Usando IDs de exemplo.")
            lista_ids = ["635849"]  # Adicione mais IDs conhecidos aqui
        
        print(f"🎯 Encontrados {len(lista_ids)} alunos para processar")
        return lista_ids
        
    except Exception as e:
        print(f"❌ Erro ao obter lista de alunos: {e}")
        return ["635849"]  # Fallback

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para usar em requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def main():
    tempo_inicio = time.time()
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        # Configurações do navegador
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
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
            return
        
        # Criar sessão com cookies do navegador
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # Inicializar scraper
        scraper = MusicLessonScraper(session)
        
        # Obter lista de alunos automaticamente
        lista_alunos = obter_lista_alunos(pagina, session)
        print(f"🎯 Total de alunos para processar: {len(lista_alunos)}")
        
        resultado = []
        
        for i, aluno_id in enumerate(lista_alunos, 1):
            if time.time() - tempo_inicio > 1800:  # 30 minutos
                print("⏹️ Tempo limite atingido. Encerrando a coleta.")
                break
            
            print(f"🔍 Processando aluno {i}/{len(lista_alunos)} - ID: {aluno_id}")
            
            dados_aluno = scraper.extrair_dados_aluno_otimizado(aluno_id)
            
            if dados_aluno:
                # Converter para formato de linha para planilha
                linha = [
                    dados_aluno['nome'],
                    dados_aluno['mts_individual'],
                    dados_aluno['mts_grupo'],
                    dados_aluno['msa_individual'],
                    dados_aluno['msa_grupo'],
                    dados_aluno['provas'],
                    dados_aluno['metodo'],
                    dados_aluno['hinario'],
                    dados_aluno['hinario_grupo'],
                    dados_aluno['escalas'],
                    dados_aluno['escalas_grupo'],
                    dados_aluno['id_aluno'],
                    dados_aluno['data_extracao']
                ]
                resultado.append(linha)
            
            # Pausa entre requisições
            time.sleep(2)
        
        print(f"📊 Total de alunos processados: {len(resultado)}")
        
        # Preparar dados para envio
        body = {
            "tipo": "licoes_musicais_otimizado",
            "dados": resultado,
            "headers": [
                "Nome", "MTS Individual", "MTS Grupo", "MSA Individual", "MSA Grupo",
                "Provas", "Método", "Hinário", "Hinário Grupo", "Escalas", "Escalas Grupo",
                "ID Aluno", "Data Extração"
            ],
            "resumo": {
                "total_alunos": len(resultado),
                "tempo_processamento": round(time.time() - tempo_inicio, 2),
                "data_coleta": datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
                "observacoes": "Capturadas apenas as datas dos dias com registro por seção"
            }
        }
        
        # Enviar dados para Apps Script
        try:
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=60)
            print("✅ Dados enviados para Google Sheets!")
            print("Status code:", resposta_post.status_code)
            print("Resposta do Apps Script:", resposta_post.text)
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
            
            # Salvar dados localmente como backup
            import csv
            with open('backup_licoes_musicais.csv', 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow(body["headers"])
                writer.writerows(resultado)
            print("💾 Dados salvos localmente como backup")
        
        print(f"\n⏱️ Tempo total de processamento: {round(time.time() - tempo_inicio, 2)} segundos")
        
        navegador.close()

if __name__ == "__main__":
    main()
        
