# script_licoes_musicais.py
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
            print(f"⚠️ Erro ao extrair nome: {e}")
            return "Erro ao extrair nome"

    def processar_data(self, data_texto):
        """Converte texto de data para formato padronizado"""
        if not data_texto or data_texto.strip() == '':
            return ""
        
        try:
            # Limpar e normalizar
            data_limpa = data_texto.strip()
            
            # Tentar diferentes formatos
            for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S']:
                try:
                    data_obj = datetime.strptime(data_limpa.split()[0], fmt)
                    return data_obj.strftime('%d/%m/%Y')
                except ValueError:
                    continue
            
            return data_limpa  # Retorna original se não conseguir converter
        except:
            return data_texto

    def extrair_mts_individual(self, soup):
        """Extrai dados de MTS Individual"""
        dados = []
        try:
            # Procurar tabela com headers específicos de MTS individual
            tabelas = soup.find_all('table')
            for tabela in tabelas:
                header_row = tabela.find('tr')
                if header_row and any(texto in header_row.get_text().upper() for texto in ['MÓDULO', 'LIÇÕES', 'DATA DA LIÇÃO']):
                    linhas = tabela.find_all('tr')[1:]  # Pular header
                    for linha in linhas:
                        colunas = linha.find_all('td')
                        if len(colunas) >= 6:
                            dados.append({
                                'data': self.processar_data(colunas[2].get_text(strip=True)),
                                'modulo': colunas[0].get_text(strip=True),
                                'licoes': colunas[1].get_text(strip=True),
                                'autorizante': colunas[3].get_text(strip=True),
                                'observacoes': colunas[5].get_text(strip=True) if len(colunas) > 5 else ''
                            })
        except Exception as e:
            print(f"⚠️ Erro ao extrair MTS Individual: {e}")
        
        return self.formatar_secao(dados)

    def extrair_mts_grupo(self, soup):
        """Extrai dados de MTS em Grupo"""
        dados = []
        try:
            # Método mais robusto baseado no exemplo real fornecido
            texto_pagina = soup.get_text()
            
            # Procurar por seção "MTS - Aulas em grupo"
            if "MTS - Aulas em grupo" in texto_pagina or "MTS" in texto_pagina:
                # Buscar tabelas após mencionar MTS
                tabelas = soup.find_all('table')
                for tabela in tabelas:
                    linhas = tabela.find_all('tr')
                    
                    # Verificar se é tabela de MTS grupo pelo padrão de dados
                    for linha in linhas:
                        colunas = linha.find_all('td')
                        if len(colunas) >= 6:  # Tabela com data, fases, páginas, lições, claves, observações
                            data_texto = colunas[0].get_text(strip=True)
                            
                            # Verificar se parece com data (formato DD/MM/AAAA)
                            if re.match(r'\d{2}/\d{2}/\d{4}', data_texto):
                                dados.append({
                                    'data': self.processar_data(data_texto),
                                    'fases': colunas[1].get_text(strip=True),
                                    'paginas': colunas[2].get_text(strip=True), 
                                    'licoes': colunas[3].get_text(strip=True),
                                    'claves': colunas[4].get_text(strip=True),
                                    'observacoes': colunas[5].get_text(strip=True),
                                    'autorizante': colunas[6].get_text(strip=True) if len(colunas) > 6 else ''
                                })
                            
        except Exception as e:
            print(f"⚠️ Erro ao extrair MTS Grupo: {e}")
        
        return self.formatar_secao(dados)

    def extrair_msa_individual(self, soup):
        """Extrai dados de MSA Individual"""
        # Similar ao MTS individual, mas procurando por padrões específicos de MSA
        return ""  # Implementar conforme necessário

    def extrair_msa_grupo(self, soup):
        """Extrai dados de MSA em Grupo"""
        dados = []
        try:
            texto_pagina = soup.get_text()
            
            # Método 1: Procurar por padrão específico do exemplo fornecido
            # "Fase(s): de 1.1 até 1.1; Página(s): de 1 até 1; Clave(s): Sol	Apostila...	03/06/2025"
            padrao_msa = re.findall(
                r'Fase\(s\): de ([\d\.]+ até [\d\.]+).*?Página\(s\): de ([\d\s]+ até [\d\s]+).*?(\d{2}/\d{2}/\d{4})', 
                texto_pagina, 
                re.DOTALL
            )
            
            for match in padrao_msa:
                dados.append({
                    'data': self.processar_data(match[2]),
                    'fases': match[0],
                    'paginas': match[1],
                    'observacoes': 'MSA em Grupo'
                })
            
            # Método 2: Se não encontrou pelo padrão, procurar tabela
            if not dados:
                tabelas = soup.find_all('table')
                for tabela in tabelas:
                    # Procurar header que indique MSA
                    header = tabela.find('tr')
                    if header and ('MSA' in header.get_text().upper() or 'Páginas' in header.get_text()):
                        linhas = tabela.find_all('tr')[1:]
                        for linha in linhas:
                            colunas = linha.find_all('td')
                            if len(colunas) >= 3:
                                # Assumir que última coluna é data se parecer com data
                                data_col = colunas[-1].get_text(strip=True)
                                if re.match(r'\d{2}/\d{2}/\d{4}', data_col):
                                    dados.append({
                                        'data': self.processar_data(data_col),
                                        'conteudo': ' | '.join([col.get_text(strip=True) for col in colunas[:-1]]),
                                        'observacoes': 'MSA Grupo'
                                    })
                                    
        except Exception as e:
            print(f"⚠️ Erro ao extrair MSA Grupo: {e}")
        
        return self.formatar_secao(dados)

    def extrair_provas(self, soup):
        """Extrai dados de Provas"""
        dados = []
        try:
            tabelas = soup.find_all('table')
            for tabela in tabelas:
                header_row = tabela.find('tr')
                if header_row and any(texto in header_row.get_text().upper() for texto in ['NOTA', 'DATA DA PROVA']):
                    linhas = tabela.find_all('tr')[1:]
                    for linha in linhas:
                        colunas = linha.find_all('td')
                        if len(colunas) >= 4:
                            dados.append({
                                'data': self.processar_data(colunas[2].get_text(strip=True)),
                                'fases': colunas[0].get_text(strip=True),
                                'nota': colunas[1].get_text(strip=True),
                                'autorizante': colunas[3].get_text(strip=True)
                            })
        except Exception as e:
            print(f"⚠️ Erro ao extrair Provas: {e}")
        
        return self.formatar_secao(dados)

    def extrair_metodo(self, soup):
        """Extrai dados de Método"""
        dados = []
        try:
            tabelas = soup.find_all('table')
            for tabela in tabelas:
                header_row = tabela.find('tr')
                if header_row and 'MÉTODO' in header_row.get_text().upper():
                    linhas = tabela.find_all('tr')[1:]
                    for linha in linhas:
                        colunas = linha.find_all('td')
                        if len(colunas) >= 4:
                            dados.append({
                                'data': self.processar_data(colunas[3].get_text(strip=True)),
                                'metodo': colunas[2].get_text(strip=True),
                                'paginas': colunas[0].get_text(strip=True),
                                'licao': colunas[1].get_text(strip=True),
                                'autorizante': colunas[4].get_text(strip=True) if len(colunas) > 4 else '',
                                'observacoes': colunas[6].get_text(strip=True) if len(colunas) > 6 else ''
                            })
        except Exception as e:
            print(f"⚠️ Erro ao extrair Método: {e}")
        
        return self.formatar_secao(dados)



    def extrair_hinario(self, soup):
        """Extrai dados de Hinário"""
        dados = []
        try:
            tabelas = soup.find_all('table')
            for tabela in tabelas:
                header_row = tabela.find('tr')
                if header_row and any(texto in header_row.get_text().upper() for texto in ['HINO', 'VOZ']):
                    linhas = tabela.find_all('tr')[1:]
                    for linha in linhas:
                        colunas = linha.find_all('td')
                        if len(colunas) >= 4:
                            dados.append({
                                'data': self.processar_data(colunas[2].get_text(strip=True)),
                                'hino': colunas[0].get_text(strip=True),
                                'voz': colunas[1].get_text(strip=True),
                                'autorizante': colunas[3].get_text(strip=True),
                                'observacoes': colunas[6].get_text(strip=True) if len(colunas) > 6 else ''
                            })
        except Exception as e:
            print(f"⚠️ Erro ao extrair Hinário: {e}")
        
        return self.formatar_secao(dados)

    def extrair_hinario_grupo(self, soup):
        """Extrai dados de Hinário em Grupo"""
        dados = []
        try:
            texto_pagina = soup.get_text()
            if "Hinos - Aulas em grupo" in texto_pagina:
                # Buscar padrões específicos
                padrao = re.findall(r'Hino (\d+).*?(\d{2}/\d{2}/\d{4})', texto_pagina)
                for match in padrao:
                    dados.append({
                        'data': self.processar_data(match[1]),
                        'hino': f"Hino {match[0]}",
                        'observacoes': ''
                    })
        except Exception as e:
            print(f"⚠️ Erro ao extrair Hinário Grupo: {e}")
        
        return self.formatar_secao(dados)

    def extrair_escalas(self, soup):
        """Extrai dados de Escalas"""
        dados = []
        try:
            tabelas = soup.find_all('table')
            for tabela in tabelas:
                header_row = tabela.find('tr')
                if header_row and 'ESCALA' in header_row.get_text().upper():
                    linhas = tabela.find_all('tr')[1:]
                    for linha in linhas:
                        colunas = linha.find_all('td')
                        if len(colunas) >= 3:
                            dados.append({
                                'data': self.processar_data(colunas[1].get_text(strip=True)),
                                'escala': colunas[0].get_text(strip=True),
                                'autorizante': colunas[2].get_text(strip=True),
                                'observacoes': colunas[5].get_text(strip=True) if len(colunas) > 5 else ''
                            })
        except Exception as e:
            print(f"⚠️ Erro ao extrair Escalas: {e}")
        
        return self.formatar_secao(dados)

    def extrair_escalas_grupo(self, soup):
        """Extrai dados de Escalas em Grupo"""
        dados = []
        try:
            texto_pagina = soup.get_text()
            if "Escalas - Aulas em grupo" in texto_pagina:
                # Implementar lógica específica se necessário
                pass
        except Exception as e:
            print(f"⚠️ Erro ao extrair Escalas Grupo: {e}")
        
        return self.formatar_secao(dados)

    def formatar_secao(self, dados_lista):
        """Formata uma seção de dados usando ';' como separador"""
        if not dados_lista:
            return ""
        
        entradas_formatadas = []
        for entrada in dados_lista:
            partes = []
            for chave, valor in entrada.items():
                if valor and str(valor).strip():
                    partes.append(f"{chave}: {valor}")
            
            if partes:
                entradas_formatadas.append(" | ".join(partes))
        
        return "; ".join(entradas_formatadas)

    def extrair_dados_aluno_robusto(self, aluno_id):
        """
        Versão mais robusta que tenta múltiplos métodos para extrair dados
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
            
            # Extrair dados com múltiplas tentativas para cada seção
            dados = {
                'nome': nome_aluno,
                'mts_individual': self.extrair_secao_robusta(soup, 'MTS', 'individual'),
                'mts_grupo': self.extrair_secao_robusta(soup, 'MTS', 'grupo'),
                'msa_individual': self.extrair_secao_robusta(soup, 'MSA', 'individual'),
                'msa_grupo': self.extrair_secao_robusta(soup, 'MSA', 'grupo'),
                'provas': self.extrair_secao_robusta(soup, 'PROVAS', ''),
                'metodo': self.extrair_secao_robusta(soup, 'MÉTODO', ''),
                'hinario': self.extrair_secao_robusta(soup, 'HINO', ''),
                'hinario_grupo': self.extrair_secao_robusta(soup, 'HINOS', 'grupo'),
                'escalas': self.extrair_secao_robusta(soup, 'ESCALA', ''),
                'escalas_grupo': self.extrair_secao_robusta(soup, 'ESCALAS', 'grupo'),
                'id_aluno': aluno_id,
                'data_extracao': datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            }
            
            # Mostrar resumo do que foi extraído
            resumo = []
            for campo, valor in dados.items():
                if campo not in ['nome', 'id_aluno', 'data_extracao'] and valor:
                    resumo.append(campo)
            
            if resumo:
                print(f"   ✅ Extraído: {', '.join(resumo)}")
            else:
                print(f"   ⚠️ Nenhum dado encontrado")
            
            return dados
            
        except Exception as e:
            print(f"   ❌ Erro: {e}")
            return None
    
    def extrair_secao_robusta(self, soup, tipo_secao, subtipo):
        """
        Método robusto que tenta extrair dados de uma seção específica
        usando múltiplas estratégias
        """
        dados_encontrados = []
        
        try:
            # Estratégia 1: Procurar por texto indicativo da seção
            texto_completo = soup.get_text()
            
            # Estratégia 2: Procurar tabelas relevantes
            tabelas = soup.find_all('table')
            
            for tabela in tabelas:
                # Verificar se a tabela pertence à seção desejada
                tabela_texto = tabela.get_text().upper()
                
                if tipo_secao in tabela_texto:
                    if subtipo and 'GRUPO' in subtipo.upper() and 'GRUPO' not in tabela_texto:
                        continue
                    if subtipo and 'INDIVIDUAL' in subtipo.upper() and 'GRUPO' in tabela_texto:
                        continue
                    
                    # Extrair dados da tabela
                    linhas = tabela.find_all('tr')
                    
                    for i, linha in enumerate(linhas):
                        if i == 0:  # Pular header
                            continue
                            
                        colunas = linha.find_all(['td', 'th'])
                        if len(colunas) >= 3:  # Precisa ter pelo menos 3 colunas
                            
                            # Procurar por coluna que contenha data
                            data_encontrada = None
                            for col in colunas:
                                col_texto = col.get_text(strip=True)
                                if re.match(r'\d{1,2}/\d{1,2}/\d{4}', col_texto):
                                    data_encontrada = self.processar_data(col_texto)
                                    break
                            
                            if data_encontrada:
                                # Construir registro com todas as informações da linha
                                registro = {'data': data_encontrada}
                                
                                for j, col in enumerate(colunas):
                                    col_texto = col.get_text(strip=True)
                                    if col_texto and not re.match(r'\d{1,2}/\d{1,2}/\d{4}', col_texto):
                                        registro[f'col_{j}'] = col_texto[:100]  # Limitar tamanho
                                
                                dados_encontrados.append(registro)
            
            # Estratégia 3: Procurar por padrões específicos no texto
            if tipo_secao == 'MSA' and 'grupo' in subtipo.lower():
                # Padrão específico do MSA: "Fase(s): de X até Y"
                padrao = re.findall(
                    r'Fase\(s\): de ([\d\.]+) até ([\d\.]+).*?(\d{2}/\d{2}/\d{4})',
                    texto_completo,
                    re.DOTALL
                )
                
                for match in padrao:
                    dados_encontrados.append({
                        'data': self.processar_data(match[2]),
                        'fases': f"{match[0]} até {match[1]}",
                        'tipo': 'MSA Grupo'
                    })
            
        except Exception as e:
            print(f"   ⚠️ Erro ao extrair {tipo_secao} {subtipo}: {e}")
        
        return self.formatar_secao(dados_encontrados)

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
        
        # Método 2: Se não encontrou links, procurar em inputs ou outros elementos
        if not lista_ids:
            # Procurar por inputs ou elementos que contenham IDs
            elementos_com_id = pagina.locator('[value*="6"], [data-id*="6"]').all()
            for elemento in elementos_com_id:
                value = elemento.get_attribute('value') or elemento.get_attribute('data-id')
                if value and value.isdigit() and len(value) >= 6:
                    if value not in lista_ids:
                        lista_ids.append(value)
        
        # Método 3: Usar requests para pegar lista via AJAX se necessário
        if not lista_ids:
            try:
                headers = {
                    'X-Requested-With': 'XMLHttpRequest',
                    'Referer': 'https://musical.congregacao.org.br/painel',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                # Tentar endpoint que pode retornar lista de alunos
                endpoints_possveis = [
                    'https://musical.congregacao.org.br/alunos/lista',
                    'https://musical.congregacao.org.br/matriculas/lista_alunos',
                    'https://musical.congregacao.org.br/licoes/lista_alunos'
                ]
                
                for endpoint in endpoints_possveis:
                    try:
                        resp = session.get(endpoint, headers=headers, timeout=10)
                        if resp.status_code == 200:
                            # Procurar IDs na resposta
                            ids_encontrados = re.findall(r'"id["\s]*:[\s]*["\']?(\d{6,})["\']?', resp.text)
                            for id_encontrado in ids_encontrados:
                                if id_encontrado not in lista_ids:
                                    lista_ids.append(id_encontrado)
                            
                            if lista_ids:
                                break
                    except:
                        continue
                        
            except Exception as e:
                print(f"⚠️ Erro ao buscar via AJAX: {e}")
        
        # Se ainda não encontrou, usar lista de exemplo
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
            
            dados_aluno = scraper.extrair_dados_aluno_robusto(aluno_id)
            
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
            "tipo": "licoes_musicais",
            "dados": resultado,
            "headers": [
                "Nome", "MTS Individual", "MTS Grupo", "MSA Individual", "MSA Grupo",
                "Provas", "Método", "Hinário", "Hinário Grupo", "Escalas", "Escalas Grupo",
                "ID Aluno", "Data Extração"
            ],
            "resumo": {
                "total_alunos": len(resultado),
                "tempo_processamento": round(time.time() - tempo_inicio, 2),
                "data_coleta": datetime.now().strftime('%d/%m/%Y %H:%M:%S')
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
        
