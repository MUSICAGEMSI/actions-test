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
                                
                                dados_encontrados.append(
