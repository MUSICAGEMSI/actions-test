from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import os
import requests
import time
import json
import concurrent.futures
from typing import List, Dict, Set
import re
from datetime import datetime
from collections import Counter

# ========================================
# CONFIGURAÇÕES
# ========================================

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzl1l143sg2_S5a6bOQy6WqWATMDZpSglIyKUp3OVZtycuHXQmGjisOpzffHTW5TvyK/exec'

NUM_THREADS = 25  # Threads para coleta paralela

print(f"🎓 COLETOR DE LIÇÕES - ALUNOS DE HORTOLÂNDIA")
print(f"🧵 Threads: {NUM_THREADS}")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

# ========================================
# BUSCAR LISTA DE ALUNOS DO GOOGLE SHEETS
# ========================================

def buscar_alunos_hortolandia() -> List[Dict]:
    """
    Busca a lista de alunos de Hortolândia do Google Sheets
    """
    print("📥 Buscando lista de alunos do Google Sheets...")
    
    try:
        params = {"acao": "listar_ids_alunos"}
        response = requests.get(URL_APPS_SCRIPT, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('sucesso'):
                alunos = data.get('alunos', [])
                print(f"✅ {len(alunos)} alunos carregados")
                return alunos
            else:
                print(f"⚠️ Erro na resposta: {data.get('erro')}")
                return []
        else:
            print(f"⚠️ Erro HTTP: Status {response.status_code}")
            return []
            
    except Exception as e:
        print(f"❌ Erro ao buscar alunos: {e}")
        return []

# ========================================
# EXTRAÇÃO DE DADOS - MTS INDIVIDUAL
# ========================================

def extrair_mts_individual(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    """
    Extrai lições MTS individuais
    """
    dados = []
    
    try:
        aba_mts = soup.find('div', {'id': 'mts'})
        if not aba_mts:
            return dados
        
        # Primeira tabela - MTS Individual
        tabela = aba_mts.find('table', {'id': 'datatable1'})
        if not tabela:
            return dados
        
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        linhas = tbody.find_all('tr')
        
        for linha in linhas:
            colunas = linha.find_all('td')
            if len(colunas) >= 7:
                dados.append([
                    id_aluno,
                    nome_aluno,
                    colunas[0].get_text(strip=True),  # Módulo
                    colunas[1].get_text(strip=True),  # Lições
                    colunas[2].get_text(strip=True),  # Data da Lição
                    colunas[3].get_text(strip=True),  # Autorizante
                    colunas[4].get_text(strip=True),  # Data Cadastro
                    colunas[5].get_text(strip=True),  # Data Alteração
                    colunas[6].get_text(strip=True)   # Observações
                ])
    
    except Exception as e:
        print(f"⚠️ Erro ao extrair MTS Individual do aluno {id_aluno}: {e}")
    
    return dados

# ========================================
# EXTRAÇÃO DE DADOS - MTS GRUPO
# ========================================

def extrair_mts_grupo(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    """
    Extrai aulas MTS em grupo
    """
    dados = []
    
    try:
        aba_mts = soup.find('div', {'id': 'mts'})
        if not aba_mts:
            return dados
        
        # Segunda tabela - MTS Grupo
        tabela = aba_mts.find('table', {'id': 'datatable_mts_grupo'})
        if not tabela:
            return dados
        
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        linhas = tbody.find_all('tr')
        
        for linha in linhas:
            colunas = linha.find_all('td')
            if len(colunas) >= 3:
                dados.append([
                    id_aluno,
                    nome_aluno,
                    colunas[0].get_text(strip=True),  # Páginas
                    colunas[1].get_text(strip=True),  # Observações
                    colunas[2].get_text(strip=True)   # Data da Lição
                ])
    
    except Exception as e:
        print(f"⚠️ Erro ao extrair MTS Grupo do aluno {id_aluno}: {e}")
    
    return dados

# ========================================
# EXTRAÇÃO DE DADOS - MSA INDIVIDUAL
# ========================================

def extrair_msa_individual(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    """
    Extrai lições MSA individuais
    """
    dados = []
    
    try:
        aba_msa = soup.find('div', {'id': 'msa'})
        if not aba_msa:
            return dados
        
        # Primeira tabela - MSA Individual
        tabela = aba_msa.find('table', {'id': 'datatable1'})
        if not tabela:
            return dados
        
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        linhas = tbody.find_all('tr')
        
        for linha in linhas:
            colunas = linha.find_all('td')
            if len(colunas) >= 7:
                dados.append([
                    id_aluno,
                    nome_aluno,
                    colunas[0].get_text(strip=True),  # Data da Lição
                    colunas[1].get_text(strip=True),  # Fases
                    colunas[2].get_text(strip=True),  # Páginas
                    colunas[3].get_text(strip=True),  # Lições
                    colunas[4].get_text(strip=True),  # Claves
                    colunas[5].get_text(strip=True),  # Observações
                    colunas[6].get_text(strip=True)   # Autorizante
                ])
    
    except Exception as e:
        print(f"⚠️ Erro ao extrair MSA Individual do aluno {id_aluno}: {e}")
    
    return dados

# ========================================
# EXTRAÇÃO DE DADOS - MSA GRUPO
# ========================================

def extrair_msa_grupo(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    """
    Extrai aulas MSA em grupo
    """
    dados = []
    
    try:
        aba_msa = soup.find('div', {'id': 'msa'})
        if not aba_msa:
            return dados
        
        # Segunda tabela - MSA Grupo
        tabela = aba_msa.find('table', {'id': 'datatable_mts_grupo'})  # Mesmo ID no HTML
        if not tabela:
            return dados
        
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        linhas = tbody.find_all('tr')
        
        for linha in linhas:
            colunas = linha.find_all('td')
            if len(colunas) >= 3:
                # Extrair fases e páginas do texto
                texto_fases_paginas = colunas[0].get_text(strip=True)
                
                # Regex para extrair fases (de X até Y)
                fases_match = re.search(r'de\s+([\d.]+)\s+até\s+([\d.]+)', texto_fases_paginas)
                fases_de = fases_match.group(1) if fases_match else ""
                fases_ate = fases_match.group(2) if fases_match else ""
                
                # Regex para extrair páginas (de X até Y)
                paginas_match = re.search(r'de\s+(\d+)\s+até\s+(\d+)', texto_fases_paginas)
                paginas_de = paginas_match.group(1) if paginas_match else ""
                paginas_ate = paginas_match.group(2) if paginas_match else ""
                
                dados.append([
                    id_aluno,
                    nome_aluno,
                    fases_de,
                    fases_ate,
                    paginas_de,
                    paginas_ate,
                    colunas[1].get_text(strip=True),  # Observações
                    colunas[2].get_text(strip=True)   # Data da Lição
                ])
    
    except Exception as e:
        print(f"⚠️ Erro ao extrair MSA Grupo do aluno {id_aluno}: {e}")
    
    return dados

# ========================================
# EXTRAÇÃO DE DADOS - PROVAS
# ========================================

def extrair_provas(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    """
    Extrai provas realizadas
    """
    dados = []
    
    try:
        aba_provas = soup.find('div', {'id': 'provas'})
        if not aba_provas:
            return dados
        
        tabela = aba_provas.find('table', {'id': 'datatable2'})
        if not tabela:
            return dados
        
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        linhas = tbody.find_all('tr')
        
        for linha in linhas:
            colunas = linha.find_all('td')
            if len(colunas) >= 5:
                dados.append([
                    id_aluno,
                    nome_aluno,
                    colunas[0].get_text(strip=True),  # Módulo/Fases
                    colunas[1].get_text(strip=True),  # Nota
                    colunas[2].get_text(strip=True),  # Data da Prova
                    colunas[3].get_text(strip=True),  # Autorizante
                    colunas[4].get_text(strip=True)   # Data de Cadastro
                ])
    
    except Exception as e:
        print(f"⚠️ Erro ao extrair Provas do aluno {id_aluno}: {e}")
    
    return dados

# ========================================
# EXTRAÇÃO DE DADOS - HINÁRIO INDIVIDUAL
# ========================================

def extrair_hinario_individual(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    """
    Extrai hinos individuais
    """
    dados = []
    
    try:
        aba_hinario = soup.find('div', {'id': 'hinario'})
        if not aba_hinario:
            return dados
        
        # Primeira tabela - Hinário Individual
        tabela = aba_hinario.find('table', {'id': 'datatable4'})
        if not tabela:
            return dados
        
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        linhas = tbody.find_all('tr')
        
        for linha in linhas:
            colunas = linha.find_all('td')
            if len(colunas) >= 7:
                dados.append([
                    id_aluno,
                    nome_aluno,
                    colunas[0].get_text(strip=True),  # Hino
                    colunas[1].get_text(strip=True),  # Voz
                    colunas[2].get_text(strip=True),  # Data da aula
                    colunas[3].get_text(strip=True),  # Autorizante
                    colunas[4].get_text(strip=True),  # Data Cadastro
                    colunas[5].get_text(strip=True),  # Data Alteração
                    colunas[6].get_text(strip=True)   # Observações
                ])
    
    except Exception as e:
        print(f"⚠️ Erro ao extrair Hinário Individual do aluno {id_aluno}: {e}")
    
    return dados

# ========================================
# EXTRAÇÃO DE DADOS - HINÁRIO GRUPO
# ========================================

def extrair_hinario_grupo(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    """
    Extrai hinos em grupo
    """
    dados = []
    
    try:
        aba_hinario = soup.find('div', {'id': 'hinario'})
        if not aba_hinario:
            return dados
        
        # Segunda tabela - Hinário Grupo
        tabela = aba_hinario.find('table', {'id': 'datatable_hinos_grupo'})
        if not tabela:
            return dados
        
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        linhas = tbody.find_all('tr')
        
        for linha in linhas:
            colunas = linha.find_all('td')
            if len(colunas) >= 3:
                dados.append([
                    id_aluno,
                    nome_aluno,
                    colunas[0].get_text(strip=True),  # Hinos
                    colunas[1].get_text(strip=True),  # Observações
                    colunas[2].get_text(strip=True)   # Data da Lição
                ])
    
    except Exception as e:
        print(f"⚠️ Erro ao extrair Hinário Grupo do aluno {id_aluno}: {e}")
    
    return dados

# ========================================
# EXTRAÇÃO DE DADOS - MÉTODOS
# ========================================

def extrair_metodos(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    """
    Extrai métodos estudados
    """
    dados = []
    
    try:
        aba_metodos = soup.find('div', {'id': 'metodos'})
        if not aba_metodos:
            return dados
        
        tabela = aba_metodos.find('table', {'id': 'datatable3'})
        if not tabela:
            return dados
        
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        linhas = tbody.find_all('tr')
        
        for linha in linhas:
            colunas = linha.find_all('td')
            if len(colunas) >= 7:
                dados.append([
                    id_aluno,
                    nome_aluno,
                    colunas[0].get_text(strip=True),  # Páginas
                    colunas[1].get_text(strip=True),  # Lição
                    colunas[2].get_text(strip=True),  # Método
                    colunas[3].get_text(strip=True),  # Data da Lição
                    colunas[4].get_text(strip=True),  # Autorizante
                    colunas[5].get_text(strip=True),  # Data Cadastro
                    colunas[6].get_text(strip=True)   # Observações
                ])
    
    except Exception as e:
        print(f"⚠️ Erro ao extrair Métodos do aluno {id_aluno}: {e}")
    
    return dados

# ========================================
# EXTRAÇÃO DE DADOS - ESCALAS INDIVIDUAL
# ========================================

def extrair_escalas_individual(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    """
    Extrai escalas individuais
    """
    dados = []
    
    try:
        aba_escalas = soup.find('div', {'id': 'escalas'})
        if not aba_escalas:
            return dados
        
        # Primeira tabela - Escalas Individual
        tabela = aba_escalas.find('table', {'id': 'datatable4'})
        if not tabela:
            return dados
        
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        linhas = tbody.find_all('tr')
        
        for linha in linhas:
            colunas = linha.find_all('td')
            if len(colunas) >= 6:
                dados.append([
                    id_aluno,
                    nome_aluno,
                    colunas[0].get_text(strip=True),  # Escala
                    colunas[1].get_text(strip=True),  # Data
                    colunas[2].get_text(strip=True),  # Autorizante
                    colunas[3].get_text(strip=True),  # Data Cadastro
                    colunas[4].get_text(strip=True),  # Data Alteração
                    colunas[5].get_text(strip=True)   # Observações
                ])
    
    except Exception as e:
        print(f"⚠️ Erro ao extrair Escalas Individual do aluno {id_aluno}: {e}")
    
    return dados

# ========================================
# EXTRAÇÃO DE DADOS - ESCALAS GRUPO
# ========================================

def extrair_escalas_grupo(soup, id_aluno: int, nome_aluno: str) -> List[List]:
    """
    Extrai escalas em grupo
    """
    dados = []
    
    try:
        aba_escalas = soup.find('div', {'id': 'escalas'})
        if not aba_escalas:
            return dados
        
        # Segunda tabela - Escalas Grupo
        tabela = aba_escalas.find('table', {'id': 'datatable_escalas_grupo'})
        if not tabela:
            return dados
        
        tbody = tabela.find('tbody')
        if not tbody:
            return dados
        
        linhas = tbody.find_all('tr')
        
        for linha in linhas:
            colunas = linha.find_all('td')
            if len(colunas) >= 3:
                dados.append([
                    id_aluno,
                    nome_aluno,
                    colunas[0].get_text(strip=True),  # Escala
                    colunas[1].get_text(strip=True),  # Observações
                    colunas[2].get_text(strip=True)   # Data da Lição
                ])
    
    except Exception as e:
        print(f"⚠️ Erro ao extrair Escalas Grupo do aluno {id_aluno}: {e}")
    
    return dados

# ========================================
# CLASSE COLETORA
# ========================================

class ColetorLicoesAlunos:
    def __init__(self, session, thread_id: int):
        self.session = session
        self.thread_id = thread_id
        self.requisicoes_feitas = 0
        self.alunos_processados = 0
        
        # Armazenar dados coletados
        self.mts_individual = []
        self.mts_grupo = []
        self.msa_individual = []
        self.msa_grupo = []
        self.provas = []
        self.hinario_individual = []
        self.hinario_grupo = []
        self.metodos = []
        self.escalas_individual = []
        self.escalas_grupo = []
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
    
    def coletar_dados_aluno(self, aluno: Dict) -> Dict:
        """
        Coleta todos os dados de um aluno específico
        """
        id_aluno = aluno['id_aluno']
        nome_aluno = aluno['nome']
        
        try:
            url = f"https://musical.congregacao.org.br/licoes/index/{id_aluno}"
            
            resp = self.session.get(url, headers=self.headers, timeout=10)
            self.requisicoes_feitas += 1
            
            if resp.status_code == 200:
                html = resp.text
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extrair dados de todas as abas
                mts_ind = extrair_mts_individual(soup, id_aluno, nome_aluno)
                mts_grp = extrair_mts_grupo(soup, id_aluno, nome_aluno)
                msa_ind = extrair_msa_individual(soup, id_aluno, nome_aluno)
                msa_grp = extrair_msa_grupo(soup, id_aluno, nome_aluno)
                provas = extrair_provas(soup, id_aluno, nome_aluno)
                hin_ind = extrair_hinario_individual(soup, id_aluno, nome_aluno)
                hin_grp = extrair_hinario_grupo(soup, id_aluno, nome_aluno)
                metodos = extrair_metodos(soup, id_aluno, nome_aluno)
                esc_ind = extrair_escalas_individual(soup, id_aluno, nome_aluno)
                esc_grp = extrair_escalas_grupo(soup, id_aluno, nome_aluno)
                
                # Armazenar
                self.mts_individual.extend(mts_ind)
                self.mts_grupo.extend(mts_grp)
                self.msa_individual.extend(msa_ind)
                self.msa_grupo.extend(msa_grp)
                self.provas.extend(provas)
                self.hinario_individual.extend(hin_ind)
                self.hinario_grupo.extend(hin_grp)
                self.metodos.extend(metodos)
                self.escalas_individual.extend(esc_ind)
                self.escalas_grupo.extend(esc_grp)
                
                self.alunos_processados += 1
                
                # Calcular totais
                total = (len(mts_ind) + len(mts_grp) + len(msa_ind) + len(msa_grp) + 
                        len(provas) + len(hin_ind) + len(hin_grp) + len(metodos) + 
                        len(esc_ind) + len(esc_grp))
                
                print(f"✅ T{self.thread_id}: Aluno {id_aluno} | {nome_aluno[:30]} | {total} registros")
                
                return {
                    'id_aluno': id_aluno,
                    'nome': nome_aluno,
                    'totais': {
                        'mts_ind': len(mts_ind),
                        'mts_grp': len(mts_grp),
                        'msa_ind': len(msa_ind),
                        'msa_grp': len(msa_grp),
                        'provas': len(provas),
                        'hinario_ind': len(hin_ind),
                        'hinario_grp': len(hin_grp),
                        'metodos': len(metodos),
                        'escalas_ind': len(esc_ind),
                        'escalas_grp': len(esc_grp)
                    }
                }
            
            # Pausa entre requisições
            time.sleep(0.05)
            
        except Exception as e:
            if "timeout" in str(e).lower():
                print(f"⏱️ T{self.thread_id}: Timeout no aluno {id_aluno}")
            else:
                print(f"⚠️ T{self.thread_id}: Erro no aluno {id_aluno} - {e}")
            
            return None
    
    def coletar_batch_alunos(self, alunos_batch: List[Dict]) -> List[Dict]:
        """
        Processa um lote de alunos
        """
        resultados = []
        
        for aluno in alunos_batch:
            resultado = self.coletar_dados_aluno(aluno)
            if resultado:
                resultados.append(resultado)
            
            # Log de progresso
            if self.requisicoes_feitas % 50 == 0:
                print(f"📊 T{self.thread_id}: {self.alunos_processados} alunos processados | {self.requisicoes_feitas} requisições")
        
        return resultados

# ========================================
# COLETA PARALELA
# ========================================

def executar_coleta_paralela(session, alunos: List[Dict], num_threads: int):
    """
    Executa coleta paralela de dados dos alunos
    """
    total_alunos = len(alunos)
    alunos_per_thread = total_alunos // num_threads
    
    print(f"📈 Dividindo {total_alunos} alunos em {num_threads} threads ({alunos_per_thread} alunos/thread)")
    
    # Dividir alunos por thread
    thread_batches = []
    for i in range(num_threads):
        inicio = i * alunos_per_thread
        fim = inicio + alunos_per_thread
        
        if i == num_threads - 1:
            fim = total_alunos
        
        thread_batches.append(alunos[inicio:fim])
    
    # Dados consolidados
    todos_dados = {
        'mts_individual': [],
        'mts_grupo': [],
        'msa_individual': [],
        'msa_grupo': [],
        'provas': [],
        'hinario_individual': [],
        'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [],
        'escalas_grupo': [],
        'resumos': []
    }
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        coletores = [ColetorLicoesAlunos(session, i) for i in range(num_threads)]
        
        futures = []
        for i, batch in enumerate(thread_batches):
            future = executor.submit(coletores[i].coletar_batch_alunos, batch)
            futures.append((future, i))
        
        # Aguardar conclusão
        for future, thread_id in futures:
            try:
                resumos = future.result(timeout=3600)
                coletor = coletores[thread_id]
                
                # Consolidar dados
                todos_dados['mts_individual'].extend(coletor.mts_individual)
                todos_dados['mts_grupo'].extend(coletor.mts_grupo)
                todos_dados['msa_individual'].extend(coletor.msa_individual)
                todos_dados['msa_grupo'].extend(coletor.msa_grupo)
                todos_dados['provas'].extend(coletor.provas)
                todos_dados['hinario_individual'].extend(coletor.hinario_individual)
                todos_dados['hinario_grupo'].extend(coletor.hinario_grupo)
                todos_dados['metodos'].extend(coletor.metodos)
                todos_dados['escalas_individual'].extend(coletor.escalas_individual)
                todos_dados['escalas_grupo'].extend(coletor.escalas_grupo)
                todos_dados['resumos'].extend(resumos)
                
                print(f"✅ Thread {thread_id}: {coletor.alunos_processados} alunos | {coletor.requisicoes_feitas} requisições")
                
            except Exception as e:
                print(f"❌ Thread {thread_id}: Erro - {e}")
    
    return todos_dados

# ========================================
# GERAR RESUMO POR ALUNO
# ========================================

def gerar_resumo_alunos(alunos: List[Dict], todos_dados: Dict) -> List[List]:
    """
    Gera tabela resumo com totais por aluno
    """
    resumo = []
    
    for aluno in alunos:
        id_aluno = aluno['id_aluno']
        nome = aluno['nome']
        id_igreja = aluno['id_igreja']
        
        # Contar totais
        total_mts_ind = sum(1 for x in todos_dados['mts_individual'] if x[0] == id_aluno)
        total_mts_grp = sum(1 for x in todos_dados['mts_grupo'] if x[0] == id_aluno)
        total_msa_ind = sum(1 for x in todos_dados['msa_individual'] if x[0] == id_aluno)
        total_msa_grp = sum(1 for x in todos_dados['msa_grupo'] if x[0] == id_aluno)
        total_provas = sum(1 for x in todos_dados['provas'] if x[0] == id_aluno)
        total_hin_ind = sum(1 for x in todos_dados['hinario_individual'] if x[0] == id_aluno)
        total_hin_grp = sum(1 for x in todos_dados['hinario_grupo'] if x[0] == id_aluno)
        total_metodos = sum(1 for x in todos_dados['metodos'] if x[0] == id_aluno)
        total_esc_ind = sum(1 for x in todos_dados['escalas_individual'] if x[0] == id_aluno)
        total_esc_grp = sum(1 for x in todos_dados['escalas_grupo'] if x[0] == id_aluno)
        
        # Calcular média de provas
        provas_aluno = [float(x[3]) for x in todos_dados['provas'] if x[0] == id_aluno and x[3].replace('.','').isdigit()]
        media_provas = round(sum(provas_aluno) / len(provas_aluno), 2) if provas_aluno else 0
        
        # Última atividade (pegar data mais recente de qualquer registro)
        ultima_atividade = "N/A"
        
        resumo.append([
            id_aluno,
            nome,
            id_igreja,
            total_mts_ind,
            total_mts_grp,
            total_msa_ind,
            total_msa_grp,
            total_provas,
            media_provas,
            total_hin_ind,
            total_hin_grp,
            total_metodos,
            total_esc_ind,
            total_esc_grp,
            ultima_atividade,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ])
    
    return resumo

# ========================================
# ENVIAR DADOS PARA GOOGLE SHEETS
# ========================================

def enviar_dados_para_sheets(alunos: List[Dict], todos_dados: Dict, tempo_execucao: float):
    """
    Envia todos os dados coletados para Google Sheets
    """
    print(f"\n📤 Enviando dados para Google Sheets...")
    
    # Gerar resumo
    resumo = gerar_resumo_alunos(alunos, todos_dados)
    
    payload = {
        "tipo": "licoes_alunos",
        "resumo": resumo,
        "mts_individual": todos_dados['mts_individual'],
        "mts_grupo": todos_dados['mts_grupo'],
        "msa_individual": todos_dados['msa_individual'],
        "msa_grupo": todos_dados['msa_grupo'],
        "provas": todos_dados['provas'],
        "hinario_individual": todos_dados['hinario_individual'],
        "hinario_grupo": todos_dados['hinario_grupo'],
        "metodos": todos_dados['metodos'],
        "escalas_individual": todos_dados['escalas_individual'],
        "escalas_grupo": todos_dados['escalas_grupo'],
        "metadata": {
            "total_alunos_processados": len(alunos),
            "tempo_execucao_min": round(tempo_execucao/60, 2),
            "threads_utilizadas": NUM_THREADS,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_registros_mts_ind": len(todos_dados['mts_individual']),
            "total_registros_mts_grp": len(todos_dados['mts_grupo']),
            "total_registros_msa_ind": len(todos_dados['msa_individual']),
            "total_registros_msa_grp": len(todos_dados['msa_grupo']),
            "total_registros_provas": len(todos_dados['provas']),
            "total_registros_hinario_ind": len(todos_dados['hinario_individual']),
            "total_registros_hinario_grp": len(todos_dados['hinario_grupo']),
            "total_registros_metodos": len(todos_dados['metodos']),
            "total_registros_escalas_ind": len(todos_dados['escalas_individual']),
            "total_registros_escalas_grp": len(todos_dados['escalas_grupo'])
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=300)
        
        if response.status_code == 200:
            print("✅ Dados enviados com sucesso para Google Sheets!")
            print(f"📄 Resposta: {response.text[:150]}")
            return True
        else:
            print(f"⚠️ Status HTTP: {response.status_code}")
            print(f"📄 Resposta: {response.text[:200]}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Timeout ao enviar para Google Sheets (>300s)")
        return False
    except Exception as e:
        print(f"❌ Erro ao enviar para Google Sheets: {e}")
        return False

# ========================================
# SALVAR BACKUP LOCAL
# ========================================

def salvar_backup_local(alunos: List[Dict], todos_dados: Dict, nome_arquivo: str = "licoes_backup.json"):
    """
    Salva backup dos dados em arquivo JSON local
    """
    try:
        backup = {
            "alunos": alunos,
            "dados": todos_dados,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        with open(nome_arquivo, 'w', encoding='utf-8') as f:
            json.dump(backup, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Backup salvo em: {nome_arquivo}")
    except Exception as e:
        print(f"❌ Erro ao salvar backup: {e}")

# ========================================
# EXTRAIR COOKIES
# ========================================

def extrair_cookies_playwright(pagina):
    """
    Extrai cookies do Playwright para requests
    """
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

# ========================================
# FUNÇÃO PRINCIPAL
# ========================================

def main():
    tempo_inicio = time.time()
    
    # Buscar lista de alunos
    alunos = buscar_alunos_hortolandia()
    
    if not alunos:
        print("❌ Nenhum aluno encontrado. Abortando...")
        return
    
    print(f"\n🎓 {len(alunos)} alunos para processar")
    print("\n🔐 Realizando login...")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        try:
            pagina.goto(URL_INICIAL)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado com sucesso!")
            
        except Exception as e:
            print(f"❌ Erro no login: {e}")
            navegador.close()
            return
        
        # Extrair cookies
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
    
    # Criar sessão requests
    session = requests.Session()
    session.cookies.update(cookies_dict)
    
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=NUM_THREADS + 10,
        pool_maxsize=NUM_THREADS + 10,
        max_retries=2
    )
    session.mount('https://', adapter)
    
    print("\n📚 Iniciando coleta de lições...")
    
    # Executar coleta paralela
    todos_dados = executar_coleta_paralela(session, alunos, NUM_THREADS)
    
    tempo_total = time.time() - tempo_inicio
    
    # Estatísticas
    print(f"\n{'='*70}")
    print(f"🏁 COLETA DE LIÇÕES FINALIZADA!")
    print(f"{'='*70}")
    print(f"🎓 Alunos processados: {len(alunos)}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    print(f"\n📊 DADOS COLETADOS:")
    print(f"   📗 MTS Individual: {len(todos_dados['mts_individual'])} registros")
    print(f"   📗 MTS Grupo: {print(f"   📗 MTS Individual: {len(todos_dados['mts_individual'])} registros")
    print(f"   📗 MTS Grupo: {len(todos_dados['mts_grupo'])} registros")
    print(f"   📘 MSA Individual: {len(todos_dados['msa_individual'])} registros")
    print(f"   📘 MSA Grupo: {len(todos_dados['msa_grupo'])} registros")
    print(f"   📝 Provas: {len(todos_dados['provas'])} registros")
    print(f"   🎵 Hinário Individual: {len(todos_dados['hinario_individual'])} registros")
    print(f"   🎵 Hinário Grupo: {len(todos_dados['hinario_grupo'])} registros")
    print(f"   📖 Métodos: {len(todos_dados['metodos'])} registros")
    print(f"   🎼 Escalas Individual: {len(todos_dados['escalas_individual'])} registros")
    print(f"   🎼 Escalas Grupo: {len(todos_dados['escalas_grupo'])} registros")
    
    total_registros = (
        len(todos_dados['mts_individual']) + len(todos_dados['mts_grupo']) +
        len(todos_dados['msa_individual']) + len(todos_dados['msa_grupo']) +
        len(todos_dados['provas']) + len(todos_dados['hinario_individual']) +
        len(todos_dados['hinario_grupo']) + len(todos_dados['metodos']) +
        len(todos_dados['escalas_individual']) + len(todos_dados['escalas_grupo'])
    )
    
    print(f"\n📦 TOTAL DE REGISTROS: {total_registros}")
    print(f"⚡ Velocidade: {len(alunos)/tempo_total:.2f} alunos/segundo")
    print(f"{'='*70}")
    
    # Salvar backup local
    print(f"\n💾 Salvando backup local...")
    salvar_backup_local(alunos, todos_dados, f"licoes_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    
    # Enviar para Google Sheets
    print(f"\n📤 Enviando dados para Google Sheets...")
    sucesso = enviar_dados_para_sheets(alunos, todos_dados, tempo_total)
    
    if sucesso:
        print(f"\n✅ PROCESSO CONCLUÍDO COM SUCESSO!")
        print(f"🎉 Todos os dados foram coletados e sincronizados!")
    else:
        print(f"\n⚠️ PROCESSO CONCLUÍDO COM RESSALVAS")
        print(f"📊 Dados coletados, mas houve problema no envio para Google Sheets")
        print(f"💾 Backup local salvo com sucesso")
    
    print(f"\n{'='*70}")
    print(f"🏁 FIM DA EXECUÇÃO")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
