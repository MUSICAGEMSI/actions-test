# script_historico_alunos.py - VERSÃO CORRIGIDA
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
URL_LISTAGEM_ALUNOS = "https://musical.congregacao.org.br/alunos/listagem"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxVW_i69_DL_UQQqVjxLsAcEv5edorXSD4g-PZUu4LC9TkGd9yEfNiTL0x92ELDNm8M/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para usar em requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def obter_lista_alunos(session):
    """Obtém a lista completa de alunos da API"""
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/alunos/listagem',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }
        
        # Dados para obter todos os alunos
        data = {
            'draw': '1',
            'start': '0',
            'length': '10000',  # Número grande para pegar todos
            'search[value]': '',
            'search[regex]': 'false'
        }
        
        resp = session.post(URL_LISTAGEM_ALUNOS, data=data, headers=headers, timeout=30)
        
        if resp.status_code == 200:
            dados_json = resp.json()
            alunos = []
            
            for linha in dados_json.get('data', []):
                if len(linha) >= 8:
                    id_aluno = linha[0]  # ID do aluno
                    nome_info = linha[1]  # Nome completo
                    comum_info = linha[2]  # Igreja/Comum
                    ministerio = linha[3]  # Ministério
                    instrumento = linha[4]  # Instrumento
                    nivel = linha[5]  # Nível
                    
                    # Limpar dados HTML
                    comum_limpo = re.sub(r'<[^>]+>', '', comum_info).strip()
                    nome_limpo = nome_info.strip()
                    
                    alunos.append({
                        'id': id_aluno,
                        'nome': nome_limpo,
                        'comum': comum_limpo,
                        'ministerio': ministerio,
                        'instrumento': instrumento,
                        'nivel': nivel
                    })
            
            print(f"✅ Encontrados {len(alunos)} alunos")
            return alunos
            
    except Exception as e:
        print(f"❌ Erro ao obter lista de alunos: {e}")
        return []

def extrair_metodo_especifico(html_content):
    """
    Função específica para extrair datas da seção Método
    que tem formato diferente: Páginas | Lição | Método | Data da Lição
    """
    if not html_content:
        return ""
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Estratégia 1: Buscar por padrões específicos do método
    pattern_data = r'\b(\d{1,2}/\d{1,2}/\d{4})\b'
    datas_encontradas = []
    
    # Buscar todas as datas no conteúdo
    texto_completo = soup.get_text()
    datas_regex = re.findall(pattern_data, texto_completo)
    datas_encontradas.extend(datas_regex)
    
    # Estratégia 2: Buscar especificamente em células que seguem o padrão do método
    for row in soup.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) >= 4:  # Páginas, Lição, Método, Data
            # A data deve estar na 4ª coluna ou última coluna
            for cell in cells[-2:]:  # Verifica as duas últimas colunas
                cell_text = cell.get_text().strip()
                if re.match(r'^\d{1,2}/\d{1,2}/\d{4}
def extrair_datas_melhorada(html_content, secao_nome=""):
    """
    Extrai TODAS as datas de um conteúdo HTML usando múltiplas estratégias
    """
    if not html_content:
        return ""
    
    # Para método, usar função específica
    if secao_nome.upper() == "METODO":
        return extrair_metodo_especifico(html_content)
    
    # Usar BeautifulSoup para parsing mais preciso
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Estratégia 1: Buscar todas as datas no texto limpo
    texto_limpo = soup.get_text()
    pattern_data = r'\b(\d{1,2}/\d{1,2}/\d{4})\b'
    datas_regex = re.findall(pattern_data, texto_limpo)
    
    # Estratégia 2: Buscar em atributos específicos (data attributes, values, etc.)
    datas_atributos = []
    for elemento in soup.find_all(attrs={'data-date': True}):
        data_attr = elemento.get('data-date')
        if data_attr and re.match(pattern_data, data_attr):
            datas_atributos.append(data_attr)
    
    # Estratégia 3: Buscar em células de tabela (td) que contenham apenas datas
    datas_tabela = []
    for td in soup.find_all('td'):
        texto_td = td.get_text().strip()
        if re.match(r'^\d{1,2}/\d{1,2}/\d{4}

def obter_historico_aluno(session, aluno_id):
    """Obtém o histórico completo de um aluno - versão melhorada"""
    try:
        url_historico = f"https://musical.congregacao.org.br/licoes/index/{aluno_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/alunos/listagem',
            'Connection': 'keep-alive',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
        }
        
        resp = session.get(url_historico, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            print(f"      ⚠️ Status HTTP {resp.status_code} para aluno {aluno_id}")
            return {}
        
        texto = resp.text
        
        historico = {
            'mts': "",
            'mts_grupo': "",
            'msa': "",
            'msa_grupo': "",
            'provas': "",
            'metodo': "",
            'hinario': "",
            'hinario_grupo': "",
            'escalas': "",
            'escalas_grupo': ""
        }
        
        # Padrões mais específicos e robustos - VERSÃO CORRIGIDA
        padroes = {
            'mts': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MTS\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'mts_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MTS\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>',
            'msa': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MSA\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'msa_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MSA\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>',
            'provas': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Provas?.*?<table[^>]*>(.*?)</table>',
            'metodo': r'(?:Método|MÉTODO).*?(?:<table[^>]*>(.*?)</table>|Páginas.*?Método.*?Data.*?<tbody[^>]*>(.*?)</tbody>)',
            'hinario': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Hinário\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'hinario_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Hinos?\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>',
            'escalas': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Escalas?\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'escalas_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Escalas?\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>'
        }
        
        # Processar seções com função melhorada
        total_datas_encontradas = 0
        for secao, padrao in padroes.items():
            matches = re.findall(padrao, texto, re.DOTALL | re.IGNORECASE)
            
            if matches:
                # Pegar o maior match (geralmente o mais completo)
                conteudo_secao = max(matches, key=len) if isinstance(matches[0], str) else matches[0]
                # Se o match tem múltiplos grupos, pegar o primeiro não-vazio
                if isinstance(conteudo_secao, tuple):
                    conteudo_secao = next((grupo for grupo in conteudo_secao if grupo), '')
                
                historico[secao] = extrair_datas_melhorada(conteudo_secao, secao.upper())
                
                if historico[secao]:
                    num_datas = len(historico[secao].split('; '))
                    total_datas_encontradas += num_datas
        
        # Log de debug para casos específicos
        if aluno_id == "622865":  # Arthur do exemplo
            print(f"      🔍 ALUNO {aluno_id} - DEBUG COMPLETO:")
            for secao, valor in historico.items():
                if valor:
                    print(f"         - {secao.upper()}: {len(valor.split('; '))} datas")
        
        return historico
        
    except Exception as e:
        print(f"      ⚠️ Erro ao obter histórico do aluno {aluno_id}: {e}")
        return {}

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
        
        # Criar sessão requests com cookies
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # Obter lista de alunos
        print("🔍 Obtendo lista de alunos...")
        alunos = obter_lista_alunos(session)
        
        if not alunos:
            print("❌ Nenhum aluno encontrado.")
            navegador.close()
            return
        
        # TESTE: Processar apenas o Arthur primeiro para verificar
        arthur = next((a for a in alunos if a['id'] == '622865'), None)
        if arthur:
            print(f"\n🎯 TESTE ESPECÍFICO - ARTHUR (ID: {arthur['id']}):")
            historico_arthur = obter_historico_aluno(session, arthur['id'])
            print(f"   MSA GRUPO: {historico_arthur.get('msa_grupo', 'VAZIO')}")
            print(f"   Número de datas MSA GRUPO: {len(historico_arthur.get('msa_grupo', '').split('; ')) if historico_arthur.get('msa_grupo') else 0}")
        
        resultado = []
        
        # Processar todos os alunos
        total_alunos = len(alunos)
        batch_size = 5
        
        for batch_start in range(0, total_alunos, batch_size):
            batch_end = min(batch_start + batch_size, total_alunos)
            batch = alunos[batch_start:batch_end]
            
            print(f"📚 Processando lote {batch_start//batch_size + 1}/{(total_alunos-1)//batch_size + 1} ({len(batch)} alunos)")
            
            for i, aluno in enumerate(batch):
                if time.time() - tempo_inicio > 1800:  # 30 minutos limite
                    print("⏰ Tempo limite atingido.")
                    break
                    
                aluno_atual = batch_start + i + 1
                print(f"   📖 {aluno_atual}/{total_alunos}: {aluno['nome'][:40]}... (ID: {aluno['id']})")
                
                # Obter histórico do aluno
                historico = obter_historico_aluno(session, aluno['id'])
                
                # Montar linha de dados
                linha = [
                    aluno['nome'],
                    aluno['id'],
                    aluno['comum'],
                    aluno['ministerio'],
                    aluno['instrumento'],
                    aluno['nivel'],
                    historico.get('mts', ''),
                    historico.get('mts_grupo', ''),
                    historico.get('msa', ''),
                    historico.get('msa_grupo', ''),
                    historico.get('provas', ''),
                    historico.get('metodo', ''),
                    historico.get('hinario', ''),
                    historico.get('hinario_grupo', ''),
                    historico.get('escalas', ''),
                    historico.get('escalas_grupo', '')
                ]
                
                resultado.append(linha)
                
                # Log de progresso melhorado
                total_datas_aluno = sum(len(x.split('; ')) if x else 0 for x in historico.values())
                if total_datas_aluno > 0:
                    print(f"      ✓ {total_datas_aluno} datas coletadas")
                else:
                    print(f"      ⚪ Nenhuma data encontrada")
            
            # Pausa entre lotes
            if batch_end < total_alunos:
                time.sleep(0.5)
        
        print(f"\n📊 Total de alunos processados: {len(resultado)}")
        
        # Preparar dados para envio
        headers = [
            "NOME", "ID", "COMUM", "MINISTERIO", "INSTRUMENTO", "NIVEL",
            "MTS", "MTS GRUPO", "MSA", "MSA GRUPO", "PROVAS", "MÉTODO",
            "HINÁRIO", "HINÁRIO GRUPO", "ESCALAS", "ESCALAS GRUPO"
        ]
        
        body = {
            "tipo": "historico_alunos",
            "dados": resultado,
            "headers": headers,
            "resumo": {
                "total_alunos": len(resultado),
                "tempo_processamento": f"{(time.time() - tempo_inicio) / 60:.1f} minutos"
            }
        }
        
        # Enviar dados para Apps Script
        try:
            print("📤 Enviando dados para Google Sheets...")
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
            print("✅ Dados enviados!")
            print("Status code:", resposta_post.status_code)
            print("Resposta do Apps Script:", resposta_post.text)
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
        
        # Resumo final detalhado
        print("\n📈 RESUMO FINAL DA COLETA:")
        print(f"   🎯 Total de alunos: {len(resultado)}")
        print(f"   ⏱️ Tempo total: {(time.time() - tempo_inicio) / 60:.1f} minutos")
        
        # Estatísticas de datas coletadas por seção
        stats_secoes = {}
        total_datas = 0
        
        for linha in resultado:
            for i, campo_data in enumerate(linha[6:]):  # Campos de data começam na posição 6
                secao_nome = headers[i + 6]
                if campo_data:
                    num_datas = len(campo_data.split('; '))
                    stats_secoes[secao_nome] = stats_secoes.get(secao_nome, 0) + num_datas
                    total_datas += num_datas
        
        print(f"   📅 Total de datas coletadas: {total_datas}")
        print(f"   📊 Média de datas por aluno: {total_datas/len(resultado):.1f}")
        
        # Mostrar estatísticas por seção
        print("   📋 Datas por seção:")
        for secao, count in stats_secoes.items():
            print(f"      - {secao}: {count} datas")
        
        navegador.close()

if __name__ == "__main__":
    main(), cell_text):
                    datas_encontradas.append(cell_text)
    
    if not datas_encontradas:
        return ""
    
    # Remover duplicatas mantendo ordem
    datas_unicas = []
    for data in datas_encontradas:
        if data not in datas_unicas:
            datas_unicas.append(data)
    
    # Ordenar cronologicamente
    try:
        datas_ordenadas = sorted(datas_unicas, key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
        return "; ".join(datas_ordenadas)
    except:
        return "; ".join(datas_unicas)
    """
    Extrai TODAS as datas de um conteúdo HTML usando múltiplas estratégias
    """
    if not html_content:
        return ""
    
    # Usar BeautifulSoup para parsing mais preciso
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Estratégia 1: Buscar todas as datas no texto limpo
    texto_limpo = soup.get_text()
    pattern_data = r'\b(\d{1,2}/\d{1,2}/\d{4})\b'
    datas_regex = re.findall(pattern_data, texto_limpo)
    
    # Estratégia 2: Buscar em atributos específicos (data attributes, values, etc.)
    datas_atributos = []
    for elemento in soup.find_all(attrs={'data-date': True}):
        data_attr = elemento.get('data-date')
        if data_attr and re.match(pattern_data, data_attr):
            datas_atributos.append(data_attr)
    
    # Estratégia 3: Buscar em células de tabela (td) que contenham apenas datas
    datas_tabela = []
    for td in soup.find_all('td'):
        texto_td = td.get_text().strip()
        if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', texto_td):
            datas_tabela.append(texto_td)
    
    # Estratégia 4: Buscar em inputs type="date" ou similar
    datas_inputs = []
    for input_elem in soup.find_all('input'):
        value = input_elem.get('value', '')
        if value and re.match(pattern_data, value):
            datas_inputs.append(value)
    
    # Combinar todas as datas encontradas
    todas_datas = datas_regex + datas_atributos + datas_tabela + datas_inputs
    
    # DEBUG: Mostrar detalhes para seções específicas
    if secao_nome and ("MSA" in secao_nome.upper() and "GRUPO" in secao_nome.upper()):
        print(f"      🔍 DEBUG {secao_nome}:")
        print(f"         - Datas regex: {len(datas_regex)}")
        print(f"         - Datas atributos: {len(datas_atributos)}")
        print(f"         - Datas tabela: {len(datas_tabela)}")
        print(f"         - Datas inputs: {len(datas_inputs)}")
        if todas_datas:
            print(f"         - Primeiras 3 datas: {todas_datas[:3]}")
    
    if not todas_datas:
        return ""
    
    # Remover duplicatas mantendo ordem
    datas_unicas = []
    for data in todas_datas:
        if data not in datas_unicas:
            datas_unicas.append(data)
    
    # Ordenar cronologicamente
    try:
        datas_ordenadas = sorted(datas_unicas, key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
        resultado = "; ".join(datas_ordenadas)
        
        # DEBUG adicional
        if secao_nome and len(datas_ordenadas) > 0:
            print(f"         - Total datas únicas: {len(datas_ordenadas)}")
            
        return resultado
    except Exception as e:
        print(f"      ⚠️ Erro ao ordenar datas para {secao_nome}: {e}")
        return "; ".join(datas_unicas)

def obter_historico_aluno(session, aluno_id):
    """Obtém o histórico completo de um aluno - versão melhorada"""
    try:
        url_historico = f"https://musical.congregacao.org.br/licoes/index/{aluno_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/alunos/listagem',
            'Connection': 'keep-alive',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
        }
        
        resp = session.get(url_historico, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            print(f"      ⚠️ Status HTTP {resp.status_code} para aluno {aluno_id}")
            return {}
        
        texto = resp.text
        
        historico = {
            'mts': "",
            'mts_grupo': "",
            'msa': "",
            'msa_grupo': "",
            'provas': "",
            'metodo': "",
            'hinario': "",
            'hinario_grupo': "",
            'escalas': "",
            'escalas_grupo': ""
        }
        
        # Padrões mais específicos e robustos - VERSÃO CORRIGIDA
        padroes = {
            'mts': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MTS\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'mts_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MTS\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>',
            'msa': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MSA\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'msa_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MSA\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>',
            'provas': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Provas?.*?<table[^>]*>(.*?)</table>',
            'metodo': r'(?:<h[1-6][^>]*>|<strong>|<b>|<div[^>]*>)?\s*Método[^<]*?(?:.*?<table[^>]*>(.*?)</table>|.*?Páginas\s+Lição\s+Método\s+Data.*?<tbody[^>]*>(.*?)</tbody>)',
            'hinario': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Hinário\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'hinario_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Hinos?\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>',
            'escalas': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Escalas?\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'escalas_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Escalas?\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>'
        }
        
        # Processar seções com função melhorada
        total_datas_encontradas = 0
        for secao, padrao in padroes.items():
            matches = re.findall(padrao, texto, re.DOTALL | re.IGNORECASE)
            
            if matches:
                # Pegar o maior match (geralmente o mais completo)
                conteudo_secao = max(matches, key=len)
                historico[secao] = extrair_datas_melhorada(conteudo_secao, secao.upper())
                
                if historico[secao]:
                    num_datas = len(historico[secao].split('; '))
                    total_datas_encontradas += num_datas
        
        # Log de debug para casos específicos
        if aluno_id == "622865":  # Arthur do exemplo
            print(f"      🔍 ALUNO {aluno_id} - DEBUG COMPLETO:")
            for secao, valor in historico.items():
                if valor:
                    print(f"         - {secao.upper()}: {len(valor.split('; '))} datas")
        
        return historico
        
    except Exception as e:
        print(f"      ⚠️ Erro ao obter histórico do aluno {aluno_id}: {e}")
        return {}

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
        
        # Criar sessão requests com cookies
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # Obter lista de alunos
        print("🔍 Obtendo lista de alunos...")
        alunos = obter_lista_alunos(session)
        
        if not alunos:
            print("❌ Nenhum aluno encontrado.")
            navegador.close()
            return
        
        # TESTE: Processar apenas o Arthur primeiro para verificar
        arthur = next((a for a in alunos if a['id'] == '622865'), None)
        if arthur:
            print(f"\n🎯 TESTE ESPECÍFICO - ARTHUR (ID: {arthur['id']}):")
            historico_arthur = obter_historico_aluno(session, arthur['id'])
            print(f"   MSA GRUPO: {historico_arthur.get('msa_grupo', 'VAZIO')}")
            print(f"   Número de datas MSA GRUPO: {len(historico_arthur.get('msa_grupo', '').split('; ')) if historico_arthur.get('msa_grupo') else 0}")
        
        resultado = []
        
        # Processar todos os alunos
        total_alunos = len(alunos)
        batch_size = 5
        
        for batch_start in range(0, total_alunos, batch_size):
            batch_end = min(batch_start + batch_size, total_alunos)
            batch = alunos[batch_start:batch_end]
            
            print(f"📚 Processando lote {batch_start//batch_size + 1}/{(total_alunos-1)//batch_size + 1} ({len(batch)} alunos)")
            
            for i, aluno in enumerate(batch):
                if time.time() - tempo_inicio > 1800:  # 30 minutos limite
                    print("⏰ Tempo limite atingido.")
                    break
                    
                aluno_atual = batch_start + i + 1
                print(f"   📖 {aluno_atual}/{total_alunos}: {aluno['nome'][:40]}... (ID: {aluno['id']})")
                
                # Obter histórico do aluno
                historico = obter_historico_aluno(session, aluno['id'])
                
                # Montar linha de dados
                linha = [
                    aluno['nome'],
                    aluno['id'],
                    aluno['comum'],
                    aluno['ministerio'],
                    aluno['instrumento'],
                    aluno['nivel'],
                    historico.get('mts', ''),
                    historico.get('mts_grupo', ''),
                    historico.get('msa', ''),
                    historico.get('msa_grupo', ''),
                    historico.get('provas', ''),
                    historico.get('metodo', ''),
                    historico.get('hinario', ''),
                    historico.get('hinario_grupo', ''),
                    historico.get('escalas', ''),
                    historico.get('escalas_grupo', '')
                ]
                
                resultado.append(linha)
                
                # Log de progresso melhorado
                total_datas_aluno = sum(len(x.split('; ')) if x else 0 for x in historico.values())
                if total_datas_aluno > 0:
                    print(f"      ✓ {total_datas_aluno} datas coletadas")
                else:
                    print(f"      ⚪ Nenhuma data encontrada")
            
            # Pausa entre lotes
            if batch_end < total_alunos:
                time.sleep(0.5)
        
        print(f"\n📊 Total de alunos processados: {len(resultado)}")
        
        # Preparar dados para envio
        headers = [
            "NOME", "ID", "COMUM", "MINISTERIO", "INSTRUMENTO", "NIVEL",
            "MTS", "MTS GRUPO", "MSA", "MSA GRUPO", "PROVAS", "MÉTODO",
            "HINÁRIO", "HINÁRIO GRUPO", "ESCALAS", "ESCALAS GRUPO"
        ]
        
        body = {
            "tipo": "historico_alunos",
            "dados": resultado,
            "headers": headers,
            "resumo": {
                "total_alunos": len(resultado),
                "tempo_processamento": f"{(time.time() - tempo_inicio) / 60:.1f} minutos"
            }
        }
        
        # Enviar dados para Apps Script
        try:
            print("📤 Enviando dados para Google Sheets...")
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
            print("✅ Dados enviados!")
            print("Status code:", resposta_post.status_code)
            print("Resposta do Apps Script:", resposta_post.text)
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
        
        # Resumo final detalhado
        print("\n📈 RESUMO FINAL DA COLETA:")
        print(f"   🎯 Total de alunos: {len(resultado)}")
        print(f"   ⏱️ Tempo total: {(time.time() - tempo_inicio) / 60:.1f} minutos")
        
        # Estatísticas de datas coletadas por seção
        stats_secoes = {}
        total_datas = 0
        
        for linha in resultado:
            for i, campo_data in enumerate(linha[6:]):  # Campos de data começam na posição 6
                secao_nome = headers[i + 6]
                if campo_data:
                    num_datas = len(campo_data.split('; '))
                    stats_secoes[secao_nome] = stats_secoes.get(secao_nome, 0) + num_datas
                    total_datas += num_datas
        
        print(f"   📅 Total de datas coletadas: {total_datas}")
        print(f"   📊 Média de datas por aluno: {total_datas/len(resultado):.1f}")
        
        # Mostrar estatísticas por seção
        print("   📋 Datas por seção:")
        for secao, count in stats_secoes.items():
            print(f"      - {secao}: {count} datas")
        
        navegador.close()

if __name__ == "__main__":
    main(), texto_td):
            datas_tabela.append(texto_td)
    
    # Estratégia 4: Buscar em inputs type="date" ou similar
    datas_inputs = []
    for input_elem in soup.find_all('input'):
        value = input_elem.get('value', '')
        if value and re.match(pattern_data, value):
            datas_inputs.append(value)
    
    # Combinar todas as datas encontradas
    todas_datas = datas_regex + datas_atributos + datas_tabela + datas_inputs
    
    # DEBUG: Mostrar detalhes para seções específicas
    if secao_nome and ("MSA" in secao_nome.upper() and "GRUPO" in secao_nome.upper()):
        print(f"      🔍 DEBUG {secao_nome}:")
        print(f"         - Datas regex: {len(datas_regex)}")
        print(f"         - Datas atributos: {len(datas_atributos)}")
        print(f"         - Datas tabela: {len(datas_tabela)}")
        print(f"         - Datas inputs: {len(datas_inputs)}")
        if todas_datas:
            print(f"         - Primeiras 3 datas: {todas_datas[:3]}")
    
    if not todas_datas:
        return ""
    
    # Remover duplicatas mantendo ordem
    datas_unicas = []
    for data in todas_datas:
        if data not in datas_unicas:
            datas_unicas.append(data)
    
    # Ordenar cronologicamente
    try:
        datas_ordenadas = sorted(datas_unicas, key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
        resultado = "; ".join(datas_ordenadas)
        
        # DEBUG adicional
        if secao_nome and len(datas_ordenadas) > 0:
            print(f"         - Total datas únicas: {len(datas_ordenadas)}")
            
        return resultado
    except Exception as e:
        print(f"      ⚠️ Erro ao ordenar datas para {secao_nome}: {e}")
        return "; ".join(datas_unicas)

def obter_historico_aluno(session, aluno_id):
    """Obtém o histórico completo de um aluno - versão melhorada"""
    try:
        url_historico = f"https://musical.congregacao.org.br/licoes/index/{aluno_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/alunos/listagem',
            'Connection': 'keep-alive',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
        }
        
        resp = session.get(url_historico, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            print(f"      ⚠️ Status HTTP {resp.status_code} para aluno {aluno_id}")
            return {}
        
        texto = resp.text
        
        historico = {
            'mts': "",
            'mts_grupo': "",
            'msa': "",
            'msa_grupo': "",
            'provas': "",
            'metodo': "",
            'hinario': "",
            'hinario_grupo': "",
            'escalas': "",
            'escalas_grupo': ""
        }
        
        # Padrões mais específicos e robustos - VERSÃO CORRIGIDA
        padroes = {
            'mts': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MTS\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'mts_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MTS\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>',
            'msa': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MSA\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'msa_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MSA\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>',
            'provas': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Provas?.*?<table[^>]*>(.*?)</table>',
            'metodo': r'(?:<h[1-6][^>]*>|<strong>|<b>|<div[^>]*>)?\s*Método[^<]*?(?:.*?<table[^>]*>(.*?)</table>|.*?Páginas\s+Lição\s+Método\s+Data.*?<tbody[^>]*>(.*?)</tbody>)',
            'hinario': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Hinário\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'hinario_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Hinos?\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>',
            'escalas': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Escalas?\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'escalas_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Escalas?\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>'
        }
        
        # Processar seções com função melhorada
        total_datas_encontradas = 0
        for secao, padrao in padroes.items():
            matches = re.findall(padrao, texto, re.DOTALL | re.IGNORECASE)
            
            if matches:
                # Pegar o maior match (geralmente o mais completo)
                conteudo_secao = max(matches, key=len)
                historico[secao] = extrair_datas_melhorada(conteudo_secao, secao.upper())
                
                if historico[secao]:
                    num_datas = len(historico[secao].split('; '))
                    total_datas_encontradas += num_datas
        
        # Log de debug para casos específicos
        if aluno_id == "622865":  # Arthur do exemplo
            print(f"      🔍 ALUNO {aluno_id} - DEBUG COMPLETO:")
            for secao, valor in historico.items():
                if valor:
                    print(f"         - {secao.upper()}: {len(valor.split('; '))} datas")
        
        return historico
        
    except Exception as e:
        print(f"      ⚠️ Erro ao obter histórico do aluno {aluno_id}: {e}")
        return {}

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
        
        # Criar sessão requests com cookies
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # Obter lista de alunos
        print("🔍 Obtendo lista de alunos...")
        alunos = obter_lista_alunos(session)
        
        if not alunos:
            print("❌ Nenhum aluno encontrado.")
            navegador.close()
            return
        
        # TESTE: Processar apenas o Arthur primeiro para verificar
        arthur = next((a for a in alunos if a['id'] == '622865'), None)
        if arthur:
            print(f"\n🎯 TESTE ESPECÍFICO - ARTHUR (ID: {arthur['id']}):")
            historico_arthur = obter_historico_aluno(session, arthur['id'])
            print(f"   MSA GRUPO: {historico_arthur.get('msa_grupo', 'VAZIO')}")
            print(f"   Número de datas MSA GRUPO: {len(historico_arthur.get('msa_grupo', '').split('; ')) if historico_arthur.get('msa_grupo') else 0}")
        
        resultado = []
        
        # Processar todos os alunos
        total_alunos = len(alunos)
        batch_size = 5
        
        for batch_start in range(0, total_alunos, batch_size):
            batch_end = min(batch_start + batch_size, total_alunos)
            batch = alunos[batch_start:batch_end]
            
            print(f"📚 Processando lote {batch_start//batch_size + 1}/{(total_alunos-1)//batch_size + 1} ({len(batch)} alunos)")
            
            for i, aluno in enumerate(batch):
                if time.time() - tempo_inicio > 1800:  # 30 minutos limite
                    print("⏰ Tempo limite atingido.")
                    break
                    
                aluno_atual = batch_start + i + 1
                print(f"   📖 {aluno_atual}/{total_alunos}: {aluno['nome'][:40]}... (ID: {aluno['id']})")
                
                # Obter histórico do aluno
                historico = obter_historico_aluno(session, aluno['id'])
                
                # Montar linha de dados
                linha = [
                    aluno['nome'],
                    aluno['id'],
                    aluno['comum'],
                    aluno['ministerio'],
                    aluno['instrumento'],
                    aluno['nivel'],
                    historico.get('mts', ''),
                    historico.get('mts_grupo', ''),
                    historico.get('msa', ''),
                    historico.get('msa_grupo', ''),
                    historico.get('provas', ''),
                    historico.get('metodo', ''),
                    historico.get('hinario', ''),
                    historico.get('hinario_grupo', ''),
                    historico.get('escalas', ''),
                    historico.get('escalas_grupo', '')
                ]
                
                resultado.append(linha)
                
                # Log de progresso melhorado
                total_datas_aluno = sum(len(x.split('; ')) if x else 0 for x in historico.values())
                if total_datas_aluno > 0:
                    print(f"      ✓ {total_datas_aluno} datas coletadas")
                else:
                    print(f"      ⚪ Nenhuma data encontrada")
            
            # Pausa entre lotes
            if batch_end < total_alunos:
                time.sleep(0.5)
        
        print(f"\n📊 Total de alunos processados: {len(resultado)}")
        
        # Preparar dados para envio
        headers = [
            "NOME", "ID", "COMUM", "MINISTERIO", "INSTRUMENTO", "NIVEL",
            "MTS", "MTS GRUPO", "MSA", "MSA GRUPO", "PROVAS", "MÉTODO",
            "HINÁRIO", "HINÁRIO GRUPO", "ESCALAS", "ESCALAS GRUPO"
        ]
        
        body = {
            "tipo": "historico_alunos",
            "dados": resultado,
            "headers": headers,
            "resumo": {
                "total_alunos": len(resultado),
                "tempo_processamento": f"{(time.time() - tempo_inicio) / 60:.1f} minutos"
            }
        }
        
        # Enviar dados para Apps Script
        try:
            print("📤 Enviando dados para Google Sheets...")
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
            print("✅ Dados enviados!")
            print("Status code:", resposta_post.status_code)
            print("Resposta do Apps Script:", resposta_post.text)
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
        
        # Resumo final detalhado
        print("\n📈 RESUMO FINAL DA COLETA:")
        print(f"   🎯 Total de alunos: {len(resultado)}")
        print(f"   ⏱️ Tempo total: {(time.time() - tempo_inicio) / 60:.1f} minutos")
        
        # Estatísticas de datas coletadas por seção
        stats_secoes = {}
        total_datas = 0
        
        for linha in resultado:
            for i, campo_data in enumerate(linha[6:]):  # Campos de data começam na posição 6
                secao_nome = headers[i + 6]
                if campo_data:
                    num_datas = len(campo_data.split('; '))
                    stats_secoes[secao_nome] = stats_secoes.get(secao_nome, 0) + num_datas
                    total_datas += num_datas
        
        print(f"   📅 Total de datas coletadas: {total_datas}")
        print(f"   📊 Média de datas por aluno: {total_datas/len(resultado):.1f}")
        
        # Mostrar estatísticas por seção
        print("   📋 Datas por seção:")
        for secao, count in stats_secoes.items():
            print(f"      - {secao}: {count} datas")
        
        navegador.close()

if __name__ == "__main__":
    main(), cell_text):
                    datas_encontradas.append(cell_text)
    
    if not datas_encontradas:
        return ""
    
    # Remover duplicatas mantendo ordem
    datas_unicas = []
    for data in datas_encontradas:
        if data not in datas_unicas:
            datas_unicas.append(data)
    
    # Ordenar cronologicamente
    try:
        datas_ordenadas = sorted(datas_unicas, key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
        return "; ".join(datas_ordenadas)
    except:
        return "; ".join(datas_unicas)
    """
    Extrai TODAS as datas de um conteúdo HTML usando múltiplas estratégias
    """
    if not html_content:
        return ""
    
    # Usar BeautifulSoup para parsing mais preciso
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Estratégia 1: Buscar todas as datas no texto limpo
    texto_limpo = soup.get_text()
    pattern_data = r'\b(\d{1,2}/\d{1,2}/\d{4})\b'
    datas_regex = re.findall(pattern_data, texto_limpo)
    
    # Estratégia 2: Buscar em atributos específicos (data attributes, values, etc.)
    datas_atributos = []
    for elemento in soup.find_all(attrs={'data-date': True}):
        data_attr = elemento.get('data-date')
        if data_attr and re.match(pattern_data, data_attr):
            datas_atributos.append(data_attr)
    
    # Estratégia 3: Buscar em células de tabela (td) que contenham apenas datas
    datas_tabela = []
    for td in soup.find_all('td'):
        texto_td = td.get_text().strip()
        if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', texto_td):
            datas_tabela.append(texto_td)
    
    # Estratégia 4: Buscar em inputs type="date" ou similar
    datas_inputs = []
    for input_elem in soup.find_all('input'):
        value = input_elem.get('value', '')
        if value and re.match(pattern_data, value):
            datas_inputs.append(value)
    
    # Combinar todas as datas encontradas
    todas_datas = datas_regex + datas_atributos + datas_tabela + datas_inputs
    
    # DEBUG: Mostrar detalhes para seções específicas
    if secao_nome and ("MSA" in secao_nome.upper() and "GRUPO" in secao_nome.upper()):
        print(f"      🔍 DEBUG {secao_nome}:")
        print(f"         - Datas regex: {len(datas_regex)}")
        print(f"         - Datas atributos: {len(datas_atributos)}")
        print(f"         - Datas tabela: {len(datas_tabela)}")
        print(f"         - Datas inputs: {len(datas_inputs)}")
        if todas_datas:
            print(f"         - Primeiras 3 datas: {todas_datas[:3]}")
    
    if not todas_datas:
        return ""
    
    # Remover duplicatas mantendo ordem
    datas_unicas = []
    for data in todas_datas:
        if data not in datas_unicas:
            datas_unicas.append(data)
    
    # Ordenar cronologicamente
    try:
        datas_ordenadas = sorted(datas_unicas, key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
        resultado = "; ".join(datas_ordenadas)
        
        # DEBUG adicional
        if secao_nome and len(datas_ordenadas) > 0:
            print(f"         - Total datas únicas: {len(datas_ordenadas)}")
            
        return resultado
    except Exception as e:
        print(f"      ⚠️ Erro ao ordenar datas para {secao_nome}: {e}")
        return "; ".join(datas_unicas)

def obter_historico_aluno(session, aluno_id):
    """Obtém o histórico completo de um aluno - versão melhorada"""
    try:
        url_historico = f"https://musical.congregacao.org.br/licoes/index/{aluno_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/alunos/listagem',
            'Connection': 'keep-alive',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
        }
        
        resp = session.get(url_historico, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            print(f"      ⚠️ Status HTTP {resp.status_code} para aluno {aluno_id}")
            return {}
        
        texto = resp.text
        
        historico = {
            'mts': "",
            'mts_grupo': "",
            'msa': "",
            'msa_grupo': "",
            'provas': "",
            'metodo': "",
            'hinario': "",
            'hinario_grupo': "",
            'escalas': "",
            'escalas_grupo': ""
        }
        
        # Padrões mais específicos e robustos - VERSÃO CORRIGIDA
        padroes = {
            'mts': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MTS\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'mts_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MTS\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>',
            'msa': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MSA\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'msa_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*MSA\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>',
            'provas': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Provas?.*?<table[^>]*>(.*?)</table>',
            'metodo': r'(?:<h[1-6][^>]*>|<strong>|<b>|<div[^>]*>)?\s*Método[^<]*?(?:.*?<table[^>]*>(.*?)</table>|.*?Páginas\s+Lição\s+Método\s+Data.*?<tbody[^>]*>(.*?)</tbody>)',
            'hinario': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Hinário\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'hinario_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Hinos?\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>',
            'escalas': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Escalas?\s*(?!</[^>]*>)(?!\s*-\s*Aulas\s+em\s+grupo).*?<table[^>]*>(.*?)</table>',
            'escalas_grupo': r'(?:<h[1-6][^>]*>|<strong>|<b>)?\s*Escalas?\s*-\s*Aulas\s+em\s+grupo.*?<table[^>]*>(.*?)</table>'
        }
        
        # Processar seções com função melhorada
        total_datas_encontradas = 0
        for secao, padrao in padroes.items():
            matches = re.findall(padrao, texto, re.DOTALL | re.IGNORECASE)
            
            if matches:
                # Pegar o maior match (geralmente o mais completo)
                conteudo_secao = max(matches, key=len)
                historico[secao] = extrair_datas_melhorada(conteudo_secao, secao.upper())
                
                if historico[secao]:
                    num_datas = len(historico[secao].split('; '))
                    total_datas_encontradas += num_datas
        
        # Log de debug para casos específicos
        if aluno_id == "622865":  # Arthur do exemplo
            print(f"      🔍 ALUNO {aluno_id} - DEBUG COMPLETO:")
            for secao, valor in historico.items():
                if valor:
                    print(f"         - {secao.upper()}: {len(valor.split('; '))} datas")
        
        return historico
        
    except Exception as e:
        print(f"      ⚠️ Erro ao obter histórico do aluno {aluno_id}: {e}")
        return {}

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
        
        # Criar sessão requests com cookies
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # Obter lista de alunos
        print("🔍 Obtendo lista de alunos...")
        alunos = obter_lista_alunos(session)
        
        if not alunos:
            print("❌ Nenhum aluno encontrado.")
            navegador.close()
            return
        
        # TESTE: Processar apenas o Arthur primeiro para verificar
        arthur = next((a for a in alunos if a['id'] == '622865'), None)
        if arthur:
            print(f"\n🎯 TESTE ESPECÍFICO - ARTHUR (ID: {arthur['id']}):")
            historico_arthur = obter_historico_aluno(session, arthur['id'])
            print(f"   MSA GRUPO: {historico_arthur.get('msa_grupo', 'VAZIO')}")
            print(f"   Número de datas MSA GRUPO: {len(historico_arthur.get('msa_grupo', '').split('; ')) if historico_arthur.get('msa_grupo') else 0}")
        
        resultado = []
        
        # Processar todos os alunos
        total_alunos = len(alunos)
        batch_size = 5
        
        for batch_start in range(0, total_alunos, batch_size):
            batch_end = min(batch_start + batch_size, total_alunos)
            batch = alunos[batch_start:batch_end]
            
            print(f"📚 Processando lote {batch_start//batch_size + 1}/{(total_alunos-1)//batch_size + 1} ({len(batch)} alunos)")
            
            for i, aluno in enumerate(batch):
                if time.time() - tempo_inicio > 1800:  # 30 minutos limite
                    print("⏰ Tempo limite atingido.")
                    break
                    
                aluno_atual = batch_start + i + 1
                print(f"   📖 {aluno_atual}/{total_alunos}: {aluno['nome'][:40]}... (ID: {aluno['id']})")
                
                # Obter histórico do aluno
                historico = obter_historico_aluno(session, aluno['id'])
                
                # Montar linha de dados
                linha = [
                    aluno['nome'],
                    aluno['id'],
                    aluno['comum'],
                    aluno['ministerio'],
                    aluno['instrumento'],
                    aluno['nivel'],
                    historico.get('mts', ''),
                    historico.get('mts_grupo', ''),
                    historico.get('msa', ''),
                    historico.get('msa_grupo', ''),
                    historico.get('provas', ''),
                    historico.get('metodo', ''),
                    historico.get('hinario', ''),
                    historico.get('hinario_grupo', ''),
                    historico.get('escalas', ''),
                    historico.get('escalas_grupo', '')
                ]
                
                resultado.append(linha)
                
                # Log de progresso melhorado
                total_datas_aluno = sum(len(x.split('; ')) if x else 0 for x in historico.values())
                if total_datas_aluno > 0:
                    print(f"      ✓ {total_datas_aluno} datas coletadas")
                else:
                    print(f"      ⚪ Nenhuma data encontrada")
            
            # Pausa entre lotes
            if batch_end < total_alunos:
                time.sleep(0.5)
        
        print(f"\n📊 Total de alunos processados: {len(resultado)}")
        
        # Preparar dados para envio
        headers = [
            "NOME", "ID", "COMUM", "MINISTERIO", "INSTRUMENTO", "NIVEL",
            "MTS", "MTS GRUPO", "MSA", "MSA GRUPO", "PROVAS", "MÉTODO",
            "HINÁRIO", "HINÁRIO GRUPO", "ESCALAS", "ESCALAS GRUPO"
        ]
        
        body = {
            "tipo": "historico_alunos",
            "dados": resultado,
            "headers": headers,
            "resumo": {
                "total_alunos": len(resultado),
                "tempo_processamento": f"{(time.time() - tempo_inicio) / 60:.1f} minutos"
            }
        }
        
        # Enviar dados para Apps Script
        try:
            print("📤 Enviando dados para Google Sheets...")
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
            print("✅ Dados enviados!")
            print("Status code:", resposta_post.status_code)
            print("Resposta do Apps Script:", resposta_post.text)
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
        
        # Resumo final detalhado
        print("\n📈 RESUMO FINAL DA COLETA:")
        print(f"   🎯 Total de alunos: {len(resultado)}")
        print(f"   ⏱️ Tempo total: {(time.time() - tempo_inicio) / 60:.1f} minutos")
        
        # Estatísticas de datas coletadas por seção
        stats_secoes = {}
        total_datas = 0
        
        for linha in resultado:
            for i, campo_data in enumerate(linha[6:]):  # Campos de data começam na posição 6
                secao_nome = headers[i + 6]
                if campo_data:
                    num_datas = len(campo_data.split('; '))
                    stats_secoes[secao_nome] = stats_secoes.get(secao_nome, 0) + num_datas
                    total_datas += num_datas
        
        print(f"   📅 Total de datas coletadas: {total_datas}")
        print(f"   📊 Média de datas por aluno: {total_datas/len(resultado):.1f}")
        
        # Mostrar estatísticas por seção
        print("   📋 Datas por seção:")
        for secao, count in stats_secoes.items():
            print(f"      - {secao}: {count} datas")
        
        navegador.close()

if __name__ == "__main__":
    main()
