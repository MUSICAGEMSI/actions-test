# script_relatorio_candidatos_localidade.py
from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time
import json
from collections import defaultdict

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_LISTAGEM_ALUNOS = "https://musical.congregacao.org.br/alunos/listagem"
URL_LISTAGEM_GRP = "https://musical.congregacao.org.br/grp_musical/listagem"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzx5wJjPYSBEeoNQMc02fxi2j4JqROJ1HKbdM59tMHmb2TD2A2Y6IYDtTpHiZvmLFsGug/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def extrair_localidade_limpa(localidade_texto):
    """
    Extrai apenas o nome da localidade, removendo HTML e informações extras
    """
    # Remove tags HTML
    localidade_texto = localidade_texto.replace('<\\/span>', '').replace('<span>', '').replace('</span>', '')
    
    # Pega apenas a parte antes do " | "
    if ' | ' in localidade_texto:
        localidade = localidade_texto.split(' | ')[0].strip()
    else:
        localidade = localidade_texto.strip()
    
    return localidade

def obter_candidatos_basicos(session):
    """
    Obtém candidatos básicos (exceto OFICIALIZADO e RJM/OFICIALIZADO) da listagem de alunos
    """
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd'
        }
        
        # Dados para o POST - estrutura DataTables completa
        form_data = {
            'draw': '1',
            'columns[0][data]': '0',
            'columns[0][name]': '',
            'columns[0][searchable]': 'true',
            'columns[0][orderable]': 'true',
            'columns[0][search][value]': '',
            'columns[0][search][regex]': 'false',
            'columns[1][data]': '1',
            'columns[1][name]': '',
            'columns[1][searchable]': 'true',
            'columns[1][orderable]': 'true',
            'columns[1][search][value]': '',
            'columns[1][search][regex]': 'false',
            'columns[2][data]': '2',
            'columns[2][name]': '',
            'columns[2][searchable]': 'true',
            'columns[2][orderable]': 'true',
            'columns[2][search][value]': '',
            'columns[2][search][regex]': 'false',
            'columns[3][data]': '3',
            'columns[3][name]': '',
            'columns[3][searchable]': 'true',
            'columns[3][orderable]': 'true',
            'columns[3][search][value]': '',
            'columns[3][search][regex]': 'false',
            'columns[4][data]': '4',
            'columns[4][name]': '',
            'columns[4][searchable]': 'true',
            'columns[4][orderable]': 'true',
            'columns[4][search][value]': '',
            'columns[4][search][regex]': 'false',
            'columns[5][data]': '5',
            'columns[5][name]': '',
            'columns[5][searchable]': 'true',
            'columns[5][orderable]': 'true',
            'columns[5][search][value]': '',
            'columns[5][search][regex]': 'false',
            'columns[6][data]': '6',
            'columns[6][name]': '',
            'columns[6][searchable]': 'false',
            'columns[6][orderable]': 'false',
            'columns[6][search][value]': '',
            'columns[6][search][regex]': 'false',
            'order[0][column]': '0',
            'order[0][dir]': 'asc',
            'start': '0',
            'length': '10000',
            'search[value]': '',
            'search[regex]': 'false'
        }
        
        print("📊 Obtendo candidatos básicos da listagem de alunos...")
        resp = session.post(URL_LISTAGEM_ALUNOS, headers=headers, data=form_data, timeout=60)
        
        print(f"📊 Status da requisição alunos: {resp.status_code}")
        
        dados_por_localidade = defaultdict(lambda: {
            'CANDIDATO(A)': 0,
            'ENSAIO': 0,
            'RJM / ENSAIO': 0,
            'RJM': 0,
            'RJM / CULTO OFICIAL': 0,
            'CULTO OFICIAL': 0
        })
        
        if resp.status_code == 200:
            try:
                data = resp.json()
                print(f"📊 JSON alunos recebido com {len(data.get('data', []))} registros")
                
                if 'data' in data and isinstance(data['data'], list):
                    for record in data['data']:
                        if isinstance(record, list) and len(record) >= 6:
                            # Estrutura: [id, nome, localidade_completa, ministério, instrumento, nível, ...]
                            localidade_completa = record[2]
                            nivel = record[5]
                            
                            # Extrair localidade limpa
                            localidade = extrair_localidade_limpa(localidade_completa)
                            
                            # Verificar se deve ser ignorado
                            if 'ORGANISTA' in nivel.upper():
                                continue
                            
                            # Ignorar OFICIALIZADO - esses vêm do GRP
                            if 'OFICIALIZADO' in nivel.upper():
                                continue
                            
                            # Contar apenas os níveis básicos
                            if nivel in dados_por_localidade[localidade]:
                                dados_por_localidade[localidade][nivel] += 1
                                print(f"📊 Alunos - {localidade}: {nivel} (+1)")
                
                print(f"📊 Candidatos básicos - Total de localidades: {len(dados_por_localidade)}")
                return dict(dados_por_localidade)
                
            except json.JSONDecodeError as e:
                print(f"❌ Erro ao decodificar JSON alunos: {e}")
                return {}
        
        return {}
        
    except Exception as e:
        print(f"⚠️ Erro ao obter candidatos básicos: {e}")
        return {}

def obter_oficializados(session):
    """
    Obtém OFICIALIZADO(A) e RJM/OFICIALIZADO(A) da listagem do grupo musical
    """
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd'
        }
        
        # Dados para o POST - estrutura DataTables para grupo musical
        form_data = {
            'draw': '1',
            'columns[0][data]': '0',
            'columns[0][name]': '',
            'columns[0][searchable]': 'true',
            'columns[0][orderable]': 'true',
            'columns[0][search][value]': '',
            'columns[0][search][regex]': 'false',
            'columns[1][data]': '1',
            'columns[1][name]': '',
            'columns[1][searchable]': 'true',
            'columns[1][orderable]': 'true',
            'columns[1][search][value]': '',
            'columns[1][search][regex]': 'false',
            'columns[2][data]': '2',
            'columns[2][name]': '',
            'columns[2][searchable]': 'true',
            'columns[2][orderable]': 'true',
            'columns[2][search][value]': '',
            'columns[2][search][regex]': 'false',
            'columns[3][data]': '3',
            'columns[3][name]': '',
            'columns[3][searchable]': 'true',
            'columns[3][orderable]': 'true',
            'columns[3][search][value]': '',
            'columns[3][search][regex]': 'false',
            'columns[4][data]': '4',
            'columns[4][name]': '',
            'columns[4][searchable]': 'true',
            'columns[4][orderable]': 'true',
            'columns[4][search][value]': '',
            'columns[4][search][regex]': 'false',
            'columns[5][data]': '5',
            'columns[5][name]': '',
            'columns[5][searchable]': 'true',
            'columns[5][orderable]': 'true',
            'columns[5][search][value]': '',
            'columns[5][search][regex]': 'false',
            'columns[6][data]': '6',
            'columns[6][name]': '',
            'columns[6][searchable]': 'false',
            'columns[6][orderable]': 'false',
            'columns[6][search][value]': '',
            'columns[6][search][regex]': 'false',
            'order[0][column]': '0',
            'order[0][dir]': 'asc',
            'start': '0',
            'length': '10000',
            'search[value]': '',
            'search[regex]': 'false'
        }
        
        print("📊 Obtendo oficializados da listagem do grupo musical...")
        resp = session.post(URL_LISTAGEM_GRP, headers=headers, data=form_data, timeout=60)
        
        print(f"📊 Status da requisição GRP: {resp.status_code}")
        
        dados_oficializados = defaultdict(lambda: {
            'RJM / OFICIALIZADO(A)': 0,
            'OFICIALIZADO(A)': 0
        })
        
        if resp.status_code == 200:
            try:
                data = resp.json()
                print(f"📊 JSON GRP recebido com {len(data.get('data', []))} registros")
                
                if 'data' in data and isinstance(data['data'], list):
                    for record in data['data']:
                        if isinstance(record, list) and len(record) >= 6:
                            # A estrutura pode ser diferente, vamos analisar
                            # Assumindo estrutura similar: [checkbox, nome, localidade, ?, ?, nivel, ...]
                            nome = record[1] if len(record) > 1 else ""
                            localidade_completa = record[2] if len(record) > 2 else ""
                            nivel = record[5] if len(record) > 5 else ""
                            
                            # Extrair localidade limpa
                            localidade = extrair_localidade_limpa(localidade_completa)
                            
                            # Verificar se é oficializado
                            if 'RJM / OFICIALIZADO(A)' in nivel:
                                dados_oficializados[localidade]['RJM / OFICIALIZADO(A)'] += 1
                                print(f"📊 GRP - {localidade}: RJM / OFICIALIZADO(A) (+1)")
                            elif 'OFICIALIZADO(A)' in nivel:
                                dados_oficializados[localidade]['OFICIALIZADO(A)'] += 1
                                print(f"📊 GRP - {localidade}: OFICIALIZADO(A) (+1)")
                
                print(f"📊 Oficializados - Total de localidades: {len(dados_oficializados)}")
                return dict(dados_oficializados)
                
            except json.JSONDecodeError as e:
                print(f"❌ Erro ao decodificar JSON GRP: {e}")
                print(f"📝 Resposta GRP: {resp.text[:500]}...")
                return {}
        
        return {}
        
    except Exception as e:
        print(f"⚠️ Erro ao obter oficializados: {e}")
        import traceback
        traceback.print_exc()
        return {}

def extrair_cookies_playwright(pagina):
    """
    Extrai cookies do Playwright para usar em requests
    """
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def main():
    tempo_inicio = time.time()

    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Login
        print("🔐 Fazendo login...")
        pagina.goto(URL_INICIAL)
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')

        try:
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado com sucesso")
        except PlaywrightTimeoutError:
            print("❌ Falha no login")
            navegador.close()
            return

        # Extrair cookies para usar com requests
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)

        # Navegar para garantir contexto correto
        print("📄 Navegando para páginas necessárias...")
        pagina.goto("https://musical.congregacao.org.br/alunos")
        pagina.wait_for_timeout(1000)
        pagina.goto("https://musical.congregacao.org.br/grp_musical")
        pagina.wait_for_timeout(1000)

        # Atualizar cookies após navegação
        cookies_dict = extrair_cookies_playwright(pagina)
        session.cookies.update(cookies_dict)

        # Obter dados de ambas as fontes
        print("📊 Coletando candidatos básicos...")
        dados_basicos = obter_candidatos_basicos(session)
        
        print("📊 Coletando oficializados...")
        dados_oficializados = obter_oficializados(session)
        
        # Combinar dados
        print("📊 Combinando dados...")
        todas_localidades = set(dados_basicos.keys()) | set(dados_oficializados.keys())
        
        dados_finais = defaultdict(lambda: {
            'CANDIDATO(A)': 0,
            'ENSAIO': 0,
            'RJM / ENSAIO': 0,
            'RJM': 0,
            'RJM / OFICIALIZADO(A)': 0,
            'OFICIALIZADO(A)': 0,
            'RJM / CULTO OFICIAL': 0,
            'CULTO OFICIAL': 0
        })
        
        # Combinar dados básicos
        for localidade, contadores in dados_basicos.items():
            for status, quantidade in contadores.items():
                dados_finais[localidade][status] += quantidade
        
        # Combinar dados de oficializados
        for localidade, contadores in dados_oficializados.items():
            for status, quantidade in contadores.items():
                dados_finais[localidade][status] += quantidade
        
        if not dados_finais:
            print("❌ Nenhum dado foi coletado")
            navegador.close()
            return

        # Preparar relatório final
        relatorio_final = []
        
        # Headers para o relatório
        headers = [
            "Localidade",
            "CANDIDATO(A)",
            "ENSAIO", 
            "RJM / ENSAIO",
            "RJM",
            "RJM / OFICIALIZADO(A)",
            "OFICIALIZADO(A)",
            "RJM / CULTO OFICIAL",
            "CULTO OFICIAL"
        ]
        
        # Gerar linhas do relatório
        for localidade in sorted(dados_finais.keys()):
            contadores = dados_finais[localidade]
            linha = [
                localidade,
                contadores['CANDIDATO(A)'],
                contadores['ENSAIO'],
                contadores['RJM / ENSAIO'],
                contadores['RJM'],
                contadores['RJM / OFICIALIZADO(A)'],
                contadores['OFICIALIZADO(A)'],
                contadores['RJM / CULTO OFICIAL'],
                contadores['CULTO OFICIAL']
            ]
            relatorio_final.append(linha)

        # Ordenar por quantidade de candidatos (decrescente)
        relatorio_final.sort(key=lambda x: x[1], reverse=True)

        # Mostrar resultado
        print(f"\n📊 RELATÓRIO DE CANDIDATOS POR LOCALIDADE:")
        print("="*150)
        print(f"{'Localidade':<25} {'CAND':<5} {'ENS':<5} {'R/E':<5} {'RJM':<5} {'R/O':<5} {'OFIC':<5} {'R/C':<5} {'CULTO':<5}")
        print("-"*150)
        
        for linha in relatorio_final:
            print(f"{linha[0]:<25} {linha[1]:<5} {linha[2]:<5} {linha[3]:<5} {linha[4]:<5} {linha[5]:<5} {linha[6]:<5} {linha[7]:<5} {linha[8]:<5}")

        # Calcular totais
        totais = [sum(linha[i] for linha in relatorio_final) for i in range(1, len(headers))]
        print("-"*150)
        print(f"{'TOTAL':<25} {totais[0]:<5} {totais[1]:<5} {totais[2]:<5} {totais[3]:<5} {totais[4]:<5} {totais[5]:<5} {totais[6]:<5} {totais[7]:<5}")

        # Preparar dados para envio (incluindo headers como primeira linha)
        dados_com_headers = [headers] + relatorio_final
        
        body = {
            "tipo": "relatorio_candidatos_localidade_com_headers",
            "dados": dados_com_headers,
            "incluir_headers": True
        }

        # Enviar para Google Sheets
        try:
            print(f"\n📤 Enviando {len(relatorio_final)} localidades para Google Sheets...")
            resposta = requests.post(URL_APPS_SCRIPT, json=body, timeout=60)
            print(f"✅ Status do envio: {resposta.status_code}")
            print(f"📝 Resposta: {resposta.text}")
        except Exception as e:
            print(f"❌ Erro no envio: {e}")

        navegador.close()
        
        tempo_total = time.time() - tempo_inicio
        print(f"\n🎯 Concluído! {len(relatorio_final)} localidades processadas em {tempo_total:.1f} segundos.")
        print(f"📊 Total geral de registros: {sum(totais)}")

if __name__ == "__main__":
    main()
