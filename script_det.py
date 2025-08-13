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
URL_LISTAGEM_GRUPOS = "https://musical.congregacao.org.br/grp_musical/listagem"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzRSGdID5WjLuukBUt-5TbQjCqSvCKjr0vOWHFfFr0rChW1vINwgQE5VJDQCKM5mc693Q/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def extrair_localidade_limpa(localidade_texto):
    """
    Extrai apenas o nome da localidade, removendo HTML e informações extras
    """
    import re
    
    # Remove todas as tags HTML e spans escapados
    localidade_texto = re.sub(r'<\\?/?span[^>]*>', '', localidade_texto)
    localidade_texto = re.sub(r'<[^>]+>', '', localidade_texto)
    
    # Remove classe CSS escapada
    localidade_texto = re.sub(r"class='[^']*'", '', localidade_texto)
    
    # Pega apenas a parte antes do " | "
    if ' | ' in localidade_texto:
        localidade = localidade_texto.split(' | ')[0].strip()
    else:
        localidade = localidade_texto.strip()
    
    # Remove espaços extras e caracteres especiais
    localidade = re.sub(r'\s+', ' ', localidade).strip()
    
    return localidade

def obter_candidatos_por_localidade_e_tipo(session, tipo_ministerio):
    """
    Obtém candidatos por localidade da listagem de alunos
    tipo_ministerio: 'MÚSICO' ou 'ORGANISTA'
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
        
        print(f"📊 Obtendo {tipo_ministerio.lower()}s da listagem de alunos...")
        resp = session.post(URL_LISTAGEM_ALUNOS, headers=headers, data=form_data, timeout=60)
        
        print(f"📊 Status da requisição: {resp.status_code}")
        
        # Níveis válidos que devemos contar (colunas B-G)
        niveis_validos = {
            'CANDIDATO(A)': 0,
            'RJM / ENSAIO': 0,
            'ENSAIO': 0,
            'RJM': 0,
            'RJM / CULTO OFICIAL': 0,
            'CULTO OFICIAL': 0
        }
        
        dados_por_localidade = defaultdict(lambda: niveis_validos.copy())
        
        if resp.status_code == 200:
            try:
                data = resp.json()
                print(f"📊 JSON recebido com {len(data.get('data', []))} registros")
                
                if 'data' in data and isinstance(data['data'], list):
                    for record in data['data']:
                        if isinstance(record, list) and len(record) >= 6:
                            # Estrutura: [id, nome, localidade_completa, ministério, instrumento, nível, ...]
                            localidade_completa = record[2]
                            ministerio = record[3]
                            nivel = record[5]
                            
                            print(f"🔍 Processando: {ministerio} | {nivel} | {localidade_completa[:50]}...")
                            
                            # Filtrar por tipo de ministério
                            if ministerio != tipo_ministerio:
                                print(f"⏭️ Pulando: ministério {ministerio} != {tipo_ministerio}")
                                continue
                            
                            # Extrair localidade limpa
                            localidade = extrair_localidade_limpa(localidade_completa)
                            
                            # Ignorar compartilhados
                            if 'COMPARTILHADO' in nivel.upper() or 'COMPARTILHADA' in nivel.upper():
                                print(f"⏭️ Pulando: {nivel} contém COMPARTILHADO")
                                continue
                            
                            # Contar apenas os níveis válidos
                            if nivel in niveis_validos:
                                dados_por_localidade[localidade][nivel] += 1
                                print(f"✅ {localidade}: {nivel} (+1)")
                            else:
                                print(f"❌ Nível inválido: {nivel}")
                
                print(f"📊 Total de localidades processadas para {tipo_ministerio}: {len(dados_por_localidade)}")
                return dict(dados_por_localidade)
                
            except json.JSONDecodeError as e:
                print(f"❌ Erro ao decodificar JSON: {e}")
                print(f"📝 Resposta recebida: {resp.text[:500]}...")
                return {}
        
        else:
            print(f"❌ Erro na requisição: {resp.status_code}")
            print(f"📝 Resposta: {resp.text[:500]}...")
            return {}
        
    except Exception as e:
        print(f"⚠️ Erro ao obter candidatos: {e}")
        import traceback
        traceback.print_exc()
        return {}

def obter_grupos_musicais_por_localidade_e_tipo(session, tipo_ministerio):
    """
    Obtém dados de grupos musicais por localidade
    Captura apenas: RJM / OFICIALIZADO(A) e OFICIALIZADO(A)
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
            'order[0][column]': '0',
            'order[0][dir]': 'asc',
            'start': '0',
            'length': '10000',
            'search[value]': '',
            'search[regex]': 'false'
        }
        
        print(f"🎵 Obtendo grupos musicais para {tipo_ministerio.lower()}s...")
        resp = session.post(URL_LISTAGEM_GRUPOS, headers=headers, data=form_data, timeout=60)
        
        print(f"🎵 Status da requisição: {resp.status_code}")
        
        # Níveis válidos para grupos (colunas H-I)
        niveis_grupos = {
            'RJM / OFICIALIZADO(A)': 0,
            'OFICIALIZADO(A)': 0
        }
        
        dados_grupos_por_localidade = defaultdict(lambda: niveis_grupos.copy())
        
        if resp.status_code == 200:
            try:
                data = resp.json()
                print(f"🎵 JSON recebido com {len(data.get('data', []))} registros")
                
                if 'data' in data and isinstance(data['data'], list):
                    for record in data['data']:
                        if isinstance(record, list) and len(record) >= 5:
                            # Estrutura: [id, nome, localidade, ministério, nível, instrumento, tom]
                            localidade = record[2]
                            ministerio = record[3]
                            nivel = record[4]
                            
                            print(f"🎵 Processando grupo: {ministerio} | {nivel} | {localidade}")
                            
                            # Filtrar por tipo de ministério
                            if ministerio != tipo_ministerio:
                                print(f"⏭️ Pulando grupo: ministério {ministerio} != {tipo_ministerio}")
                                continue
                            
                            # Ignorar compartilhados
                            if 'COMPARTILHADO' in nivel.upper() or 'COMPARTILHADA' in nivel.upper():
                                print(f"⏭️ Pulando grupo: {nivel} contém COMPARTILHADO")
                                continue
                            
                            # Contar apenas os níveis válidos para grupos
                            if nivel in niveis_grupos:
                                dados_grupos_por_localidade[localidade][nivel] += 1
                                print(f"✅ Grupo {localidade}: {nivel} (+1)")
                            else:
                                print(f"❌ Nível de grupo inválido: {nivel}")
                
                print(f"🎵 Total de localidades processadas nos grupos para {tipo_ministerio}: {len(dados_grupos_por_localidade)}")
                return dict(dados_grupos_por_localidade)
                
            except json.JSONDecodeError as e:
                print(f"❌ Erro ao decodificar JSON dos grupos: {e}")
                return {}
        
        else:
            print(f"❌ Erro na requisição dos grupos: {resp.status_code}")
            return {}
        
    except Exception as e:
        print(f"⚠️ Erro ao obter grupos musicais: {e}")
        import traceback
        traceback.print_exc()
        return {}

def extrair_cookies_playwright(pagina):
    """
    Extrai cookies do Playwright para usar em requests
    """
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def gerar_relatorio_por_tipo(dados_candidatos, dados_grupos, tipo_ministerio):
    """
    Gera o relatório para um tipo específico (MÚSICO ou ORGANISTA)
    """
    # Headers para o relatório
    headers = [
        "Localidade",
        "CANDIDATO(A)",
        "RJM / ENSAIO", 
        "ENSAIO",
        "RJM",
        "RJM / CULTO OFICIAL",
        "CULTO OFICIAL",
        "RJM / OFICIALIZADO(A)",
        "OFICIALIZADO(A)"
    ]
    
    # Combinar todas as localidades
    todas_localidades = set(dados_candidatos.keys()) | set(dados_grupos.keys())
    
    relatorio = []
    
    for localidade in sorted(todas_localidades):
        # Dados dos candidatos (colunas B-G)
        contadores_candidatos = dados_candidatos.get(localidade, {
            'CANDIDATO(A)': 0,
            'RJM / ENSAIO': 0,
            'ENSAIO': 0,
            'RJM': 0,
            'RJM / CULTO OFICIAL': 0,
            'CULTO OFICIAL': 0
        })
        
        # Dados dos grupos (colunas H-I)
        contadores_grupos = dados_grupos.get(localidade, {
            'RJM / OFICIALIZADO(A)': 0,
            'OFICIALIZADO(A)': 0
        })
        
        linha = [
            localidade,
            contadores_candidatos['CANDIDATO(A)'],
            contadores_candidatos['RJM / ENSAIO'],
            contadores_candidatos['ENSAIO'],
            contadores_candidatos['RJM'],
            contadores_candidatos['RJM / CULTO OFICIAL'],
            contadores_candidatos['CULTO OFICIAL'],
            contadores_grupos['RJM / OFICIALIZADO(A)'],
            contadores_grupos['OFICIALIZADO(A)']
        ]
        relatorio.append(linha)
    
    return headers, relatorio

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

        # Navegar para as páginas para garantir contexto
        print("📄 Navegando para listagem de alunos...")
        pagina.goto("https://musical.congregacao.org.br/alunos")
        pagina.wait_for_timeout(1000)
        
        print("📄 Navegando para listagem de grupos...")
        pagina.goto("https://musical.congregacao.org.br/grp_musical")
        pagina.wait_for_timeout(1000)

        # Atualizar cookies após navegação
        cookies_dict = extrair_cookies_playwright(pagina)
        session.cookies.update(cookies_dict)

        # === PROCESSAMENTO PARA MÚSICOS ===
        print("\n" + "="*60)
        print("🎸 PROCESSANDO DADOS PARA MÚSICOS")
        print("="*60)
        
        dados_candidatos_musicos = obter_candidatos_por_localidade_e_tipo(session, "MÚSICO")
        dados_grupos_musicos = obter_grupos_musicais_por_localidade_e_tipo(session, "MÚSICO")
        
        headers_musicos, relatorio_musicos = gerar_relatorio_por_tipo(
            dados_candidatos_musicos, dados_grupos_musicos, "MÚSICO"
        )

        # === PROCESSAMENTO PARA ORGANISTAS ===
        print("\n" + "="*60)
        print("🎹 PROCESSANDO DADOS PARA ORGANISTAS")
        print("="*60)
        
        dados_candidatos_organistas = obter_candidatos_por_localidade_e_tipo(session, "ORGANISTA")
        dados_grupos_organistas = obter_grupos_musicais_por_localidade_e_tipo(session, "ORGANISTA")
        
        headers_organistas, relatorio_organistas = gerar_relatorio_por_tipo(
            dados_candidatos_organistas, dados_grupos_organistas, "ORGANISTA"
        )

        # === EXIBIÇÃO DOS RESULTADOS ===
        print(f"\n🎸 RELATÓRIO DE MÚSICOS POR LOCALIDADE:")
        print("="*150)
        print(f"{'Localidade':<25} {'CAND':<5} {'R/E':<5} {'ENS':<5} {'RJM':<5} {'R/C':<5} {'CULTO':<6} {'R/OF':<5} {'OFIC':<5}")
        print("-"*150)
        
        for linha in relatorio_musicos[:10]:  # Mostrar apenas as primeiras 10
            print(f"{linha[0]:<25} {linha[1]:<5} {linha[2]:<5} {linha[3]:<5} {linha[4]:<5} {linha[5]:<5} {linha[6]:<6} {linha[7]:<5} {linha[8]:<5}")
        
        if len(relatorio_musicos) > 10:
            print(f"... e mais {len(relatorio_musicos) - 10} localidades")

        print(f"\n🎹 RELATÓRIO DE ORGANISTAS POR LOCALIDADE:")
        print("="*150)
        print(f"{'Localidade':<25} {'CAND':<5} {'R/E':<5} {'ENS':<5} {'RJM':<5} {'R/C':<5} {'CULTO':<6} {'R/OF':<5} {'OFIC':<5}")
        print("-"*150)
        
        for linha in relatorio_organistas[:10]:  # Mostrar apenas as primeiras 10
            print(f"{linha[0]:<25} {linha[1]:<5} {linha[2]:<5} {linha[3]:<5} {linha[4]:<5} {linha[5]:<5} {linha[6]:<6} {linha[7]:<5} {linha[8]:<5}")
        
        if len(relatorio_organistas) > 10:
            print(f"... e mais {len(relatorio_organistas) - 10} localidades")

        # === ENVIO PARA GOOGLE SHEETS ===
        
        # Preparar dados com headers
        dados_musicos_com_headers = [headers_musicos] + relatorio_musicos
        dados_organistas_com_headers = [headers_organistas] + relatorio_organistas
        
        # Enviar dados dos músicos
        try:
            print(f"\n📤 Enviando {len(relatorio_musicos)} localidades de MÚSICOS para Google Sheets...")
            body_musicos = {
                "tipo": "relatorio_musicos_localidade",
                "dados": dados_musicos_com_headers,
                "incluir_headers": True
            }
            resposta = requests.post(URL_APPS_SCRIPT, json=body_musicos, timeout=60)
            print(f"✅ Status do envio (músicos): {resposta.status_code}")
        except Exception as e:
            print(f"❌ Erro no envio (músicos): {e}")

        # Enviar dados dos organistas
        try:
            print(f"\n📤 Enviando {len(relatorio_organistas)} localidades de ORGANISTAS para Google Sheets...")
            body_organistas = {
                "tipo": "relatorio_organistas_localidade",
                "dados": dados_organistas_com_headers,
                "incluir_headers": True
            }
            resposta = requests.post(URL_APPS_SCRIPT, json=body_organistas, timeout=60)
            print(f"✅ Status do envio (organistas): {resposta.status_code}")
        except Exception as e:
            print(f"❌ Erro no envio (organistas): {e}")

        navegador.close()
        
        tempo_total = time.time() - tempo_inicio
        print(f"\n🎯 Concluído!")
        print(f"🎸 Músicos: {len(relatorio_musicos)} localidades processadas")
        print(f"🎹 Organistas: {len(relatorio_organistas)} localidades processadas")
        print(f"⏱️ Tempo total: {tempo_total:.1f} segundos")

if __name__ == "__main__":
    main()
