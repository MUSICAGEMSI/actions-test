from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time
from bs4 import BeautifulSoup
import json

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"

# URL do Apps Script
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwHlf2VUjfwX7KcHGKgvf0v2FlXZ7Y53ubkfcIPxihSb3VVUzbyzlBr5Fyx0OHrxwBx/exec'

def carregar_ids_do_apps_script():
    """
    Busca IDs de ministros que têm instrumentos (colunas D/E/F/G com "{")
    """
    print("\n📂 Buscando IDs de ministros via Apps Script...")
    
    try:
        url = f"{URL_APPS_SCRIPT}?acao=obter_ids"
        response = requests.get(url, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Erro HTTP {response.status_code}")
            return []
        
        dados = response.json()
        
        if dados['status'] != 'sucesso':
            print(f"❌ Erro: {dados.get('mensagem', 'Erro desconhecido')}")
            return []
        
        ids = dados['ids']
        print(f"✅ {dados['total_ids']} IDs de ministros encontrados!")
        print(f"   Fonte: {dados.get('fonte', 'N/A')}")
        
        if dados.get('amostra'):
            print(f"\n   📋 Amostra dos IDs encontrados:")
            for item in dados['amostra'][:5]:
                print(f"      • ID {item['id']} (linha {item['linha']}) - Instrumentos em: {item['tem_em']}")
        
        return ids
        
    except Exception as e:
        print(f"❌ Erro ao carregar IDs: {e}")
        return []

def extrair_dados_ministro(html_content, pessoa_id):
    """
    Extrai dados do formulário de ministério
    BASEADO NO HTML REAL DO SISTEMA
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Verifica se o formulário existe
        form = soup.find('form', id='ministerio')
        if not form:
            return None
        
        dados = {
            'ID_Ministro': pessoa_id,
            'Nome': '',
            'Email': '',
            'ID_Localidade': '',
            'Comum': '',
            'Ministerio': '',
            'Telefone_Celular': '',
            'Telefone_Fixo': '',
            'Cadastrado_em': '',
            'Cadastrado_por': '',
        }
        
        # ========== NOME ==========
        nome_input = form.find('input', {'name': 'nome'})
        if nome_input and nome_input.get('value'):
            dados['Nome'] = nome_input.get('value', '').strip()
        
        # Se não tem nome, não é válido
        if not dados['Nome']:
            return None
        
        # ========== EMAIL ==========
        email_input = form.find('input', {'name': 'email'})
        if email_input:
            dados['Email'] = email_input.get('value', '').strip()
        
        # ========== COMUM/LOCALIDADE ==========
        # IMPORTANTE: No HTML real, NÃO há option selected
        # Precisamos buscar pelo hidden input "id" que indica qual igreja está cadastrada
        # OU verificar via script/hidden fields
        
        # Primeiro tenta pegar do hidden input (se existir)
        id_hidden = form.find('input', {'name': 'id', 'type': 'hidden'})
        
        # Tenta encontrar option selected (caso tenha)
        comum_select = form.find('select', {'name': 'id_igreja'})
        if comum_select:
            # Verifica se tem alguma option com selected
            comum_option = comum_select.find('option', selected=True)
            if comum_option and comum_option.get('value') and comum_option.get('value') != '':
                dados['ID_Localidade'] = comum_option.get('value', '').strip()
                dados['Comum'] = comum_option.get_text(strip=True)
        
        # ========== MINISTÉRIO/CARGO ==========
        cargo_select = form.find('select', {'id': 'id_cargo'})
        if cargo_select:
            cargo_option = cargo_select.find('option', selected=True)
            if cargo_option and cargo_option.get('value') and cargo_option.get('value') != '':
                dados['Ministerio'] = cargo_option.get_text(strip=True)
        
        # ========== TELEFONES ==========
        
        # Telefone Celular
        telefone_input = form.find('input', {'id': 'telefone', 'name': 'telefone'})
        if telefone_input:
            dados['Telefone_Celular'] = telefone_input.get('value', '').strip()
        
        # Telefone Fixo
        telefone2_input = form.find('input', {'id': 'telefone2', 'name': 'telefone2'})
        if telefone2_input:
            dados['Telefone_Fixo'] = telefone2_input.get('value', '').strip()
        
        # ========== HISTÓRICO ==========
        historico_div = soup.find('div', id='collapseOne')
        if historico_div:
            paragrafos = historico_div.find_all('p')
            for p in paragrafos:
                texto = p.get_text(strip=True)
                
                # Cadastrado em: 29/08/2021 22:47:50 por: RONIE RODRIGUES DE OLIVEIRA
                if 'Cadastrado em:' in texto:
                    texto_limpo = texto.replace('Cadastrado em:', '').strip()
                    partes = texto_limpo.split('por:')
                    if len(partes) >= 2:
                        dados['Cadastrado_em'] = partes[0].strip()
                        dados['Cadastrado_por'] = partes[1].strip()
        
        return dados
        
    except Exception as e:
        print(f"      ⚠️  Erro na extração: {e}")
        return None

def buscar_dados_completos_na_planilha_membros(ids_ministros):
    """
    OPCIONAL: Busca dados completos da planilha Membros
    para complementar as informações
    """
    print("\n📊 Buscando dados complementares na planilha Membros...")
    try:
        url = f"{URL_APPS_SCRIPT}?acao=obter_dados_membros"
        payload = {"ids": ids_ministros}
        response = requests.post(url, json=payload, timeout=60)
        
        if response.status_code == 200:
            dados = response.json()
            if dados.get('status') == 'sucesso':
                print(f"✅ Dados complementares carregados!")
                return dados.get('membros', {})
        
        print("⚠️  Não foi possível carregar dados complementares")
        return {}
        
    except Exception as e:
        print(f"⚠️  Erro ao buscar dados complementares: {e}")
        return {}

def main():
    tempo_inicio = time.time()
    
    print("=" * 80)
    print("🎵 COLETOR DE MINISTROS - SISTEMA MUSICAL")
    print("=" * 80)
    
    # Carregar IDs
    ids_ministros = carregar_ids_do_apps_script()
    
    if not ids_ministros:
        print("\n❌ Nenhum ID para processar.")
        return
    
    print(f"\n📊 Total de ministros a processar: {len(ids_ministros)}")
    
    # Login via Playwright
    print("\n🔐 Realizando login...")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        try:
            pagina.goto(URL_INICIAL, timeout=30000)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado com sucesso!")
        except PlaywrightTimeoutError:
            print("❌ Falha no login. Verifique as credenciais.")
            navegador.close()
            return
        
        # Extrair cookies para requests
        cookies = pagina.context.cookies()
        cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # Processar ministros
        print(f"\n{'=' * 80}")
        print("🔄 Iniciando coleta de dados...")
        print(f"{'=' * 80}\n")
        
        resultado = []
        processados = 0
        sucesso = 0
        erros = 0
        
        for i, pessoa_id in enumerate(ids_ministros, 1):
            processados += 1
            
            try:
                url = f"https://musical.congregacao.org.br/ministros/editar/{pessoa_id}"
                
                # Primeiros 20 com Playwright para garantir
                if i <= 20:
                    pagina.goto(url, wait_until='domcontentloaded', timeout=15000)
                    time.sleep(0.5)
                    html_content = pagina.content()
                else:
                    # Resto com requests (mais rápido)
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }
                    resp = session.get(url, headers=headers, timeout=10)
                    html_content = resp.text
                
                dados = extrair_dados_ministro(html_content, pessoa_id)
                
                if dados:
                    sucesso += 1
                    
                    # Monta linha com os dados
                    linha = [
                        dados['ID_Ministro'],
                        dados['Nome'],
                        dados['Email'],
                        dados['ID_Localidade'],
                        dados['Comum'],
                        dados['Ministerio'],
                        dados['Telefone_Celular'],
                        dados['Telefone_Fixo'],
                        dados['Cadastrado_em'],
                        dados['Cadastrado_por'],
                        'Coletado',
                        time.strftime('%d/%m/%Y %H:%M:%S')
                    ]
                    
                    resultado.append(linha)
                    
                    # Log resumido
                    info_ministro = f"[{i}/{len(ids_ministros)}] ✓ ID {pessoa_id}: {dados['Nome'][:40]}"
                    if dados['Ministerio']:
                        info_ministro += f" | {dados['Ministerio']}"
                    if dados['Comum']:
                        info_ministro += f" | {dados['Comum']}"
                    print(info_ministro)
                    
                else:
                    erros += 1
                    linha_vazia = [pessoa_id, '', '', '', '', '', '', '', '', '', 'Não encontrado', time.strftime('%d/%m/%Y %H:%M:%S')]
                    resultado.append(linha_vazia)
                    print(f"[{i}/{len(ids_ministros)}] ✗ ID {pessoa_id}: Não encontrado")
                
                # Progress report a cada 25
                if processados % 25 == 0:
                    tempo_decorrido = time.time() - tempo_inicio
                    print(f"\n{'-' * 80}")
                    print(f"📈 PROGRESSO: {processados}/{len(ids_ministros)} | ✓ {sucesso} | ✗ {erros} | ⏱️  {tempo_decorrido:.1f}s")
                    print(f"{'-' * 80}\n")
                
                # Delay entre requisições
                if i > 20:
                    time.sleep(0.15)
                
            except Exception as e:
                erros += 1
                linha_vazia = [pessoa_id, '', '', '', '', '', '', '', '', '', f'Erro: {str(e)[:50]}', time.strftime('%d/%m/%Y %H:%M:%S')]
                resultado.append(linha_vazia)
                print(f"[{i}/{len(ids_ministros)}] ⚠️  ID {pessoa_id}: Erro - {str(e)[:50]}")
        
        navegador.close()
    
    # Resumo final
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'=' * 80}")
    print("✅ COLETA FINALIZADA")
    print(f"{'=' * 80}")
    print(f"📊 Total processado: {processados}")
    print(f"✓  Sucesso: {sucesso}")
    print(f"✗  Erros: {erros}")
    print(f"⏱️  Tempo total: {tempo_total/60:.2f} minutos")
    print(f"{'=' * 80}\n")
    
    # Preparar payload
    cabecalho = [
        "ID_Ministro",
        "Nome",
        "Email",
        "ID_Localidade",
        "Comum",
        "Ministerio",
        "Telefone_Celular",
        "Telefone_Fixo",
        "Cadastrado_em",
        "Cadastrado_por",
        "Status_Coleta",
        "Data_Coleta"
    ]
    
    payload = {
        "tipo": "ministerio",
        "relatorio_formatado": [cabecalho] + resultado,
        "metadata": {
            "total_processados": processados,
            "sucesso": sucesso,
            "erros": erros,
            "tempo_minutos": round(tempo_total/60, 2),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "fonte": "IDs de Membros com instrumentos (colunas D/E/F/G com '{')"
        }
    }
    
    # Backup local
    backup_file = f"backup_ministerio_{time.strftime('%Y%m%d_%H%M%S')}.json"
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo: {backup_file}")
    
    # Enviar para Google Sheets
    print("\n📤 Enviando dados para Google Sheets...")
    try:
        resposta = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
        
        if resposta.status_code == 200:
            resp_json = resposta.json()
            print(f"✅ Dados enviados com sucesso!")
            print(f"   Status: {resp_json.get('status', 'N/A')}")
            print(f"   Linhas escritas: {resp_json.get('linhas_escritas', 'N/A')}")
            print(f"   Planilha: https://docs.google.com/spreadsheets/d/{resp_json.get('planilha', '')}")
        else:
            print(f"⚠️  Status HTTP: {resposta.status_code}")
            print(f"   Resposta: {resposta.text[:200]}")
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
        print(f"   Os dados estão salvos no backup local: {backup_file}")
    
    print(f"\n{'=' * 80}")
    print("🎉 PROCESSO CONCLUÍDO!")
    print(f"{'=' * 80}\n")

if __name__ == "__main__":
    main()
