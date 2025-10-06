from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time
from bs4 import BeautifulSoup
import json
import re

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"

# URL do Apps Script
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbwHlf2VUjfwX7KcHGKgvf0v2FlXZ7Y53ubkfcIPxihSb3VVUzbyzlBr5Fyx0OHrxwBx/exec'

def carregar_ids_do_apps_script():
    """
    Busca IDs únicos direto do Apps Script
    """
    print("\n📂 Buscando IDs de ministros via Apps Script...")
    
    try:
        url = f"{URL_APPS_SCRIPT}?acao=obter_ids"
        response = requests.get(url, timeout=30)
        
        if response.status_code != 200:
            print(f"Erro HTTP {response.status_code}")
            return []
        
        dados = response.json()
        
        if dados['status'] != 'sucesso':
            print(f"Erro: {dados.get('mensagem', 'Erro desconhecido')}")
            return []
        
        ids = dados['ids']
        print(f"✓ {dados['total_ids']} IDs únicos carregados!")
        
        if dados.get('faixa'):
            print(f"  Faixa: {dados['faixa']['menor']} até {dados['faixa']['maior']}")
        
        if dados.get('amostra'):
            print(f"\n  Amostra dos primeiros IDs:")
            for item in dados['amostra'][:3]:
                print(f"    - ID {item['id']}: {item['nome']}")
        
        return ids
        
    except Exception as e:
        print(f"Erro ao carregar IDs: {e}")
        return []

def extrair_dados_ministro(html_content, pessoa_id):
    """
    Extrai os dados do formulário de ministério
    CORRIGIDO para o HTML compartilhado
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Verifica se o formulário existe
        form = soup.find('form', id='ministerio')
        if not form:
            return None
        
        dados = {
            'id_ministro': pessoa_id,
            'nome': '',
            'email': '',
            'comum': '',
            'ministerio': '',
            'telefone_celular': '',
            'telefone_fixo': '',
            'cadastrado_em': '',
            'cadastrado_por': ''
        }
        
        # NOME (input com name="nome")
        nome_input = form.find('input', {'name': 'nome'})
        if nome_input and nome_input.get('value'):
            dados['nome'] = nome_input.get('value', '').strip()
        
        # Se não tem nome, não é um registro válido
        if not dados['nome']:
            return None
        
        # EMAIL (input com name="email")
        email_input = form.find('input', {'name': 'email'})
        if email_input:
            dados['email'] = email_input.get('value', '').strip()
        
        # COMUM CONGREGAÇÃO (select com name="id_igreja")
        # IMPORTANTE: No HTML do ministério, o select NÃO tem option selected
        # Precisa verificar se há value no hidden input ou script
        comum_select = form.find('select', {'name': 'id_igreja'})
        if comum_select:
            # Tenta pegar option selected
            comum_option = comum_select.find('option', selected=True)
            if comum_option and comum_option.get('value'):
                dados['comum'] = comum_option.get_text(strip=True)
        
        # MINISTÉRIO/CARGO (select com id="id_cargo")
        cargo_select = form.find('select', {'id': 'id_cargo'})
        if cargo_select:
            cargo_option = cargo_select.find('option', selected=True)
            if cargo_option and cargo_option.get('value'):
                dados['ministerio'] = cargo_option.get_text(strip=True)
        
        # TELEFONE CELULAR (input com id="telefone")
        telefone_input = form.find('input', {'id': 'telefone'})
        if telefone_input:
            dados['telefone_celular'] = telefone_input.get('value', '').strip()
        
        # TELEFONE FIXO (input com id="telefone2")
        telefone2_input = form.find('input', {'id': 'telefone2'})
        if telefone2_input:
            dados['telefone_fixo'] = telefone2_input.get('value', '').strip()
        
        # HISTÓRICO (div com id="collapseOne")
        historico_div = soup.find('div', id='collapseOne')
        if historico_div:
            paragrafos = historico_div.find_all('p')
            for p in paragrafos:
                texto = p.get_text(strip=True)
                
                # Cadastrado em: 27/01/2024 23:36:03 por: CLEBER DOS SANTOS
                if 'Cadastrado em:' in texto:
                    # Remove "Cadastrado em:" e divide por "por:"
                    texto_limpo = texto.replace('Cadastrado em:', '').strip()
                    partes = texto_limpo.split('por:')
                    if len(partes) >= 2:
                        dados['cadastrado_em'] = partes[0].strip()
                        dados['cadastrado_por'] = partes[1].strip()
        
        return dados
        
    except Exception as e:
        return None

def main():
    tempo_inicio = time.time()
    
    print("=" * 80)
    print("COLETOR DE MINISTÉRIO - SISTEMA MUSICAL")
    print("=" * 80)
    
    # Carregar IDs
    ids_ministros = carregar_ids_do_apps_script()
    
    if not ids_ministros:
        print("\nNenhum ID para processar.")
        return
    
    print(f"\n📊 Total de ministros a processar: {len(ids_ministros)}")
    
    # Login via Playwright
    print("\n🔐 Realizando login...")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.goto(URL_INICIAL)
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')
        
        try:
            pagina.wait_for_selector("nav", timeout=15000)
            print("✓ Login realizado com sucesso!")
        except PlaywrightTimeoutError:
            print("Falha no login. Verifique as credenciais.")
            navegador.close()
            return
        
        # Extrair cookies
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
        
        # Teste com os primeiros 5 usando Playwright para debug
        print("DEBUG: Testando primeiros 5 com Playwright...\n")
        
        for i, pessoa_id in enumerate(ids_ministros[:5], 1):
            try:
                url = f"https://musical.congregacao.org.br/ministros/editar/{pessoa_id}"
                print(f"[{i}/5] Acessando: {url}")
                
                pagina.goto(url, wait_until='domcontentloaded', timeout=15000)
                time.sleep(1)
                
                html_content = pagina.content()
                
                # Debug: verifica se tem o formulário
                if 'id="ministerio"' in html_content:
                    print(f"  ✓ Formulário encontrado")
                else:
                    print(f"  ✗ Formulário NÃO encontrado")
                    print(f"  HTML Preview: {html_content[:500]}")
                
                # Tenta extrair
                dados = extrair_dados_ministro(html_content, pessoa_id)
                
                if dados:
                    print(f"  ✓ Nome: {dados['nome']}")
                    print(f"  ✓ Ministério: {dados['ministerio']}")
                    print(f"  ✓ Comum: {dados['comum']}")
                else:
                    print(f"  ✗ Falha na extração")
                
                print()
                
            except Exception as e:
                print(f"  ✗ Erro: {e}\n")
        
        print("\n" + "=" * 80)
        print("FIM DO DEBUG - Pressione Ctrl+C para parar")
        print("Ou aguarde para processar todos os IDs...")
        print("=" * 80 + "\n")
        
        time.sleep(3)
        
        # Processa todos os IDs
        for i, pessoa_id in enumerate(ids_ministros, 1):
            processados += 1
            
            try:
                url = f"https://musical.congregacao.org.br/ministros/editar/{pessoa_id}"
                
                # Primeiros 10 com Playwright
                if i <= 10:
                    pagina.goto(url, wait_until='domcontentloaded', timeout=15000)
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
                    resultado.append([
                        dados['id_ministro'],
                        dados['nome'],
                        dados['email'],
                        dados['comum'],
                        dados['ministerio'],
                        dados['telefone_celular'],
                        dados['telefone_fixo'],
                        dados['cadastrado_em'],
                        dados['cadastrado_por'],
                        'Coletado',
                        time.strftime('%d/%m/%Y %H:%M:%S')
                    ])
                    
                    resumo = f"[{i}/{len(ids_ministros)}] ID {pessoa_id}: {dados['nome'][:40]}"
                    if dados['ministerio']:
                        resumo += f" | {dados['ministerio']}"
                    print(resumo)
                else:
                    erros += 1
                    resultado.append([
                        pessoa_id, '', '', '', '', '', '', '', '',
                        'Não encontrado', time.strftime('%d/%m/%Y %H:%M:%S')
                    ])
                    print(f"[{i}/{len(ids_ministros)}] ID {pessoa_id}: Não encontrado")
                
                if processados % 25 == 0:
                    tempo_decorrido = time.time() - tempo_inicio
                    print(f"\n{'-' * 80}")
                    print(f"PROGRESSO: {processados}/{len(ids_ministros)} | Sucesso: {sucesso} | Erros: {erros} | Tempo: {tempo_decorrido:.1f}s")
                    print(f"{'-' * 80}\n")
                
                if i > 10:
                    time.sleep(0.1)
                
            except Exception as e:
                erros += 1
                resultado.append([
                    pessoa_id, '', '', '', '', '', '', '', '',
                    f'Erro: {str(e)[:30]}', time.strftime('%d/%m/%Y %H:%M:%S')
                ])
        
        navegador.close()
    
    # Resumo final
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'=' * 80}")
    print("COLETA FINALIZADA")
    print(f"{'=' * 80}")
    print(f"Total processado: {processados}")
    print(f"Sucesso: {sucesso}")
    print(f"Erros: {erros}")
    print(f"Tempo total: {tempo_total/60:.2f} minutos")
    print(f"{'=' * 80}\n")
    
    # Preparar payload
    payload = {
        "tipo": "ministerio",
        "relatorio_formatado": [
            [
                "ID_MINISTRO", "NOME", "EMAIL", "COMUM", "MINISTERIO",
                "TELEFONE_CELULAR", "TELEFONE_FIXO", 
                "CADASTRADO_EM", "CADASTRADO_POR",
                "STATUS_COLETA", "DATA_COLETA"
            ],
            *resultado
        ],
        "metadata": {
            "total_processados": processados,
            "sucesso": sucesso,
            "erros": erros,
            "tempo_minutos": round(tempo_total/60, 2),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    }
    
    # Backup local
    backup_file = f"backup_ministerio_{time.strftime('%Y%m%d_%H%M%S')}.json"
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"Backup salvo: {backup_file}")
    
    # Enviar para Apps Script
    print("\nEnviando para Google Sheets...")
    try:
        resposta = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
        
        if resposta.status_code == 200:
            resp_json = resposta.json()
            print(f"Dados enviados com sucesso!")
            print(f"Status: {resp_json.get('status', 'N/A')}")
        else:
            print(f"Status HTTP: {resposta.status_code}")
    except Exception as e:
        print(f"Erro ao enviar: {e}")
    
    print(f"\nPROCESSO CONCLUÍDO\n")

if __name__ == "__main__":
    main()
