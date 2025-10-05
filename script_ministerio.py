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

# URL do Apps Script (atualize após deploy)
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxb9NPBjodXgDiax8-yV_c0YqVnUEHGv2cyeanJBnm7OsVxVjBj7M2Q_Wtc_cJZh21udw/exec'

def carregar_ids_do_apps_script():
    """
    Busca IDs únicos direto do Apps Script (membros com "{" em D, E, F ou G)
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
        print(f"✅ {dados['total_ids']} IDs únicos carregados!")
        
        if dados.get('faixa'):
            print(f"📊 Faixa: {dados['faixa']['menor']} até {dados['faixa']['maior']}")
        
        if dados.get('amostra'):
            print(f"\n📋 Amostra dos primeiros IDs:")
            for item in dados['amostra']:
                print(f"   - ID {item['id']}: {item['nome']}")
        
        return ids
        
    except requests.exceptions.Timeout:
        print("❌ Timeout ao conectar com Apps Script")
        return []
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro de conexão: {e}")
        return []
    except json.JSONDecodeError:
        print("❌ Erro ao decodificar resposta JSON")
        return []
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return []

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright e retorna como dicionário"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def coletar_dados_ministro(session, pessoa_id, pagina_playwright=None):
    """
    Coleta todos os dados de um ministro específico
    """
    try:
        url = f"https://musical.congregacao.org.br/ministros/editar/{pessoa_id}"
        
        # Usar Playwright para páginas com Select2
        if pagina_playwright:
            pagina_playwright.goto(url, wait_until='networkidle')
            
            try:
                pagina_playwright.wait_for_selector('input[name="nome"]', timeout=5000)
            except:
                pass
            
            html_content = pagina_playwright.content()
        else:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            resp = session.get(url, headers=headers, timeout=10)
            
            if resp.status_code != 200:
                return None
            
            html_content = resp.text
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Verificar se o ministro existe
        form = soup.find('form', id='pessoa')
        if not form:
            return None
        
        dados = {
            'id_pessoa': pessoa_id,
            'nome': '',
            'data_nascimento': '',
            'sexo': '',
            'comum': '',
            'endereco': '',
            'numero': '',
            'complemento': '',
            'bairro': '',
            'cidade': '',
            'uf': '',
            'cep': '',
            'telefone_1': '',
            'telefone_2': '',
            'email': '',
            'cpf': '',
            'rg': '',
            'sangue': '',
            'cargo': '',
            'instrumento_1': '',
            'tonalidade_1': '',
            'instrumento_2': '',
            'tonalidade_2': '',
            'instrumento_3': '',
            'tonalidade_3': '',
            'ativo': 'Não',
            'cadastrado_em': '',
            'cadastrado_por': '',
            'atualizado_em': '',
            'atualizado_por': ''
        }
        
        # 1. Nome
        nome_input = soup.find('input', {'name': 'nome'})
        if nome_input:
            dados['nome'] = nome_input.get('value', '').strip()
        
        # 2. Data de Nascimento
        dt_nasc_input = soup.find('input', {'name': 'dt_nascimento'})
        if dt_nasc_input:
            dados['data_nascimento'] = dt_nasc_input.get('value', '').strip()
        
        # 3. Sexo (select)
        sexo_select = soup.find('select', {'name': 'id_genero'})
        if sexo_select:
            sexo_option = sexo_select.find('option', selected=True)
            if sexo_option:
                dados['sexo'] = sexo_option.get_text(strip=True)
        
        # 4. Comum (select)
        comum_select = soup.find('select', {'name': 'id_igreja'})
        if comum_select:
            comum_option = comum_select.find('option', selected=True)
            if comum_option:
                texto_completo = comum_option.get_text(strip=True)
                dados['comum'] = texto_completo.split('|')[0].strip()
        
        # 5-11. Endereço
        endereco_input = soup.find('input', {'name': 'endereco'})
        if endereco_input:
            dados['endereco'] = endereco_input.get('value', '').strip()
        
        numero_input = soup.find('input', {'name': 'numero'})
        if numero_input:
            dados['numero'] = numero_input.get('value', '').strip()
        
        complemento_input = soup.find('input', {'name': 'complemento'})
        if complemento_input:
            dados['complemento'] = complemento_input.get('value', '').strip()
        
        bairro_input = soup.find('input', {'name': 'bairro'})
        if bairro_input:
            dados['bairro'] = bairro_input.get('value', '').strip()
        
        cidade_input = soup.find('input', {'name': 'cidade'})
        if cidade_input:
            dados['cidade'] = cidade_input.get('value', '').strip()
        
        uf_input = soup.find('input', {'name': 'uf'})
        if uf_input:
            dados['uf'] = uf_input.get('value', '').strip()
        
        cep_input = soup.find('input', {'name': 'cep'})
        if cep_input:
            dados['cep'] = cep_input.get('value', '').strip()
        
        # 12-13. Telefones
        telefone1_input = soup.find('input', {'name': 'telefone'})
        if telefone1_input:
            dados['telefone_1'] = telefone1_input.get('value', '').strip()
        
        telefone2_input = soup.find('input', {'name': 'telefone2'})
        if telefone2_input:
            dados['telefone_2'] = telefone2_input.get('value', '').strip()
        
        # 14. Email
        email_input = soup.find('input', {'name': 'email'})
        if email_input:
            dados['email'] = email_input.get('value', '').strip()
        
        # 15-16. CPF e RG
        cpf_input = soup.find('input', {'name': 'cpf'})
        if cpf_input:
            dados['cpf'] = cpf_input.get('value', '').strip()
        
        rg_input = soup.find('input', {'name': 'rg'})
        if rg_input:
            dados['rg'] = rg_input.get('value', '').strip()
        
        # 17. Tipo Sanguíneo
        sangue_select = soup.find('select', {'name': 'id_sanguineo'})
        if sangue_select:
            sangue_option = sangue_select.find('option', selected=True)
            if sangue_option:
                dados['sangue'] = sangue_option.get_text(strip=True)
        
        # 18. Cargo
        cargo_select = soup.find('select', {'name': 'id_cargo'})
        if cargo_select:
            cargo_option = cargo_select.find('option', selected=True)
            if cargo_option:
                dados['cargo'] = cargo_option.get_text(strip=True)
        
        # 19-24. Instrumentos e Tonalidades
        # Instrumento 1
        inst1_select = soup.find('select', {'id': 'id_instrumento'})
        if inst1_select:
            inst1_option = inst1_select.find('option', selected=True)
            if inst1_option:
                dados['instrumento_1'] = inst1_option.get_text(strip=True)
        
        tom1_select = soup.find('select', {'id': 'id_tom'})
        if tom1_select:
            tom1_option = tom1_select.find('option', selected=True)
            if tom1_option:
                dados['tonalidade_1'] = tom1_option.get_text(strip=True)
        
        # Instrumento 2
        inst2_select = soup.find('select', {'id': 'id_instrumento2'})
        if inst2_select:
            inst2_option = inst2_select.find('option', selected=True)
            if inst2_option:
                dados['instrumento_2'] = inst2_option.get_text(strip=True)
        
        tom2_select = soup.find('select', {'id': 'id_tom2'})
        if tom2_select:
            tom2_option = tom2_select.find('option', selected=True)
            if tom2_option:
                dados['tonalidade_2'] = tom2_option.get_text(strip=True)
        
        # Instrumento 3
        inst3_select = soup.find('select', {'id': 'id_instrumento3'})
        if inst3_select:
            inst3_option = inst3_select.find('option', selected=True)
            if inst3_option:
                dados['instrumento_3'] = inst3_option.get_text(strip=True)
        
        tom3_select = soup.find('select', {'id': 'id_tom3'})
        if tom3_select:
            tom3_option = tom3_select.find('option', selected=True)
            if tom3_option:
                dados['tonalidade_3'] = tom3_option.get_text(strip=True)
        
        # 25. Ativo (checkbox)
        status_checkbox = soup.find('input', {'name': 'status'})
        if status_checkbox and status_checkbox.has_attr('checked'):
            dados['ativo'] = 'Sim'
        
        # 26-29. Histórico (dentro do painel collapse)
        historico_div = soup.find('div', id='collapseOne')
        if historico_div:
            paragrafos = historico_div.find_all('p')
            
            for p in paragrafos:
                texto = p.get_text(strip=True)
                
                if 'Cadastrado em:' in texto:
                    partes = texto.split('por:')
                    if len(partes) >= 2:
                        dados['cadastrado_em'] = partes[0].replace('Cadastrado em:', '').strip()
                        dados['cadastrado_por'] = partes[1].strip()
                
                elif 'Atualizado em:' in texto:
                    partes = texto.split('por:')
                    if len(partes) >= 2:
                        dados['atualizado_em'] = partes[0].replace('Atualizado em:', '').strip()
                        dados['atualizado_por'] = partes[1].strip()
        
        return dados
        
    except Exception as e:
        return None

def main():
    tempo_inicio = time.time()
    
    print("=" * 80)
    print("COLETOR DE DADOS DE MINISTROS - SISTEMA MUSICAL")
    print("=" * 80)
    
    # Carregar IDs únicos via Apps Script
    ids_ministros = carregar_ids_do_apps_script()
    
    if not ids_ministros:
        print("\n❌ Nenhum ID para processar. Verifique:")
        print("1. URL do Apps Script está correta")
        print("2. Deploy foi feito como Web App")
        print("3. Permissões: 'Executar como: Eu' e 'Acesso: Qualquer pessoa'")
        print("4. Existe '{' nas colunas D, E, F ou G da aba 'Membros'")
        return
    
    print(f"\n📊 Total de ministros a processar: {len(ids_ministros)}")
    
    # Login via Playwright
    print("\n🔐 Realizando login...")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
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
            print("❌ Falha no login. Verifique as credenciais.")
            navegador.close()
            return
        
        # Extrair cookies para usar com Requests
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # Processar ministros
        resultado = []
        processadas = 0
        sucesso = 0
        erros = 0
        
        print(f"\n{'=' * 80}")
        print("🔄 Iniciando coleta de dados...")
        print(f"{'=' * 80}\n")
        
        # Estratégia híbrida:
        # - Primeiras 10: usar Playwright (para garantir Select2)
        # - Restantes: usar Requests (mais rápido)
        
        for i, pessoa_id in enumerate(ids_ministros, 1):
            processadas += 1
            
            # Primeiras 10 com Playwright
            if i <= 10:
                dados = coletar_dados_ministro(session, pessoa_id, pagina_playwright=pagina)
            else:
                # Restantes com Requests (muito mais rápido)
                dados = coletar_dados_ministro(session, pessoa_id)
            
            if dados:
                sucesso += 1
                resultado.append([
                    dados['id_pessoa'],
                    dados['nome'],
                    dados['data_nascimento'],
                    dados['sexo'],
                    dados['comum'],
                    dados['endereco'],
                    dados['numero'],
                    dados['complemento'],
                    dados['bairro'],
                    dados['cidade'],
                    dados['uf'],
                    dados['cep'],
                    dados['telefone_1'],
                    dados['telefone_2'],
                    dados['email'],
                    dados['cpf'],
                    dados['rg'],
                    dados['sangue'],
                    dados['cargo'],
                    dados['instrumento_1'],
                    dados['tonalidade_1'],
                    dados['instrumento_2'],
                    dados['tonalidade_2'],
                    dados['instrumento_3'],
                    dados['tonalidade_3'],
                    dados['ativo'],
                    dados['cadastrado_em'],
                    dados['cadastrado_por'],
                    dados['atualizado_em'],
                    dados['atualizado_por'],
                    'Coletado',
                    time.strftime('%d/%m/%Y %H:%M:%S')
                ])
                
                # Exibir resumo da linha
                resumo = f"[{i}/{len(ids_ministros)}] ID {pessoa_id}: {dados['nome']}"
                if dados['cargo']:
                    resumo += f" | {dados['cargo']}"
                if dados['instrumento_1']:
                    resumo += f" | {dados['instrumento_1']}"
                if dados['comum']:
                    resumo += f" | {dados['comum']}"
                
                print(resumo)
            else:
                erros += 1
                resultado.append([
                    pessoa_id, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                    'Erro/Não encontrado', time.strftime('%d/%m/%Y %H:%M:%S')
                ])
                print(f"[{i}/{len(ids_ministros)}] ID {pessoa_id}: ❌ Não encontrado ou erro")
            
            # Progresso a cada 25
            if processadas % 25 == 0:
                tempo_decorrido = time.time() - tempo_inicio
                print(f"\n{'-' * 80}")
                print(f"📊 PROGRESSO: {processadas}/{len(ids_ministros)} | ✅ Sucesso: {sucesso} | ❌ Erros: {erros} | ⏱️ Tempo: {tempo_decorrido:.1f}s")
                print(f"{'-' * 80}\n")
            
            # Pausa mínima entre requisições (após as primeiras 10)
            if i > 10:
                time.sleep(0.1)
        
        navegador.close()
    
    # Resumo final
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'=' * 80}")
    print("✅ COLETA FINALIZADA!")
    print(f"{'=' * 80}")
    print(f"📊 Total processado: {processadas}")
    print(f"✅ Sucesso: {sucesso}")
    print(f"❌ Erros: {erros}")
    print(f"⏱️  Tempo total: {tempo_total/60:.2f} minutos")
    print(f"⚡ Velocidade média: {processadas/(tempo_total/60):.1f} ministros/min")
    print(f"{'=' * 80}\n")
    
    # Preparar envio
    body = {
        "tipo": "dados_ministros",
        "dados": resultado,
        "headers": [
            "ID_Pessoa", "Nome", "Data_Nascimento", "Sexo", "Comum",
            "Endereco", "Numero", "Complemento", "Bairro", "Cidade",
            "UF", "CEP", "Telefone_1", "Telefone_2", "Email",
            "CPF", "RG", "Sangue", "Cargo",
            "Instrumento_1", "Tonalidade_1",
            "Instrumento_2", "Tonalidade_2",
            "Instrumento_3", "Tonalidade_3",
            "Ativo", "Cadastrado_em", "Cadastrado_por",
            "Atualizado_em", "Atualizado_por",
            "Status_Coleta", "Data_Coleta"
        ],
        "resumo": {
            "total_processadas": processadas,
            "sucesso": sucesso,
            "erros": erros,
            "tempo_minutos": round(tempo_total/60, 2),
            "velocidade_por_minuto": round(processadas/(tempo_total/60), 1)
        }
    }
    
    # Salvar backup local
    backup_file = f"backup_ministros_{time.strftime('%Y%m%d_%H%M%S')}.json"
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(body, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo em: {backup_file}")
    
    # Enviar para Apps Script
    print("\n📤 Enviando dados para Google Sheets...")
    try:
        resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
        
        if resposta_post.status_code == 200:
            resposta_json = resposta_post.json()
            print(f"✅ Dados enviados com sucesso!")
            print(f"📊 Status: {resposta_json.get('status', 'desconhecido')}")
            print(f"💬 Mensagem: {resposta_json.get('mensagem', 'N/A')}")
        else:
            print(f"⚠️  Status HTTP: {resposta_post.status_code}")
            print(f"Resposta: {resposta_post.text[:200]}")
    except Exception as e:
        print(f"❌ Erro ao enviar para Sheets: {e}")
        print(f"💾 Dados salvos localmente em: {backup_file}")
    
    print(f"\n{'=' * 80}")
    print("🎉 PROCESSO CONCLUÍDO!")
    print(f"{'=' * 80}\n")

if __name__ == "__main__":
    main()
