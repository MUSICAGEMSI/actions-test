from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time
from bs4 import BeautifulSoup
import json
from datetime import datetime

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"

# URL do seu Apps Script
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbyw2E0QH0ucHRdCMNOY_La7r4ElK6xcf0OWlnQGa9w7yCcg82mG_bJV_5fxbhuhbfuY/exec'

def gerar_timestamp():
    """
    Gera timestamp no formato DD_MM_YYYY-HH:MM
    """
    return datetime.now().strftime('%d_%m_%Y-%H:%M')

def carregar_ids_do_apps_script():
    """
    Busca IDs únicos direto do Apps Script
    """
    print("\n📂 Buscando IDs únicos via Apps Script...")
    
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
        print(f"📊 Faixa: {dados['faixa']['menor']} até {dados['faixa']['maior']}")
        
        return ids
        
    except requests.exceptions.Timeout:
        print("❌ Timeout ao conectar com Apps Script")
        print("💡 Verifique se a URL está correta e se o deploy está ativo")
        return []
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro de conexão: {e}")
        return []
    except json.JSONDecodeError:
        print("❌ Erro ao decodificar resposta JSON")
        print(f"Resposta recebida: {response.text[:200]}")
        return []
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return []

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright e retorna como dicionário"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def coletar_dados_turma(session, turma_id, pagina_playwright=None):
    """
    Coleta todos os dados de uma turma específica
    Usa Playwright quando fornecido (para aguardar JS), senão usa Requests
    """
    try:
        url = f"https://musical.congregacao.org.br/turmas/editar/{turma_id}"
        
        if pagina_playwright:
            pagina_playwright.goto(url, wait_until='networkidle')
            
            try:
                pagina_playwright.wait_for_selector('#id_responsavel option[selected]', timeout=5000)
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
        
        form = soup.find('form', id='turmas')
        if not form:
            return None
        
        dados = {
            'id_turma': turma_id,
            'curso': '',
            'descricao': '',
            'comum': '',
            'dia_semana': '',
            'data_inicio': '',
            'data_encerramento': '',
            'hora_inicio': '',
            'hora_termino': '',
            'responsavel_1': '',
            'responsavel_2': '',
            'destinado_ao': '',
            'ativo': 'Não',
            'cadastrado_em': '',
            'cadastrado_por': '',
            'atualizado_em': '',
            'atualizado_por': ''
        }
        
        # Curso
        curso_select = soup.find('select', {'name': 'id_curso'})
        if curso_select:
            curso_option = curso_select.find('option', selected=True)
            if curso_option:
                dados['curso'] = curso_option.get_text(strip=True)
        
        # Descrição
        descricao_input = soup.find('input', {'name': 'descricao'})
        if descricao_input:
            dados['descricao'] = descricao_input.get('value', '').strip()
        
        # Comum
        comum_select = soup.find('select', {'name': 'id_igreja'})
        if comum_select:
            comum_option = comum_select.find('option', selected=True)
            if comum_option:
                texto_completo = comum_option.get_text(strip=True)
                dados['comum'] = texto_completo.split('|')[0].strip()
        
        # Dia da Semana
        dia_select = soup.find('select', {'name': 'dia_semana'})
        if dia_select:
            dia_option = dia_select.find('option', selected=True)
            if dia_option:
                dados['dia_semana'] = dia_option.get_text(strip=True)
        
        # Data de Início
        dt_inicio_input = soup.find('input', {'name': 'dt_inicio'})
        if dt_inicio_input:
            dados['data_inicio'] = dt_inicio_input.get('value', '').strip()
        
        # Data de Encerramento
        dt_fim_input = soup.find('input', {'name': 'dt_fim'})
        if dt_fim_input:
            dados['data_encerramento'] = dt_fim_input.get('value', '').strip()
        
        # Hora de Início
        hr_inicio_input = soup.find('input', {'name': 'hr_inicio'})
        if hr_inicio_input:
            hora_completa = hr_inicio_input.get('value', '').strip()
            dados['hora_inicio'] = hora_completa[:5] if hora_completa else ''
        
        # Hora de Término
        hr_fim_input = soup.find('input', {'name': 'hr_fim'})
        if hr_fim_input:
            hora_completa = hr_fim_input.get('value', '').strip()
            dados['hora_termino'] = hora_completa[:5] if hora_completa else ''
        
        # Responsável 1
        resp1_select = soup.find('select', {'id': 'id_responsavel'})
        if resp1_select:
            resp1_option = resp1_select.find('option', selected=True)
            if resp1_option:
                texto_completo = resp1_option.get_text(strip=True)
                dados['responsavel_1'] = texto_completo.split(' - ')[0].strip()
        
        # Responsável 2
        resp2_select = soup.find('select', {'id': 'id_responsavel2'})
        if resp2_select:
            resp2_option = resp2_select.find('option', selected=True)
            if resp2_option:
                texto_completo = resp2_option.get_text(strip=True)
                dados['responsavel_2'] = texto_completo.split(' - ')[0].strip()
        
        # Destinado ao
        genero_select = soup.find('select', {'name': 'id_turma_genero'})
        if genero_select:
            genero_option = genero_select.find('option', selected=True)
            if genero_option:
                dados['destinado_ao'] = genero_option.get_text(strip=True)
        
        # Ativo
        status_checkbox = soup.find('input', {'name': 'status'})
        if status_checkbox and status_checkbox.has_attr('checked'):
            dados['ativo'] = 'Sim'
        
        # Histórico
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
    timestamp_execucao = gerar_timestamp()
    
    print("=" * 80)
    print("COLETOR DE DADOS DE TURMAS - SISTEMA MUSICAL")
    print(f"Execução: {timestamp_execucao}")
    print("=" * 80)
    
    # Carregar IDs únicos via Apps Script
    ids_turmas = carregar_ids_do_apps_script()
    
    if not ids_turmas:
        print("\nNenhum ID para processar. Verifique:")
        print("1. URL do Apps Script está correta")
        print("2. Deploy foi feito como Web App")
        print("3. Permissões: 'Executar como: Eu' e 'Acesso: Qualquer pessoa'")
        return
    
    print(f"\nTotal de turmas a processar: {len(ids_turmas)}")
    
    # Login via Playwright
    print("\nRealizando login...")
    
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
            print("Login realizado com sucesso!")
        except PlaywrightTimeoutError:
            print("Falha no login. Verifique as credenciais.")
            navegador.close()
            return
        
        # Extrair cookies para usar com Requests
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # Processar turmas
        resultado = []
        processadas = 0
        sucesso = 0
        erros = 0
        
        print(f"\n{'=' * 80}")
        print("Iniciando coleta de dados...")
        print(f"{'=' * 80}\n")
        
        # Estratégia híbrida:
        # - Primeiras 10: usar Playwright (para garantir Select2)
        # - Restantes: usar Requests (mais rápido)
        
        for i, turma_id in enumerate(ids_turmas, 1):
            processadas += 1
            
            # Primeiras 10 com Playwright
            if i <= 10:
                dados = coletar_dados_turma(session, turma_id, pagina_playwright=pagina)
            else:
                # Restantes com Requests (muito mais rápido)
                dados = coletar_dados_turma(session, turma_id)
            
            if dados:
                sucesso += 1
                resultado.append([
                    dados['id_turma'],
                    dados['curso'],
                    dados['descricao'],
                    dados['comum'],
                    dados['dia_semana'],
                    dados['data_inicio'],
                    dados['data_encerramento'],
                    dados['hora_inicio'],
                    dados['hora_termino'],
                    dados['responsavel_1'],
                    dados['responsavel_2'],
                    dados['destinado_ao'],
                    dados['ativo'],
                    dados['cadastrado_em'],
                    dados['cadastrado_por'],
                    dados['atualizado_em'],
                    dados['atualizado_por'],
                    'Coletado',
                    time.strftime('%d/%m/%Y %H:%M:%S')
                ])
                
                print(f"[{i}/{len(ids_turmas)}] ID {turma_id}: {dados['curso']} | {dados['descricao']} | {dados['comum']}")
            else:
                erros += 1
                resultado.append([
                    turma_id, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                    'Erro/Não encontrado', time.strftime('%d/%m/%Y %H:%M:%S')
                ])
                print(f"[{i}/{len(ids_turmas)}] ID {turma_id}: Não encontrado ou erro")
            
            # Progresso a cada 50
            if processadas % 50 == 0:
                tempo_decorrido = time.time() - tempo_inicio
                print(f"\n{'-' * 80}")
                print(f"PROGRESSO: {processadas}/{len(ids_turmas)} | Sucesso: {sucesso} | Erros: {erros} | Tempo: {tempo_decorrido:.1f}s")
                print(f"{'-' * 80}\n")
            
            # Pausa mínima entre requisições (após as primeiras 10)
            if i > 10:
                time.sleep(0.1)
        
        navegador.close()
    
    # Resumo final
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'=' * 80}")
    print("COLETA FINALIZADA!")
    print(f"{'=' * 80}")
    print(f"Total processado: {processadas}")
    print(f"Sucesso: {sucesso}")
    print(f"Erros: {erros}")
    print(f"Tempo total: {tempo_total/60:.2f} minutos")
    print(f"Velocidade média: {processadas/(tempo_total/60):.1f} turmas/min")
    print(f"{'=' * 80}\n")
    
    # Preparar envio com timestamp
    body = {
        "tipo": "dados_turmas",
        "timestamp": timestamp_execucao,  # IMPORTANTE: Envia timestamp para o Apps Script
        "dados": resultado,
        "headers": [
            "ID_Turma", "Curso", "Descricao", "Comum", "Dia_Semana",
            "Data_Inicio", "Data_Encerramento", "Hora_Inicio", "Hora_Termino",
            "Responsavel_1", "Responsavel_2", "Destinado_ao", "Ativo",
            "Cadastrado_em", "Cadastrado_por", "Atualizado_em", "Atualizado_por",
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
    
    # Salvar backup local com timestamp no nome
    backup_file = f"backup_turmas_{timestamp_execucao.replace(':', '-')}.json"
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(body, f, ensure_ascii=False, indent=2)
    print(f"💾 Backup salvo em: {backup_file}")
    
    # Enviar para Apps Script
    print("\n📤 Enviando dados para Google Sheets...")
    print(f"📊 Nova planilha será criada: Turmas_{timestamp_execucao}")
    
    try:
        resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
        
        if resposta_post.status_code == 200:
            resposta_json = resposta_post.json()
            
            if resposta_json.get('status') == 'sucesso':
                print(f"\n✅ SUCESSO! Planilha criada com sucesso!")
                print(f"📝 Nome: {resposta_json['planilha']['nome']}")
                print(f"🔗 URL: {resposta_json['planilha']['url']}")
                print(f"🆔 ID: {resposta_json['planilha']['id']}")
                
                # Salvar URL da planilha em arquivo
                with open(f"planilha_url_{timestamp_execucao.replace(':', '-')}.txt", 'w') as f:
                    f.write(f"Planilha criada em: {timestamp_execucao}\n")
                    f.write(f"Nome: {resposta_json['planilha']['nome']}\n")
                    f.write(f"URL: {resposta_json['planilha']['url']}\n")
                    f.write(f"ID: {resposta_json['planilha']['id']}\n")
            else:
                print(f"\n❌ Erro na resposta: {resposta_json.get('mensagem', 'Erro desconhecido')}")
        else:
            print(f"\n❌ Erro HTTP {resposta_post.status_code}")
            print(f"Resposta: {resposta_post.text[:300]}")
            
    except requests.exceptions.Timeout:
        print(f"\n⏱️ Timeout ao enviar dados (120s)")
        print(f"💡 Os dados foram salvos localmente em: {backup_file}")
        print(f"💡 Tente aumentar o timeout ou enviar manualmente")
    except Exception as e:
        print(f"\n❌ Erro ao enviar para Sheets: {e}")
        print(f"💾 Dados salvos localmente em: {backup_file}")
    
    print(f"\n{'=' * 80}")
    print("PROCESSO CONCLUÍDO!")
    print(f"{'=' * 80}")

if __name__ == "__main__":
    main()
