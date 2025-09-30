from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbyw2E0QH0ucHRdCMNOY_La7r4ElK6xcf0OWlnQGa9w7yCcg82mG_bJV_5fxbhuhbfuY/exec'

# Google Sheets
SPREADSHEET_ID = '1bL6_Ai2DRROUYeAnqOgxNfqX1jAGDJIDgzZNi82jmdo'
SHEET_NAME = 'Histórico de Aulas'

def extrair_ids_turmas_do_sheets():
    """
    Lê o Google Sheets via Apps Script e extrai IDs únicos de turmas
    Retorna lista de IDs ordenados
    """
    try:
        print("\n[PASSO 1] Lendo Google Sheets para extrair IDs de turmas...")
        print(f"  📊 Planilha: {SPREADSHEET_ID}")
        print(f"  📑 Aba: {SHEET_NAME}\n")
        
        # Fazer requisição ao Apps Script para ler dados
        params = {
            'action': 'lerDados',
            'spreadsheetId': SPREADSHEET_ID,
            'sheetName': SHEET_NAME,
            'coluna': 'ID_Turma'  # Coluna específica que queremos
        }
        
        response = requests.get(URL_APPS_SCRIPT, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"✗ Erro ao acessar planilha: Status {response.status_code}")
            return None
        
        data = response.json()
        
        if data.get('status') == 'error':
            print(f"✗ Erro retornado pelo Apps Script: {data.get('message')}")
            return None
        
        # Extrair IDs únicos
        ids_turmas = set()
        valores = data.get('dados', [])
        total_linhas = len(valores)
        
        for valor in valores:
            if valor:  # Se não for vazio
                try:
                    id_turma = int(float(valor))
                    ids_turmas.add(id_turma)
                except (ValueError, TypeError):
                    continue
        
        # Converter para lista ordenada
        ids_lista = sorted(list(ids_turmas))
        
        print(f"✓ Planilha lida com sucesso!")
        print(f"  Total de linhas: {total_linhas}")
        print(f"  Turmas únicas: {len(ids_lista)}")
        
        if ids_lista:
            print(f"  Faixa de IDs: {min(ids_lista)} até {max(ids_lista)}")
            print(f"  Primeiros IDs: {ids_lista[:10]}")
        
        return ids_lista
        
    except Exception as e:
        print(f"✗ Erro ao ler Google Sheets: {e}")
        print("\n  VERIFIQUE:")
        print("  1. O Apps Script está configurado corretamente?")
        print("  2. A função 'lerDados' existe no Apps Script?")
        print("  3. O ID da planilha está correto?")
        return None
def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def coletar_dados_turma(session, turma_id):
    """
    Coleta dados completos de uma turma específica
    Retorna dict com todas as informações ou None se houver erro
    """
    try:
        url = f"https://musical.congregacao.org.br/turmas/editar/{turma_id}"
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        resp = session.get(url, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            return None
            
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Extrair CURSO (select id_curso)
        curso = ""
        curso_select = soup.find('select', {'name': 'id_curso'})
        if curso_select:
            selected_option = curso_select.find('option', selected=True)
            if selected_option:
                curso = selected_option.get_text(strip=True)
        
        # Extrair DESCRIÇÃO COMPLETA DA TURMA
        descricao_completa = ""
        desc_input = soup.find('input', {'name': 'descricao'})
        if desc_input:
            descricao_completa = desc_input.get('value', '').strip()
        
        # Extrair COMUM
        comum = ""
        comum_select = soup.find('select', {'id': 'id_igreja', 'name': 'id_igreja'})
        if comum_select:
            selected_option = comum_select.find('option', selected=True)
            if selected_option:
                # Formato: "Nome|BR-SP-CAMPINAS-HORTOLÂNDIA"
                texto_completo = selected_option.get_text(strip=True)
                # Pegar só o nome antes do "|"
                comum = texto_completo.split('|')[0].strip() if '|' in texto_completo else texto_completo
        
        # Extrair DIA DA SEMANA
        dia_semana = ""
        dia_select = soup.find('select', {'name': 'dia_semana'})
        if dia_select:
            selected_option = dia_select.find('option', selected=True)
            if selected_option:
                dia_semana = selected_option.get_text(strip=True)
        
        # Extrair DATA DE INÍCIO
        data_inicio = ""
        dt_inicio_input = soup.find('input', {'name': 'dt_inicio'})
        if dt_inicio_input:
            data_inicio = dt_inicio_input.get('value', '').strip()
        
        # Extrair DATA DE ENCERRAMENTO
        data_fim = ""
        dt_fim_input = soup.find('input', {'name': 'dt_fim'})
        if dt_fim_input:
            data_fim = dt_fim_input.get('value', '').strip()
        
        # Extrair HORA DE INÍCIO
        hora_inicio = ""
        hr_inicio_input = soup.find('input', {'name': 'hr_inicio'})
        if hr_inicio_input:
            hora_inicio = hr_inicio_input.get('value', '').strip()[:5]  # Pegar só HH:MM
        
        # Extrair HORA DE TÉRMINO
        hora_fim = ""
        hr_fim_input = soup.find('input', {'name': 'hr_fim'})
        if hr_fim_input:
            hora_fim = hr_fim_input.get('value', '').strip()[:5]
        
        # Extrair RESPONSÁVEL 1 (do script JS)
        responsavel_1 = ""
        script_tag = soup.find('script', string=lambda t: t and 'id_responsavel' in t if t else False)
        if script_tag:
            script_content = script_tag.string
            # Procurar pelo padrão: <option value="601825" selected>NOME - INFO</option>
            import re
            match = re.search(r"<option value=\"\d+\" selected>([^<]+)</option>", script_content)
            if match:
                responsavel_1 = match.group(1).strip()
        
        # Extrair RESPONSÁVEL 2 (do script JS)
        responsavel_2 = ""
        if script_tag:
            script_content = script_tag.string
            match = re.search(r"option2 = '<option value=\"\d+\" selected>([^<]+)</option>'", script_content)
            if match:
                responsavel_2 = match.group(1).strip()
        
        # Extrair DESTINADO AO (gênero)
        destinado_ao = ""
        genero_select = soup.find('select', {'name': 'id_turma_genero'})
        if genero_select:
            selected_option = genero_select.find('option', selected=True)
            if selected_option:
                destinado_ao = selected_option.get_text(strip=True)
        
        # Extrair STATUS (ativo/inativo)
        status = "Inativo"
        status_checkbox = soup.find('input', {'name': 'status', 'type': 'checkbox'})
        if status_checkbox and status_checkbox.has_attr('checked'):
            status = "Ativo"
        
        return {
            'id_turma': turma_id,
            'curso': curso,
            'descricao_completa': descricao_completa,
            'comum': comum,
            'dia_semana': dia_semana,
            'data_inicio': data_inicio,
            'data_fim': data_fim,
            'hora_inicio': hora_inicio,
            'hora_fim': hora_fim,
            'responsavel_1': responsavel_1,
            'responsavel_2': responsavel_2,
            'destinado_ao': destinado_ao,
            'status': status
        }
        
    except Exception as e:
        print(f"Erro ao processar turma {turma_id}: {e}")
        return None

def main():
    tempo_inicio = time.time()
    
    print("=" * 80)
    print("COLETA DE INFORMAÇÕES DETALHADAS DAS TURMAS")
    print("=" * 80)
    
    # 1. Ler Google Sheets e extrair IDs únicos de turmas
    ids_turmas = extrair_ids_turmas_do_sheets()
    
    if not ids_turmas or len(ids_turmas) == 0:
        print("\n✗ Nenhuma turma encontrada para processar!")
        return
    
    print(f"\n✓ {len(ids_turmas)} turmas serão processadas")
    
    # Confirmação do usuário
    print("\n" + "-" * 80)
    resposta = input("Deseja continuar com a coleta? (s/n): ").strip().lower()
    if resposta != 's':
        print("Operação cancelada pelo usuário.")
        return
    
    # 2. Login com Playwright
    print("\n[PASSO 2] Realizando login no sistema...")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        pagina.goto(URL_INICIAL)
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')
        
        try:
            pagina.wait_for_selector("nav", timeout=15000)
            print("✓ Login realizado com sucesso!")
        except PlaywrightTimeoutError:
            print("✗ Falha no login - verifique suas credenciais")
            navegador.close()
            return
        
        # Extrair cookies para usar com requests
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        navegador.close()
    
    # 3. Coletar dados das turmas
    print(f"\n[PASSO 3] Coletando dados de {len(ids_turmas)} turmas...")
    print("-" * 80)
    
    resultado = []
    turmas_processadas = 0
    turmas_sucesso = 0
    turmas_erro = 0
    
    # Processar em paralelo (5 threads)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(coletar_dados_turma, session, turma_id): turma_id 
            for turma_id in ids_turmas
        }
        
        for future in as_completed(futures):
            turmas_processadas += 1
            turma_id = futures[future]
            dados_turma = future.result()
            
            if dados_turma:
                turmas_sucesso += 1
                resultado.append([
                    dados_turma['id_turma'],
                    dados_turma['curso'],
                    dados_turma['descricao_completa'],
                    dados_turma['comum'],
                    dados_turma['dia_semana'],
                    dados_turma['data_inicio'],
                    dados_turma['data_fim'],
                    dados_turma['hora_inicio'],
                    dados_turma['hora_fim'],
                    dados_turma['responsavel_1'],
                    dados_turma['responsavel_2'],
                    dados_turma['destinado_ao'],
                    dados_turma['status']
                ])
                
                print(f"[{turmas_processadas:3d}/{len(ids_turmas)}] ✓ Turma {turma_id:5d}: {dados_turma['curso'][:20]:20s} - {dados_turma['comum'][:30]:30s} - {dados_turma['status']}")
            else:
                turmas_erro += 1
                print(f"[{turmas_processadas:3d}/{len(ids_turmas)}] ✗ Turma {turma_id:5d}: Erro ao coletar")
            
            # Mostrar progresso a cada 50
            if turmas_processadas % 50 == 0:
                tempo_decorrido = time.time() - tempo_inicio
                print(f"\n--- PROGRESSO: {turmas_processadas}/{len(ids_turmas)} | Sucesso: {turmas_sucesso} | Erro: {turmas_erro} | Tempo: {tempo_decorrido:.1f}s ---\n")
    
    # 4. Resumo final
    print("\n" + "=" * 80)
    print("COLETA FINALIZADA!")
    print("=" * 80)
    print(f"Total de turmas processadas: {turmas_processadas}")
    print(f"  ✓ Sucesso: {turmas_sucesso}")
    print(f"  ✗ Erro: {turmas_erro}")
    print(f"Tempo total: {(time.time() - tempo_inicio)/60:.2f} minutos")
    
    # 5. Preparar envio para Apps Script
    print(f"\n[PASSO 4] Enviando dados para Google Sheets...")
    
    body = {
        "tipo": "dados_turmas_detalhados",
        "dados": resultado,
        "headers": [
            "ID_Turma", "Curso", "Descrição_Completa", "Comum", "Dia_Semana",
            "Data_Início", "Data_Encerramento", "Hora_Início", "Hora_Término",
            "Responsável_1", "Responsável_2", "Destinado_ao", "Status"
        ],
        "resumo": {
            "total_turmas": len(resultado),
            "turmas_processadas": turmas_processadas,
            "tempo_minutos": round((time.time() - tempo_inicio)/60, 2)
        }
    }
    
    # Enviar para Apps Script
    try:
        resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=60)
        print(f"✓ Dados enviados! Status: {resposta_post.status_code}")
        print(f"  Resposta: {resposta_post.text}")
    except Exception as e:
        print(f"✗ Erro ao enviar: {e}")
        # Salvar localmente como backup
        import json
        with open('backup_turmas.json', 'w', encoding='utf-8') as f:
            json.dump(body, f, ensure_ascii=False, indent=2)
        print("✓ Dados salvos em backup_turmas.json")

if __name__ == "__main__":
    main()
