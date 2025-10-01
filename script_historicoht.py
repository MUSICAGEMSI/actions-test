from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbyvEGIUPIvgbSuT_yikqg03nEjqXryd6RfI121A3pRt75v9oJoFNLTdvo3-onNdEsJd/exec'

# Cache de instrutores (será preenchido no início)
INSTRUTORES_HORTOLANDIA = {}  # {id: nome_completo}
NOMES_INSTRUTORES = set()     # Set com nomes para busca rápida

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def carregar_instrutores_hortolandia(session):
    """
    Carrega a lista completa de instrutores de Hortolândia
    Retorna dicionário {id: nome_completo}
    """
    print("\n🔍 Carregando lista de instrutores de Hortolândia...")
    
    try:
        url = "https://musical.congregacao.org.br/licoes/instrutores?q=a"
        resp = session.get(url, timeout=10)
        
        if resp.status_code != 200:
            print("❌ Erro ao carregar instrutores")
            return {}, set()
        
        # Parse do JSON
        instrutores = json.loads(resp.text)
        
        ids_dict = {}
        nomes_set = set()
        
        for instrutor in instrutores:
            id_instrutor = instrutor['id']
            texto_completo = instrutor['text']
            
            # Extrair apenas o nome (antes do " - ")
            nome = texto_completo.split(' - ')[0].strip()
            
            ids_dict[id_instrutor] = nome
            nomes_set.add(nome)
        
        print(f"✅ {len(ids_dict)} instrutores de Hortolândia carregados!")
        return ids_dict, nomes_set
        
    except Exception as e:
        print(f"❌ Erro ao carregar instrutores: {e}")
        return {}, set()

def normalizar_nome(nome):
    """
    Normaliza nome para comparação
    Remove espaços extras, converte para maiúsculas
    """
    return ' '.join(nome.upper().split())

def coletar_dados_aula(session, aula_id):
    """
    Coleta dados de uma aula específica
    Retorna None se não existir ou não for de Hortolândia
    """
    try:
        url = f"https://musical.congregacao.org.br/aulas_abertas/editar/{aula_id}"
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        resp = session.get(url, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            return None
            
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Extrair dados dos inputs readonly
        descricao = ""
        comum = ""
        dia_semana = ""
        hora_inicio = ""
        hora_termino = ""
        id_turma = ""
        
        # Descrição (Curso)
        desc_input = soup.find('label', string='Descrição')
        if desc_input:
            parent = desc_input.find_parent('div', class_='form-group')
            if parent:
                input_field = parent.find('input', class_='form-control')
                if input_field:
                    descricao = input_field.get('value', '').strip()
        
        # Comum
        comum_input = soup.find('label', string='Comum')
        if comum_input:
            parent = comum_input.find_parent('div', class_='form-group')
            if parent:
                input_field = parent.find('input', class_='form-control')
                if input_field:
                    comum = input_field.get('value', '').strip().upper()
        
        # Dia da Semana
        dia_input = soup.find('label', string='Dia da Semana')
        if dia_input:
            parent = dia_input.find_parent('div', class_='form-group')
            if parent:
                input_field = parent.find('input', class_='form-control')
                if input_field:
                    dia_semana = input_field.get('value', '').strip()
        
        # Hora de Início
        hora_ini_input = soup.find('label', string='Hora de Início')
        if hora_ini_input:
            parent = hora_ini_input.find_parent('div', class_='form-group')
            if parent:
                input_field = parent.find('input', class_='form-control')
                if input_field:
                    hora_inicio = input_field.get('value', '').strip()[:5]
        
        # Hora de Término
        hora_fim_input = soup.find('label', string='Hora de Término')
        if hora_fim_input:
            parent = hora_fim_input.find_parent('div', class_='form-group')
            if parent:
                input_field = parent.find('input', class_='form-control')
                if input_field:
                    hora_termino = input_field.get('value', '').strip()[:5]
        
        # ID da Turma
        turma_input = soup.find('input', {'name': 'id_turma'})
        if turma_input:
            id_turma = turma_input.get('value', '').strip()
        
        # Verificar se tem dados mínimos
        if not descricao or not comum:
            return None
        
        return {
            'id_aula': aula_id,
            'id_turma': id_turma,
            'descricao': descricao,
            'comum': comum,
            'dia_semana': dia_semana,
            'hora_inicio': hora_inicio,
            'hora_termino': hora_termino
        }
        
    except Exception as e:
        return None

def coletar_data_ata_e_instrutor(session, aula_id):
    """
    Coleta a data correta da aula, se tem ata e o nome do instrutor
    Retorna: (data_aula, tem_ata, nome_instrutor, eh_hortolandia)
    """
    try:
        url = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        resp = session.get(url, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            return "", "Não", "", False
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Extrair data da aula
        data_aula = ""
        modal_header = soup.find('div', class_='modal-header')
        if modal_header:
            date_span = modal_header.find('span', class_='pull-right')
            if date_span:
                texto = date_span.get_text(strip=True)
                data_aula = texto.strip()
        
        # Verificar se tem ata
        tem_ata = "Não"
        ata_thead = soup.find('thead', class_='bg-green-gradient')
        if ata_thead:
            ata_td = ata_thead.find('td')
            if ata_td and 'ATA DA AULA' in ata_td.get_text():
                tem_ata = "Sim"
        
        # Extrair nome do instrutor que ministrou a aula
        nome_instrutor = ""
        tbody = soup.find('tbody')
        if tbody:
            rows = tbody.find_all('tr')
            for row in rows:
                td_strong = row.find('strong')
                if td_strong and 'Instrutor(a) que ministrou a aula' in td_strong.get_text():
                    td_valor = row.find_all('td')[1]
                    nome_completo = td_valor.get_text(strip=True)
                    # Extrair apenas o nome (antes do " - ")
                    nome_instrutor = nome_completo.split(' - ')[0].strip()
                    break
        
        # Verificar se o instrutor é de Hortolândia
        eh_hortolandia = False
        if nome_instrutor:
            nome_normalizado = normalizar_nome(nome_instrutor)
            # Verificar se o nome está na lista de instrutores de Hortolândia
            for nome_htl in NOMES_INSTRUTORES:
                if normalizar_nome(nome_htl) == nome_normalizado:
                    eh_hortolandia = True
                    break
        
        return data_aula, tem_ata, nome_instrutor, eh_hortolandia
        
    except Exception:
        return "", "Não", "", False

def coletar_frequencias(session, aula_id, turma_id):
    """
    Coleta quantidade de alunos presentes (fa-check) e total de alunos
    """
    try:
        url = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_frequencias/{aula_id}/{turma_id}"
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': f'https://musical.congregacao.org.br/aulas_abertas/editar/{aula_id}',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        resp = session.get(url, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            return 0, 0
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Contar total de alunos (linhas na tabela)
        tbody = soup.find('tbody')
        if not tbody:
            return 0, 0
        
        linhas = tbody.find_all('tr')
        total_alunos = len(linhas)
        
        # Contar fa-check (presentes)
        presentes = 0
        for linha in linhas:
            icon = linha.find('i', class_='fa-check')
            if icon:
                presentes += 1
        
        return total_alunos, presentes
        
    except Exception:
        return 0, 0

def main():
    global INSTRUTORES_HORTOLANDIA, NOMES_INSTRUTORES
    
    tempo_inicio = time.time()
    
    print("=" * 70)
    print("🎵 COLETOR DE HISTÓRICO DE AULAS - HORTOLÂNDIA")
    print("=" * 70)
    
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
            print("✅ Login realizado!")
        except PlaywrightTimeoutError:
            print("❌ Falha no login.")
            navegador.close()
            return
        
        # Extrair cookies
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        navegador.close()
    
    # Carregar lista de instrutores de Hortolândia
    INSTRUTORES_HORTOLANDIA, NOMES_INSTRUTORES = carregar_instrutores_hortolandia(session)
    
    if not INSTRUTORES_HORTOLANDIA:
        print("❌ Não foi possível carregar a lista de instrutores. Abortando.")
        return
    
    # Coletar aulas
    resultado = []
    aulas_processadas = 0
    aulas_hortolandia = 0
    
    # Range de IDs (pode ajustar conforme necessário)
    ID_INICIAL = 327184
    ID_FINAL = 360000
    
    # Processar em lotes
    LOTE_SIZE = 100
    
    print(f"\n{'=' * 70}")
    print(f"📊 Processando aulas de {ID_INICIAL} a {ID_FINAL}...")
    print(f"🎯 Filtrando por instrutores de Hortolândia")
    print(f"{'=' * 70}\n")
    
    for lote_inicio in range(ID_INICIAL, ID_FINAL + 1, LOTE_SIZE):
        lote_fim = min(lote_inicio + LOTE_SIZE - 1, ID_FINAL)
        
        # Processar lote em paralelo (5 threads)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(coletar_dados_aula, session, aula_id): aula_id 
                for aula_id in range(lote_inicio, lote_fim + 1)
            }
            
            for future in as_completed(futures):
                aulas_processadas += 1
                dados_aula = future.result()
                
                if dados_aula:
                    # Coletar data, ata e instrutor
                    data_aula, tem_ata, nome_instrutor, eh_hortolandia = coletar_data_ata_e_instrutor(
                        session,
                        dados_aula['id_aula']
                    )
                    
                    # FILTRO PRINCIPAL: Só prossegue se o instrutor for de Hortolândia
                    if not eh_hortolandia:
                        continue
                    
                    # Coletar frequências
                    total_alunos, presentes = coletar_frequencias(
                        session, 
                        dados_aula['id_aula'], 
                        dados_aula['id_turma']
                    )
                    
                    dados_aula['data_aula'] = data_aula
                    dados_aula['tem_ata'] = tem_ata
                    dados_aula['instrutor'] = nome_instrutor
                    dados_aula['total_alunos'] = total_alunos
                    dados_aula['presentes'] = presentes
                    
                    resultado.append([
                        dados_aula['id_aula'],
                        dados_aula['id_turma'],
                        dados_aula['descricao'],
                        dados_aula['comum'],
                        dados_aula['dia_semana'],
                        dados_aula['hora_inicio'],
                        dados_aula['hora_termino'],
                        dados_aula['data_aula'],
                        dados_aula['tem_ata'],
                        dados_aula['instrutor'],
                        dados_aula['total_alunos'],
                        dados_aula['presentes']
                    ])
                    
                    aulas_hortolandia += 1
                    print(f"✅ [{aulas_processadas}/{ID_FINAL-ID_INICIAL+1}] ID {dados_aula['id_aula']}: {dados_aula['descricao']} | {dados_aula['comum']} | {data_aula} | {nome_instrutor} | Ata: {tem_ata} | {presentes}/{total_alunos}")
                
                # Mostrar progresso a cada 100
                if aulas_processadas % 100 == 0:
                    tempo_decorrido = time.time() - tempo_inicio
                    print(f"\n{'─' * 70}")
                    print(f"📈 PROGRESSO: {aulas_processadas} verificadas | {aulas_hortolandia} de Hortolândia | {tempo_decorrido:.1f}s")
                    print(f"{'─' * 70}\n")
        
        time.sleep(1)  # Pausa entre lotes
    
    print(f"\n{'=' * 70}")
    print(f"🎉 COLETA FINALIZADA!")
    print(f"{'=' * 70}")
    print(f"📊 Total processado: {aulas_processadas}")
    print(f"✅ Aulas de Hortolândia: {aulas_hortolandia}")
    print(f"⏱️  Tempo total: {(time.time() - tempo_inicio)/60:.1f} minutos")
    print(f"{'=' * 70}\n")
    
    # Preparar envio
    body = {
        "tipo": "historico_aulas_hortolandia",
        "dados": resultado,
        "headers": [
            "ID_Aula", "ID_Turma", "Descrição", "Comum", "Dia_Semana",
            "Hora_Início", "Hora_Término", "Data_Aula", "Tem_Ata", "Instrutor",
            "Total_Alunos", "Presentes"
        ],
        "resumo": {
            "total_aulas": len(resultado),
            "aulas_processadas": aulas_processadas,
            "total_instrutores_htl": len(INSTRUTORES_HORTOLANDIA),
            "tempo_minutos": round((time.time() - tempo_inicio)/60, 2)
        }
    }
    
    # Enviar para Apps Script
    print("📤 Enviando dados para Google Sheets...")
    try:
        resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=60)
        print(f"✅ Dados enviados! Status: {resposta_post.status_code}")
        print(f"📝 Resposta: {resposta_post.text}")
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
        # Salvar localmente como backup
        with open('backup_aulas.json', 'w', encoding='utf-8') as f:
            json.dump(body, f, ensure_ascii=False, indent=2)
        print("💾 Dados salvos em backup_aulas.json")

if __name__ == "__main__":
    main()
