from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os
import requests
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import json

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
BASE_URL = "https://musical.congregacao.org.br"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxGBDSwoFQTJ8m-H1keAEMOm-iYAZpnQc5CVkcNNgilDDL3UL8ptdTP45TiaxHDw8Am/exec'

# RANGE DE BUSCA: Julho/2025 até o mais recente
ID_INICIO = 327184  # Início julho 2025
ID_FIM = 400000     # Ajustar conforme necessário (ou buscar o último ID da listagem)

# PERÍODO DO SEGUNDO SEMESTRE 2025
DATA_INICIO = datetime.strptime("04/07/2025", "%d/%m/%Y")
DATA_FIM = datetime.strptime("31/12/2025", "%d/%m/%Y")

# Termos que identificam Hortolândia
CONGREGACOES_HORTOLANDIA = [
    "HORTOLÂNDIA", "HORTOLANDIA",
    "BELLAVILLE", "BELLA VILLE",
    "REMANSO CAMPINEIRO",
    "AMANDA", "NOVA HORTOLÂNDIA",
]

def fazer_login_e_obter_session():
    """Login e extração de cookies"""
    print("Fazendo login...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        page.goto(BASE_URL)
        page.fill('input[name="login"]', EMAIL)
        page.fill('input[name="password"]', SENHA)
        page.click('button[type="submit"]')
        
        try:
            page.wait_for_selector("nav", timeout=15000)
            print("Login realizado com sucesso!")
        except:
            print("Falha no login")
            browser.close()
            return None
        
        cookies = page.context.cookies()
        session = requests.Session()
        for cookie in cookies:
            session.cookies.set(cookie['name'], cookie['value'])
        
        browser.close()
        return session

def buscar_ultimo_id_da_listagem(session):
    """Busca o ID mais recente na listagem para definir range"""
    print("\nBuscando ID mais recente da listagem...")
    
    url = f"{BASE_URL}/aulas_abertas/listagem"
    params = {
        'draw': 1,
        'start': 0,
        'length': 100,
        'order[0][column]': 0,
        'order[0][dir]': 'desc',  # Ordenar por ID decrescente
    }
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': 'application/json',
    }
    
    try:
        response = session.get(url, params=params, headers=headers, timeout=30)
        data = response.json()
        registros = data.get('data', [])
        
        if registros:
            primeiro_id = str(registros[0][0])
            if primeiro_id.isdigit():
                ultimo_id = int(primeiro_id)
                print(f"ID mais recente encontrado: {ultimo_id}")
                return ultimo_id
        
        return ID_FIM
        
    except Exception as e:
        print(f"Erro ao buscar último ID: {e}")
        return ID_FIM

def verificar_aula(session, aula_id):
    """Verifica se uma aula existe e é de Hortolândia"""
    try:
        url = f"{BASE_URL}/aulas_abertas/visualizar_aula/{aula_id}"
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Referer': f'{BASE_URL}/aulas_abertas/listagem',
        }
        
        response = session.get(url, headers=headers, timeout=5)
        
        if response.status_code != 200:
            return None
        
        html = response.text
        
        # Verificar se é de Hortolândia
        if not any(termo.upper() in html.upper() for termo in CONGREGACOES_HORTOLANDIA):
            return None
        
        # Parsear dados básicos
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extrair congregação e seu ID
        congregacao_cell = soup.find('strong', string='Comum Congregação')
        congregacao = "N/A"
        congregacao_id = "N/A"
        if congregacao_cell:
            td_parent = congregacao_cell.find_parent('td')
            if td_parent:
                next_td = td_parent.find_next_sibling('td')
                if next_td:
                    congregacao = next_td.get_text(strip=True)
        
        # Tentar extrair congregacao_id da URL ou de elementos ocultos
        import re
        match_cong_id = re.search(r'congregacao[_\-]?id["\s:=]+(\d+)', html, re.IGNORECASE)
        if match_cong_id:
            congregacao_id = match_cong_id.group(1)
        
        # Extrair data do cabeçalho
        data_span = soup.find('span', class_='pull-right')
        data_str = ""
        if data_span:
            data_text = data_span.get_text(strip=True)
            match = re.search(r'\d{2}/\d{2}/\d{4}', data_text)
            if match:
                data_str = match.group(0)
        
        # Extrair curso do cabeçalho da tabela
        curso_header = soup.find('td', class_='bg-blue-gradient')
        curso = "N/A"
        if curso_header:
            curso_text = curso_header.get_text(strip=True)
            curso = curso_text.replace('', '').strip()
        
        # Extrair turma e turma_id
        turma = "N/A"
        turma_id = "N/A"
        
        # Tentar extrair turma_id da URL ou de elementos
        match_turma_id = re.search(r'turma[_\-]?id["\s:=]+(\d+)', html, re.IGNORECASE)
        if match_turma_id:
            turma_id = match_turma_id.group(1)
        
        # Verificar ATA
        tem_ata = "OK" if "ATA DA AULA" in html else "FANTASMA"
        
        # Extrair frequência do mesmo HTML
        freq_data = extrair_frequencia_do_html_aula(html)
        
        # Calcular quantidade de presentes
        qtd_presentes = len(freq_data['presentes_ids'])
        qtd_ausentes = len(freq_data['ausentes_ids'])
        
        return {
            'aula_id': aula_id,
            'congregacao_id': congregacao_id,
            'congregacao': congregacao,
            'turma_id': turma_id,
            'curso': curso,
            'turma': turma,
            'data': data_str,
            'qtd_presentes': qtd_presentes,
            'qtd_ausentes': qtd_ausentes,
            'presentes_ids': freq_data['presentes_ids'],
            'presentes_nomes': freq_data['presentes_nomes'],
            'ausentes_ids': freq_data['ausentes_ids'],
            'ausentes_nomes': freq_data['ausentes_nomes'],
            'tem_presenca': freq_data['tem_presenca'],
            'tem_ata': tem_ata
        }
        
    except Exception as e:
        return None

def extrair_frequencia_do_html_aula(html):
    """Extrai frequência diretamente do HTML da página de visualização da aula"""
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        presentes_ids = []
        presentes_nomes = []
        ausentes_ids = []
        ausentes_nomes = []
        
        # Buscar tabela de frequência (pode estar em uma seção específica)
        # Procurar por tabelas com classe table-bordered
        tabelas = soup.find_all('table', class_='table-bordered')
        
        for tabela in tabelas:
            linhas = tabela.find_all('tr')
            
            for linha in linhas:
                colunas = linha.find_all('td')
                
                # Precisa ter pelo menos 2 colunas (nome e status)
                if len(colunas) < 2:
                    continue
                
                # Pegar nome da primeira coluna
                nome = colunas[0].get_text(strip=True)
                
                # Ignorar linhas vazias ou cabeçalhos
                if not nome or nome in ['Nome', 'Aluno', 'Membro']:
                    continue
                
                # Buscar link com status na última coluna
                link_status = colunas[-1].find('a')
                
                if link_status:
                    # Extrair ID do membro
                    id_membro = link_status.get('data-id-membro')
                    
                    if not id_membro:
                        continue
                    
                    # Verificar ícone de presença/ausência
                    icone = link_status.find('i')
                    
                    if icone:
                        classes = ' '.join(icone.get('class', []))
                        
                        # Presente: fa-check ou text-success
                        if 'fa-check' in classes or 'text-success' in classes:
                            presentes_ids.append(id_membro)
                            presentes_nomes.append(nome)
                        
                        # Ausente: fa-remove, fa-times ou text-danger
                        elif any(cls in classes for cls in ['fa-remove', 'fa-times', 'text-danger']):
                            ausentes_ids.append(id_membro)
                            ausentes_nomes.append(nome)
        
        # Determinar status de presença
        if presentes_ids or ausentes_ids:
            tem_presenca = "OK"
        else:
            tem_presenca = "FANTASMA"
        
        return {
            'presentes_ids': presentes_ids,
            'presentes_nomes': presentes_nomes,
            'ausentes_ids': ausentes_ids,
            'ausentes_nomes': ausentes_nomes,
            'tem_presenca': tem_presenca
        }
        
    except Exception as e:
        return {
            'presentes_ids': [],
            'presentes_nomes': [],
            'ausentes_ids': [],
            'ausentes_nomes': [],
            'tem_presenca': "ERRO"
        }

def extrair_frequencia_via_modal(session, aula_id, professor_id):
    """Tenta extrair frequência via endpoint do modal (se existir)"""
    
    endpoints_possiveis = [
        f"/frequencias/aula/{aula_id}",
        f"/aulas_abertas/frequencia/{aula_id}",
        f"/frequencias/visualizar/{aula_id}/{professor_id}",
        f"/aulas_abertas/get_frequencia?aula_id={aula_id}",
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': 'text/html, application/json',
        'Referer': f'{BASE_URL}/aulas_abertas/listagem',
    }
    
    for endpoint in endpoints_possiveis:
        try:
            url = BASE_URL + endpoint
            response = session.get(url, headers=headers, timeout=5)
            
            if response.status_code == 200:
                html = response.text
                return extrair_frequencia_do_html_aula(html)
        except:
            continue
    
    # Se nenhum endpoint funcionou
    return {
        'presentes_ids': [],
        'presentes_nomes': [],
        'ausentes_ids': [],
        'ausentes_nomes': [],
        'tem_presenca': "ERRO"
    }

def buscar_aulas_paralelo(session, id_inicio, id_fim, max_workers=30):
    """Busca aulas em paralelo dentro de um range"""
    
    print(f"\nBuscando aulas de {id_inicio} a {id_fim}...")
    print(f"Usando {max_workers} threads paralelas")
    
    aulas_encontradas = []
    total_testados = id_fim - id_inicio + 1
    contador = 0
    ultimo_print = 0
    
    tempo_inicio = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submeter todas as tarefas
        futures = {
            executor.submit(verificar_aula, session, aula_id): aula_id 
            for aula_id in range(id_inicio, id_fim + 1)
        }
        
        # Processar resultados conforme completam
        for future in as_completed(futures):
            contador += 1
            resultado = future.result()
            
            if resultado:
                aulas_encontradas.append(resultado)
                print(f"ENCONTRADA [{contador}/{total_testados}] ID {resultado['aula_id']} - {resultado['congregacao']} - {resultado['data']} - Presentes: {resultado['qtd_presentes']}")
            else:
                # Mostrar progresso a cada 500 aulas testadas
                if contador - ultimo_print >= 500:
                    tempo_decorrido = time.time() - tempo_inicio
                    velocidade = contador / tempo_decorrido if tempo_decorrido > 0 else 0
                    tempo_restante = (total_testados - contador) / velocidade if velocidade > 0 else 0
                    
                    print(f"Progresso: {contador}/{total_testados} ({contador*100/total_testados:.1f}%) - "
                          f"{len(aulas_encontradas)} encontradas - "
                          f"Velocidade: {velocidade:.0f} req/s - "
                          f"Tempo restante: {tempo_restante/60:.1f} min")
                    ultimo_print = contador
    
    tempo_total = time.time() - tempo_inicio
    print(f"\nBusca concluída em {tempo_total/60:.1f} minutos")
    print(f"Total encontrado: {len(aulas_encontradas)} aulas de Hortolândia")
    
    return aulas_encontradas

def filtrar_por_periodo(aulas):
    """Filtra aulas do período desejado"""
    
    aulas_periodo = []
    
    for aula in aulas:
        data_str = aula['data']
        
        if not data_str:
            continue
        
        try:
            # Tentar diferentes formatos
            for formato in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]:
                try:
                    data = datetime.strptime(data_str, formato)
                    
                    if DATA_INICIO <= data <= DATA_FIM:
                        aulas_periodo.append(aula)
                    break
                except:
                    continue
        except:
            continue
    
    return aulas_periodo

def enviar_para_sheets(aulas):
    """Envia dados para Google Sheets"""
    
    if not aulas:
        print("\nNenhuma aula para enviar")
        return
    
    print(f"\nPreparando envio de {len(aulas)} aulas para Google Sheets...")
    
    # Montar linhas
    linhas = []
    for aula in aulas:
        linha = [
            aula['aula_id'],
            aula.get('congregacao_id', 'N/A'),
            aula['congregacao'],
            aula.get('turma_id', 'N/A'),
            aula['curso'],
            aula['turma'],
            aula['data'],
            aula.get('qtd_presentes', 0),
            aula.get('qtd_ausentes', 0),
            "; ".join(aula.get('presentes_ids', [])),
            "; ".join(aula.get('presentes_nomes', [])),
            "; ".join(aula.get('ausentes_ids', [])),
            "; ".join(aula.get('ausentes_nomes', [])),
            aula.get('tem_presenca', 'N/A'),
            aula['tem_ata']
        ]
        linhas.append(linha)
    
    headers = [
        "ID AULA",
        "ID CONGREGAÇÃO",
        "CONGREGAÇÃO",
        "ID TURMA",
        "CURSO",
        "TURMA",
        "DATA",
        "QTD PRESENTES",
        "QTD AUSENTES",
        "PRESENTES IDs",
        "PRESENTES Nomes",
        "AUSENTES IDs",
        "AUSENTES Nomes",
        "TEM PRESENÇA",
        "ATA DA AULA"
    ]
    
    body = {
        "tipo": "historico_aulas_hortolandia_2sem_2025",
        "dados": linhas,
        "headers": headers,
        "resumo": {
            "total_aulas": len(aulas),
            "periodo": f"{DATA_INICIO.strftime('%d/%m/%Y')} a {DATA_FIM.strftime('%d/%m/%Y')}",
            "range_ids": f"{ID_INICIO} a {ID_FIM}"
        }
    }
    
    try:
        print("Enviando dados...")
        response = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
        print(f"Dados enviados! Status: {response.status_code}")
        print(f"Resposta: {response.text}")
    except Exception as e:
        print(f"Erro ao enviar: {e}")
        
        # Salvar localmente como backup
        with open('backup_aulas_hortolandia.json', 'w', encoding='utf-8') as f:
            json.dump(body, f, indent=2, ensure_ascii=False)
        print("Backup salvo em: backup_aulas_hortolandia.json")

def main():
    print("="*60)
    print("BUSCA DE AULAS DE HORTOLÂNDIA - 2º SEMESTRE 2025")
    print("="*60)
    
    tempo_inicio_total = time.time()
    
    # Login
    session = fazer_login_e_obter_session()
    if not session:
        return
    
    # Buscar último ID para otimizar range
    ultimo_id = buscar_ultimo_id_da_listagem(session)
    id_fim_ajustado = min(ID_FIM, ultimo_id + 1000)  # Adicionar margem de segurança
    
    print(f"\nRange de busca: {ID_INICIO} a {id_fim_ajustado}")
    print(f"Total de IDs a verificar: {id_fim_ajustado - ID_INICIO + 1}")
    
    # Buscar aulas
    aulas = buscar_aulas_paralelo(session, ID_INICIO, id_fim_ajustado, max_workers=30)
    
    # Filtrar por período
    print("\nFiltrando por período...")
    aulas_periodo = filtrar_por_periodo(aulas)
    
    print("\n" + "="*60)
    print("RESULTADOS FINAIS")
    print("="*60)
    print(f"Aulas de Hortolândia encontradas: {len(aulas)}")
    print(f"No período 2º semestre 2025: {len(aulas_periodo)}")
    
    tempo_total = time.time() - tempo_inicio_total
    print(f"Tempo total de execução: {tempo_total/60:.1f} minutos")
    
    # Salvar resultados
    if aulas_periodo:
        with open('aulas_hortolandia_2sem2025.json', 'w', encoding='utf-8') as f:
            json.dump(aulas_periodo, f, indent=2, ensure_ascii=False)
        print(f"\nResultados salvos em: aulas_hortolandia_2sem2025.json")
        
        # Estatísticas
        congregacoes = {}
        total_presentes = 0
        total_ausentes = 0
        
        for aula in aulas_periodo:
            cong = aula['congregacao']
            congregacoes[cong] = congregacoes.get(cong, 0) + 1
            total_presentes += len(aula.get('presentes_ids', []))
            total_ausentes += len(aula.get('ausentes_ids', []))
        
        print("\nAulas por congregação:")
        for cong, qtd in sorted(congregacoes.items(), key=lambda x: x[1], reverse=True):
            print(f"  {cong}: {qtd} aulas")
        
        aulas_com_ata = sum(1 for a in aulas_periodo if a['tem_ata'] == "OK")
        aulas_com_presenca = sum(1 for a in aulas_periodo if a.get('tem_presenca') == "OK")
        
        print(f"\nAulas com ATA: {aulas_com_ata}/{len(aulas_periodo)}")
        print(f"Aulas com frequência: {aulas_com_presenca}/{len(aulas_periodo)}")
        print(f"Total de presenças: {total_presentes}")
        print(f"Total de ausências: {total_ausentes}")
        
        # Enviar para Sheets
        enviar_para_sheets(aulas_periodo)

if __name__ == "__main__":
    main()
