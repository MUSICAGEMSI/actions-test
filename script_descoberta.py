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
import re

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
BASE_URL = "https://musical.congregacao.org.br"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbyvzPcDI3I1zWl3YOxpAzG5O41eyYp7chybtLIjWhhrmGMf7FUreDLiSqHCuvfbFXfwig/exec'

# RANGE DE BUSCA
ID_INICIO = 327184  # Início julho 2025
ID_FIM = 400000

# PERÍODO DO SEGUNDO SEMESTRE 2025
DATA_INICIO = datetime.strptime("04/07/2025", "%d/%m/%Y")
DATA_FIM = datetime.strptime("31/12/2025", "%d/%m/%Y")

# Congregações específicas de Hortolândia
CONGREGACOES_HORTOLANDIA = [
    "JARDIM SANTANA",
    "JARDIM SANTA IZABEL",
    "JARDIM SÃO PEDRO",
    "JARDIM MIRANTE",
    "JARDIM NOVO ÂNGULO",
    "VILA REAL",
    "JARDIM AMANDA I",
    "JARDIM SANTA ESMERALDA",
    "JARDIM SANTA LUZIA",
    "PARQUE DO HORTO",
    "JARDIM SANTA CLARA DO LAGO",
    "JARDIM AMANDA II",
    "JARDIM ADELAIDE",
    "PARQUE ORESTES ÔNGARO",
    "JARDIM SÃO JORGE",
    "JARDIM SÃO SEBASTIÃO",
    "JARDIM DO BOSQUE",
    "VILA INEMA",
    "JARDIM ALINE",
    "JARDIM AMANDA III",
    "CHÁCARAS RECREIO 2000",
    "JARDIM NOVA EUROPA",
    "JARDIM DAS COLINAS",
    "RESIDENCIAL JOÃO LUIZ",
    "JARDIM AUXILIADORA",
    "JARDIM NOVO HORIZONTE",
    "JARDIM NOVA AMÉRICA",
    "JARDIM INTERLAGOS",
    "JARDIM TERRAS DE SANTO ANTÔNIO",
    "JARDIM RESIDENCIAL FIRENZE",
    "JARDIM BOA VISTA",
    "JARDIM NOVO CAMBUI I",
    "ESTRADA DO FURLAN, 1121",
    "RECANTO DO SOL",
    "NOVA HORTOLÂNDIA",
    "JARDIM AMANDA IV",
    "RESIDENCIAL BELLAVILLE",
    "PARQUE TERRAS DE SANTA MARIA"
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
    """Busca o ID mais recente na listagem"""
    print("\nBuscando ID mais recente da listagem...")
    
    url = f"{BASE_URL}/aulas_abertas/listagem"
    params = {
        'draw': 1,
        'start': 0,
        'length': 100,
        'order[0][column]': 0,
        'order[0][dir]': 'desc',
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
    """Verifica se uma aula existe e coleta dados simplificados"""
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
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extrair congregação
        congregacao_cell = soup.find('strong', string='Comum Congregação')
        congregacao = "N/A"
        if congregacao_cell:
            td_parent = congregacao_cell.find_parent('td')
            if td_parent:
                next_td = td_parent.find_next_sibling('td')
                if next_td:
                    congregacao = next_td.get_text(strip=True)
        
        # Verificar se é uma das congregações de Hortolândia
        if congregacao not in CONGREGACOES_HORTOLANDIA:
            return None
        
        # Extrair data do cabeçalho
        data_span = soup.find('span', class_='pull-right')
        data_str = ""
        if data_span:
            data_text = data_span.get_text(strip=True)
            match = re.search(r'\d{2}/\d{2}/\d{4}', data_text)
            if match:
                data_str = match.group(0)
        
        # Verificar se tem ATA
        tem_ata = "SIM" if "ATA DA AULA" in html else "NÃO"
        
        return {
            'aula_id': aula_id,
            'congregacao': congregacao,
            'data': data_str,
            'tem_ata': tem_ata
        }
        
    except Exception as e:
        return None

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
        futures = {
            executor.submit(verificar_aula, session, aula_id): aula_id 
            for aula_id in range(id_inicio, id_fim + 1)
        }
        
        for future in as_completed(futures):
            contador += 1
            resultado = future.result()
            
            if resultado:
                aulas_encontradas.append(resultado)
                print(f"✓ [{contador}/{total_testados}] ID {resultado['aula_id']} - {resultado['congregacao']} - {resultado['data']} - ATA: {resultado['tem_ata']}")
            else:
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
    print(f"Total encontrado: {len(aulas_encontradas)} aulas")
    
    return aulas_encontradas

def filtrar_por_periodo(aulas):
    """Filtra aulas do período desejado"""
    aulas_periodo = []
    
    for aula in aulas:
        data_str = aula['data']
        
        if not data_str:
            continue
        
        try:
            data = datetime.strptime(data_str, "%d/%m/%Y")
            
            if DATA_INICIO <= data <= DATA_FIM:
                aulas_periodo.append(aula)
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
            aula['congregacao'],
            aula['data'],
            aula['tem_ata']
        ]
        linhas.append(linha)
    
    headers = [
        "ID AULA",
        "CONGREGAÇÃO",
        "DATA",
        "TEM ATA"
    ]
    
    body = {
        "tipo": "aulas_hortolandia_simples",
        "dados": linhas,
        "headers": headers,
        "resumo": {
            "total_aulas": len(aulas),
            "periodo": f"{DATA_INICIO.strftime('%d/%m/%Y')} a {DATA_FIM.strftime('%d/%m/%Y')}",
            "range_ids": f"{ID_INICIO} a {ID_FIM}",
            "aulas_com_ata": sum(1 for a in aulas if a['tem_ata'] == 'SIM')
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
        with open('backup_aulas_simples.json', 'w', encoding='utf-8') as f:
            json.dump(body, f, indent=2, ensure_ascii=False)
        print("Backup salvo em: backup_aulas_simples.json")

def main():
    print("="*60)
    print("BUSCA SIMPLIFICADA DE AULAS - HORTOLÂNDIA 2º SEM 2025")
    print("="*60)
    
    tempo_inicio_total = time.time()
    
    # Login
    session = fazer_login_e_obter_session()
    if not session:
        return
    
    # Buscar último ID
    ultimo_id = buscar_ultimo_id_da_listagem(session)
    id_fim_ajustado = min(ID_FIM, ultimo_id + 1000)
    
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
    print(f"Aulas encontradas: {len(aulas)}")
    print(f"No período 2º semestre 2025: {len(aulas_periodo)}")
    
    tempo_total = time.time() - tempo_inicio_total
    print(f"Tempo total de execução: {tempo_total/60:.1f} minutos")
    
    # Salvar resultados
    if aulas_periodo:
        with open('aulas_hortolandia_simples.json', 'w', encoding='utf-8') as f:
            json.dump(aulas_periodo, f, indent=2, ensure_ascii=False)
        print(f"\nResultados salvos em: aulas_hortolandia_simples.json")
        
        # Estatísticas
        congregacoes = {}
        for aula in aulas_periodo:
            cong = aula['congregacao']
            congregacoes[cong] = congregacoes.get(cong, 0) + 1
        
        print("\nAulas por congregação:")
        for cong, qtd in sorted(congregacoes.items(), key=lambda x: x[1], reverse=True):
            print(f"  {cong}: {qtd} aulas")
        
        aulas_com_ata = sum(1 for a in aulas_periodo if a['tem_ata'] == "SIM")
        print(f"\nAulas com ATA: {aulas_com_ata}/{len(aulas_periodo)} ({aulas_com_ata*100/len(aulas_periodo):.1f}%)")
        
        # Enviar para Sheets
        enviar_para_sheets(aulas_periodo)

if __name__ == "__main__":
    main()
