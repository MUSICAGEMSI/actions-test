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

# IDs das congregações de Hortolândia
IDS_CONGREGACOES_HORTOLANDIA = [
    20388, 20391, 20462, 20485, 20486, 20616, 20642, 20672, 20678, 20686,
    20714, 20736, 20737, 20771, 20872, 20873, 20922, 20923, 20964, 21067,
    21088, 21144, 21181, 21233, 21271, 21323, 21413, 21519, 21572, 21631,
    21867, 22134, 22226, 25699, 25703, 25704, 25705, 26144
]

# PERÍODO DO SEGUNDO SEMESTRE 2025
DATA_INICIO = datetime.strptime("04/07/2025", "%d/%m/%Y")
DATA_FIM = datetime.strptime("31/12/2025", "%d/%m/%Y")

# RANGE DE IDs DE AULAS
ID_AULA_INICIO = 327184
ID_AULA_FIM = 360000

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

def buscar_turmas_hortolandia(session):
    """Busca todas as turmas de Hortolândia via listagem"""
    print("\nBuscando turmas de Hortolândia...")
    
    url = f"{BASE_URL}/turmas/listagem"
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': 'application/json',
    }
    
    turmas = []
    
    # Buscar todas as páginas
    start = 0
    length = 1000  # Buscar muitas de uma vez
    
    while True:
        params = {
            'draw': 1,
            'start': start,
            'length': length,
        }
        
        try:
            response = session.get(url, params=params, headers=headers, timeout=30)
            data = response.json()
            registros = data.get('data', [])
            
            if not registros:
                break
            
            for reg in registros:
                # Extrair ID da congregação do HTML
                soup = BeautifulSoup(str(reg), 'html.parser')
                
                # Buscar link de edição para pegar ID da turma
                link_editar = None
                for item in reg:
                    if isinstance(item, str) and 'turmas/editar' in item:
                        match = re.search(r'turmas/editar/(\d+)', item)
                        if match:
                            turma_id = int(match.group(1))
                            link_editar = turma_id
                            break
                
                if not link_editar:
                    continue
                
                # Pegar congregação do registro
                congregacao_html = str(reg[2]) if len(reg) > 2 else ""
                
                # Verificar se contém ID de Hortolândia
                encontrou_horto = False
                for id_cong in IDS_CONGREGACOES_HORTOLANDIA:
                    if f'value="{id_cong}"' in congregacao_html or f'/{id_cong}' in congregacao_html:
                        encontrou_horto = True
                        break
                
                if encontrou_horto:
                    turmas.append({
                        'turma_id': link_editar,
                        'registro_bruto': reg
                    })
            
            print(f"Processadas {start + len(registros)} turmas, encontradas {len(turmas)} de Hortolândia")
            
            start += length
            
            # Se retornou menos que o esperado, acabou
            if len(registros) < length:
                break
                
        except Exception as e:
            print(f"Erro ao buscar turmas: {e}")
            break
    
    print(f"\nTotal de turmas de Hortolândia encontradas: {len(turmas)}")
    return turmas

def obter_detalhes_turma(session, turma_id):
    """Obtém detalhes completos de uma turma"""
    try:
        url = f"{BASE_URL}/turmas/editar/{turma_id}"
        headers = {
            'User-Agent': 'Mozilla/5.0',
        }
        
        response = session.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return None
        
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extrair curso
        curso_select = soup.find('select', {'name': 'id_curso'})
        curso = "N/A"
        if curso_select:
            curso_option = curso_select.find('option', selected=True)
            if curso_option:
                curso = curso_option.get_text(strip=True)
        
        # Extrair descrição
        descricao_input = soup.find('input', {'name': 'descricao'})
        descricao = "N/A"
        if descricao_input:
            descricao = descricao_input.get('value', 'N/A')
        
        # Extrair congregação
        congregacao_select = soup.find('select', {'name': 'id_igreja'})
        congregacao = "N/A"
        congregacao_id = "N/A"
        if congregacao_select:
            cong_option = congregacao_select.find('option', selected=True)
            if cong_option:
                congregacao = cong_option.get_text(strip=True)
                congregacao_id = cong_option.get('value', 'N/A')
        
        # Contar alunos matriculados (se houver uma seção de alunos)
        # Isso pode variar dependendo da estrutura da página
        alunos = 0
        
        return {
            'turma_id': turma_id,
            'curso': curso,
            'descricao': descricao,
            'congregacao': congregacao,
            'congregacao_id': congregacao_id,
            'qtd_alunos': alunos
        }
        
    except Exception as e:
        return None

def buscar_aulas_da_turma(session, turma_id):
    """Busca todas as aulas de uma turma específica"""
    print(f"  Buscando aulas da turma {turma_id}...")
    
    url = f"{BASE_URL}/aulas_abertas/listagem"
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': 'application/json',
    }
    
    aulas = []
    
    params = {
        'draw': 1,
        'start': 0,
        'length': 1000,
        'search[value]': str(turma_id),  # Filtrar por ID da turma
    }
    
    try:
        response = session.get(url, params=params, headers=headers, timeout=30)
        data = response.json()
        registros = data.get('data', [])
        
        for reg in registros:
            # ID da aula é o primeiro campo
            aula_id = str(reg[0]) if len(reg) > 0 else None
            
            if not aula_id or not aula_id.isdigit():
                continue
            
            aula_id = int(aula_id)
            
            # Filtrar por range de IDs
            if aula_id < ID_AULA_INICIO or aula_id > ID_AULA_FIM:
                continue
            
            # Data da aula (geralmente no campo da data)
            data_str = ""
            for campo in reg:
                if isinstance(campo, str):
                    match = re.search(r'\d{2}/\d{2}/\d{4}', campo)
                    if match:
                        data_str = match.group(0)
                        break
            
            aulas.append({
                'aula_id': aula_id,
                'turma_id': turma_id,
                'data': data_str
            })
        
    except Exception as e:
        print(f"  Erro ao buscar aulas da turma {turma_id}: {e}")
    
    return aulas

def obter_detalhes_aula(session, aula_id, turma_id):
    """Obtém detalhes de uma aula específica"""
    try:
        # Buscar página de visualização para pegar data e ATA
        url = f"{BASE_URL}/aulas_abertas/visualizar_aula/{aula_id}"
        headers = {
            'User-Agent': 'Mozilla/5.0',
        }
        
        response = session.get(url, headers=headers, timeout=5)
        
        if response.status_code != 200:
            return None
        
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extrair data
        data_span = soup.find('span', class_='pull-right')
        data_str = ""
        if data_span:
            data_text = data_span.get_text(strip=True)
            match = re.search(r'\d{2}/\d{2}/\d{4}', data_text)
            if match:
                data_str = match.group(0)
        
        # Verificar ATA
        tem_ata = "SIM" if "ATA DA AULA" in html else "NÃO"
        
        # Buscar frequência
        qtd_presentes = 0
        
        try:
            url_freq = f"{BASE_URL}/aulas_abertas/visualizar_frequencias/{aula_id}/{turma_id}"
            response_freq = session.get(url_freq, headers=headers, timeout=5)
            
            if response_freq.status_code == 200:
                html_freq = response_freq.text
                
                # Contar fa-check (presentes)
                qtd_presentes = html_freq.count('fa-check')
        except:
            pass
        
        return {
            'aula_id': aula_id,
            'turma_id': turma_id,
            'data': data_str,
            'qtd_presentes': qtd_presentes,
            'tem_ata': tem_ata
        }
        
    except Exception as e:
        return None

def processar_turma_completa(session, turma_info):
    """Processa uma turma: detalhes + aulas"""
    turma_id = turma_info['turma_id']
    
    # Obter detalhes da turma
    detalhes = obter_detalhes_turma(session, turma_id)
    
    if not detalhes:
        return None, []
    
    print(f"\nProcessando turma {turma_id} - {detalhes['curso']} - {detalhes['congregacao']}")
    
    # Buscar aulas da turma
    aulas = buscar_aulas_da_turma(session, turma_id)
    
    # Obter detalhes de cada aula
    aulas_completas = []
    
    for aula_info in aulas:
        detalhes_aula = obter_detalhes_aula(session, aula_info['aula_id'], turma_id)
        
        if detalhes_aula:
            # Filtrar por período
            if detalhes_aula['data']:
                try:
                    data = datetime.strptime(detalhes_aula['data'], "%d/%m/%Y")
                    
                    if DATA_INICIO <= data <= DATA_FIM:
                        aulas_completas.append({
                            **detalhes_aula,
                            'congregacao': detalhes['congregacao'],
                            'congregacao_id': detalhes['congregacao_id'],
                            'curso': detalhes['curso']
                        })
                        print(f"  ✓ Aula {detalhes_aula['aula_id']} - {detalhes_aula['data']} - Presentes: {detalhes_aula['qtd_presentes']} - ATA: {detalhes_aula['tem_ata']}")
                except:
                    pass
    
    return detalhes, aulas_completas

def enviar_para_sheets(turmas, aulas):
    """Envia dados para Google Sheets"""
    
    print(f"\nPreparando envio de {len(turmas)} turmas e {len(aulas)} aulas...")
    
    # Dados de turmas
    linhas_turmas = []
    for turma in turmas:
        linha = [
            turma['turma_id'],
            turma['congregacao_id'],
            turma['congregacao'],
            turma['curso'],
            turma['descricao'],
            turma['qtd_alunos']
        ]
        linhas_turmas.append(linha)
    
    headers_turmas = [
        "ID TURMA",
        "ID CONGREGAÇÃO",
        "CONGREGAÇÃO",
        "CURSO",
        "DESCRIÇÃO",
        "QTD ALUNOS"
    ]
    
    # Dados de aulas
    linhas_aulas = []
    for aula in aulas:
        linha = [
            aula['aula_id'],
            aula['turma_id'],
            aula['congregacao_id'],
            aula['congregacao'],
            aula['curso'],
            aula['data'],
            aula['qtd_presentes'],
            aula['tem_ata']
        ]
        linhas_aulas.append(linha)
    
    headers_aulas = [
        "ID AULA",
        "ID TURMA",
        "ID CONGREGAÇÃO",
        "CONGREGAÇÃO",
        "CURSO",
        "DATA",
        "QTD PRESENTES",
        "TEM ATA"
    ]
    
    body = {
        "tipo": "aulas_turmas_hortolandia",
        "turmas": {
            "dados": linhas_turmas,
            "headers": headers_turmas
        },
        "aulas": {
            "dados": linhas_aulas,
            "headers": headers_aulas
        },
        "resumo": {
            "total_turmas": len(turmas),
            "total_aulas": len(aulas),
            "periodo": f"{DATA_INICIO.strftime('%d/%m/%Y')} a {DATA_FIM.strftime('%d/%m/%Y')}",
            "aulas_com_ata": sum(1 for a in aulas if a['tem_ata'] == 'SIM'),
            "total_presencas": sum(a['qtd_presentes'] for a in aulas)
        }
    }
    
    try:
        print("Enviando dados...")
        response = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
        print(f"Dados enviados! Status: {response.status_code}")
        print(f"Resposta: {response.text}")
    except Exception as e:
        print(f"Erro ao enviar: {e}")
        
        with open('backup_completo.json', 'w', encoding='utf-8') as f:
            json.dump(body, f, indent=2, ensure_ascii=False)
        print("Backup salvo em: backup_completo.json")

def main():
    print("="*60)
    print("BUSCA DE AULAS POR TURMAS - HORTOLÂNDIA 2º SEM 2025")
    print("="*60)
    
    tempo_inicio = time.time()
    
    # Login
    session = fazer_login_e_obter_session()
    if not session:
        return
    
    # Buscar turmas de Hortolândia
    turmas_info = buscar_turmas_hortolandia(session)
    
    if not turmas_info:
        print("Nenhuma turma encontrada!")
        return
    
    # Processar cada turma
    todas_turmas = []
    todas_aulas = []
    
    for turma_info in turmas_info:
        turma, aulas = processar_turma_completa(session, turma_info)
        
        if turma:
            todas_turmas.append(turma)
            todas_aulas.extend(aulas)
    
    print("\n" + "="*60)
    print("RESULTADOS FINAIS")
    print("="*60)
    print(f"Turmas de Hortolândia: {len(todas_turmas)}")
    print(f"Aulas no período: {len(todas_aulas)}")
    print(f"Range de IDs considerados: {ID_AULA_INICIO} a {ID_AULA_FIM}")
    
    tempo_total = time.time() - tempo_inicio
    print(f"Tempo total: {tempo_total/60:.1f} minutos")
    
    if todas_aulas:
        # Estatísticas
        aulas_com_ata = sum(1 for a in todas_aulas if a['tem_ata'] == 'SIM')
        total_presentes = sum(a['qtd_presentes'] for a in todas_aulas)
        
        print(f"\nAulas com ATA: {aulas_com_ata}/{len(todas_aulas)} ({aulas_com_ata*100/len(todas_aulas):.1f}%)")
        print(f"Total de presenças: {total_presentes}")
        
        # Salvar localmente
        with open('resultado_completo.json', 'w', encoding='utf-8') as f:
            json.dump({
                'turmas': todas_turmas,
                'aulas': todas_aulas
            }, f, indent=2, ensure_ascii=False)
        print("\nResultados salvos em: resultado_completo.json")
        
        # Enviar para Sheets
        enviar_para_sheets(todas_turmas, todas_aulas)

if __name__ == "__main__":
    main()
