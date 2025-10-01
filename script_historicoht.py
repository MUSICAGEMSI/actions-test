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

def verificar_aula_existe(session, aula_id):
    """
    Verificação RÁPIDA se a aula existe (apenas HEAD request)
    Retorna True se existe, False caso contrário
    """
    try:
        url = f"https://musical.congregacao.org.br/aulas_abertas/editar/{aula_id}"
        resp = session.head(url, timeout=3)
        return resp.status_code == 200
    except:
        return False

def coletar_tudo_de_uma_vez(session, aula_id):
    """
    OTIMIZAÇÃO PRINCIPAL: Coleta TODOS os dados em uma única chamada
    Faz apenas 2 requests por aula (visualizar_aula + frequencias)
    """
    try:
        # REQUEST 1: visualizar_aula (pega quase tudo)
        url = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_aula/{aula_id}"
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        resp = session.get(url, headers=headers, timeout=8)
        
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Extrair data da aula
        data_aula = ""
        modal_header = soup.find('div', class_='modal-header')
        if modal_header:
            date_span = modal_header.find('span', class_='pull-right')
            if date_span:
                texto = date_span.get_text(strip=True)
                data_aula = texto.strip()
        
        # Extrair TODOS os dados da tabela principal
        tbody = soup.find('tbody')
        if not tbody:
            return None
        
        descricao = ""
        comum = ""
        hora_inicio = ""
        hora_termino = ""
        data_hora_abertura = ""
        nome_instrutor = ""
        id_turma = ""
        
        rows = tbody.find_all('tr')
        for row in rows:
            td_strong = row.find('strong')
            if not td_strong:
                continue
            
            label = td_strong.get_text(strip=True)
            tds = row.find_all('td')
            if len(tds) < 2:
                continue
            
            valor = tds[1].get_text(strip=True)
            
            if 'Comum Congregação' in label:
                comum = valor.upper()
            elif 'Início' in label and 'Horário' not in label:
                hora_inicio = valor[:5]
            elif 'Término' in label:
                hora_termino = valor[:5]
            elif 'Data e Horário de abertura' in label:
                data_hora_abertura = valor
            elif 'Instrutor(a) que ministrou a aula' in label:
                nome_instrutor = valor.split(' - ')[0].strip()
        
        # Extrair descrição do header
        modal_title = soup.find('h4', class_='modal-title')
        if modal_title:
            # Pegar o texto do <td> dentro da tabela que tem bg-blue-gradient
            table = soup.find('table', class_='table')
            if table:
                thead = table.find('thead')
                if thead:
                    td_desc = thead.find('td', class_='bg-blue-gradient')
                    if td_desc:
                        texto_desc = td_desc.get_text(strip=True)
                        # Remove o ícone e pega só o texto
                        descricao = texto_desc.replace('CLARINETE', '').strip()
                        if not descricao:
                            descricao = texto_desc.split()[-1] if texto_desc else ""
        
        # Se não achou descrição, tenta pegar do colspan
        if not descricao:
            td_colspan = soup.find('td', {'colspan': '2'})
            if td_colspan:
                descricao = td_colspan.get_text(strip=True)
        
        # Verificar se o instrutor é de Hortolândia
        eh_hortolandia = False
        if nome_instrutor:
            nome_normalizado = normalizar_nome(nome_instrutor)
            for nome_htl in NOMES_INSTRUTORES:
                if normalizar_nome(nome_htl) == nome_normalizado:
                    eh_hortolandia = True
                    break
        
        # Se não é de Hortolândia, retorna None imediatamente
        if not eh_hortolandia:
            return None
        
        # Verificar se tem ata e extrair o texto
        tem_ata = "Não"
        texto_ata = ""
        ata_table = soup.find_all('table', class_='table table-bordered table-striped table-hover')
        
        for table in ata_table:
            thead = table.find('thead', class_='bg-green-gradient')
            if thead:
                td_thead = thead.find('td')
                if td_thead and 'ATA DA AULA' in td_thead.get_text():
                    tem_ata = "Sim"
                    tbody_ata = table.find('tbody')
                    if tbody_ata:
                        td_ata = tbody_ata.find('td')
                        if td_ata:
                            texto_ata = td_ata.get_text(strip=True)
                    break
        
        # Pegar dia da semana do data_aula
        dia_semana = ""
        if data_aula:
            # Converter para dia da semana
            try:
                from datetime import datetime
                data_obj = datetime.strptime(data_aula, '%d/%m/%Y')
                dias = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
                dia_semana = dias[data_obj.weekday()]
            except:
                dia_semana = ""
        
        # REQUEST 2: Frequências (só se passou no filtro de Hortolândia)
        # Primeiro precisamos do id_turma - vamos tentar extrair do visualizar_aula
        # Mas não temos ele aqui... vamos fazer um request extra rápido
        url_editar = f"https://musical.congregacao.org.br/aulas_abertas/editar/{aula_id}"
        resp_editar = session.get(url_editar, headers=headers, timeout=5)
        
        if resp_editar.status_code == 200:
            soup_editar = BeautifulSoup(resp_editar.text, 'html.parser')
            turma_input = soup_editar.find('input', {'name': 'id_turma'})
            if turma_input:
                id_turma = turma_input.get('value', '').strip()
        
        # Agora sim, buscar frequências
        total_alunos = 0
        presentes = 0
        lista_presentes = ""
        lista_ausentes = ""
        
        if id_turma:
            url_freq = f"https://musical.congregacao.org.br/aulas_abertas/visualizar_frequencias/{aula_id}/{id_turma}"
            resp_freq = session.get(url_freq, headers=headers, timeout=8)
            
            if resp_freq.status_code == 200:
                soup_freq = BeautifulSoup(resp_freq.text, 'html.parser')
                tbody_freq = soup_freq.find('tbody')
                
                if tbody_freq:
                    linhas = tbody_freq.find_all('tr')
                    total_alunos = len(linhas)
                    
                    presentes_lista = []
                    ausentes_lista = []
                    
                    for linha in linhas:
                        td_nome = linha.find('td')
                        if not td_nome:
                            continue
                        
                        nome_completo = td_nome.get_text(strip=True)
                        nome_aluno = nome_completo.split(' - ')[0].strip()
                        
                        link = linha.find('a', {'data-id-membro': True})
                        id_membro = ""
                        if link:
                            id_membro = link.get('data-id-membro', '')
                        
                        icon_presente = linha.find('i', class_='fa-check')
                        
                        if icon_presente:
                            presentes_lista.append(f"{id_membro}-{nome_aluno}")
                        else:
                            ausentes_lista.append(f"{id_membro}-{nome_aluno}")
                    
                    lista_presentes = "; ".join(presentes_lista) if presentes_lista else ""
                    lista_ausentes = "; ".join(ausentes_lista) if ausentes_lista else ""
                    presentes = len(presentes_lista)
        
        return {
            'id_aula': aula_id,
            'id_turma': id_turma,
            'descricao': descricao,
            'comum': comum,
            'dia_semana': dia_semana,
            'hora_inicio': hora_inicio,
            'hora_termino': hora_termino,
            'data_aula': data_aula,
            'data_hora_abertura': data_hora_abertura,
            'tem_ata': tem_ata,
            'texto_ata': texto_ata,
            'instrutor': nome_instrutor,
            'total_alunos': total_alunos,
            'presentes': presentes,
            'lista_presentes': lista_presentes,
            'lista_ausentes': lista_ausentes
        }
        
    except Exception as e:
        return None

def main():
    global INSTRUTORES_HORTOLANDIA, NOMES_INSTRUTORES
    
    tempo_inicio = time.time()
    
    print("=" * 70)
    print("🚀 COLETOR ULTRA-RÁPIDO - HORTOLÂNDIA (META: 15 MINUTOS)")
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
    
    # Range de IDs
    ID_INICIAL = 327184
    ID_FINAL = 360000
    
    # OTIMIZAÇÃO: Aumentar paralelismo e reduzir timeout
    LOTE_SIZE = 200  # Dobrado
    MAX_WORKERS = 20  # Quadruplicado!
    
    print(f"\n{'=' * 70}")
    print(f"⚡ MODO TURBO ATIVADO!")
    print(f"📊 Range: {ID_INICIAL} a {ID_FINAL} ({ID_FINAL - ID_INICIAL + 1} IDs)")
    print(f"🔥 {MAX_WORKERS} threads paralelas | Lotes de {LOTE_SIZE}")
    print(f"🎯 Apenas 2-3 requests por aula válida")
    print(f"{'=' * 70}\n")
    
    for lote_inicio in range(ID_INICIAL, ID_FINAL + 1, LOTE_SIZE):
        lote_fim = min(lote_inicio + LOTE_SIZE - 1, ID_FINAL)
        
        # Processar lote em paralelo com MUITAS threads
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(coletar_tudo_de_uma_vez, session, aula_id): aula_id 
                for aula_id in range(lote_inicio, lote_fim + 1)
            }
            
            for future in as_completed(futures):
                aulas_processadas += 1
                dados_completos = future.result()
                
                if dados_completos:
                    resultado.append([
                        dados_completos['id_aula'],
                        dados_completos['id_turma'],
                        dados_completos['descricao'],
                        dados_completos['comum'],
                        dados_completos['dia_semana'],
                        dados_completos['hora_inicio'],
                        dados_completos['hora_termino'],
                        dados_completos['data_aula'],
                        dados_completos['data_hora_abertura'],
                        dados_completos['tem_ata'],
                        dados_completos['texto_ata'],
                        dados_completos['instrutor'],
                        dados_completos['total_alunos'],
                        dados_completos['presentes'],
                        dados_completos['lista_presentes'],
                        dados_completos['lista_ausentes']
                    ])
                    
                    aulas_hortolandia += 1
                    print(f"✅ [{aulas_processadas:5d}] ID {dados_completos['id_aula']}: {dados_completos['descricao'][:20]:20s} | {dados_completos['instrutor'][:25]:25s} | {dados_completos['presentes']}/{dados_completos['total_alunos']}")
                
                # Mostrar progresso a cada 200
                if aulas_processadas % 200 == 0:
                    tempo_decorrido = time.time() - tempo_inicio
                    velocidade = aulas_processadas / tempo_decorrido
                    tempo_estimado = (ID_FINAL - ID_INICIAL + 1 - aulas_processadas) / velocidade / 60
                    print(f"\n{'─' * 70}")
                    print(f"⚡ {aulas_processadas} processadas | {aulas_hortolandia} HTL | {velocidade:.1f} aulas/s | ETA: {tempo_estimado:.1f}min")
                    print(f"{'─' * 70}\n")
        
        time.sleep(0.5)  # Pausa mínima entre lotes
    
    print(f"\n{'=' * 70}")
    print(f"🎉 COLETA FINALIZADA!")
    print(f"{'=' * 70}")
    print(f"📊 Total processado: {aulas_processadas}")
    print(f"✅ Aulas de Hortolândia: {aulas_hortolandia}")
    print(f"⏱️  Tempo total: {(time.time() - tempo_inicio)/60:.1f} minutos")
    print(f"⚡ Velocidade média: {aulas_processadas/(time.time() - tempo_inicio):.1f} aulas/segundo")
    print(f"{'=' * 70}\n")
    
    # Preparar envio
    body = {
        "tipo": "historico_aulas_hortolandia",
        "dados": resultado,
        "headers": [
            "ID_Aula", "ID_Turma", "Descrição", "Comum", "Dia_Semana",
            "Hora_Início", "Hora_Término", "Data_Aula", "Data_Hora_Abertura", 
            "Tem_Ata", "Texto_Ata", "Instrutor",
            "Total_Alunos", "Presentes", "IDs_Nomes_Presentes", "IDs_Nomes_Ausentes"
        ],
        "resumo": {
            "total_aulas": len(resultado),
            "aulas_processadas": aulas_processadas,
            "total_instrutores_htl": len(INSTRUTORES_HORTOLANDIA),
            "tempo_minutos": round((time.time() - tempo_inicio)/60, 2),
            "velocidade_aulas_por_segundo": round(aulas_processadas/(time.time() - tempo_inicio), 2)
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
