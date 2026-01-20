from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
import json
from bs4 import BeautifulSoup
import concurrent.futures

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbysZ9IbdgG8Vayz8B4Kp-C5_Pd0NZiBCIIo-uA5GmJbaLMpvHcT-qQLYnrQyuUsI1fX/exec'

DIAS_SEMANA = {
    "0": "DOMINGO",
    "1": "SEGUNDA-FEIRA",
    "2": "TERÇA-FEIRA",
    "3": "QUARTA-FEIRA",
    "4": "QUINTA-FEIRA",
    "5": "SEXTA-FEIRA",
    "6": "SÁBADO"
}

print("🎵 Coletor de Turmas - Hortolândia")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

def descobrir_igrejas_hortolandia(session, range_inicio=20000, range_fim=27000):
    """
    Descobre dinamicamente IDs de igrejas de Hortolândia
    """
    print(f"🔍 Descobrindo igrejas de Hortolândia (range {range_inicio}-{range_fim})...")
    
    igrejas = []
    headers = {
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    # Buscar em lotes para otimizar
    for igreja_id in range(range_inicio, range_fim + 1):
        try:
            url = f"https://musical.congregacao.org.br/igrejas/filtra_igreja_setor?id_igreja={igreja_id}"
            resp = session.get(url, headers=headers, timeout=5)
            
            if resp.status_code == 200:
                data = resp.json()
                if data and len(data) > 0:
                    texto = data[0].get('text', '')
                    
                    # Verificar se contém HORTOLÂNDIA (com ou sem acento/unicode)
                    if 'HORTOL' in texto.upper():
                        igreja_info = {
                            'id': data[0].get('id', str(igreja_id)),
                            'nome': texto
                        }
                        igrejas.append(igreja_info)
                        print(f"  ✓ Encontrada: {texto[:50]}")
            
            # Pequena pausa para não sobrecarregar
            if igreja_id % 100 == 0:
                print(f"  ... processados {igreja_id - range_inicio} IDs")
                time.sleep(0.5)
            else:
                time.sleep(0.05)
                
        except Exception:
            continue
    
    print(f"✅ {len(igrejas)} igrejas de Hortolândia encontradas")
    return igrejas

def extrair_turmas_igreja(session, igreja_id, igreja_nome):
    """
    Extrai todas as turmas de uma igreja específica via scraping
    """
    turmas = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://musical.congregacao.org.br/painel'
    }
    
    try:
        # Buscar página de listagem de turmas filtrada por igreja
        url = f"https://musical.congregacao.org.br/turmas?id_igreja={igreja_id}"
        resp = session.get(url, headers=headers, timeout=15)
        
        if resp.status_code != 200:
            return turmas
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Procurar links de edição de turmas
        # Padrão: /turmas/editar/{id}
        links_turmas = soup.find_all('a', href=re.compile(r'/turmas/editar/\d+'))
        
        ids_turmas_encontrados = set()
        for link in links_turmas:
            href = link.get('href', '')
            match = re.search(r'/turmas/editar/(\d+)', href)
            if match:
                ids_turmas_encontrados.add(match.group(1))
        
        # Para cada ID de turma, buscar detalhes
        for turma_id in ids_turmas_encontrados:
            turma_info = extrair_detalhes_turma(session, turma_id, igreja_id, igreja_nome)
            if turma_info:
                turmas.append(turma_info)
                time.sleep(0.2)
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair turmas da igreja {igreja_id}: {e}")
    
    return turmas

def extrair_detalhes_turma(session, turma_id, igreja_id, igreja_nome):
    """
    Extrai detalhes completos de uma turma específica
    """
    try:
        url = f"https://musical.congregacao.org.br/turmas/editar/{turma_id}"
        resp = session.get(url, timeout=10)
        
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Extrair dados do formulário
        descricao = soup.find('input', {'name': 'descricao'})
        descricao = descricao.get('value', '') if descricao else ''
        
        dia_semana_select = soup.find('select', {'name': 'dia_semana'})
        dia_semana_code = ''
        if dia_semana_select:
            option_selected = dia_semana_select.find('option', {'selected': True})
            if option_selected:
                dia_semana_code = option_selected.get('value', '')
        
        dia_semana = DIAS_SEMANA.get(dia_semana_code, '')
        
        # Horários
        hr_inicio = soup.find('input', {'name': 'hr_inicio'})
        hr_inicio = hr_inicio.get('value', '00:00')[:5] if hr_inicio else '00:00'
        
        hr_fim = soup.find('input', {'name': 'hr_fim'})
        hr_fim = hr_fim.get('value', '00:00')[:5] if hr_fim else '00:00'
        
        # Curso (instrumento)
        curso_select = soup.find('select', {'name': 'id_curso'})
        curso = ''
        if curso_select:
            option_selected = curso_select.find('option', {'selected': True})
            if option_selected:
                curso = option_selected.text.strip()
        
        # Buscar quantidade de matriculados
        qtd_matriculados = buscar_quantidade_matriculados(session, turma_id)
        
        turma_info = {
            'id_turma': turma_id,
            'id_igreja': igreja_id,
            'igreja': igreja_nome,
            'descricao': descricao,
            'curso': curso,
            'dia_semana': dia_semana,
            'horario_inicio': hr_inicio,
            'horario_fim': hr_fim,
            'qtd_matriculados': qtd_matriculados
        }
        
        return turma_info
        
    except Exception as e:
        print(f"⚠️ Erro ao extrair detalhes da turma {turma_id}: {e}")
        return None

def buscar_quantidade_matriculados(session, turma_id):
    """
    Busca quantidade de alunos matriculados em uma turma
    """
    try:
        url = f"https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/{turma_id}"
        resp = session.get(url, timeout=10)
        
        if resp.status_code != 200:
            return 0
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Contar linhas da tabela (excluindo header)
        tbody = soup.find('tbody')
        if tbody:
            rows = tbody.find_all('tr')
            return len(rows)
        
        return 0
        
    except Exception:
        return 0

def processar_igreja(args):
    """
    Processa uma igreja (para paralelização)
    """
    session, igreja = args
    igreja_id = igreja['id']
    igreja_nome = igreja['nome']
    
    print(f"🔄 Processando: {igreja_nome[:50]}...")
    
    turmas = extrair_turmas_igreja(session, igreja_id, igreja_nome)
    
    print(f"  ✓ {len(turmas)} turmas encontradas")
    
    return turmas

def criar_relatorio_turmas(todas_turmas):
    """
    Cria relatório formatado para Google Sheets
    """
    relatorio = [
        ["ID_TURMA", "ID_IGREJA", "IGREJA", "DIA_SEMANA", "HORARIO_INICIO", 
         "HORARIO_FIM", "CURSO", "DESCRICAO", "QTD_MATRICULADOS"]
    ]
    
    for turma in todas_turmas:
        linha = [
            turma.get('id_turma', ''),
            turma.get('id_igreja', ''),
            turma.get('igreja', ''),
            turma.get('dia_semana', ''),
            turma.get('horario_inicio', ''),
            turma.get('horario_fim', ''),
            turma.get('curso', ''),
            turma.get('descricao', ''),
            turma.get('qtd_matriculados', 0)
        ]
        relatorio.append(linha)
    
    return relatorio

def extrair_cookies_playwright(pagina):
    """
    Extrai cookies do Playwright para requests
    """
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def main():
    tempo_inicio = time.time()
    
    print("🔐 Realizando login...")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        try:
            pagina.goto(URL_INICIAL)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado com sucesso!")
            
        except Exception as e:
            print(f"❌ Erro no login: {e}")
            navegador.close()
            return
        
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
    
    # Criar sessão requests
    session = requests.Session()
    session.cookies.update(cookies_dict)
    
    # Fase 1: Descobrir igrejas de Hortolândia
    igrejas_hortolandia = descobrir_igrejas_hortolandia(session)
    
    if not igrejas_hortolandia:
        print("❌ Nenhuma igreja de Hortolândia encontrada")
        return
    
    # Fase 2: Coletar turmas de cada igreja
    print(f"\n📚 Coletando turmas de {len(igrejas_hortolandia)} igrejas...")
    
    todas_turmas = []
    
    # Processar sequencialmente para evitar sobrecarga
    for igreja in igrejas_hortolandia:
        turmas = processar_igreja((session, igreja))
        todas_turmas.extend(turmas)
        time.sleep(1)  # Pausa entre igrejas
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n🏁 COLETA FINALIZADA!")
    print(f"📊 Total de turmas coletadas: {len(todas_turmas)}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    
    # Mostrar resumo por dia da semana
    if todas_turmas:
        print("\n📅 Resumo por dia da semana:")
        resumo_dias = {}
        for turma in todas_turmas:
            dia = turma.get('dia_semana', 'NÃO DEFINIDO')
            resumo_dias[dia] = resumo_dias.get(dia, 0) + 1
        
        for dia, qtd in sorted(resumo_dias.items()):
            print(f"   {dia}: {qtd} turmas")
        
        # Mostrar exemplos
        print("\n📋 Exemplos de turmas coletadas:")
        for i, turma in enumerate(todas_turmas[:5]):
            print(f"   {i+1}. {turma['igreja'][:30]} | {turma['dia_semana']} | {turma['descricao'][:40]} | {turma['qtd_matriculados']} alunos")
    
    # Enviar para Google Sheets
    if todas_turmas:
        print("\n📤 Enviando dados para Google Sheets...")
        
        relatorio = criar_relatorio_turmas(todas_turmas)
        
        payload = {
            "tipo": "turmas_hortolandia",
            "relatorio_formatado": relatorio,
            "metadata": {
                "total_turmas": len(todas_turmas),
                "total_igrejas": len(igrejas_hortolandia),
                "tempo_execucao_min": round(tempo_total/60, 2),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        }
        
        try:
            response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=120)
            if response.status_code == 200:
                print("✅ Dados enviados com sucesso!")
                print(f"📄 Resposta: {response.text[:100]}")
            else:
                print(f"⚠️ Status HTTP: {response.status_code}")
        except Exception as e:
            print(f"❌ Erro no envio: {e}")
    
    else:
        print("⚠️ Nenhuma turma foi coletada")
    
    print("\n🎯 Processo finalizado!")

if __name__ == "__main__":
    main()
