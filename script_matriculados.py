from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import requests
import time
from bs4 import BeautifulSoup

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxnp24RMIG4zQEsot0KATnFjdeoEHP7nyrr4WXnp-LLLptQTT-Vc_UPYoy__VWipill/exec'

# URL da planilha de origem (leitura dos IDs)
PLANILHA_ORIGEM_ID = "1DHvQewO7luUqDrO3IVzdlaNuN2Fsl5Dm_bXI_O2RF8g"
URL_LEITURA_IDS = f"https://docs.google.com/spreadsheets/d/{PLANILHA_ORIGEM_ID}/gviz/tq?tqx=out:csv&sheet=Dados das Turmas"

# ID da planilha de destino (onde serão escritos os resultados)
PLANILHA_DESTINO_ID = "1ADdprL1glmSTCH3PPJ5hnNAEhYK-OXXWKUURrA98ZDs"

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def buscar_ids_planilha():
    """
    Busca os IDs das turmas da planilha do Google Sheets
    """
    try:
        print("📥 Buscando IDs da planilha...")
        
        response = requests.get(URL_LEITURA_IDS, timeout=30)
        response.encoding = 'utf-8'
        
        if response.status_code == 200:
            linhas = response.text.strip().split('\n')
            
            # Identificar o índice da coluna ID_Turma
            cabecalho = linhas[0].split(',')
            
            # Limpar aspas do cabeçalho
            cabecalho = [col.strip('"') for col in cabecalho]
            
            if 'ID_Turma' not in cabecalho:
                print("❌ Coluna 'ID_Turma' não encontrada no cabeçalho!")
                print(f"Colunas disponíveis: {cabecalho}")
                return []
            
            idx_id_turma = cabecalho.index('ID_Turma')
            
            ids_turmas = []
            for i, linha in enumerate(linhas[1:], start=2):
                try:
                    colunas = linha.split(',')
                    if len(colunas) > idx_id_turma:
                        id_turma = colunas[idx_id_turma].strip('"').strip()
                        if id_turma and id_turma.isdigit():
                            ids_turmas.append(int(id_turma))
                except Exception as e:
                    print(f"⚠️ Erro ao processar linha {i}: {e}")
                    continue
            
            print(f"✅ {len(ids_turmas)} IDs encontrados na planilha")
            return ids_turmas
        else:
            print(f"❌ Erro ao acessar planilha. Status: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"❌ Erro ao buscar IDs da planilha: {e}")
        return []

def extrair_dados_alunos(session, turma_id):
    """
    Extrai dados detalhados de todos os alunos matriculados em uma turma
    Retorna lista de dicionários com: Nome, Comum, Instrumento, Status
    """
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        url = f"https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/{turma_id}"
        resp = session.get(url, headers=headers, timeout=15)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            tbody = soup.find('tbody')
            if not tbody:
                return []
            
            alunos = []
            rows = tbody.find_all('tr')
            
            for row in rows:
                tds = row.find_all('td')
                
                # Verificar se a linha tem dados válidos
                if len(tds) >= 4:
                    primeiro_td = tds[0].get_text(strip=True)
                    
                    # Ignorar linhas vazias ou mensagens de "Nenhum registro"
                    if not primeiro_td or 'Nenhum registro' in primeiro_td:
                        continue
                    
                    # Extrair dados
                    nome = tds[0].get_text(strip=True)
                    comum = tds[1].get_text(strip=True)
                    instrumento = tds[2].get_text(strip=True)
                    status = tds[3].get_text(strip=True)
                    
                    aluno = {
                        'ID_Turma': turma_id,
                        'Nome': nome,
                        'Comum': comum,
                        'Instrumento': instrumento,
                        'Status': status
                    }
                    
                    alunos.append(aluno)
            
            return alunos
        
        else:
            print(f"   ⚠️ Status {resp.status_code} para turma {turma_id}")
            return None
        
    except Exception as e:
        print(f"   ❌ Erro ao processar turma {turma_id}: {e}")
        return None

def contar_matriculados(session, turma_id):
    """
    Conta o número de alunos matriculados em uma turma específica
    """
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        url = f"https://musical.congregacao.org.br/matriculas/lista_alunos_matriculados_turma/{turma_id}"
        resp = session.get(url, headers=headers, timeout=15)
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Método 1: Contar linhas no tbody da tabela
            tbody = soup.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                valid_rows = []
                for row in rows:
                    tds = row.find_all('td')
                    if len(tds) >= 4:
                        primeiro_td = tds[0].get_text(strip=True)
                        if primeiro_td and 'Nenhum registro' not in primeiro_td:
                            valid_rows.append(row)
                
                if valid_rows:
                    return len(valid_rows)
            
            # Método 2: Contar botões "Desmatricular"
            botoes_desmatricular = soup.find_all('button', class_='btn-danger')
            if botoes_desmatricular:
                return len(botoes_desmatricular)
            
            # Método 3: Contar por função onclick="cancelarMatricula"
            onclick_count = str(resp.text).count('onclick="cancelarMatricula')
            if onclick_count > 0:
                return onclick_count
            
            return 0
        
        else:
            print(f"   ⚠️ Status {resp.status_code} para turma {turma_id}")
            return -1
        
    except Exception as e:
        print(f"   ❌ Erro ao processar turma {turma_id}: {e}")
        return -1

def extrair_cookies_playwright(pagina):
    """
    Extrai cookies do Playwright para usar em requests
    """
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def main():
    tempo_inicio = time.time()
    
    print("🚀 Iniciando processo de coleta de matrículas...")
    
    # Buscar IDs da planilha
    IDS_TURMAS = buscar_ids_planilha()
    
    if not IDS_TURMAS:
        print("❌ Nenhum ID encontrado. Encerrando...")
        return
    
    print(f"\n🎯 Total de turmas a processar: {len(IDS_TURMAS)}")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Login
        print("\n🔐 Realizando login...")
        pagina.goto(URL_INICIAL)
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')

        try:
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado com sucesso!")
        except PlaywrightTimeoutError:
            print("❌ Falha no login. Verifique suas credenciais.")
            navegador.close()
            return

        # Extrair cookies para usar com requests
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # Fechar navegador após obter cookies
        navegador.close()
        
        # Coletar dados de cada turma
        resultados_resumo = []
        todos_alunos = []
        total = len(IDS_TURMAS)
        
        print(f"\n📊 Processando {total} turmas...")
        
        for idx, turma_id in enumerate(IDS_TURMAS, 1):
            print(f"[{idx}/{total}] Turma {turma_id}...", end=" ")
            
            # Extrair dados detalhados dos alunos
            alunos = extrair_dados_alunos(session, turma_id)
            
            if alunos is not None:
                quantidade = len(alunos)
                print(f"✅ {quantidade} alunos")
                status = "Sucesso"
                
                # Adicionar alunos à lista geral
                todos_alunos.extend(alunos)
            else:
                print(f"⚠️ Erro na coleta")
                quantidade = 0
                status = "Erro"
            
            resultados_resumo.append([turma_id, quantidade, status])
            
            # Pausa para não sobrecarregar o servidor
            time.sleep(0.3)
        
        # Preparar dados para envio ao Google Sheets
        print("\n📤 Enviando dados para Google Sheets...")
        
        from datetime import datetime
        data_coleta = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        
        # Preparar dados de resumo (contagem)
        dados_resumo_com_cabecalho = [["ID_Turma", "Quantidade_Matriculados", "Status_Coleta"]] + resultados_resumo
        
        # Preparar dados detalhados (alunos)
        dados_alunos_para_envio = [["ID_Turma", "Nome", "Comum", "Instrumento", "Status"]]
        for aluno in todos_alunos:
            dados_alunos_para_envio.append([
                aluno['ID_Turma'],
                aluno['Nome'],
                aluno['Comum'],
                aluno['Instrumento'],
                aluno['Status']
            ])
        
        # Enviar dados de resumo
        body_resumo = {
            "tipo": "contagem_matriculas",
            "dados": dados_resumo_com_cabecalho,
            "data_coleta": data_coleta
        }
        
        # Enviar dados detalhados
        body_detalhado = {
            "tipo": "alunos_detalhados",
            "dados": dados_alunos_para_envio,
            "data_coleta": data_coleta
        }
        
        try:
            # Enviar resumo
            print("📊 Enviando resumo de contagem...")
            resposta_resumo = requests.post(URL_APPS_SCRIPT, json=body_resumo, timeout=60)
            print(f"   ✅ Resumo enviado - Status: {resposta_resumo.status_code}")
            
            # Enviar dados detalhados
            print("📋 Enviando dados detalhados dos alunos...")
            resposta_detalhado = requests.post(URL_APPS_SCRIPT, json=body_detalhado, timeout=60)
            print(f"   ✅ Detalhes enviados - Status: {resposta_detalhado.status_code}")
            print(f"   Total de alunos enviados: {len(todos_alunos)}")
            
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
            # Salvar backup local em caso de falha
            import json
            with open('backup_resumo.json', 'w', encoding='utf-8') as f:
                json.dump(resultados_resumo, f, indent=2, ensure_ascii=False)
            with open('backup_alunos.json', 'w', encoding='utf-8') as f:
                json.dump(todos_alunos, f, indent=2, ensure_ascii=False)
            print("💾 Backups salvos em 'backup_resumo.json' e 'backup_alunos.json'")
        
        tempo_total = time.time() - tempo_inicio
        print(f"\n⏱️ Tempo total de execução: {tempo_total:.2f} segundos")
        print(f"📊 Resumo: {len([r for r in resultados_resumo if r[2] == 'Sucesso'])} turmas processadas com sucesso")
        print(f"👥 Total de alunos coletados: {len(todos_alunos)}")

if __name__ == "__main__":
    main()
