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

# Lista de IDs das turmas
IDS_TURMAS = [
    174, 2802, 7933, 26002, 26141, 27292, 27294, 27297, 27300, 27534, 27629, 27630, 27749, 27751, 27752, 27753, 27755, 27791, 27792, 27793, 27795, 27839, 27842, 27843, 27844, 27845, 27847, 27850, 27851, 27857, 27858, 27859, 27860, 27866, 27894, 27895, 27896, 27924, 27927, 27928, 27953, 28014, 28015, 28016, 28029, 28030, 28031, 28034, 28074, 28078, 28093, 28099, 28141, 28218, 28219, 28234, 28235, 28236, 28243, 28246, 28252, 28255, 28304, 28305, 28307, 28311, 28397, 28483, 28897, 28900, 28903, 28904, 29031, 29042, 29418, 29419, 29436, 29445, 29538, 29539, 29776, 29820, 29823, 29825, 30029, 30500, 30502, 30503, 31019, 31022, 31381, 31577, 31800, 31804, 32397, 32493, 32515, 32516, 33173, 33573, 33680, 33751, 33821, 34370, 35356, 36008, 36412, 36877, 36879, 36999, 37000, 37048, 37311, 37454, 39302, 39811, 40056, 40119, 40165, 40195, 40196, 40320, 40941, 41143, 41161, 41355, 41434, 41492, 41628, 41913, 41915, 41932, 42181, 42233, 42733, 42807, 42846, 43120, 43237, 43238, 43242, 43243, 43244, 43466, 43472, 43473, 43474, 43475, 43478, 43519, 43531, 43533, 43534, 43535, 43536, 43537, 43920, 43921, 43929, 43963, 43980, 44028, 44318, 44324, 44524, 44564, 44568, 45041, 45545, 45574, 45678, 46012, 46013, 46014, 46017, 46021, 46023, 46032, 46533, 46758, 46771, 46777, 46779, 46929, 47136, 47137, 47196, 47505, 47612, 47978, 48019, 48100, 48101, 48183, 48513, 48531, 48639, 48655, 48761, 48836, 48905, 49008, 49059, 49060, 49258, 50027, 50045, 50113, 50167, 50488, 50489, 50490, 50609, 51527, 51542, 51553, 51570, 51660, 51713, 51764, 51836, 51935, 52063, 52206, 52363, 52896, 53150, 53153, 53155, 53281, 53404, 53527, 53546
]

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

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
                # Filtrar apenas linhas válidas (que contêm dados de alunos)
                valid_rows = []
                for row in rows:
                    tds = row.find_all('td')
                    # Verificar se a linha tem pelo menos 4 colunas e não é mensagem de "sem registros"
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
    
    print(f"🚀 Iniciando coleta de {len(IDS_TURMAS)} turmas...")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Login
        print("🔐 Realizando login...")
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
        resultados = []
        total = len(IDS_TURMAS)
        
        print(f"\n📊 Processando {total} turmas...")
        
        for idx, turma_id in enumerate(IDS_TURMAS, 1):
            print(f"[{idx}/{total}] Turma {turma_id}...", end=" ")
            
            quantidade = contar_matriculados(session, turma_id)
            
            if quantidade >= 0:
                print(f"✅ {quantidade} matriculados")
                status = "Sucesso"
            else:
                print(f"⚠️ Erro na coleta")
                quantidade = 0
                status = "Erro"
            
            resultados.append([turma_id, quantidade, status])
            
            # Pausa para não sobrecarregar o servidor
            time.sleep(0.3)
        
        # Preparar dados para envio ao Google Sheets
        print("\n📤 Enviando dados para Google Sheets...")
        
        # Adicionar cabeçalho
        dados_com_cabecalho = [["ID", "QUANTIDADE", "STATUS"]] + resultados
        
        body = {
            "tipo": "contagem_matriculas",
            "dados": dados_com_cabecalho
        }
        
        # Enviar para Apps Script
        try:
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=60)
            print("✅ Dados enviados com sucesso!")
            print(f"   Status code: {resposta_post.status_code}")
            print(f"   Resposta: {resposta_post.text}")
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
            # Salvar backup local em caso de falha
            import json
            with open('backup_matriculas.json', 'w', encoding='utf-8') as f:
                json.dump(resultados, f, indent=2, ensure_ascii=False)
            print("💾 Backup salvo em 'backup_matriculas.json'")
        
        tempo_total = time.time() - tempo_inicio
        print(f"\n⏱️ Tempo total de execução: {tempo_total:.2f} segundos")
        print(f"📊 Resumo: {len([r for r in resultados if r[2] == 'Sucesso'])} sucessos, "
              f"{len([r for r in resultados if r[2] == 'Erro'])} erros")

if __name__ == "__main__":
    main()
