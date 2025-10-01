from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os, requests, time, json, concurrent.futures
from typing import List, Set

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzJv9YlseCXdvXwi0OOpxh-Q61rmCly2kMUBEtcv5VSyPEKdcKg7MAVvIgDYSM1yWpV/exec'

RANGE_INICIO = 1
RANGE_FIM = 30000
NUM_THREADS = 20

print(f"🔍 COLETOR DE IDs - IGREJAS DE HORTOLÂNDIA")
print(f"📊 Range de busca: {RANGE_INICIO:,} - {RANGE_FIM:,}")
print(f"🧵 Threads: {NUM_THREADS}")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

# =========================
# Funções auxiliares
# =========================

def verificar_hortolandia(texto: str) -> bool:
    if not texto: return False
    texto_upper = texto.upper()
    variacoes = ["HORTOL", "HORTOLANDIA", "HORTOLÃNDIA", "HORTOLÂNDIA"]
    return any(var in texto_upper for var in variacoes)

class ColetorIDsHortolandia:
    def __init__(self, session, thread_id: int):
        self.session = session
        self.thread_id = thread_id
        self.ids_encontrados: Set[int] = set()
        self.requisicoes_feitas = 0
        self.headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }

    def coletar_batch_ids(self, ids_batch: List[int]) -> Set[int]:
        for igreja_id in ids_batch:
            try:
                url = f"https://musical.congregacao.org.br/igrejas/filtra_igreja_setor?id_igreja={igreja_id}"
                resp = self.session.get(url, headers=self.headers, timeout=8)
                self.requisicoes_feitas += 1
                if resp.status_code == 200:
                    try:
                        json_data = resp.json()
                        if isinstance(json_data, list) and len(json_data) > 0:
                            texto_completo = json_data[0].get('text', '')
                            if verificar_hortolandia(texto_completo):
                                self.ids_encontrados.add(igreja_id)
                                print(f"✅ T{self.thread_id}: ID {igreja_id} | {texto_completo[:60]}")
                    except:
                        pass
                time.sleep(0.05)
            except Exception as e:
                if "timeout" in str(e).lower():
                    print(f"⏱️ T{self.thread_id}: Timeout no ID {igreja_id}")
                continue
        return self.ids_encontrados

def executar_coleta_paralela_ids(session, range_inicio: int, range_fim: int, num_threads: int) -> List[int]:
    total_ids = range_fim - range_inicio + 1
    ids_per_thread = total_ids // num_threads
    print(f"📈 Dividindo {total_ids:,} IDs em {num_threads} threads ({ids_per_thread:,} IDs/thread)")

    thread_ranges = []
    for i in range(num_threads):
        inicio = range_inicio + (i * ids_per_thread)
        fim = inicio + ids_per_thread - 1
        if i == num_threads - 1:
            fim = range_fim
        thread_ranges.append(list(range(inicio, fim + 1)))

    todos_ids = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        coletores = [ColetorIDsHortolandia(session, i) for i in range(num_threads)]
        futures = [executor.submit(coletores[i].coletar_batch_ids, ids_thread) for i, ids_thread in enumerate(thread_ranges)]
        for i, future in enumerate(futures):
            try:
                ids_thread = future.result(timeout=1800)
                todos_ids.update(ids_thread)
                print(f"✅ Thread {i}: {len(ids_thread)} IDs encontrados")
            except Exception as e:
                print(f"❌ Thread {i}: Erro - {e}")
    return sorted(list(todos_ids))

def extrair_cookies_playwright(pagina):
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def salvar_ids_em_arquivo(ids: List[int], nome_arquivo: str = "ids_hortolandia.txt"):
    try:
        with open(nome_arquivo, 'w') as f:
            f.write(f"# IDs de Igrejas de Hortolândia\n")
            f.write(f"# Total: {len(ids)} IDs\n")
            f.write(f"# Gerado em: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            for igreja_id in ids:
                f.write(f"{igreja_id}\n")
        print(f"💾 IDs salvos em: {nome_arquivo}")
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo: {e}")

def enviar_para_planilha(ids: List[int]):
    payload = {
        "tipo": "ids_hortolandia",
        "headers": ["ID_Igreja"],
        "dados": [[igreja_id] for igreja_id in ids],
        "resumo": {
            "total_ids": len(ids),
            "range_inicio": RANGE_INICIO,
            "range_fim": RANGE_FIM
        }
    }
    try:
        resp = requests.post(URL_APPS_SCRIPT, json=payload, timeout=20)
        if resp.status_code == 200:
            print("✅ Dados enviados para planilha com sucesso!")
            print("Resposta:", resp.json())
        else:
            print(f"⚠️ Erro ao enviar: {resp.status_code} | {resp.text}")
    except Exception as e:
        print(f"❌ Erro na requisição: {e}")

# =========================
# MAIN
# =========================

def main():
    tempo_inicio = time.time()
    print("🔐 Realizando login...")

    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        pagina.goto(URL_INICIAL)
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')
        pagina.wait_for_selector("nav", timeout=15000)
        print("✅ Login realizado com sucesso!")
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()

    session = requests.Session()
    session.cookies.update(cookies_dict)
    adapter = requests.adapters.HTTPAdapter(pool_connections=NUM_THREADS + 5, pool_maxsize=NUM_THREADS + 5, max_retries=2)
    session.mount('https://', adapter)

    print("🔍 Iniciando busca de IDs de Hortolândia...")
    ids_hortolandia = executar_coleta_paralela_ids(session, RANGE_INICIO, RANGE_FIM, NUM_THREADS)
    tempo_total = time.time() - tempo_inicio

    print(f"\n🏁 COLETA DE IDs FINALIZADA!")
    print(f"📊 IDs de Hortolândia encontrados: {len(ids_hortolandia)}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s")

    if ids_hortolandia:
        salvar_ids_em_arquivo(ids_hortolandia)
        enviar_para_planilha(ids_hortolandia)

if __name__ == "__main__":
    main()
