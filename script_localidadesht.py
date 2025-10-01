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
BATCH_SIZE_ENVIO = 3000   # envios em batches de X IDs
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0  # segundos, multiplicativo

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

def verificar_hortolandia(texto: str) -> bool:
    if not texto:
        return False
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
                    except Exception:
                        pass
                time.sleep(0.05)
            except Exception as e:
                if "timeout" in str(e).lower():
                    print(f"⏱️ T{self.thread_id}: Timeout no ID {igreja_id}")
                # ignora e segue
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
                coletor = coletores[i]
                print(f"✅ Thread {i}: {len(ids_thread)} IDs encontrados | {coletor.requisicoes_feitas} requisições")
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

def enviar_chunk_para_planilha(chunk: List[int], total_ids: int, chunk_index: int, total_chunks: int) -> bool:
    payload = {
        "tipo": "ids_hortolandia",
        "headers": ["ID_Igreja"],
        "dados": [[igreja_id] for igreja_id in chunk],
        "resumo": {
            "total_ids": total_ids,
            "batch": f"{chunk_index}/{total_chunks}",
            "range_inicio": RANGE_INICIO,
            "range_fim": RANGE_FIM
        }
    }

    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            resp = requests.post(URL_APPS_SCRIPT, json=payload, timeout=30)
            # Apps Script retorna um JSON com campo statusCode no body — validamos 200 HTTP
            if resp.status_code == 200:
                try:
                    j = resp.json()
                    print(f"✅ Envio chunk {chunk_index}/{total_chunks} OK. Resposta: {j}")
                except Exception:
                    print(f"✅ Envio chunk {chunk_index}/{total_chunks} OK. (resposta não JSON)")
                return True
            else:
                print(f"⚠️ Erro HTTP ({resp.status_code}) no envio chunk {chunk_index}: {resp.text[:200]}")
        except Exception as e:
            print(f"❌ Erro envio chunk {chunk_index}: {e}")
        attempt += 1
        backoff = RETRY_BACKOFF * (attempt)
        print(f"🔁 Tentando novamente em {backoff}s (attempt {attempt}/{MAX_RETRIES})...")
        time.sleep(backoff)
    print(f"❌ Falha no envio do chunk {chunk_index} após {MAX_RETRIES} tentativas.")
    return False

def enviar_para_planilha(ids: List[int], batch_size: int = BATCH_SIZE_ENVIO):
    total = len(ids)
    if total == 0:
        print("⚠️ Nenhum ID para enviar.")
        return

    chunks = [ids[i:i+batch_size] for i in range(0, total, batch_size)]
    total_chunks = len(chunks)
    print(f"📤 Enviando {total} IDs em {total_chunks} chunk(s) (batch_size={batch_size})")

    all_ok = True
    for idx, chunk in enumerate(chunks, start=1):
        ok = enviar_chunk_para_planilha(chunk, total, idx, total_chunks)
        if not ok:
            all_ok = False
            # decide se para ou continua; aqui continuamos para tentar enviar o máximo possível
    if all_ok:
        print("✅ Todos os chunks enviados com sucesso.")
    else:
        print("⚠️ Alguns chunks falharam — verifique logs e reenvie os chunks faltantes manualmente, se necessário.")

# =========================
# MAIN
# =========================
def main():
    tempo_inicio = time.time()
    print("🔐 Realizando login...")

    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        pagina.set_extra_http_headers({'User-Agent': 'Mozilla/5.0'})
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

    session = requests.Session()
    session.cookies.update(cookies_dict)
    adapter = requests.adapters.HTTPAdapter(pool_connections=NUM_THREADS + 5, pool_maxsize=NUM_THREADS + 5, max_retries=2)
    session.mount('https://', adapter)

    print("🔍 Iniciando busca de IDs de Hortolândia...")
    ids_hortolandia = executar_coleta_paralela_ids(session, RANGE_INICIO, RANGE_FIM, NUM_THREADS)
    tempo_total = time.time() - tempo_inicio

    print("\n🏁 COLETA DE IDs FINALIZADA!")
    print(f"📊 IDs de Hortolândia encontrados: {len(ids_hortolandia)}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s")

    if ids_hortolandia:
        salvar_ids_em_arquivo(ids_hortolandia)
        enviar_para_planilha(ids_hortolandia)
    else:
        print("⚠️ Nenhum ID de Hortolândia foi encontrado neste range")

if __name__ == "__main__":
    main()
