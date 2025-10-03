from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os, requests, time, json, concurrent.futures
from typing import List, Dict, Set

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzJv9YlseCXdvXwi0OOpxh-Q61rmCly2kMUBEtcv5VSyPEKdcKg7MAVvIgDYSM1yWpV/exec'

RANGE_INICIO = 1
RANGE_FIM = 30000
NUM_THREADS = 30  # Aumentado de 20 para 30
BATCH_SIZE_ENVIO = 3000
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0

if not EMAIL or not SENHA:
    print("Erro: Credenciais nao definidas")
    exit(1)

def verificar_hortolandia(texto: str) -> bool:
    if not texto:
        return False
    texto_upper = texto.upper()
    variacoes = ["HORTOL", "HORTOLANDIA", "HORTOLÃNDIA", "HORTOLÂNDIA"]
    return any(var in texto_upper for var in variacoes)

def extrair_dados_localidade(texto_completo: str, igreja_id: int) -> Dict:
    """
    Extrai e separa os dados da localidade
    Formato: "JARDIM SANTANA - BR-SP-CAMPINAS-HORTOLÂNDIA"
    """
    try:
        # Corrigir encoding UTF-8
        if isinstance(texto_completo, str):
            # Decodificar unicode escapes
            texto_completo = texto_completo.encode('latin1').decode('utf-8')
        
        # Dividir por ' - '
        partes = texto_completo.split(' - ')
        
        if len(partes) >= 2:
            nome_localidade = partes[0].strip()
            caminho_completo = partes[1].strip()
            
            # Dividir o caminho: BR-SP-CAMPINAS-HORTOLÂNDIA
            caminho_partes = caminho_completo.split('-')
            
            if len(caminho_partes) >= 4:
                pais = caminho_partes[0].strip()
                estado = caminho_partes[1].strip()
                regiao = caminho_partes[2].strip()
                cidade = caminho_partes[3].strip()
                
                setor = f"{pais}-{estado}-{regiao}"
                
                return {
                    'id_igreja': igreja_id,
                    'nome_localidade': nome_localidade,
                    'setor': setor,
                    'cidade': cidade,
                    'texto_completo': texto_completo
                }
            elif len(caminho_partes) >= 3:
                # Formato alternativo
                setor = '-'.join(caminho_partes[:-1])
                cidade = caminho_partes[-1].strip()
                
                return {
                    'id_igreja': igreja_id,
                    'nome_localidade': nome_localidade,
                    'setor': setor,
                    'cidade': cidade,
                    'texto_completo': texto_completo
                }
        
        # Fallback - retorna dados completos sem divisão
        return {
            'id_igreja': igreja_id,
            'nome_localidade': texto_completo,
            'setor': '',
            'cidade': 'HORTOLANDIA',
            'texto_completo': texto_completo
        }
        
    except Exception as e:
        print(f"Erro ao extrair dados do ID {igreja_id}: {e}")
        return {
            'id_igreja': igreja_id,
            'nome_localidade': texto_completo,
            'setor': '',
            'cidade': 'HORTOLANDIA',
            'texto_completo': texto_completo
        }

class ColetorIDsHortolandia:
    def __init__(self, session, thread_id: int):
        self.session = session
        self.thread_id = thread_id
        self.localidades_encontradas: List[Dict] = []
        self.requisicoes_feitas = 0
        self.headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/painel',
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }

    def coletar_batch_ids(self, ids_batch: List[int]) -> List[Dict]:
        for igreja_id in ids_batch:
            try:
                url = f"https://musical.congregacao.org.br/igrejas/filtra_igreja_setor?id_igreja={igreja_id}"
                resp = self.session.get(url, headers=self.headers, timeout=8)
                self.requisicoes_feitas += 1
                
                if resp.status_code == 200:
                    try:
                        # Forçar encoding UTF-8
                        resp.encoding = 'utf-8'
                        json_data = resp.json()
                        
                        if isinstance(json_data, list) and len(json_data) > 0:
                            texto_completo = json_data[0].get('text', '')
                            
                            if verificar_hortolandia(texto_completo):
                                dados = extrair_dados_localidade(texto_completo, igreja_id)
                                self.localidades_encontradas.append(dados)
                                print(f"T{self.thread_id}: ID {igreja_id} | {dados['nome_localidade'][:40]} | {dados['cidade']}")
                    except Exception as e:
                        print(f"Erro JSON ID {igreja_id}: {e}")
                        pass
                
                time.sleep(0.03)  # Reduzido de 0.05 para 0.03
                
            except Exception as e:
                if "timeout" in str(e).lower():
                    print(f"Timeout T{self.thread_id}: ID {igreja_id}")
                continue
                
        return self.localidades_encontradas

def executar_coleta_paralela_ids(session, range_inicio: int, range_fim: int, num_threads: int) -> List[Dict]:
    total_ids = range_fim - range_inicio + 1
    ids_per_thread = total_ids // num_threads
    print(f"Dividindo {total_ids:,} IDs em {num_threads} threads ({ids_per_thread:,} IDs/thread)")

    thread_ranges = []
    for i in range(num_threads):
        inicio = range_inicio + (i * ids_per_thread)
        fim = inicio + ids_per_thread - 1
        if i == num_threads - 1:
            fim = range_fim
        thread_ranges.append(list(range(inicio, fim + 1)))

    todas_localidades = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        coletores = [ColetorIDsHortolandia(session, i) for i in range(num_threads)]
        futures = [executor.submit(coletores[i].coletar_batch_ids, ids_thread) for i, ids_thread in enumerate(thread_ranges)]
        
        for i, future in enumerate(futures):
            try:
                localidades_thread = future.result(timeout=1800)
                todas_localidades.extend(localidades_thread)
                coletor = coletores[i]
                print(f"Thread {i}: {len(localidades_thread)} localidades | {coletor.requisicoes_feitas} requisicoes")
            except Exception as e:
                print(f"Thread {i}: Erro - {e}")
                
    return todas_localidades

def extrair_cookies_playwright(pagina):
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def salvar_localidades_em_arquivo(localidades: List[Dict], nome_arquivo: str = "localidades_hortolandia.txt"):
    try:
        with open(nome_arquivo, 'w', encoding='utf-8') as f:
            f.write(f"# Localidades de Hortolandia\n")
            f.write(f"# Total: {len(localidades)} localidades\n")
            f.write(f"# Gerado em: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("ID | Nome Localidade | Setor | Cidade | Texto Completo\n")
            f.write("-" * 100 + "\n")
            
            for loc in localidades:
                f.write(f"{loc['id_igreja']} | {loc['nome_localidade']} | {loc['setor']} | {loc['cidade']} | {loc['texto_completo']}\n")
                
        print(f"Localidades salvas em: {nome_arquivo}")
    except Exception as e:
        print(f"Erro ao salvar arquivo: {e}")

def enviar_chunk_para_planilha(chunk: List[Dict], total_localidades: int, chunk_index: int, total_chunks: int) -> bool:
    # Preparar dados no formato correto
    dados_formatados = [
        [
            loc['id_igreja'],
            loc['nome_localidade'],
            loc['setor'],
            loc['cidade'],
            loc['texto_completo']
        ]
        for loc in chunk
    ]
    
    payload = {
        "tipo": "localidades_hortolandia",
        "headers": ["ID_Igreja", "Nome_Localidade", "Setor", "Cidade", "Texto_Completo"],
        "dados": dados_formatados,
        "resumo": {
            "total_localidades": total_localidades,
            "batch": f"{chunk_index}/{total_chunks}",
            "range_inicio": RANGE_INICIO,
            "range_fim": RANGE_FIM
        }
    }

    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            resp = requests.post(URL_APPS_SCRIPT, json=payload, timeout=30)
            if resp.status_code == 200:
                try:
                    j = resp.json()
                    print(f"Envio chunk {chunk_index}/{total_chunks} OK. Resposta: {j}")
                except Exception:
                    print(f"Envio chunk {chunk_index}/{total_chunks} OK.")
                return True
            else:
                print(f"Erro HTTP ({resp.status_code}) no envio chunk {chunk_index}: {resp.text[:200]}")
        except Exception as e:
            print(f"Erro envio chunk {chunk_index}: {e}")
            
        attempt += 1
        backoff = RETRY_BACKOFF * attempt
        print(f"Tentando novamente em {backoff}s (attempt {attempt}/{MAX_RETRIES})...")
        time.sleep(backoff)
        
    print(f"Falha no envio do chunk {chunk_index} apos {MAX_RETRIES} tentativas.")
    return False

def enviar_para_planilha(localidades: List[Dict], batch_size: int = BATCH_SIZE_ENVIO):
    total = len(localidades)
    if total == 0:
        print("Nenhuma localidade para enviar.")
        return

    chunks = [localidades[i:i+batch_size] for i in range(0, total, batch_size)]
    total_chunks = len(chunks)
    print(f"Enviando {total} localidades em {total_chunks} chunk(s) (batch_size={batch_size})")

    all_ok = True
    for idx, chunk in enumerate(chunks, start=1):
        ok = enviar_chunk_para_planilha(chunk, total, idx, total_chunks)
        if not ok:
            all_ok = False
            
    if all_ok:
        print("Todos os chunks enviados com sucesso.")
    else:
        print("Alguns chunks falharam - verifique logs.")

def main():
    tempo_inicio = time.time()
    print("Realizando login...")

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
            print("Login realizado com sucesso!")
        except Exception as e:
            print(f"Erro no login: {e}")
            navegador.close()
            return
            
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()

    session = requests.Session()
    session.cookies.update(cookies_dict)
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=NUM_THREADS + 10,  # Aumentado
        pool_maxsize=NUM_THREADS + 10,
        max_retries=2
    )
    session.mount('https://', adapter)

    print("Iniciando busca de localidades de Hortolandia...")
    localidades = executar_coleta_paralela_ids(session, RANGE_INICIO, RANGE_FIM, NUM_THREADS)
    tempo_total = time.time() - tempo_inicio

    print("\nCOLETA DE LOCALIDADES FINALIZADA!")
    print(f"Localidades de Hortolandia encontradas: {len(localidades)}")
    print(f"Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    print(f"Velocidade: {(RANGE_FIM - RANGE_INICIO + 1)/tempo_total:.1f} IDs/segundo")

    if localidades:
        salvar_localidades_em_arquivo(localidades)
        enviar_para_planilha(localidades)
    else:
        print("Nenhuma localidade de Hortolandia foi encontrada neste range")

if __name__ == "__main__":
    main()
