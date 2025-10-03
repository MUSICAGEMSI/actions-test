from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os, requests, time, json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzJv9YlseCXdvXwi0OOpxh-Q61rmCly2kMUBEtcv5VSyPEKdcKg7MAVvIgDYSM1yWpV/exec'

RANGE_INICIO = 1
RANGE_FIM = 30000
NUM_THREADS = 50  # Aumentado para 50
BATCH_SIZE_ENVIO = 3000
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0

# Cache de IDs já verificados (evita reprocessamento)
cache_verificados = set()

if not EMAIL or not SENHA:
    print("Erro: Credenciais não definidas")
    exit(1)

def verificar_hortolandia(texto: str) -> bool:
    if not texto:
        return False
    texto_upper = texto.upper()
    variacoes = ["HORTOL", "HORTOLANDIA", "HORTOLÃNDIA", "HORTOLÂNDIA"]
    return any(var in texto_upper for var in variacoes)

def extrair_dados_localidade(texto_completo: str, igreja_id: int) -> Dict:
    try:
        if isinstance(texto_completo, str):
            texto_completo = texto_completo.encode('latin1').decode('utf-8')
        
        partes = texto_completo.split(' - ')
        
        if len(partes) >= 2:
            nome_localidade = partes[0].strip()
            caminho_completo = partes[1].strip()
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
                setor = '-'.join(caminho_partes[:-1])
                cidade = caminho_partes[-1].strip()
                
                return {
                    'id_igreja': igreja_id,
                    'nome_localidade': nome_localidade,
                    'setor': setor,
                    'cidade': cidade,
                    'texto_completo': texto_completo
                }
        
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

def criar_sessao_otimizada(cookies_dict: Dict) -> requests.Session:
    """Cria sessão com configurações otimizadas"""
    session = requests.Session()
    session.cookies.update(cookies_dict)
    
    # Retry automático
    retry_strategy = Retry(
        total=2,
        backoff_factor=0.1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(
        pool_connections=NUM_THREADS * 2,
        pool_maxsize=NUM_THREADS * 2,
        max_retries=retry_strategy
    )
    
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    
    return session

def verificar_id_hortolandia(igreja_id: int, session: requests.Session) -> Dict | None:
    """Verifica um único ID e retorna dados se for de Hortolândia"""
    if igreja_id in cache_verificados:
        return None
    
    try:
        url = f"https://musical.congregacao.org.br/igrejas/filtra_igreja_setor?id_igreja={igreja_id}"
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json'
        }
        
        resp = session.get(url, headers=headers, timeout=5)
        cache_verificados.add(igreja_id)
        
        if resp.status_code == 200:
            resp.encoding = 'utf-8'
            json_data = resp.json()
            
            if isinstance(json_data, list) and len(json_data) > 0:
                texto_completo = json_data[0].get('text', '')
                
                if verificar_hortolandia(texto_completo):
                    return extrair_dados_localidade(texto_completo, igreja_id)
        
        return None
        
    except requests.Timeout:
        return None
    except Exception:
        return None

def coletar_batch_paralelo(ids_batch: List[int], session: requests.Session, batch_num: int) -> List[Dict]:
    """Processa um batch de IDs em paralelo"""
    localidades = []
    total = len(ids_batch)
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(verificar_id_hortolandia, id_igreja, session): id_igreja 
                  for id_igreja in ids_batch}
        
        processados = 0
        for future in as_completed(futures):
            processados += 1
            try:
                resultado = future.result()
                if resultado:
                    localidades.append(resultado)
                    print(f"Batch {batch_num} [{processados}/{total}]: ID {resultado['id_igreja']} | {resultado['nome_localidade'][:40]}")
            except Exception:
                pass
    
    return localidades

def executar_coleta_paralela_ids(session: requests.Session, range_inicio: int, range_fim: int, num_threads: int) -> List[Dict]:
    """Executa coleta paralela dividindo em batches"""
    total_ids = range_fim - range_inicio + 1
    batch_size = max(100, total_ids // num_threads)
    
    print(f"Dividindo {total_ids:,} IDs em batches de {batch_size} IDs")
    print(f"Threads: {num_threads} | Timeout: 5s | Workers por batch: 10\n")
    
    batches = []
    for i in range(range_inicio, range_fim + 1, batch_size):
        fim_batch = min(i + batch_size - 1, range_fim)
        batches.append(list(range(i, fim_batch + 1)))
    
    todas_localidades = []
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {executor.submit(coletar_batch_paralelo, batch, session, idx): idx 
                  for idx, batch in enumerate(batches, 1)}
        
        for future in as_completed(futures):
            batch_num = futures[future]
            try:
                localidades = future.result()
                todas_localidades.extend(localidades)
                print(f"✓ Batch {batch_num}/{len(batches)} concluído: {len(localidades)} localidades encontradas")
            except Exception as e:
                print(f"✗ Batch {batch_num}/{len(batches)} falhou: {e}")
    
    return todas_localidades

def extrair_cookies_playwright(pagina):
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def salvar_localidades_em_arquivo(localidades: List[Dict], nome_arquivo: str = "localidades_hortolandia.txt"):
    try:
        with open(nome_arquivo, 'w', encoding='utf-8') as f:
            f.write(f"# Localidades de Hortolândia\n")
            f.write(f"# Total: {len(localidades)} localidades\n")
            f.write(f"# Gerado em: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("ID | Nome Localidade | Setor | Cidade | Texto Completo\n")
            f.write("-" * 100 + "\n")
            
            for loc in sorted(localidades, key=lambda x: x['id_igreja']):
                f.write(f"{loc['id_igreja']} | {loc['nome_localidade']} | {loc['setor']} | {loc['cidade']} | {loc['texto_completo']}\n")
                
        print(f"\n✓ Localidades salvas em: {nome_arquivo}")
    except Exception as e:
        print(f"✗ Erro ao salvar arquivo: {e}")

def enviar_chunk_para_planilha(chunk: List[Dict], total_localidades: int, chunk_index: int, total_chunks: int) -> bool:
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
            "range_fim": RANGE_FIM,
            "data_coleta": time.strftime('%Y-%m-%d %H:%M:%S')
        }
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(URL_APPS_SCRIPT, json=payload, timeout=30)
            if resp.status_code == 200:
                print(f"✓ Envio chunk {chunk_index}/{total_chunks} OK")
                return True
            else:
                print(f"✗ Erro HTTP ({resp.status_code}) no chunk {chunk_index}: {resp.text[:200]}")
        except Exception as e:
            print(f"✗ Erro envio chunk {chunk_index} (tentativa {attempt}/{MAX_RETRIES}): {e}")
            
        if attempt < MAX_RETRIES:
            backoff = RETRY_BACKOFF * attempt
            print(f"  Aguardando {backoff}s antes de tentar novamente...")
            time.sleep(backoff)
        
    print(f"✗ Falha no envio do chunk {chunk_index} após {MAX_RETRIES} tentativas")
    return False

def enviar_para_planilha(localidades: List[Dict], batch_size: int = BATCH_SIZE_ENVIO):
    total = len(localidades)
    if total == 0:
        print("\n⚠ Nenhuma localidade para enviar")
        return

    chunks = [localidades[i:i+batch_size] for i in range(0, total, batch_size)]
    total_chunks = len(chunks)
    print(f"\n📤 Enviando {total} localidades em {total_chunks} chunk(s) (batch_size={batch_size})")

    sucesso = 0
    falhas = 0
    
    for idx, chunk in enumerate(chunks, start=1):
        if enviar_chunk_para_planilha(chunk, total, idx, total_chunks):
            sucesso += 1
        else:
            falhas += 1
            
    print(f"\n{'✓' if falhas == 0 else '⚠'} Envio finalizado: {sucesso} sucesso(s), {falhas} falha(s)")

def main():
    tempo_inicio = time.time()
    print("=" * 60)
    print("COLETA DE LOCALIDADES DE HORTOLÂNDIA")
    print("=" * 60)
    print(f"Range: {RANGE_INICIO:,} até {RANGE_FIM:,} ({RANGE_FIM - RANGE_INICIO + 1:,} IDs)")
    print(f"Threads: {NUM_THREADS} | Timeout: 5s\n")
    
    print("🔐 Realizando login...")
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        pagina.set_extra_http_headers({'User-Agent': 'Mozilla/5.0'})
        
        try:
            pagina.goto(URL_INICIAL, timeout=15000)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=15000)
            print("✓ Login realizado com sucesso!\n")
        except Exception as e:
            print(f"✗ Erro no login: {e}")
            navegador.close()
            return
            
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()

    session = criar_sessao_otimizada(cookies_dict)

    print("🔍 Iniciando busca de localidades...\n")
    localidades = executar_coleta_paralela_ids(session, RANGE_INICIO, RANGE_FIM, NUM_THREADS)
    tempo_total = time.time() - tempo_inicio

    print("\n" + "=" * 60)
    print("COLETA FINALIZADA!")
    print("=" * 60)
    print(f"✓ Localidades encontradas: {len(localidades)}")
    print(f"⏱ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    print(f"⚡ Velocidade: {(RANGE_FIM - RANGE_INICIO + 1)/tempo_total:.1f} IDs/segundo")
    print("=" * 60)

    if localidades:
        salvar_localidades_em_arquivo(localidades)
        enviar_para_planilha(localidades)
    else:
        print("\n⚠ Nenhuma localidade de Hortolândia encontrada neste range")

if __name__ == "__main__":
    main()
