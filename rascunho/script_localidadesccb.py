from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os, requests, time, json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from threading import Lock

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbyjoA-eeVR1VTVZ1xMshuOl3tONsN1l6QPkvlkoMMSGnSOJkSeAJd1xjEy1ETLPUH04_Q/exec'

# ✅ COLETA COMPLETA: Todos os IDs
RANGE_INICIO = 1
RANGE_FIM = 50000

NUM_THREADS = 25
WORKERS_POR_BATCH = 8
BATCH_SIZE_ENVIO = 2000  # Envia a cada 2000 localidades
MAX_RETRIES = 3
RETRY_BACKOFF = 0.5
TIMEOUT_REQUEST = 10

# Locks para thread-safety
cache_lock = Lock()
resultado_lock = Lock()
envio_lock = Lock()

# Cache com controle
cache_verificados = set()
cache_falhas = {}
MAX_TENTATIVAS_ID = 2

# Contador de localidades enviadas
localidades_coletadas = []
contador_envios = 0

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

def extrair_dados_localidade(texto_completo: str, igreja_id: int) -> Dict:
    """Extrai dados estruturados da localidade"""
    try:
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
            'cidade': '',
            'texto_completo': texto_completo
        }
        
    except Exception as e:
        return {
            'id_igreja': igreja_id,
            'nome_localidade': texto_completo,
            'setor': '',
            'cidade': '',
            'texto_completo': texto_completo
        }

def criar_sessao_otimizada(cookies_dict: Dict) -> requests.Session:
    """Cria sessão HTTP com configurações robustas"""
    session = requests.Session()
    session.cookies.update(cookies_dict)
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    
    adapter = HTTPAdapter(
        pool_connections=NUM_THREADS * 3,
        pool_maxsize=NUM_THREADS * 3,
        max_retries=retry_strategy
    )
    
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    
    return session

def coletar_id_igreja(igreja_id: int, session: requests.Session) -> Optional[Dict]:
    """Coleta dados de um único ID"""
    
    with cache_lock:
        if igreja_id in cache_verificados:
            return None
        
        if cache_falhas.get(igreja_id, 0) >= MAX_TENTATIVAS_ID:
            return None
    
    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            url = f"https://musical.congregacao.org.br/igrejas/filtra_igreja_setor?id_igreja={igreja_id}"
            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'pt-BR,pt;q=0.9',
                'Connection': 'keep-alive'
            }
            
            resp = session.get(url, headers=headers, timeout=TIMEOUT_REQUEST)
            
            with cache_lock:
                cache_verificados.add(igreja_id)
            
            if resp.status_code == 200:
                resp.encoding = 'utf-8'
                
                try:
                    json_data = resp.json()
                except json.JSONDecodeError:
                    try:
                        json_data = json.loads(resp.content.decode('utf-8', errors='replace'))
                    except:
                        return None
                
                if isinstance(json_data, list) and len(json_data) > 0:
                    texto_completo = json_data[0].get('text', '')
                    
                    if texto_completo:  # ✅ PEGA TODOS, sem filtro
                        return extrair_dados_localidade(texto_completo, igreja_id)
                
                return None
            
            elif resp.status_code == 404:
                with cache_lock:
                    cache_verificados.add(igreja_id)
                return None
            
            else:
                if tentativa < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * tentativa * 0.3)
        
        except requests.Timeout:
            if tentativa < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * tentativa * 0.3)
        
        except requests.RequestException:
            if tentativa < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * tentativa * 0.5)
        
        except Exception as e:
            with cache_lock:
                cache_falhas[igreja_id] = cache_falhas.get(igreja_id, 0) + 1
            return None
    
    with cache_lock:
        cache_falhas[igreja_id] = MAX_TENTATIVAS_ID
    return None

def enviar_lote_para_planilha(localidades: List[Dict], lote_num: int) -> bool:
    """Envia um lote de localidades para o Google Sheets"""
    if not localidades:
        return True
    
    # Formata os dados
    dados_formatados = [
        [
            loc['id_igreja'],
            loc['nome_localidade'],
            loc['setor'],
            loc['cidade'],
            loc['texto_completo']
        ]
        for loc in localidades
    ]
    
    payload = {
        "tipo": "localidades_completas",
        "headers": ["ID_Igreja", "Nome_Localidade", "Setor", "Cidade", "Texto_Completo"],
        "dados": dados_formatados,
        "resumo": {
            "lote": lote_num,
            "total_neste_lote": len(localidades),
            "range_inicio": RANGE_INICIO,
            "range_fim": RANGE_FIM,
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        }
    }
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(URL_APPS_SCRIPT, json=payload, timeout=60)
            
            if resp.status_code == 200:
                try:
                    resultado = resp.json()
                    print(f"✓ Lote {lote_num} enviado: {len(localidades)} localidades ({resultado.get('status', 'ok')})")
                    return True
                except:
                    print(f"✓ Lote {lote_num} enviado: {len(localidades)} localidades")
                    return True
            else:
                print(f"⚠ Erro HTTP {resp.status_code} no lote {lote_num}: {resp.text[:100]}")
        except Exception as e:
            print(f"❌ Erro ao enviar lote {lote_num} (tentativa {attempt}/{MAX_RETRIES}): {e}")
            
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF * attempt)
    
    print(f"❌ Falha no envio do lote {lote_num} após {MAX_RETRIES} tentativas")
    return False

def coletar_batch_paralelo(ids_batch: List[int], session: requests.Session, batch_num: int, total_batches: int) -> List[Dict]:
    """Processa um batch de IDs em paralelo e envia automaticamente"""
    global localidades_coletadas, contador_envios
    
    localidades = []
    total = len(ids_batch)
    
    with ThreadPoolExecutor(max_workers=WORKERS_POR_BATCH) as executor:
        futures = {executor.submit(coletar_id_igreja, id_igreja, session): id_igreja 
                  for id_igreja in ids_batch}
        
        processados = 0
        for future in as_completed(futures):
            processados += 1
            try:
                resultado = future.result()
                if resultado:
                    localidades.append(resultado)
                    
                    # Feedback a cada 100 processados
                    if processados % 100 == 0:
                        print(f"  [{batch_num}/{total_batches}] Processados: {processados}/{total}")
            except Exception as e:
                pass
    
    # Adiciona ao buffer global
    with resultado_lock:
        localidades_coletadas.extend(localidades)
        
        # Envia automaticamente quando atingir o limite
        if len(localidades_coletadas) >= BATCH_SIZE_ENVIO:
            with envio_lock:
                contador_envios += 1
                lote_envio = localidades_coletadas[:BATCH_SIZE_ENVIO]
                localidades_coletadas = localidades_coletadas[BATCH_SIZE_ENVIO:]
                
                print(f"\n📤 Enviando lote {contador_envios} ({len(lote_envio)} localidades)...")
                enviar_lote_para_planilha(lote_envio, contador_envios)
                print("")
    
    return localidades

def executar_coleta_completa(session: requests.Session, range_inicio: int, range_fim: int, num_threads: int) -> List[Dict]:
    """Executa coleta completa de TODOS os IDs"""
    total_ids = range_fim - range_inicio + 1
    batch_size = max(100, total_ids // num_threads)
    
    print(f"\n{'='*80}")
    print(f"📊 CONFIGURAÇÃO DA COLETA COMPLETA")
    print(f"{'='*80}")
    print(f"   • Total IDs: {total_ids:,}")
    print(f"   • Batch size: {batch_size}")
    print(f"   • Threads principais: {num_threads}")
    print(f"   • Workers por batch: {WORKERS_POR_BATCH}")
    print(f"   • Envio automático a cada: {BATCH_SIZE_ENVIO} localidades")
    print(f"   • Timeout: {TIMEOUT_REQUEST}s")
    print(f"   • Max retries: {MAX_RETRIES}")
    print(f"{'='*80}\n")
    
    batches = []
    for i in range(range_inicio, range_fim + 1, batch_size):
        fim_batch = min(i + batch_size - 1, range_fim)
        batches.append(list(range(i, fim_batch + 1)))
    
    todas_localidades = []
    total_batches = len(batches)
    
    print(f"🚀 Iniciando coleta em {total_batches} batches...\n")
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {executor.submit(coletar_batch_paralelo, batch, session, idx, total_batches): idx 
                  for idx, batch in enumerate(batches, 1)}
        
        for future in as_completed(futures):
            batch_num = futures[future]
            try:
                localidades = future.result()
                with resultado_lock:
                    todas_localidades.extend(localidades)
                
                with cache_lock:
                    total_coletado = len(cache_verificados)
                    taxa = (total_coletado / total_ids) * 100
                
                print(f"✓ Batch {batch_num}/{total_batches} concluído | Progresso: {total_coletado:,}/{total_ids:,} ({taxa:.1f}%)")
            except Exception as e:
                print(f"❌ Batch {batch_num}/{total_batches} falhou: {e}")
    
    return todas_localidades

def extrair_cookies_playwright(pagina):
    """Extrai cookies da sessão do navegador"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def main():
    """Função principal"""
    global localidades_coletadas, contador_envios
    
    tempo_inicio = time.time()
    
    print("\n" + "=" * 80)
    print(" " * 25 + "COLETA COMPLETA DE LOCALIDADES")
    print("=" * 80)
    print(f"🎯 Objetivo: Coletar TODOS os IDs de {RANGE_INICIO:,} até {RANGE_FIM:,}")
    print(f"📊 Total: {RANGE_FIM - RANGE_INICIO + 1:,} IDs para processar")
    print(f"🔧 Threads: {NUM_THREADS} | Timeout: {TIMEOUT_REQUEST}s")
    print(f"📤 Envio automático a cada {BATCH_SIZE_ENVIO} localidades")
    print("=" * 80)
    
    print("\n🔐 Realizando login...")
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        pagina.set_extra_http_headers({'User-Agent': 'Mozilla/5.0'})
        
        try:
            pagina.goto(URL_INICIAL, timeout=20000)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=20000)
            print("✓ Login realizado com sucesso!\n")
        except Exception as e:
            print(f"❌ Erro no login: {e}")
            navegador.close()
            return
            
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()

    session = criar_sessao_otimizada(cookies_dict)

    print("🔍 Iniciando coleta completa...\n")
    localidades = executar_coleta_completa(session, RANGE_INICIO, RANGE_FIM, NUM_THREADS)
    
    # Envia o restante que ficou no buffer
    if localidades_coletadas:
        with envio_lock:
            contador_envios += 1
            print(f"\n📤 Enviando lote final {contador_envios} ({len(localidades_coletadas)} localidades)...")
            enviar_lote_para_planilha(localidades_coletadas, contador_envios)
            localidades_coletadas = []
    
    tempo_total = time.time() - tempo_inicio

    print("\n" + "=" * 80)
    print(" " * 30 + "COLETA FINALIZADA!")
    print("=" * 80)
    print(f"✓ Localidades encontradas: {len(localidades):,}")
    print(f"📤 Total de lotes enviados: {contador_envios}")
    print(f"⏱ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    print(f"⚡ Velocidade: {(RANGE_FIM - RANGE_INICIO + 1)/tempo_total:.1f} IDs/segundo")
    
    with cache_lock:
        total_verificados = len(cache_verificados)
        total_falhas = len([k for k, v in cache_falhas.items() if v >= MAX_TENTATIVAS_ID])
    
    print(f"📊 IDs verificados: {total_verificados:,} / {RANGE_FIM - RANGE_INICIO + 1:,}")
    print(f"📈 Taxa de sucesso: {(len(localidades)/total_verificados*100):.1f}%")
    
    if total_falhas > 0:
        print(f"⚠ IDs com falha permanente: {total_falhas}")
    
    print("=" * 80)
    
    # Salva arquivo local de backup
    if localidades:
        with open("localidades_completas.txt", 'w', encoding='utf-8') as f:
            f.write(f"Total: {len(localidades)} localidades\n")
            f.write(f"Range: {RANGE_INICIO} - {RANGE_FIM}\n")
            f.write(f"Data: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            for loc in sorted(localidades, key=lambda x: x['id_igreja']):
                f.write(f"{loc['id_igreja']}|{loc['nome_localidade']}|{loc['setor']}|{loc['cidade']}\n")
        print(f"\n💾 Backup local salvo: localidades_completas.txt")

if __name__ == "__main__":
    main()
