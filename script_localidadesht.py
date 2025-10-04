from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os, requests, time, json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Set
from threading import Lock
import random

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzJv9YlseCXdvXwi0OOpxh-Q61rmCly2kMUBEtcv5VSyPEKdcKg7MAVvIgDYSM1yWpV/exec'

# CONFIGURAÇÕES BALANCEADAS
RANGE_INICIO = 1
RANGE_FIM = 30000
NUM_THREADS = 30  # Balanceado
TIMEOUT = 6  # Mais generoso
DELAY_BASE = 0.02  # Pequeno delay para não sobrecarregar
MAX_RETRIES_PER_ID = 2  # Retry por ID
BATCH_SIZE_ENVIO = 3000

# Controle thread-safe
lock = Lock()
ids_encontrados = set()
requisicoes_totais = 0
erros_totais = 0

if not EMAIL or not SENHA:
    print("Erro: Credenciais não definidas")
    exit(1)

def verificar_hortolandia(texto: str) -> bool:
    if not texto:
        return False
    texto_upper = texto.upper()
    variacoes = ["HORTOL", "HORTOLANDIA", "HORTOLÃNDIA", "HORTOLÂNDIA"]
    return any(var in texto_upper for var in variacoes)

def verificar_id_unico(igreja_id: int, session: requests.Session, thread_id: int) -> Dict | None:
    """Verifica um único ID com retry"""
    global requisicoes_totais, erros_totais
    
    for tentativa in range(MAX_RETRIES_PER_ID):
        try:
            url = f"https://musical.congregacao.org.br/igrejas/filtra_igreja_setor?id_igreja={igreja_id}"
            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'User-Agent': 'Mozilla/5.0',
                'Accept': 'application/json'
            }
            
            resp = session.get(url, headers=headers, timeout=TIMEOUT)
            
            with lock:
                requisicoes_totais += 1
            
            if resp.status_code == 200:
                try:
                    resp.encoding = 'utf-8'
                    json_data = resp.json()
                    
                    if isinstance(json_data, list) and len(json_data) > 0:
                        texto_completo = json_data[0].get('text', '')
                        
                        if verificar_hortolandia(texto_completo):
                            # Extrair dados básicos
                            partes = texto_completo.split(' - ')
                            nome = partes[0].strip() if len(partes) > 0 else texto_completo
                            
                            return {
                                'id': igreja_id,
                                'nome': nome,
                                'texto': texto_completo
                            }
                except Exception as e:
                    with lock:
                        erros_totais += 1
                    if tentativa == MAX_RETRIES_PER_ID - 1:
                        print(f"T{thread_id}: Erro JSON no ID {igreja_id}: {e}")
                    continue
            
            # Sucesso (mesmo que não seja Hortolândia)
            return None
            
        except requests.Timeout:
            with lock:
                erros_totais += 1
            if tentativa < MAX_RETRIES_PER_ID - 1:
                time.sleep(0.5)  # Aguarda antes de retry
                continue
            else:
                print(f"T{thread_id}: Timeout final no ID {igreja_id}")
                return None
                
        except Exception as e:
            with lock:
                erros_totais += 1
            if tentativa < MAX_RETRIES_PER_ID - 1:
                time.sleep(0.3)
                continue
            return None
    
    return None

def processar_batch(ids_batch: List[int], session: requests.Session, thread_id: int) -> List[Dict]:
    """Processa um batch de IDs"""
    locais = []
    total = len(ids_batch)
    processados = 0
    
    for igreja_id in ids_batch:
        resultado = verificar_id_unico(igreja_id, session, thread_id)
        
        if resultado:
            with lock:
                if resultado['id'] not in ids_encontrados:
                    ids_encontrados.add(resultado['id'])
                    locais.append(resultado)
                    print(f"T{thread_id} [{processados+1}/{total}]: ID {resultado['id']} | {resultado['nome'][:50]}")
        
        processados += 1
        
        # Progresso a cada 50 IDs
        if processados % 50 == 0:
            print(f"T{thread_id}: {processados}/{total} processados")
        
        # Pequeno delay randomizado para evitar rate limit
        time.sleep(DELAY_BASE + random.uniform(0, 0.01))
    
    return locais

def executar_coleta_paralela(session: requests.Session, range_inicio: int, range_fim: int, num_threads: int) -> List[Dict]:
    """Executa coleta com threads balanceadas"""
    total_ids = range_fim - range_inicio + 1
    batch_size = max(100, total_ids // num_threads)
    
    print(f"Configuração:")
    print(f"  Range: {range_inicio:,} - {range_fim:,} ({total_ids:,} IDs)")
    print(f"  Threads: {num_threads}")
    print(f"  Batch size: {batch_size}")
    print(f"  Timeout: {TIMEOUT}s")
    print(f"  Retry por ID: {MAX_RETRIES_PER_ID}")
    print()
    
    # Dividir em batches
    batches = []
    for i in range(range_inicio, range_fim + 1, batch_size):
        fim_batch = min(i + batch_size - 1, range_fim)
        batches.append(list(range(i, fim_batch + 1)))
    
    print(f"Iniciando {len(batches)} batches...\n")
    
    todos_locais = []
    tempo_inicio = time.time()
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {executor.submit(processar_batch, batch, session, idx): idx 
                  for idx, batch in enumerate(batches)}
        
        concluidos = 0
        for future in as_completed(futures):
            thread_id = futures[future]
            try:
                locais = future.result()
                todos_locais.extend(locais)
                
                concluidos += 1
                progresso = (concluidos / len(batches)) * 100
                tempo_decorrido = time.time() - tempo_inicio
                velocidade = requisicoes_totais / tempo_decorrido if tempo_decorrido > 0 else 0
                
                if concluidos % 5 == 0 or concluidos == len(batches):
                    print(f"\n[{progresso:.1f}%] Batch {concluidos}/{len(batches)} | "
                          f"{len(todos_locais)} encontrados | "
                          f"{velocidade:.1f} req/s | "
                          f"{erros_totais} erros\n")
                
            except Exception as e:
                print(f"Erro no batch {thread_id}: {e}")
    
    return todos_locais

def extrair_cookies_playwright(pagina):
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def salvar_resultados(localidades: List[Dict], arquivo: str = "localidades_hortolandia.txt"):
    try:
        with open(arquivo, 'w', encoding='utf-8') as f:
            f.write(f"# Localidades de Hortolândia\n")
            f.write(f"# Total: {len(localidades)}\n")
            f.write(f"# Data: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("ID | Nome | Texto Completo\n")
            f.write("-" * 100 + "\n")
            
            for loc in sorted(localidades, key=lambda x: x['id']):
                f.write(f"{loc['id']} | {loc['nome']} | {loc['texto']}\n")
        
        print(f"\nArquivo salvo: {arquivo}")
    except Exception as e:
        print(f"Erro ao salvar: {e}")

def enviar_para_planilha(localidades: List[Dict]):
    """Envia dados para Google Sheets"""
    if not localidades:
        print("\nNenhuma localidade para enviar")
        return
    
    print(f"\nEnviando {len(localidades)} localidades para Google Sheets...")
    
    # Preparar dados
    dados_formatados = []
    for loc in sorted(localidades, key=lambda x: x['id']):
        # Extrair campos do texto completo
        partes = loc['texto'].split(' - ')
        if len(partes) >= 2:
            nome = partes[0].strip()
            caminho = partes[1].strip()
            caminho_partes = caminho.split('-')
            
            if len(caminho_partes) >= 4:
                setor = '-'.join(caminho_partes[:3])
                cidade = caminho_partes[3]
            else:
                setor = ''
                cidade = 'HORTOLANDIA'
        else:
            nome = loc['nome']
            setor = ''
            cidade = 'HORTOLANDIA'
        
        dados_formatados.append([
            loc['id'],
            nome,
            setor,
            cidade,
            loc['texto']
        ])
    
    payload = {
        "tipo": "localidades_hortolandia",
        "headers": ["ID_Igreja", "Nome_Localidade", "Setor", "Cidade", "Texto_Completo"],
        "dados": dados_formatados,
        "resumo": {
            "total_localidades": len(localidades),
            "range_inicio": RANGE_INICIO,
            "range_fim": RANGE_FIM,
            "data_coleta": time.strftime('%Y-%m-%d %H:%M:%S')
        }
    }
    
    try:
        resp = requests.post(URL_APPS_SCRIPT, json=payload, timeout=60)
        if resp.status_code == 200:
            print("Dados enviados com sucesso!")
        else:
            print(f"Erro no envio: Status {resp.status_code}")
            print(resp.text[:200])
    except Exception as e:
        print(f"Erro ao enviar: {e}")

def main():
    tempo_inicio = time.time()
    
    print("=" * 70)
    print("COLETA DE LOCALIDADES DE HORTOLÂNDIA - VERSÃO BALANCEADA")
    print("=" * 70)
    
    print("\nRealizando login...")
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
            print("Login realizado!\n")
        except Exception as e:
            print(f"Erro no login: {e}")
            navegador.close()
            return
        
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
    
    # Configurar sessão
    session = requests.Session()
    session.cookies.update(cookies_dict)
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=NUM_THREADS * 2,
        pool_maxsize=NUM_THREADS * 2,
        max_retries=1
    )
    session.mount('https://', adapter)
    
    # Executar coleta
    print("Iniciando busca...\n")
    localidades = executar_coleta_paralela(session, RANGE_INICIO, RANGE_FIM, NUM_THREADS)
    
    tempo_total = time.time() - tempo_inicio
    
    # Resultados
    print("\n" + "=" * 70)
    print("COLETA FINALIZADA")
    print("=" * 70)
    print(f"Localidades encontradas: {len(localidades)}")
    print(f"Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    print(f"Requisições: {requisicoes_totais:,}")
    print(f"Erros: {erros_totais}")
    print(f"Velocidade: {requisicoes_totais/tempo_total:.1f} req/s")
    print(f"Taxa de sucesso: {((requisicoes_totais-erros_totais)/requisicoes_totais*100):.1f}%")
    print("=" * 70)
    
    if localidades:
        # Mostrar primeiras 10
        print(f"\nPrimeiras 10 localidades:")
        for i, loc in enumerate(sorted(localidades, key=lambda x: x['id'])[:10], 1):
            print(f"  {i}. ID {loc['id']} - {loc['nome']}")
        
        if len(localidades) > 10:
            print(f"  ... e mais {len(localidades) - 10}")
        
        salvar_resultados(localidades)
        enviar_para_planilha(localidades)
    else:
        print("\nNenhuma localidade encontrada!")
    
    print("\nProcesso finalizado!")

if __name__ == "__main__":
    main()
