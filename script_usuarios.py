from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
import json
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import concurrent.futures
from threading import Lock, Thread
import queue
from urllib.parse import urljoin

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxhthGne_F6y_rmFkqJenpuvMPN6nWPO2h8WU5D7nulMape6rYbxcEPZ9Sxhi0gEeWm/exec'

# CONFIGURAÇÃO ULTRA RÁPIDA - 47.100 usuários em ~30-45 minutos
ID_INICIO = 1
ID_FIM = 47100
MAX_WORKERS = 50  # Muito mais workers
BATCH_SIZE = 1000  # Processa em lotes
CHUNK_SIZE = 100   # Chunks para Google Sheets
TIMEOUT = 3        # Timeout reduzido
DELAY = 0.01       # Delay mínimo entre requests

# Locks e contadores
print_lock = Lock()
processados_count = 0
sucessos_count = 0
total_usuarios = 0
resultado_queue = queue.Queue()

def safe_print(*args, **kwargs):
    """Print thread-safe"""
    with print_lock:
        print(*args, **kwargs)

def update_progress(sucesso=False):
    """Atualiza contador de progresso"""
    global processados_count, sucessos_count
    with print_lock:
        processados_count += 1
        if sucesso:
            sucessos_count += 1
        
        if processados_count % 500 == 0:
            progresso = (processados_count / total_usuarios) * 100
            taxa_sucesso = (sucessos_count / processados_count) * 100 if processados_count > 0 else 0
            safe_print(f"🚀 Progresso: {processados_count}/{total_usuarios} ({progresso:.1f}%) - Sucessos: {sucessos_count} ({taxa_sucesso:.1f}%)")

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para usar em requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

async def coletar_dados_usuario_async(session, usuario_id, semaforo):
    """Versão assíncrona da coleta de dados"""
    async with semaforo:
        try:
            url_usuario = f"https://musical.congregacao.org.br/usuarios/visualizar/{usuario_id}"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Connection': 'keep-alive'
            }
            
            async with session.get(url_usuario, headers=headers, timeout=TIMEOUT) as resp:
                if resp.status == 404:
                    update_progress()
                    return None
                
                if resp.status != 200:
                    update_progress()
                    return None
                
                html = await resp.text()
                
                if len(html) < 300:
                    update_progress()
                    return None
                
                # Parsing ultra rápido com regex
                dados_usuario = {'id': str(usuario_id)}
                
                # Regex para extrair dados rapidamente
                nome_match = re.search(r'<td[^>]*>Nome</td>\s*<td[^>]*>([^<]+)', html, re.I)
                if nome_match:
                    dados_usuario['nome'] = nome_match.group(1).strip()
                
                grupo_match = re.search(r'<td[^>]*>Grupo</td>\s*<td[^>]*>([^<]+)', html, re.I)
                if grupo_match:
                    dados_usuario['grupo'] = grupo_match.group(1).strip()
                
                login_match = re.search(r'<td[^>]*>(?:Último login|Último acesso)</td>\s*<td[^>]*>([^<]+)', html, re.I)
                if login_match:
                    dados_usuario['ultimo_login'] = login_match.group(1).strip()
                
                acessos_match = re.search(r'<td[^>]*>Acessos</td>\s*<td[^>]*>(\d+)', html, re.I)
                if acessos_match:
                    dados_usuario['acessos'] = acessos_match.group(1)
                else:
                    dados_usuario['acessos'] = '0'
                
                # Se encontrou pelo menos nome, é válido
                if 'nome' in dados_usuario:
                    if 'grupo' not in dados_usuario:
                        dados_usuario['grupo'] = 'N/A'
                    if 'ultimo_login' not in dados_usuario:
                        dados_usuario['ultimo_login'] = 'N/A'
                    
                    update_progress(True)
                    return dados_usuario
                
                update_progress()
                return None
                
        except Exception:
            update_progress()
            return None
        
        await asyncio.sleep(DELAY)

async def processar_lote_async(cookies_dict, ids_lote):
    """Processa um lote de IDs de forma assíncrona"""
    connector = aiohttp.TCPConnector(
        limit=100,
        limit_per_host=50,
        keepalive_timeout=30,
        enable_cleanup_closed=True
    )
    
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    semaforo = asyncio.Semaphore(MAX_WORKERS)
    
    async with aiohttp.ClientSession(
        connector=connector, 
        timeout=timeout,
        cookies=cookies_dict,
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    ) as session:
        
        tasks = []
        for usuario_id in ids_lote:
            task = coletar_dados_usuario_async(session, usuario_id, semaforo)
            tasks.append(task)
        
        resultados = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filtrar resultados válidos
        resultados_validos = [r for r in resultados if r is not None and not isinstance(r, Exception)]
        return resultados_validos

def processar_lote_sync(cookies_dict, ids_lote):
    """Wrapper síncrono para processar lote assíncrono"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        resultado = loop.run_until_complete(processar_lote_async(cookies_dict, ids_lote))
        loop.close()
        return resultado
    except Exception as e:
        safe_print(f"❌ Erro no lote: {e}")
        return []

def enviar_dados_para_sheets(dados, chunk_num, total_chunks):
    """Envia dados para Google Sheets em chunks"""
    try:
        headers_sheet = ["ID", "NOME", "GRUPO", "ULTIMO_LOGIN", "ACESSOS"]
        
        dados_sheets = []
        for usuario in dados:
            linha = [
                usuario.get('id', ''),
                usuario.get('nome', ''),
                usuario.get('grupo', ''),
                usuario.get('ultimo_login', ''),
                usuario.get('acessos', '0')
            ]
            dados_sheets.append(linha)
        
        body = {
            "tipo": "usuarios_chunk",
            "chunk": chunk_num,
            "total_chunks": total_chunks,
            "dados": dados_sheets,
            "headers": headers_sheet,
            "range_ids": f"{ID_INICIO}-{ID_FIM}"
        }
        
        resp = requests.post(URL_APPS_SCRIPT, json=body, timeout=30)
        if resp.status_code == 200:
            safe_print(f"✅ Chunk {chunk_num}/{total_chunks} enviado com {len(dados)} usuários")
        else:
            safe_print(f"⚠️ Erro no chunk {chunk_num}: Status {resp.status_code}")
            
    except Exception as e:
        safe_print(f"❌ Erro ao enviar chunk {chunk_num}: {e}")

def main():
    global total_usuarios, processados_count, sucessos_count
    tempo_inicio = time.time()
    
    safe_print("🚀 INICIANDO COLETA ULTRA RÁPIDA")
    safe_print(f"🎯 Range: {ID_INICIO} até {ID_FIM} ({ID_FIM - ID_INICIO + 1} usuários)")
    safe_print(f"⚡ Workers: {MAX_WORKERS} | Batch: {BATCH_SIZE} | Timeout: {TIMEOUT}s")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-dev-shm-usage',
                '--no-first-run',
                '--disable-gpu',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows'
            ]
        )
        
        context = navegador.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        
        pagina = context.new_page()
        
        # Login super rápido
        safe_print("🔐 Fazendo login...")
        pagina.goto(URL_INICIAL, wait_until='domcontentloaded')
        
        try:
            pagina.fill('input[name="login"], input[name="email"]', EMAIL)
            pagina.fill('input[name="password"], input[name="senha"]', SENHA)
            pagina.click('button[type="submit"], input[type="submit"]')
            pagina.wait_for_load_state('domcontentloaded', timeout=10000)
            safe_print("✅ Login realizado!")
        except Exception as e:
            safe_print(f"❌ Erro no login: {e}")
            navegador.close()
            return
        
        # Extrair cookies
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
        
        # Preparar processamento
        ids_usuarios = list(range(ID_INICIO, ID_FIM + 1))
        total_usuarios = len(ids_usuarios)
        processados_count = 0
        sucessos_count = 0
        
        # Dividir em lotes para processamento paralelo
        lotes = [ids_usuarios[i:i + BATCH_SIZE] for i in range(0, len(ids_usuarios), BATCH_SIZE)]
        safe_print(f"📦 Dividido em {len(lotes)} lotes de até {BATCH_SIZE} usuários")
        
        resultado_final = []
        
        # Processar lotes em paralelo (mais conservador para não sobrecarregar)
        max_lotes_paralelos = 5
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_lotes_paralelos) as executor:
            futures = []
            
            for i, lote in enumerate(lotes):
                future = executor.submit(processar_lote_sync, cookies_dict, lote)
                futures.append((future, i + 1, len(lotes)))
            
            # Coletar resultados e enviar para sheets em tempo real
            for future, lote_num, total_lotes in futures:
                try:
                    resultado_lote = future.result()
                    if resultado_lote:
                        resultado_final.extend(resultado_lote)
                        
                        # Enviar para sheets em chunks se temos dados suficientes
                        if len(resultado_lote) >= CHUNK_SIZE:
                            chunks = [resultado_lote[i:i + CHUNK_SIZE] for i in range(0, len(resultado_lote), CHUNK_SIZE)]
                            for chunk_idx, chunk in enumerate(chunks):
                                Thread(
                                    target=enviar_dados_para_sheets, 
                                    args=(chunk, f"{lote_num}-{chunk_idx + 1}", f"lote-{total_lotes}")
                                ).start()
                    
                    # Status do lote
                    tempo_decorrido = (time.time() - tempo_inicio) / 60
                    velocidade = len(resultado_final) / tempo_decorrido if tempo_decorrido > 0 else 0
                    safe_print(f"📊 Lote {lote_num}/{total_lotes} concluído - Total coletado: {len(resultado_final)} - {velocidade:.0f} usuários/min")
                    
                except Exception as e:
                    safe_print(f"⚠️ Erro no lote {lote_num}: {e}")
        
        # Finalizar
        tempo_total = (time.time() - tempo_inicio) / 60
        velocidade_final = len(resultado_final) / tempo_total if tempo_total > 0 else 0
        
        safe_print(f"\n🎉 COLETA FINALIZADA!")
        safe_print(f"   ⏱️  Tempo total: {tempo_total:.1f} minutos")
        safe_print(f"   👥 Usuários coletados: {len(resultado_final)}")
        safe_print(f"   📊 Processados: {processados_count}")
        safe_print(f"   🚀 Velocidade: {velocidade_final:.0f} usuários/minuto")
        safe_print(f"   📈 Taxa de sucesso: {(len(resultado_final)/processados_count)*100:.1f}%" if processados_count > 0 else "0%")
        
        # Backup final
        if resultado_final:
            backup_filename = f'backup_usuarios_completo_{int(time.time())}.json'
            try:
                with open(backup_filename, 'w', encoding='utf-8') as f:
                    json.dump({
                        'dados': resultado_final,
                        'estatisticas': {
                            'total_coletados': len(resultado_final),
                            'tempo_minutos': tempo_total,
                            'velocidade_por_minuto': velocidade_final,
                            'range': f"{ID_INICIO}-{ID_FIM}"
                        }
                    }, f, ensure_ascii=False, indent=2)
                safe_print(f"💾 Backup completo salvo: {backup_filename}")
            except Exception as e:
                safe_print(f"⚠️ Erro ao salvar backup: {e}")

if __name__ == "__main__":
    if not EMAIL or not SENHA:
        print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos!")
        exit(1)
    
    main()
