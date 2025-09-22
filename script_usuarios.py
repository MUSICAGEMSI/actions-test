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
from threading import Lock

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxhthGne_F6y_rmFkqJenpuvMPN6nWPO2h8WU5D7nulMape6rYbxcEPZ9Sxhi0gEeWm/exec'

# Configuração para IDs específicos (300 até 350)
ID_INICIO = 300
ID_FIM = 350

# Lock para thread safety
print_lock = Lock()
processados_count = 0
total_usuarios = 0

def safe_print(*args, **kwargs):
    """Print thread-safe"""
    with print_lock:
        print(*args, **kwargs)

def update_progress():
    """Atualiza contador de progresso"""
    global processados_count
    with print_lock:
        processados_count += 1
        if processados_count % 10 == 0 or processados_count <= 5:
            progresso = (processados_count / total_usuarios) * 100
            safe_print(f"📈 Progresso: {processados_count}/{total_usuarios} ({progresso:.1f}%)")

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para usar em requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def coletar_dados_usuario(session, usuario_id):
    """Coleta dados de um usuário específico"""
    try:
        url_usuario = f"https://musical.congregacao.org.br/usuarios/visualizar/{usuario_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin'
        }
        
        # Adicionar delay para evitar rate limiting
        time.sleep(0.5)
        
        resp = session.get(url_usuario, headers=headers, timeout=15)
        
        if resp.status_code == 404:
            safe_print(f"❌ Usuário {usuario_id} não existe (404)")
            update_progress()
            return None
        
        if resp.status_code == 403:
            safe_print(f"⚠️ Acesso negado para usuário {usuario_id} (403)")
            update_progress()
            return None
            
        if resp.status_code != 200:
            safe_print(f"⚠️ Status {resp.status_code} para usuário {usuario_id}")
            update_progress()
            return None
        
        # Verificar se a página contém dados do usuário
        if "não encontrado" in resp.text.lower() or len(resp.text) < 500:
            update_progress()
            return None
        
        # Usar BeautifulSoup para parsing
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Inicializar dados do usuário
        dados_usuario = {'id': usuario_id}
        
        # Procurar por diferentes estruturas possíveis
        dados_encontrados = False
        
        # Método 1: Procurar tabela com classe table
        tabela = soup.find('table', class_='table')
        if tabela:
            for tr in tabela.find_all('tr'):
                cells = tr.find_all(['td', 'th'])
                if len(cells) >= 2:
                    campo = cells[0].get_text().strip().lower()
                    valor = cells[1].get_text().strip()
                    
                    if 'nome' in campo:
                        dados_usuario['nome'] = valor
                        dados_encontrados = True
                    elif 'grupo' in campo:
                        dados_usuario['grupo'] = valor
                        dados_encontrados = True
                    elif 'login' in campo or 'último acesso' in campo:
                        dados_usuario['ultimo_login'] = valor
                        dados_encontrados = True
                    elif 'acesso' in campo:
                        # Extrair apenas números
                        acessos = re.search(r'(\d+)', valor)
                        dados_usuario['acessos'] = acessos.group(1) if acessos else '0'
                        dados_encontrados = True
        
        # Método 2: Procurar por divs ou spans com classes específicas
        if not dados_encontrados:
            # Procurar por elementos com texto específico
            elementos_nome = soup.find_all(text=re.compile(r'Nome:', re.I))
            for elem in elementos_nome:
                parent = elem.parent
                if parent and parent.next_sibling:
                    nome = parent.next_sibling.get_text().strip() if hasattr(parent.next_sibling, 'get_text') else str(parent.next_sibling).strip()
                    if nome:
                        dados_usuario['nome'] = nome
                        dados_encontrados = True
                        break
        
        # Método 3: Procurar no HTML bruto usando regex
        if not dados_encontrados:
            # Buscar padrões no HTML
            nome_match = re.search(r'Nome[:\s]*</[^>]*>\s*([^<\n]+)', resp.text, re.I)
            if nome_match:
                dados_usuario['nome'] = nome_match.group(1).strip()
                dados_encontrados = True
            
            grupo_match = re.search(r'Grupo[:\s]*</[^>]*>\s*([^<\n]+)', resp.text, re.I)
            if grupo_match:
                dados_usuario['grupo'] = grupo_match.group(1).strip()
        
        # Método 4: Procurar por estrutura de lista de definições
        dl_elements = soup.find_all('dl')
        for dl in dl_elements:
            dts = dl.find_all('dt')
            dds = dl.find_all('dd')
            if len(dts) == len(dds):
                for dt, dd in zip(dts, dds):
                    campo = dt.get_text().strip().lower()
                    valor = dd.get_text().strip()
                    
                    if 'nome' in campo:
                        dados_usuario['nome'] = valor
                        dados_encontrados = True
                    elif 'grupo' in campo:
                        dados_usuario['grupo'] = valor
                        dados_encontrados = True
        
        # Se encontrou dados, processar
        if dados_encontrados or 'nome' in dados_usuario:
            # Garantir que campos essenciais existam
            if 'nome' not in dados_usuario:
                dados_usuario['nome'] = 'Nome não encontrado'
            if 'grupo' not in dados_usuario:
                dados_usuario['grupo'] = 'Grupo não informado'
            if 'ultimo_login' not in dados_usuario:
                dados_usuario['ultimo_login'] = 'Não disponível'
            if 'acessos' not in dados_usuario:
                dados_usuario['acessos'] = '0'
            
            safe_print(f"✅ Usuário {usuario_id}: {dados_usuario.get('nome', 'N/A')}")
            update_progress()
            return dados_usuario
        
        # Se chegou aqui, não encontrou dados
        update_progress()
        return None
        
    except requests.exceptions.Timeout:
        safe_print(f"⚠️ Timeout para usuário {usuario_id}")
        update_progress()
        return None
    except requests.exceptions.ConnectionError:
        safe_print(f"⚠️ Erro de conexão para usuário {usuario_id}")
        update_progress()
        return None
    except Exception as e:
        safe_print(f"⚠️ Erro ao processar usuário {usuario_id}: {str(e)}")
        update_progress()
        return None

def processar_usuario_individual(session, usuario_id):
    """Processa um usuário individual"""
    return coletar_dados_usuario(session, usuario_id)

def criar_sessoes_otimizadas(cookies_dict, num_sessoes=3):
    """Cria múltiplas sessões otimizadas"""
    sessoes = []
    for i in range(num_sessoes):
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8'
        })
        
        # Configurar adapter com retry
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=5,
            pool_maxsize=10,
            max_retries=requests.adapters.Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[500, 502, 503, 504]
            )
        )
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        
        sessoes.append(session)
    
    return sessoes

def main():
    global total_usuarios, processados_count
    tempo_inicio = time.time()
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        
        context = navegador.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        
        pagina = context.new_page()
        
        # Navegar para a página inicial
        safe_print("🌐 Acessando a página inicial...")
        pagina.goto(URL_INICIAL, wait_until='domcontentloaded')
        
        # Aguardar a página carregar
        time.sleep(3)
        
        # Fazer login
        safe_print("🔐 Tentando fazer login...")
        try:
            # Procurar pelos campos de login
            login_field = pagina.query_selector('input[name="login"], input[name="email"], #login, #email')
            password_field = pagina.query_selector('input[name="password"], input[name="senha"], #password, #senha')
            submit_button = pagina.query_selector('button[type="submit"], input[type="submit"], .btn-login')
            
            if not login_field or not password_field:
                safe_print("❌ Campos de login não encontrados!")
                # Tentar salvar screenshot para debug
                pagina.screenshot(path="debug_login.png")
                navegador.close()
                return
            
            # Preencher campos
            login_field.fill(EMAIL)
            password_field.fill(SENHA)
            
            # Aguardar um pouco antes de submeter
            time.sleep(1)
            
            # Submeter o formulário
            if submit_button:
                submit_button.click()
            else:
                password_field.press('Enter')
            
            # Aguardar redirecionamento após login
            pagina.wait_for_load_state('networkidle', timeout=15000)
            
            # Verificar se o login foi bem-sucedido
            # Procurar por elementos que indicam que está logado
            elementos_logado = [
                "nav", ".navbar", "#menu", ".user-info", 
                'a[href*="logout"]', 'a[href*="sair"]',
                ".dashboard", "#dashboard"
            ]
            
            logado = False
            for selector in elementos_logado:
                if pagina.query_selector(selector):
                    logado = True
                    break
            
            # Verificar se ainda está na página de login
            if not logado and ("login" in pagina.url.lower() or "entrar" in pagina.url.lower()):
                safe_print("❌ Login falhou - ainda na página de login")
                pagina.screenshot(path="debug_login_failed.png")
                navegador.close()
                return
            
            safe_print("✅ Login realizado com sucesso!")
            
        except PlaywrightTimeoutError:
            safe_print("❌ Timeout no login. Verifique suas credenciais.")
            pagina.screenshot(path="debug_timeout.png")
            navegador.close()
            return
        except Exception as e:
            safe_print(f"❌ Erro no login: {e}")
            navegador.close()
            return
        
        # Extrair cookies
        cookies_dict = extrair_cookies_playwright(pagina)
        safe_print(f"🍪 Extraídos {len(cookies_dict)} cookies")
        
        # Gerar lista de IDs para processar (300 até 350)
        ids_usuarios = [str(i) for i in range(ID_INICIO, ID_FIM + 1)]
        total_usuarios = len(ids_usuarios)
        processados_count = 0
        
        safe_print(f"📊 Processando usuários de {ID_INICIO} até {ID_FIM} ({len(ids_usuarios)} usuários)")
        
        resultado_final = []
        
        # Criar sessões (reduzido para 3 para evitar rate limiting)
        max_workers = 3
        sessoes = criar_sessoes_otimizadas(cookies_dict, max_workers)
        
        # Processar usuários em paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            for i, usuario_id in enumerate(ids_usuarios):
                session_para_usuario = sessoes[i % len(sessoes)]
                future = executor.submit(processar_usuario_individual, session_para_usuario, usuario_id)
                futures.append(future)
            
            # Coletar resultados conforme completam
            for future in concurrent.futures.as_completed(futures):
                try:
                    resultado = future.result()
                    if resultado:  # Só adiciona se coletou dados válidos
                        resultado_final.append(resultado)
                except Exception as e:
                    safe_print(f"⚠️ Erro em future: {e}")
                
                # Status a cada 10 usuários
                if len(resultado_final) % 10 == 0 and len(resultado_final) > 0:
                    tempo_decorrido = (time.time() - tempo_inicio) / 60
                    velocidade = len(resultado_final) / tempo_decorrido if tempo_decorrido > 0 else 0
                    safe_print(f"🚀 Coletados: {len(resultado_final)} usuários válidos - {velocidade:.1f} usuários/min")
        
        # Fechar navegador
        navegador.close()
        
        # Processar resultados
        safe_print(f"\n📊 Coleta concluída: {len(resultado_final)} usuários válidos de {len(ids_usuarios)} processados")
        tempo_total = (time.time() - tempo_inicio) / 60
        safe_print(f"⏱️ Tempo total: {tempo_total:.1f} minutos")
        
        if resultado_final:
            # Preparar dados para Google Sheets
            headers = ["ID", "NOME", "GRUPO", "ULTIMO_LOGIN", "ACESSOS"]
            
            dados_sheets = []
            for usuario in resultado_final:
                linha = [
                    usuario.get('id', ''),
                    usuario.get('nome', ''),
                    usuario.get('grupo', ''),
                    usuario.get('ultimo_login', ''),
                    usuario.get('acessos', '0')
                ]
                dados_sheets.append(linha)
            
            # Calcular estatísticas
            grupos_stats = {}
            total_acessos = 0
            
            for usuario in resultado_final:
                grupo = usuario.get('grupo', 'SEM_GRUPO')
                grupos_stats[grupo] = grupos_stats.get(grupo, 0) + 1
                try:
                    acessos = int(usuario.get('acessos', '0'))
                    total_acessos += acessos
                except:
                    pass
            
            body = {
                "tipo": "usuarios",
                "dados": dados_sheets,
                "headers": headers,
                "resumo": {
                    "total_usuarios": len(resultado_final),
                    "range_ids": f"{ID_INICIO}-{ID_FIM}",
                    "tempo_processamento": f"{tempo_total:.1f} minutos",
                    "velocidade": f"{len(resultado_final)/tempo_total:.1f}" if tempo_total > 0 else "0",
                    "total_acessos": total_acessos,
                    "media_acessos": f"{total_acessos/len(resultado_final):.1f}" if resultado_final else "0",
                    "grupos_stats": grupos_stats
                }
            }
            
            # Salvar backup local primeiro
            backup_filename = f'backup_usuarios_{ID_INICIO}_{ID_FIM}_{int(time.time())}.json'
            try:
                with open(backup_filename, 'w', encoding='utf-8') as f:
                    json.dump(body, f, ensure_ascii=False, indent=2)
                safe_print(f"💾 Backup salvo: {backup_filename}")
            except Exception as e:
                safe_print(f"⚠️ Erro ao salvar backup: {e}")
            
            # Enviar para Google Sheets
            if URL_APPS_SCRIPT:
                try:
                    safe_print("📤 Enviando dados para Google Sheets...")
                    resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
                    if resposta_post.status_code == 200:
                        safe_print(f"✅ Dados enviados com sucesso!")
                        safe_print(f"Resposta: {resposta_post.text[:200]}...")
                    else:
                        safe_print(f"⚠️ Status da resposta: {resposta_post.status_code}")
                        safe_print(f"Resposta: {resposta_post.text[:500]}...")
                except Exception as e:
                    safe_print(f"❌ Erro ao enviar para Apps Script: {e}")
            
            # Resumo final
            safe_print(f"\n📈 RESUMO FINAL:")
            safe_print(f"   🎯 Range processado: {ID_INICIO} até {ID_FIM}")
            safe_print(f"   👥 Usuários válidos encontrados: {len(resultado_final)}")
            safe_print(f"   📊 Total de acessos: {total_acessos}")
            safe_print(f"   ⏱️ Tempo total: {tempo_total:.1f} minutos")
            if tempo_total > 0:
                safe_print(f"   🚀 Velocidade: {len(resultado_final)/tempo_total:.1f} usuários/min")
            
            if grupos_stats:
                safe_print("   👤 Usuários por grupo:")
                for grupo, count in sorted(grupos_stats.items(), key=lambda x: x[1], reverse=True):
                    safe_print(f"      - {grupo}: {count} usuários")
            
            # Mostrar alguns exemplos dos dados coletados
            safe_print("\n📋 Exemplos dos dados coletados:")
            for i, usuario in enumerate(resultado_final[:3]):
                safe_print(f"   {i+1}. ID: {usuario['id']} - {usuario['nome']} ({usuario['grupo']})")
        
        else:
            safe_print("❌ Nenhum dado válido foi coletado")
            safe_print("💡 Sugestões:")
            safe_print("   - Verifique se as credenciais estão corretas")
            safe_print("   - Verifique se você tem permissão para acessar os perfis de usuários")
            safe_print("   - Teste manualmente o acesso a um perfil específico")
            safe_print(f"   - URL de teste: https://musical.congregacao.org.br/usuarios/visualizar/{ID_INICIO}")

if __name__ == "__main__":
    if not EMAIL or not SENHA:
        safe_print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos no arquivo credencial.env")
        exit(1)
    
    safe_print("🚀 Iniciando coleta de dados de usuários...")
    safe_print(f"📧 Email configurado: {EMAIL}")
    safe_print(f"🎯 Range de IDs: {ID_INICIO} até {ID_FIM}")
    
    main()
