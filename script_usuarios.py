# script_usuarios.py
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
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxVW_i69_DL_UQQqVjxLsAcEv5edorXSD4g-PZUu4LC9TkGd9yEfNiTL0x92ELDNm8M/exec'

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
        if processados_count % 20 == 0 or processados_count <= 5:
            progresso = (processados_count / total_usuarios) * 100
            safe_print(f"📈 Progresso: {processados_count}/{total_usuarios} ({progresso:.1f}%)")

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para usar em requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def descobrir_usuarios(session):
    """Descobre a lista de usuários disponíveis"""
    # Tentar diferentes endpoints para encontrar lista de usuários
    endpoints_teste = [
        "https://musical.congregacao.org.br/usuarios",
        "https://musical.congregacao.org.br/usuarios/listagem",
        "https://musical.congregacao.org.br/ajax/usuarios",
        "https://musical.congregacao.org.br/admin/usuarios"
    ]
    
    for endpoint in endpoints_teste:
        try:
            resp = session.get(endpoint, timeout=10)
            if resp.status_code == 200 and len(resp.text) > 1000:
                safe_print(f"✅ Endpoint funcionou: {endpoint}")
                
                # Procurar por IDs de usuários no HTML
                ids_usuarios = re.findall(r'/usuarios/visualizar/(\d+)', resp.text)
                if ids_usuarios:
                    ids_unicos = list(set(ids_usuarios))
                    safe_print(f"📊 Encontrados {len(ids_unicos)} usuários únicos")
                    return ids_unicos
                    
        except Exception as e:
            safe_print(f"❌ Erro em {endpoint}: {e}")
    
    safe_print("⚠️ Não foi possível descobrir usuários automaticamente")
    return []

def coletar_dados_usuario(session, usuario_id):
    """Coleta dados de um usuário específico"""
    try:
        url_usuario = f"https://musical.congregacao.org.br/usuarios/visualizar/{usuario_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        
        resp = session.get(url_usuario, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            return None
        
        # Usar BeautifulSoup para parsing mais confiável
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Encontrar a tabela com os dados
        tabela = soup.find('table', class_='table table-striped')
        if not tabela:
            return None
        
        # Extrair dados específicos
        dados_usuario = {'id': usuario_id}
        
        for tr in tabela.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) == 2:
                campo = tds[0].get_text().strip()
                valor = tds[1].get_text().strip()
                
                if campo == 'Nome':
                    dados_usuario['nome'] = valor
                elif campo == 'Grupo':
                    dados_usuario['grupo'] = valor
                elif campo == 'Último login':
                    dados_usuario['ultimo_login'] = valor
                elif campo == 'Acessos':
                    # Remover tags e extrair só o número
                    acessos = re.search(r'\d+', valor)
                    dados_usuario['acessos'] = acessos.group() if acessos else '0'
        
        # Verificar se coletou os dados essenciais
        if 'nome' in dados_usuario and 'grupo' in dados_usuario:
            update_progress()
            return dados_usuario
        
        return None
        
    except Exception as e:
        safe_print(f"⚠️ Erro ao processar usuário {usuario_id}: {e}")
        return None

def processar_usuario_individual(session, usuario_id):
    """Processa um usuário individual"""
    return coletar_dados_usuario(session, usuario_id)

def criar_sessoes_otimizadas(cookies_dict, num_sessoes=6):
    """Cria múltiplas sessões otimizadas"""
    sessoes = []
    for i in range(num_sessoes):
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        })
        
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=2
        )
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        
        sessoes.append(session)
    
    return sessoes

def main():
    global total_usuarios, processados_count
    tempo_inicio = time.time()
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.goto(URL_INICIAL)
        
        # Login
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
        
        # Criar sessão principal
        cookies_dict = extrair_cookies_playwright(pagina)
        session_principal = requests.Session()
        session_principal.cookies.update(cookies_dict)
        
        # Descobrir usuários
        print("🔍 Descobrindo usuários disponíveis...")
        ids_usuarios = descobrir_usuarios(session_principal)
        
        if not ids_usuarios:
            print("❌ Nenhum usuário encontrado.")
            
            # Tentar IDs sequenciais como fallback
            print("🔄 Tentando busca por IDs sequenciais...")
            ids_usuarios = [str(i) for i in range(30000, 35000)]  # Ajuste o range conforme necessário
        
        # Limitar para teste
        # ids_usuarios = ids_usuarios[:50]  # Descomente para testar com poucos usuários
        
        total_usuarios = len(ids_usuarios)
        processados_count = 0
        
        print(f"📊 Processando {len(ids_usuarios)} usuários...")
        
        resultado_final = []
        
        # Criar múltiplas sessões
        max_workers = 6
        sessoes = criar_sessoes_otimizadas(cookies_dict, max_workers)
        
        # Processar usuários em paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            for i, usuario_id in enumerate(ids_usuarios):
                session_para_usuario = sessoes[i % len(sessoes)]
                future = executor.submit(processar_usuario_individual, session_para_usuario, usuario_id)
                futures.append(future)
            
            # Coletar resultados
            for future in concurrent.futures.as_completed(futures):
                try:
                    resultado = future.result()
                    if resultado:  # Só adiciona se coletou dados válidos
                        resultado_final.append(resultado)
                except Exception as e:
                    safe_print(f"⚠️ Erro em future: {e}")
                
                # Status a cada 50 usuários
                if len(resultado_final) % 50 == 0 and len(resultado_final) > 0:
                    tempo_decorrido = (time.time() - tempo_inicio) / 60
                    velocidade = len(resultado_final) / tempo_decorrido if tempo_decorrido > 0 else 0
                    safe_print(f"🚀 Coletados: {len(resultado_final)} usuários válidos - {velocidade:.1f} usuários/min")
        
        print(f"\n📊 Coleta concluída: {len(resultado_final)} usuários válidos")
        tempo_total = (time.time() - tempo_inicio) / 60
        print(f"⏱️ Tempo total: {tempo_total:.1f} minutos")
        
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
                    "tempo_processamento": f"{tempo_total:.1f} minutos",
                    "velocidade": f"{len(resultado_final)/tempo_total:.1f} usuarios/min",
                    "total_acessos": total_acessos,
                    "media_acessos": f"{total_acessos/len(resultado_final):.1f}" if resultado_final else "0",
                    "grupos_stats": grupos_stats
                }
            }
            
            # Enviar para Google Sheets
            try:
                print("📤 Enviando dados para Google Sheets...")
                resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=60)
                print(f"✅ Dados enviados! Status: {resposta_post.status_code}")
                print(f"Resposta: {resposta_post.text[:200]}...")
            except Exception as e:
                print(f"❌ Erro ao enviar para Apps Script: {e}")
                
                # Backup local
                with open(f'backup_usuarios_{int(time.time())}.json', 'w', encoding='utf-8') as f:
                    json.dump(body, f, ensure_ascii=False, indent=2)
                print("💾 Backup salvo localmente")
            
            # Resumo final
            print(f"\n📈 RESUMO FINAL:")
            print(f"   👥 Total de usuários válidos: {len(resultado_final)}")
            print(f"   📊 Total de acessos: {total_acessos}")
            print(f"   ⏱️ Tempo total: {tempo_total:.1f} minutos")
            print(f"   🚀 Velocidade: {len(resultado_final)/tempo_total:.1f} usuários/min")
            
            if grupos_stats:
                print("   👤 Usuários por grupo:")
                for grupo, count in sorted(grupos_stats.items(), key=lambda x: x[1], reverse=True):
                    print(f"      - {grupo}: {count} usuários")
        
        else:
            print("❌ Nenhum dado válido foi coletado")
        
        navegador.close()

if __name__ == "__main__":
    if not EMAIL or not SENHA:
        print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
        exit(1)
    
    main()
