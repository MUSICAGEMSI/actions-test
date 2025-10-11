from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os
import requests
import time
import json
from typing import List, Set, Dict

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_LISTAGEM = "https://musical.congregacao.org.br/alunos/listagem"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzl1l143sg2_S5a6bOQy6WqWATMDZpSglIyKUp3OVZtycuHXQmGjisOpzffHTW5TvyK/exec'

print(f"🎓 COLETOR DE ALUNOS DE HORTOLÂNDIA (DA LISTAGEM)")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

def buscar_ids_igrejas_hortolandia() -> Set[int]:
    """
    Busca os IDs das igrejas de Hortolândia do Google Sheets
    """
    print("📥 Buscando IDs das igrejas de Hortolândia do Google Sheets...")
    
    try:
        params = {"acao": "listar_ids_hortolandia"}
        response = requests.get(URL_APPS_SCRIPT, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            ids = set(data.get('ids', []))
            print(f"✅ {len(ids)} IDs de igrejas carregados: {sorted(list(ids))}")
            return ids
        else:
            print(f"⚠️ Erro ao buscar IDs: Status {response.status_code}")
            return set()
            
    except Exception as e:
        print(f"❌ Erro ao buscar IDs das igrejas: {e}")
        return set()

def extrair_alunos_do_json(json_data: dict, ids_igrejas: Set[int]) -> List[Dict]:
    """
    Extrai alunos de Hortolândia do JSON da listagem
    O JSON tem formato: {"data": [[id, nome, igreja, cargo, nivel, status, id2, flag], ...]}
    """
    alunos_hortolandia = []
    
    try:
        data_array = json_data.get('data', [])
        
        print(f"📊 Processando {len(data_array)} alunos da listagem...")
        
        for row in data_array:
            # row[0] = ID do aluno
            # row[1] = Nome completo
            # row[2] = Igreja (contém "HORTOLÂNDIA" no texto)
            # row[4] = Nível (instrumento)
            
            id_aluno = row[0]
            nome_completo = row[1]
            igreja_info = row[2]
            nivel = row[4]
            
            # Verificar se é de Hortolândia pelo texto
            if "HORTOLÂNDIA" in igreja_info.upper() or "HORTOLANDIA" in igreja_info.upper():
                aluno = {
                    'id_aluno': id_aluno,
                    'nome': nome_completo,
                    'igreja': igreja_info,
                    'nivel': nivel
                }
                
                alunos_hortolandia.append(aluno)
                
        print(f"✅ {len(alunos_hortolandia)} alunos de Hortolândia encontrados")
        
    except Exception as e:
        print(f"❌ Erro ao processar JSON: {e}")
    
    return alunos_hortolandia

def extrair_cookies_playwright(pagina):
    """
    Extrai cookies do Playwright para requests
    """
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def salvar_alunos_em_arquivo(alunos: List[Dict], nome_arquivo: str = "alunos_hortolandia.json"):
    """
    Salva os dados dos alunos em arquivo JSON
    """
    try:
        with open(nome_arquivo, 'w', encoding='utf-8') as f:
            json.dump({
                "alunos": alunos,
                "total": len(alunos),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Dados salvos em: {nome_arquivo}")
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo: {e}")

def enviar_alunos_para_sheets(alunos: List[Dict], tempo_execucao: float, ids_igrejas: Set[int]):
    """
    Envia os dados dos alunos para Google Sheets via Apps Script
    """
    if not alunos:
        print("⚠️ Nenhum aluno para enviar")
        return False
    
    print(f"\n📤 Enviando {len(alunos)} alunos para Google Sheets...")
    
    relatorio = [
        ["ID_ALUNO", "NOME_COMPLETO", "IGREJA", "NIVEL"]
    ]
    
    for aluno in alunos:
        relatorio.append([
            str(aluno['id_aluno']),
            aluno['nome'],
            aluno['igreja'],
            aluno['nivel']
        ])
    
    payload = {
        "acao": "criar_aba_alunos_hortolandia",
        "dados": relatorio,
        "nome_aba": f"Alunos HT {time.strftime('%d-%m-%Y')}",
        "metadata": {
            "total_alunos": len(alunos),
            "total_igrejas_monitoradas": len(ids_igrejas),
            "tempo_execucao_seg": round(tempo_execucao, 2),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "ids_igrejas": sorted(list(ids_igrejas)),
            "fonte": "listagem_json"
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=180)
        
        if response.status_code == 200:
            print("✅ Dados dos alunos enviados com sucesso para Google Sheets!")
            print(f"📄 Resposta: {response.text[:150]}")
            return True
        else:
            print(f"⚠️ Status HTTP: {response.status_code}")
            print(f"📄 Resposta: {response.text[:200]}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Timeout ao enviar para Google Sheets (>180s)")
        return False
    except Exception as e:
        print(f"❌ Erro ao enviar para Google Sheets: {e}")
        return False

def main():
    tempo_inicio = time.time()
    
    # Buscar IDs das igrejas de Hortolândia
    ids_igrejas = buscar_ids_igrejas_hortolandia()
    
    if not ids_igrejas:
        print("❌ Nenhum ID de igreja encontrado. Abortando...")
        return
    
    print("\n🔐 Realizando login...")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        try:
            # Login
            pagina.goto(URL_INICIAL, wait_until='domcontentloaded', timeout=30000)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=20000)
            print("✅ Login realizado com sucesso!")
            
            # Acessar listagem
            print(f"\n🔗 Acessando listagem de alunos...")
            pagina.goto(URL_LISTAGEM, wait_until='domcontentloaded', timeout=30000)
            time.sleep(2)  # Aguardar carregamento
            
            # Extrair o conteúdo da página (que é JSON)
            print("📥 Extraindo dados JSON da listagem...")
            conteudo = pagina.content()
            
            # Tentar extrair JSON do body
            try:
                # A página retorna JSON direto no body
                body_text = pagina.locator("body").inner_text()
                json_data = json.loads(body_text)
                
                print(f"✅ JSON extraído com sucesso!")
                print(f"📊 Total de registros: {json_data.get('recordsTotal', 0)}")
                
            except json.JSONDecodeError as e:
                print(f"❌ Erro ao decodificar JSON: {e}")
                print(f"📄 Primeiros 500 caracteres do conteúdo:")
                print(body_text[:500])
                navegador.close()
                return
            
            # Extrair cookies para possíveis requisições futuras
            cookies_dict = extrair_cookies_playwright(pagina)
            navegador.close()
            
        except Exception as e:
            print(f"❌ Erro: {e}")
            
            try:
                pagina.screenshot(path="erro_screenshot.png")
                print("📸 Screenshot de erro salvo: erro_screenshot.png")
            except:
                pass
                
            navegador.close()
            return
    
    # Processar JSON e filtrar alunos de Hortolândia
    print(f"\n🎓 Filtrando alunos de Hortolândia...")
    alunos_hortolandia = extrair_alunos_do_json(json_data, ids_igrejas)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'='*60}")
    print(f"🏁 COLETA DE ALUNOS FINALIZADA!")
    print(f"{'='*60}")
    print(f"🎓 Alunos de Hortolândia encontrados: {len(alunos_hortolandia)}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s")
    
    if alunos_hortolandia:
        print(f"\n📋 Primeiros 10 alunos encontrados:")
        for i, aluno in enumerate(alunos_hortolandia[:10]):
            print(f"   {i+1}. ID: {aluno['id_aluno']} | {aluno['nome'][:50]}")
            print(f"      Igreja: {aluno['igreja'][:60]}")
            print(f"      Nível: {aluno['nivel']}")
        
        if len(alunos_hortolandia) > 10:
            print(f"   ... e mais {len(alunos_hortolandia) - 10} alunos")
        
        # Estatísticas por nível/instrumento
        print(f"\n📊 Distribuição por instrumento:")
        from collections import Counter
        distribuicao = Counter([a['nivel'] for a in alunos_hortolandia])
        for nivel, qtd in distribuicao.most_common():
            print(f"   {nivel}: {qtd} alunos")
        
        # Salvar em arquivo
        salvar_alunos_em_arquivo(alunos_hortolandia)
        
        # Enviar para Google Sheets
        enviar_alunos_para_sheets(alunos_hortolandia, tempo_total, ids_igrejas)
    
    else:
        print("⚠️ Nenhum aluno de Hortolândia foi encontrado na listagem")
    
    print(f"\n🎯 Processo finalizado!")

if __name__ == "__main__":
    main()
