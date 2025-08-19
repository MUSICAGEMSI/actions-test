# script_historico_alunos.py
from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import requests
import time
import json
from bs4 import BeautifulSoup
from datetime import datetime

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_LISTAGEM_ALUNOS = "https://musical.congregacao.org.br/alunos/listagem"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbyDhrvHOn9afWBRxDPEMtmAcUcuUzLgfxUZRSjZRSaheUs52pOOb1N6sTDtTbBYCmvu/exec'

if not EMAIL or not SENHA:
    print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
    exit(1)

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para usar em requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def obter_lista_alunos(session):
    """Obtém a lista completa de alunos da API"""
    try:
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://musical.congregacao.org.br/alunos/listagem',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }
        
        # Dados para obter todos os alunos
        data = {
            'draw': '1',
            'start': '0',
            'length': '10000',  # Número grande para pegar todos
            'search[value]': '',
            'search[regex]': 'false'
        }
        
        resp = session.post(URL_LISTAGEM_ALUNOS, data=data, headers=headers, timeout=30)
        
        if resp.status_code == 200:
            dados_json = resp.json()
            alunos = []
            
            for linha in dados_json.get('data', []):
                if len(linha) >= 8:
                    id_aluno = linha[0]  # ID do aluno
                    nome_info = linha[1]  # Nome completo
                    comum_info = linha[2]  # Igreja/Comum
                    ministerio = linha[3]  # Ministério
                    instrumento = linha[4]  # Instrumento
                    nivel = linha[5]  # Nível
                    
                    # Limpar dados HTML
                    comum_limpo = re.sub(r'<[^>]+>', '', comum_info).strip()
                    nome_limpo = nome_info.strip()
                    
                    alunos.append({
                        'id': id_aluno,
                        'nome': nome_limpo,
                        'comum': comum_limpo,
                        'ministerio': ministerio,
                        'instrumento': instrumento,
                        'nivel': nivel
                    })
            
            print(f"✅ Encontrados {len(alunos)} alunos")
            return alunos
            
    except Exception as e:
        print(f"❌ Erro ao obter lista de alunos: {e}")
        return []

def extrair_data_mais_recente(texto):
    """Extrai a data mais recente de um texto"""
    # Padrões de data: dd/mm/yyyy
    pattern = r'\b(\d{1,2}/\d{1,2}/\d{4})\b'
    datas = re.findall(pattern, texto)
    
    if not datas:
        return None
        
    # Converter para objetos datetime e encontrar a mais recente
    datas_obj = []
    for data_str in datas:
        try:
            data_obj = datetime.strptime(data_str, '%d/%m/%Y')
            datas_obj.append((data_obj, data_str))
        except:
            continue
    
    if datas_obj:
        # Retorna a data mais recente no formato string
        return max(datas_obj, key=lambda x: x[0])[1]
    
    return None

def obter_historico_aluno(session, aluno_id):
    """Obtém o histórico completo de um aluno"""
    try:
        url_historico = f"https://musical.congregacao.org.br/licoes/index/{aluno_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/alunos/listagem'
        }
        
        resp = session.get(url_historico, headers=headers, timeout=20)
        
        if resp.status_code != 200:
            return {}
            
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        historico = {
            'mts': None,
            'mts_grupo': None,
            'msa': None,
            'msa_grupo': None,
            'provas': None,
            'metodo': None,
            'hinario': None,
            'hinario_grupo': None,
            'escalas': None,
            'escalas_grupo': None
        }
        
        # Buscar seções específicas e extrair datas
        texto_completo = resp.text
        
        # 1. MTS - Buscar tabela individual
        mts_match = re.search(r'MTS.*?<table.*?>(.*?)</table>', texto_completo, re.DOTALL | re.IGNORECASE)
        if mts_match:
            historico['mts'] = extrair_data_mais_recente(mts_match.group(1))
        
        # 2. MTS - Aulas em grupo
        mts_grupo_match = re.search(r'MTS - Aulas em grupo.*?<table.*?>(.*?)</table>', texto_completo, re.DOTALL | re.IGNORECASE)
        if mts_grupo_match:
            historico['mts_grupo'] = extrair_data_mais_recente(mts_grupo_match.group(1))
        
        # 3. MSA
        msa_match = re.search(r'MSA.*?<table.*?>(.*?)</table>', texto_completo, re.DOTALL | re.IGNORECASE)
        if msa_match and 'grupo' not in msa_match.group(0).lower():
            historico['msa'] = extrair_data_mais_recente(msa_match.group(1))
        
        # 4. MSA - Aulas em grupo
        msa_grupo_match = re.search(r'MSA - Aulas em grupo.*?<table.*?>(.*?)</table>', texto_completo, re.DOTALL | re.IGNORECASE)
        if msa_grupo_match:
            historico['msa_grupo'] = extrair_data_mais_recente(msa_grupo_match.group(1))
        
        # 5. Provas
        provas_match = re.search(r'Provas.*?<table.*?>(.*?)</table>', texto_completo, re.DOTALL | re.IGNORECASE)
        if provas_match:
            historico['provas'] = extrair_data_mais_recente(provas_match.group(1))
        
        # 6. Método
        metodo_match = re.search(r'Método.*?<table.*?>(.*?)</table>', texto_completo, re.DOTALL | re.IGNORECASE)
        if metodo_match:
            historico['metodo'] = extrair_data_mais_recente(metodo_match.group(1))
        
        # 7. Hinário
        hinario_match = re.search(r'Hinário.*?<table.*?>(.*?)</table>', texto_completo, re.DOTALL | re.IGNORECASE)
        if hinario_match and 'grupo' not in hinario_match.group(0).lower():
            historico['hinario'] = extrair_data_mais_recente(hinario_match.group(1))
        
        # 8. Hinos - Aulas em grupo
        hinario_grupo_match = re.search(r'Hinos - Aulas em grupo.*?<table.*?>(.*?)</table>', texto_completo, re.DOTALL | re.IGNORECASE)
        if hinario_grupo_match:
            historico['hinario_grupo'] = extrair_data_mais_recente(hinario_grupo_match.group(1))
        
        # 9. Escalas
        escalas_match = re.search(r'Escalas.*?<table.*?>(.*?)</table>', texto_completo, re.DOTALL | re.IGNORECASE)
        if escalas_match and 'grupo' not in escalas_match.group(0).lower():
            historico['escalas'] = extrair_data_mais_recente(escalas_match.group(1))
        
        # 10. Escalas - Aulas em grupo
        escalas_grupo_match = re.search(r'Escalas - Aulas em grupo.*?<table.*?>(.*?)</table>', texto_completo, re.DOTALL | re.IGNORECASE)
        if escalas_grupo_match:
            historico['escalas_grupo'] = extrair_data_mais_recente(escalas_grupo_match.group(1))
        
        return historico
        
    except Exception as e:
        print(f"⚠️ Erro ao obter histórico do aluno {aluno_id}: {e}")
        return {}

def main():
    tempo_inicio = time.time()
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        # Configurações do navegador
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        })
        
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
        
        # Criar sessão requests com cookies
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        # Obter lista de alunos
        print("🔍 Obtendo lista de alunos...")
        alunos = obter_lista_alunos(session)
        
        if not alunos:
            print("❌ Nenhum aluno encontrado.")
            navegador.close()
            return
        
        resultado = []
        
        # Processar cada aluno
        for i, aluno in enumerate(alunos, 1):
            if time.time() - tempo_inicio > 3600:  # 1 hora limite
                print("⏰ Tempo limite atingido.")
                break
                
            print(f"📚 Processando aluno {i}/{len(alunos)}: {aluno['nome']} (ID: {aluno['id']})")
            
            # Obter histórico do aluno
            historico = obter_historico_aluno(session, aluno['id'])
            
            # Montar linha de dados
            linha = [
                aluno['nome'],
                aluno['id'],
                aluno['comum'],
                aluno['ministerio'],
                aluno['instrumento'],
                aluno['nivel'],
                historico.get('mts', ''),
                historico.get('mts_grupo', ''),
                historico.get('msa', ''),
                historico.get('msa_grupo', ''),
                historico.get('provas', ''),
                historico.get('metodo', ''),
                historico.get('hinario', ''),
                historico.get('hinario_grupo', ''),
                historico.get('escalas', ''),
                historico.get('escalas_grupo', '')
            ]
            
            resultado.append(linha)
            
            # Mostrar progresso
            datas_encontradas = sum(1 for x in historico.values() if x)
            print(f"   📊 {datas_encontradas} datas encontradas")
            
            # Pausa para não sobrecarregar
            time.sleep(1)
        
        print(f"📊 Total de alunos processados: {len(resultado)}")
        
        # Preparar dados para envio
        headers = [
            "NOME", "ID", "COMUM", "MINISTERIO", "INSTRUMENTO", "NIVEL",
            "MTS", "MTS GRUPO", "MSA", "MSA GRUPO", "PROVAS", "MÉTODO",
            "HINÁRIO", "HINÁRIO GRUPO", "ESCALAS", "ESCALAS GRUPO"
        ]
        
        body = {
            "tipo": "historico_alunos",
            "dados": resultado,
            "headers": headers,
            "resumo": {
                "total_alunos": len(resultado),
                "tempo_processamento": f"{(time.time() - tempo_inicio) / 60:.1f} minutos"
            }
        }
        
        # Enviar dados para Apps Script
        try:
            print("📤 Enviando dados para Google Sheets...")
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body, timeout=120)
            print("✅ Dados enviados!")
            print("Status code:", resposta_post.status_code)
            print("Resposta do Apps Script:", resposta_post.text)
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
        
        # Mostrar resumo final
        print("\n📈 RESUMO DA COLETA:")
        print(f"   🎯 Total de alunos: {len(resultado)}")
        print(f"   ⏱️ Tempo total: {(time.time() - tempo_inicio) / 60:.1f} minutos")
        
        # Estatísticas de datas coletadas
        total_datas = 0
        for linha in resultado:
            total_datas += sum(1 for x in linha[6:] if x)  # Contar datas não vazias
        
        print(f"   📅 Total de datas coletadas: {total_datas}")
        print(f"   📊 Média de datas por aluno: {total_datas/len(resultado):.1f}")
        
        navegador.close()

if __name__ == "__main__":
    main()
