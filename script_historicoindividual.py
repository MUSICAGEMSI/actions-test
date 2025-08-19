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
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbxVW_i69_DL_UQQqVjxLsAcEv5edorXSD4g-PZUu4LC9TkGd9yEfNiTL0x92ELDNm8M/exec'

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

def extrair_todas_as_datas(texto):
    """Extrai TODAS as datas de um texto e retorna como string separada por ;"""
    # Padrão para capturar datas no formato dd/mm/yyyy
    pattern = r'\b(\d{1,2}/\d{1,2}/\d{4})\b'
    datas = re.findall(pattern, texto)
    
    if not datas:
        return ""
    
    # Remover duplicatas mantendo a ordem
    datas_unicas = []
    for data in datas:
        if data not in datas_unicas:
            datas_unicas.append(data)
    
    # Ordenar as datas cronologicamente (opcional)
    try:
        datas_ordenadas = sorted(datas_unicas, key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
        return "; ".join(datas_ordenadas)
    except:
        # Se houver erro na ordenação, retorna na ordem encontrada
        return "; ".join(datas_unicas)

def obter_historico_aluno(session, aluno_id):
    """Obtém o histórico completo de um aluno - coletando TODAS as datas"""
    try:
        url_historico = f"https://musical.congregacao.org.br/licoes/index/{aluno_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/alunos/listagem',
            'Connection': 'keep-alive',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
        }
        
        resp = session.get(url_historico, headers=headers, timeout=8)
        
        if resp.status_code != 200:
            return {}
        
        # Usar regex diretamente no texto
        texto = resp.text
        
        historico = {
            'mts': "",
            'mts_grupo': "",
            'msa': "",
            'msa_grupo': "",
            'provas': "",
            'metodo': "",
            'hinario': "",
            'hinario_grupo': "",
            'escalas': "",
            'escalas_grupo': ""
        }
        
        # Padrões otimizados para capturar as seções
        padroes = {
            'mts': r'(?<!grupo)(?<!Grupo)MTS(?!\s*-\s*Aulas\s+em\s+grupo).*?<table.*?>(.*?)</table>',
            'mts_grupo': r'MTS\s*-\s*Aulas\s+em\s+grupo.*?<table.*?>(.*?)</table>',
            'msa': r'(?<!grupo)(?<!Grupo)MSA(?!\s*-\s*Aulas\s+em\s+grupo).*?<table.*?>(.*?)</table>',
            'msa_grupo': r'MSA\s*-\s*Aulas\s+em\s+grupo.*?<table.*?>(.*?)</table>',
            'provas': r'Provas.*?<table.*?>(.*?)</table>',
            'metodo': r'Método.*?<table.*?>(.*?)</table>',
            'hinario': r'(?<!grupo)(?<!Grupo)Hinário(?!\s*-\s*Aulas\s+em\s+grupo).*?<table.*?>(.*?)</table>',
            'hinario_grupo': r'Hinos\s*-\s*Aulas\s+em\s+grupo.*?<table.*?>(.*?)</table>',
            'escalas': r'(?<!grupo)(?<!Grupo)Escalas(?!\s*-\s*Aulas\s+em\s+grupo).*?<table.*?>(.*?)</table>',
            'escalas_grupo': r'Escalas\s*-\s*Aulas\s+em\s+grupo.*?<table.*?>(.*?)</table>'
        }
        
        # Processar todos os padrões coletando TODAS as datas
        for secao, padrao in padroes.items():
            match = re.search(padrao, texto, re.DOTALL | re.IGNORECASE)
            if match:
                # Agora coleta TODAS as datas da seção
                historico[secao] = extrair_todas_as_datas(match.group(1))
        
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
        total_alunos = len(alunos)
        batch_size = 5  # Processar em lotes para melhor controle
        
        for batch_start in range(0, total_alunos, batch_size):
            batch_end = min(batch_start + batch_size, total_alunos)
            batch = alunos[batch_start:batch_end]
            
            print(f"📚 Processando lote {batch_start//batch_size + 1}/{(total_alunos-1)//batch_size + 1} ({len(batch)} alunos)")
            
            for i, aluno in enumerate(batch):
                if time.time() - tempo_inicio > 1800:  # 30 minutos limite
                    print("⏰ Tempo limite atingido.")
                    break
                    
                aluno_atual = batch_start + i + 1
                print(f"   📖 {aluno_atual}/{total_alunos}: {aluno['nome'][:40]}... (ID: {aluno['id']})")
                
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
                
                # Mostrar progresso compacto - agora conta total de datas (não seções)
                total_datas_aluno = sum(len(x.split('; ')) if x else 0 for x in historico.values())
                if total_datas_aluno > 0:
                    print(f"      ✓ {total_datas_aluno} datas coletadas")
            
            # Pausa menor entre lotes
            if batch_end < total_alunos:
                time.sleep(0.5)
        
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
        
        # Mostrar resumo final - atualizado para contar todas as datas
        print("\n📈 RESUMO DA COLETA:")
        print(f"   🎯 Total de alunos: {len(resultado)}")
        print(f"   ⏱️ Tempo total: {(time.time() - tempo_inicio) / 60:.1f} minutos")
        
        # Estatísticas de datas coletadas - agora conta corretamente
        total_datas = 0
        for linha in resultado:
            for campo_data in linha[6:]:  # Campos de data começam na posição 6
                if campo_data:  # Se não está vazio
                    total_datas += len(campo_data.split('; '))  # Conta quantas datas separadas por ;
        
        print(f"   📅 Total de datas coletadas: {total_datas}")
        print(f"   📊 Média de datas por aluno: {total_datas/len(resultado):.1f}")
        
        navegador.close()

if __name__ == "__main__":
    main()
