from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os
import requests
import time
import json
from typing import List, Dict
from collections import Counter

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzl1l143sg2_S5a6bOQy6WqWATMDZpSglIyKUp3OVZtycuHXQmGjisOpzffHTW5TvyK/exec'

print(f"🎓 COLETOR DE LIÇÕES INDIVIDUAIS - HORTOLÂNDIA")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

def buscar_alunos_da_planilha() -> List[Dict]:
    """
    Busca a lista de alunos (ID_ALUNO, ID_IGREJA, NOME) do Google Sheets
    """
    print("📥 Buscando lista de alunos do Google Sheets...")
    
    try:
        params = {"acao": "listar_ids_alunos"}
        response = requests.get(URL_APPS_SCRIPT, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('sucesso'):
                alunos = data.get('alunos', [])
                print(f"✅ {len(alunos)} alunos carregados da planilha")
                return alunos
            else:
                print(f"⚠️ Erro na resposta: {data.get('erro', 'Desconhecido')}")
                return []
        else:
            print(f"⚠️ Erro ao buscar alunos: Status {response.status_code}")
            return []
            
    except Exception as e:
        print(f"❌ Erro ao buscar alunos: {e}")
        return []

def coletar_licoes_aluno(pagina, id_aluno: int, nome_aluno: str) -> Dict:
    """
    Coleta todas as lições de um aluno específico
    """
    url_aluno = f"https://musical.congregacao.org.br/alunos/perfil/{id_aluno}"
    
    dados_aluno = {
        'id_aluno': id_aluno,
        'nome': nome_aluno,
        'mts_individual': [],
        'mts_grupo': [],
        'msa_individual': [],
        'msa_grupo': [],
        'provas': [],
        'hinario_individual': [],
        'hinario_grupo': [],
        'metodos': [],
        'escalas_individual': [],
        'escalas_grupo': []
    }
    
    try:
        pagina.goto(url_aluno, wait_until='domcontentloaded', timeout=30000)
        time.sleep(1)
        
        # MTS Individual
        try:
            mts_ind = pagina.query_selector_all('div[id*="mts-individual"] tbody tr')
            for linha in mts_ind:
                colunas = linha.query_selector_all('td')
                if len(colunas) >= 7:
                    dados_aluno['mts_individual'].append({
                        'modulo': colunas[0].inner_text().strip(),
                        'licoes': colunas[1].inner_text().strip(),
                        'data_licao': colunas[2].inner_text().strip(),
                        'autorizante': colunas[3].inner_text().strip(),
                        'data_cadastro': colunas[4].inner_text().strip(),
                        'data_alteracao': colunas[5].inner_text().strip(),
                        'observacoes': colunas[6].inner_text().strip()
                    })
        except: pass
        
        # MTS Grupo
        try:
            mts_grupo = pagina.query_selector_all('div[id*="mts-grupo"] tbody tr')
            for linha in mts_grupo:
                colunas = linha.query_selector_all('td')
                if len(colunas) >= 3:
                    dados_aluno['mts_grupo'].append({
                        'paginas': colunas[0].inner_text().strip(),
                        'observacoes': colunas[1].inner_text().strip(),
                        'data_licao': colunas[2].inner_text().strip()
                    })
        except: pass
        
        # MSA Individual
        try:
            msa_ind = pagina.query_selector_all('div[id*="msa-individual"] tbody tr')
            for linha in msa_ind:
                colunas = linha.query_selector_all('td')
                if len(colunas) >= 7:
                    dados_aluno['msa_individual'].append({
                        'data_licao': colunas[0].inner_text().strip(),
                        'fases': colunas[1].inner_text().strip(),
                        'paginas': colunas[2].inner_text().strip(),
                        'licoes': colunas[3].inner_text().strip(),
                        'claves': colunas[4].inner_text().strip(),
                        'observacoes': colunas[5].inner_text().strip(),
                        'autorizante': colunas[6].inner_text().strip()
                    })
        except: pass
        
        # MSA Grupo
        try:
            msa_grupo = pagina.query_selector_all('div[id*="msa-grupo"] tbody tr')
            for linha in msa_grupo:
                colunas = linha.query_selector_all('td')
                if len(colunas) >= 7:
                    dados_aluno['msa_grupo'].append({
                        'fases_de': colunas[0].inner_text().strip(),
                        'fases_ate': colunas[1].inner_text().strip(),
                        'paginas_de': colunas[2].inner_text().strip(),
                        'paginas_ate': colunas[3].inner_text().strip(),
                        'claves': colunas[4].inner_text().strip(),
                        'observacoes': colunas[5].inner_text().strip(),
                        'data_licao': colunas[6].inner_text().strip()
                    })
        except: pass
        
        # Provas
        try:
            provas = pagina.query_selector_all('div[id*="provas"] tbody tr')
            for linha in provas:
                colunas = linha.query_selector_all('td')
                if len(colunas) >= 5:
                    dados_aluno['provas'].append({
                        'modulo_fases': colunas[0].inner_text().strip(),
                        'nota': colunas[1].inner_text().strip(),
                        'data_prova': colunas[2].inner_text().strip(),
                        'autorizante': colunas[3].inner_text().strip(),
                        'data_cadastro': colunas[4].inner_text().strip()
                    })
        except: pass
        
        # Hinário Individual
        try:
            hinario_ind = pagina.query_selector_all('div[id*="hinario-individual"] tbody tr')
            for linha in hinario_ind:
                colunas = linha.query_selector_all('td')
                if len(colunas) >= 7:
                    dados_aluno['hinario_individual'].append({
                        'hino': colunas[0].inner_text().strip(),
                        'voz': colunas[1].inner_text().strip(),
                        'data_aula': colunas[2].inner_text().strip(),
                        'autorizante': colunas[3].inner_text().strip(),
                        'data_cadastro': colunas[4].inner_text().strip(),
                        'data_alteracao': colunas[5].inner_text().strip(),
                        'observacoes': colunas[6].inner_text().strip()
                    })
        except: pass
        
        # Hinário Grupo
        try:
            hinario_grupo = pagina.query_selector_all('div[id*="hinario-grupo"] tbody tr')
            for linha in hinario_grupo:
                colunas = linha.query_selector_all('td')
                if len(colunas) >= 3:
                    dados_aluno['hinario_grupo'].append({
                        'hinos': colunas[0].inner_text().strip(),
                        'observacoes': colunas[1].inner_text().strip(),
                        'data_licao': colunas[2].inner_text().strip()
                    })
        except: pass
        
        # Métodos
        try:
            metodos = pagina.query_selector_all('div[id*="metodos"] tbody tr')
            for linha in metodos:
                colunas = linha.query_selector_all('td')
                if len(colunas) >= 7:
                    dados_aluno['metodos'].append({
                        'paginas': colunas[0].inner_text().strip(),
                        'licao': colunas[1].inner_text().strip(),
                        'metodo': colunas[2].inner_text().strip(),
                        'data_licao': colunas[3].inner_text().strip(),
                        'autorizante': colunas[4].inner_text().strip(),
                        'data_cadastro': colunas[5].inner_text().strip(),
                        'observacoes': colunas[6].inner_text().strip()
                    })
        except: pass
        
        # Escalas Individual
        try:
            escalas_ind = pagina.query_selector_all('div[id*="escalas-individual"] tbody tr')
            for linha in escalas_ind:
                colunas = linha.query_selector_all('td')
                if len(colunas) >= 6:
                    dados_aluno['escalas_individual'].append({
                        'escala': colunas[0].inner_text().strip(),
                        'data': colunas[1].inner_text().strip(),
                        'autorizante': colunas[2].inner_text().strip(),
                        'data_cadastro': colunas[3].inner_text().strip(),
                        'data_alteracao': colunas[4].inner_text().strip(),
                        'observacoes': colunas[5].inner_text().strip()
                    })
        except: pass
        
        # Escalas Grupo
        try:
            escalas_grupo = pagina.query_selector_all('div[id*="escalas-grupo"] tbody tr')
            for linha in escalas_grupo:
                colunas = linha.query_selector_all('td')
                if len(colunas) >= 3:
                    dados_aluno['escalas_grupo'].append({
                        'escala': colunas[0].inner_text().strip(),
                        'observacoes': colunas[1].inner_text().strip(),
                        'data_licao': colunas[2].inner_text().strip()
                    })
        except: pass
        
    except Exception as e:
        print(f"   ⚠️ Erro ao coletar dados do aluno {id_aluno}: {e}")
    
    return dados_aluno

def preparar_dados_para_envio(alunos_dados: List[Dict], tempo_total: float) -> Dict:
    """
    Prepara os dados no formato esperado pelo Apps Script
    """
    resumo = []
    mts_individual = []
    mts_grupo = []
    msa_individual = []
    msa_grupo = []
    provas = []
    hinario_individual = []
    hinario_grupo = []
    metodos = []
    escalas_individual = []
    escalas_grupo = []
    
    alunos_com_dados = 0
    alunos_sem_dados = 0
    
    for aluno in alunos_dados:
        id_aluno = aluno['id_aluno']
        nome = aluno['nome']
        id_igreja = aluno.get('id_igreja', 0)
        
        # Contadores
        total_mts_ind = len(aluno['mts_individual'])
        total_mts_grupo = len(aluno['mts_grupo'])
        total_msa_ind = len(aluno['msa_individual'])
        total_msa_grupo = len(aluno['msa_grupo'])
        total_provas = len(aluno['provas'])
        total_hinos_ind = len(aluno['hinario_individual'])
        total_hinos_grupo = len(aluno['hinario_grupo'])
        total_metodos = len(aluno['metodos'])
        total_escalas_ind = len(aluno['escalas_individual'])
        total_escalas_grupo = len(aluno['escalas_grupo'])
        
        # Média de provas
        media_provas = 0
        if total_provas > 0:
            notas = [float(p['nota']) for p in aluno['provas'] if p['nota'].replace('.', '').isdigit()]
            if notas:
                media_provas = round(sum(notas) / len(notas), 2)
        
        tem_dados = any([
            total_mts_ind, total_mts_grupo, total_msa_ind, total_msa_grupo,
            total_provas, total_hinos_ind, total_hinos_grupo, total_metodos,
            total_escalas_ind, total_escalas_grupo
        ])
        
        if tem_dados:
            alunos_com_dados += 1
        else:
            alunos_sem_dados += 1
        
        # Resumo (14 colunas - sem ULTIMA_ATIVIDADE e DATA_COLETA)
        resumo.append([
            id_aluno, nome, id_igreja,
            total_mts_ind, total_mts_grupo,
            total_msa_ind, total_msa_grupo,
            total_provas, media_provas,
            total_hinos_ind, total_hinos_grupo,
            total_metodos,
            total_escalas_ind, total_escalas_grupo
        ])
        
        # MTS Individual
        for item in aluno['mts_individual']:
            mts_individual.append([
                id_aluno, nome,
                item['modulo'], item['licoes'], item['data_licao'],
                item['autorizante'], item['data_cadastro'], item['data_alteracao'],
                item['observacoes']
            ])
        
        # MTS Grupo
        for item in aluno['mts_grupo']:
            mts_grupo.append([
                id_aluno, nome,
                item['paginas'], item['observacoes'], item['data_licao']
            ])
        
        # MSA Individual
        for item in aluno['msa_individual']:
            msa_individual.append([
                id_aluno, nome,
                item['data_licao'], item['fases'], item['paginas'],
                item['licoes'], item['claves'], item['observacoes'],
                item['autorizante']
            ])
        
        # MSA Grupo
        for item in aluno['msa_grupo']:
            msa_grupo.append([
                id_aluno, nome,
                item['fases_de'], item['fases_ate'],
                item['paginas_de'], item['paginas_ate'],
                item['claves'], item['observacoes'], item['data_licao']
            ])
        
        # Provas
        for item in aluno['provas']:
            provas.append([
                id_aluno, nome,
                item['modulo_fases'], item['nota'], item['data_prova'],
                item['autorizante'], item['data_cadastro']
            ])
        
        # Hinário Individual
        for item in aluno['hinario_individual']:
            hinario_individual.append([
                id_aluno, nome,
                item['hino'], item['voz'], item['data_aula'],
                item['autorizante'], item['data_cadastro'], item['data_alteracao'],
                item['observacoes']
            ])
        
        # Hinário Grupo
        for item in aluno['hinario_grupo']:
            hinario_grupo.append([
                id_aluno, nome,
                item['hinos'], item['observacoes'], item['data_licao']
            ])
        
        # Métodos
        for item in aluno['metodos']:
            metodos.append([
                id_aluno, nome,
                item['paginas'], item['licao'], item['metodo'],
                item['data_licao'], item['autorizante'], item['data_cadastro'],
                item['observacoes']
            ])
        
        # Escalas Individual
        for item in aluno['escalas_individual']:
            escalas_individual.append([
                id_aluno, nome,
                item['escala'], item['data'], item['autorizante'],
                item['data_cadastro'], item['data_alteracao'], item['observacoes']
            ])
        
        # Escalas Grupo
        for item in aluno['escalas_grupo']:
            escalas_grupo.append([
                id_aluno, nome,
                item['escala'], item['observacoes'], item['data_licao']
            ])
    
    return {
        'tipo': 'licoes_alunos',
        'resumo': resumo,
        'mts_individual': mts_individual,
        'mts_grupo': mts_grupo,
        'msa_individual': msa_individual,
        'msa_grupo': msa_grupo,
        'provas': provas,
        'hinario_individual': hinario_individual,
        'hinario_grupo': hinario_grupo,
        'metodos': metodos,
        'escalas_individual': escalas_individual,
        'escalas_grupo': escalas_grupo,
        'metadata': {
            'total_alunos_processados': len(alunos_dados),
            'alunos_com_dados': alunos_com_dados,
            'alunos_sem_dados': alunos_sem_dados,
            'tempo_coleta_segundos': round(tempo_total, 2),
            'data_coleta': time.strftime("%Y-%m-%d %H:%M:%S")
        }
    }

def enviar_para_sheets(dados: Dict) -> bool:
    """
    Envia dados para Google Sheets
    """
    print(f"\n📤 Enviando dados para Google Sheets...")
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=dados, timeout=300)
        
        if response.status_code == 200:
            resultado = response.json()
            if resultado.get('sucesso'):
                print("✅ Dados enviados com sucesso!")
                return True
            else:
                print(f"⚠️ Erro: {resultado.get('erro', 'Desconhecido')}")
                return False
        else:
            print(f"⚠️ Status HTTP: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
        return False

def main():
    tempo_inicio = time.time()
    
    # Buscar lista de alunos
    alunos = buscar_alunos_da_planilha()
    
    if not alunos:
        print("❌ Nenhum aluno encontrado. Abortando...")
        return
    
    print(f"\n🎯 Iniciando coleta de {len(alunos)} alunos...")
    
    alunos_dados = []
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        try:
            # Login
            print("\n🔐 Realizando login...")
            pagina.goto(URL_INICIAL, wait_until='domcontentloaded', timeout=30000)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=20000)
            print("✅ Login realizado!")
            
            # Coletar dados de cada aluno
            for i, aluno in enumerate(alunos, 1):
                id_aluno = aluno['id_aluno']
                nome = aluno['nome']
                
                print(f"\n[{i}/{len(alunos)}] Coletando: {nome} (ID: {id_aluno})")
                
                dados = coletar_licoes_aluno(pagina, id_aluno, nome)
                dados['id_igreja'] = aluno['id_igreja']
                alunos_dados.append(dados)
                
                # Mostrar progresso
                totais = sum([
                    len(dados['mts_individual']),
                    len(dados['mts_grupo']),
                    len(dados['msa_individual']),
                    len(dados['msa_grupo']),
                    len(dados['provas']),
                    len(dados['hinario_individual']),
                    len(dados['hinario_grupo']),
                    len(dados['metodos']),
                    len(dados['escalas_individual']),
                    len(dados['escalas_grupo'])
                ])
                print(f"   ✓ {totais} registros coletados")
                
                time.sleep(0.5)  # Pausa entre requisições
            
            navegador.close()
            
        except Exception as e:
            print(f"❌ Erro: {e}")
            navegador.close()
            return
    
    tempo_total = time.time() - tempo_inicio
    
    # Preparar e enviar dados
    print(f"\n📊 Preparando dados para envio...")
    dados_envio = preparar_dados_para_envio(alunos_dados, tempo_total)
    
    print(f"\n{'='*60}")
    print(f"📈 ESTATÍSTICAS DA COLETA")
    print(f"{'='*60}")
    print(f"Total de alunos: {len(alunos_dados)}")
    print(f"Alunos com dados: {dados_envio['metadata']['alunos_com_dados']}")
    print(f"Alunos sem dados: {dados_envio['metadata']['alunos_sem_dados']}")
    print(f"Tempo total: {tempo_total:.1f}s")
    print(f"{'='*60}")
    
    # Enviar para Google Sheets
    enviar_para_sheets(dados_envio)
    
    print(f"\n🎯 Processo finalizado!")

if __name__ == "__main__":
    main()
