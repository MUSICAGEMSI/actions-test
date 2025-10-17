from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os
import requests
import time
import json
from typing import List, Dict

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzl1l143sg2_S5a6bOQy6WqWATMDZpSglIyKUp3OVZtycuHXQmGjisOpzffHTW5TvyK/exec'

print("=" * 70)
print("🎓 COLETOR DE LIÇÕES INDIVIDUAIS - HORTOLÂNDIA")
print("=" * 70)
print(f"🔗 URL do Apps Script: {URL_APPS_SCRIPT}")
print("=" * 70)

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas no arquivo credencial.env")
    print("   Certifique-se de que LOGIN_MUSICAL e SENHA_MUSICAL estão configurados")
    exit(1)

def buscar_alunos_da_planilha() -> List[Dict]:
    """
    Busca a lista de alunos (ID_ALUNO, ID_IGREJA, NOME) do Google Sheets
    """
    print("\n📥 BUSCANDO LISTA DE ALUNOS DO GOOGLE SHEETS")
    print("-" * 70)
    
    try:
        params = {"acao": "listar_ids_alunos"}
        print(f"📤 Enviando requisição GET...")
        print(f"   URL: {URL_APPS_SCRIPT}")
        print(f"   Parâmetros: {params}")
        
        response = requests.get(URL_APPS_SCRIPT, params=params, timeout=30)
        
        print(f"\n📡 RESPOSTA DO SERVIDOR:")
        print(f"   Status HTTP: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"   Formato: JSON válido ✓")
                print(f"\n📊 CONTEÚDO DA RESPOSTA:")
                print(json.dumps(data, indent=2, ensure_ascii=False)[:1000])
                
                if data.get('sucesso'):
                    alunos = data.get('alunos', [])
                    total = data.get('total', 0)
                    
                    print(f"\n✅ SUCESSO!")
                    print(f"   Total de alunos carregados: {len(alunos)}")
                    print(f"   Total informado pelo servidor: {total}")
                    
                    # Mostrar primeiros 5 alunos como exemplo
                    if alunos:
                        print(f"\n📋 PRIMEIROS ALUNOS (exemplo):")
                        print("-" * 70)
                        for i, aluno in enumerate(alunos[:5], 1):
                            print(f"   {i}. ID: {aluno['id_aluno']:6d} | Igreja: {aluno['id_igreja']:5d} | {aluno['nome']}")
                        if len(alunos) > 5:
                            print(f"   ... e mais {len(alunos) - 5} alunos")
                        print("-" * 70)
                    
                    return alunos
                else:
                    erro = data.get('erro', 'Desconhecido')
                    print(f"\n⚠️ ERRO NA RESPOSTA DO SERVIDOR:")
                    print(f"   {erro}")
                    
                    # Informações de debug
                    if 'parametros_recebidos' in data:
                        print(f"\n🔍 Debug - Parâmetros recebidos pelo servidor:")
                        print(f"   {data['parametros_recebidos']}")
                    
                    if 'acoes_disponiveis' in data:
                        print(f"\n💡 Ações disponíveis no servidor:")
                        for acao in data['acoes_disponiveis']:
                            print(f"   - {acao}")
                    
                    if 'abas_disponiveis' in data:
                        print(f"\n📑 Abas disponíveis na planilha:")
                        for aba in data['abas_disponiveis']:
                            print(f"   - {aba}")
                    
                    return []
                    
            except json.JSONDecodeError as e:
                print(f"\n❌ ERRO: Resposta não é JSON válido")
                print(f"   Erro: {e}")
                print(f"\n📄 Resposta bruta (primeiros 1000 caracteres):")
                print(response.text[:1000])
                return []
        else:
            print(f"\n⚠️ ERRO HTTP {response.status_code}")
            print(f"📄 Resposta (primeiros 500 caracteres):")
            print(response.text[:500])
            return []
            
    except requests.exceptions.Timeout:
        print(f"\n❌ TIMEOUT: Servidor não respondeu em 30 segundos")
        print(f"   Possíveis causas:")
        print(f"   1. URL do Apps Script incorreta")
        print(f"   2. Web App não está publicado")
        print(f"   3. Problemas de rede")
        return []
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ ERRO DE CONEXÃO: {e}")
        return []
        
    except Exception as e:
        print(f"\n❌ ERRO INESPERADO: {e}")
        import traceback
        print(f"\n🔍 Traceback completo:")
        print(traceback.format_exc())
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
        
        # Resumo (14 colunas)
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
    print(f"\n📤 ENVIANDO DADOS PARA GOOGLE SHEETS")
    print("-" * 70)
    
    try:
        print(f"📦 Preparando envio...")
        print(f"   Resumo: {len(dados['resumo'])} linhas")
        print(f"   MTS Individual: {len(dados['mts_individual'])} linhas")
        print(f"   Provas: {len(dados['provas'])} linhas")
        
        response = requests.post(URL_APPS_SCRIPT, json=dados, timeout=300)
        
        print(f"\n📡 RESPOSTA DO SERVIDOR:")
        print(f"   Status HTTP: {response.status_code}")
        
        if response.status_code == 200:
            resultado = response.json()
            if resultado.get('sucesso'):
                print(f"\n✅ DADOS ENVIADOS COM SUCESSO!")
                print(f"   Total de alunos: {resultado.get('total_alunos', 0)}")
                print(f"   Timestamp: {resultado.get('timestamp', '')}")
                return True
            else:
                print(f"\n⚠️ ERRO: {resultado.get('erro', 'Desconhecido')}")
                return False
        else:
            print(f"\n⚠️ ERRO HTTP {response.status_code}")
            print(f"   Resposta: {response.text[:500]}")
            return False
            
    except Exception as e:
        print(f"\n❌ ERRO AO ENVIAR: {e}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return False

def main():
    tempo_inicio = time.time()
    
    # Buscar lista de alunos
    alunos = buscar_alunos_da_planilha()
    
    if not alunos:
        print("\n" + "=" * 70)
        print("❌ NENHUM ALUNO ENCONTRADO - VERIFIQUE:")
        print("=" * 70)
        print("1. A URL do Apps Script está correta?")
        print("   Execute testarUrl() no Apps Script para obter a URL")
        print()
        print("2. O Web App foi publicado?")
        print("   Implantar → Nova implementação → Web app")
        print()
        print("3. As permissões estão corretas?")
        print("   'Quem tem acesso' deve ser 'Qualquer pessoa'")
        print()
        print("4. A aba 'alunos_hortolandia' existe na planilha?")
        print("   ID da planilha: 1lnzzToyBao-c5sptw4IcnXA0QCvS4bKFpyiQUcxbA3Q")
        print()
        print("5. Execute testarListarIds() no Apps Script para verificar")
        print("=" * 70)
        return
    
    print(f"\n{'=' * 70}")
    print(f"🎯 INICIANDO COLETA DE {len(alunos)} ALUNOS")
    print(f"{'=' * 70}")
    
    alunos_dados = []
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        try:
            # Login
            print("\n🔐 Realizando login no sistema musical...")
            pagina.goto(URL_INICIAL, wait_until='domcontentloaded', timeout=30000)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=20000)
            print("✅ Login realizado com sucesso!")
            
            # Coletar dados de cada aluno
            print(f"\n{'=' * 70}")
            print(f"📊 COLETANDO DADOS DOS ALUNOS")
            print(f"{'=' * 70}")
            
            for i, aluno in enumerate(alunos, 1):
                id_aluno = aluno['id_aluno']
                nome = aluno['nome']
                
                print(f"\n[{i:3d}/{len(alunos)}] {nome}")
                print(f"         ID: {id_aluno}")
                
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
                print(f"         ✓ {totais} registros coletados")
                
                time.sleep(0.5)  # Pausa entre requisições
            
            navegador.close()
            
        except Exception as e:
            print(f"\n❌ ERRO DURANTE COLETA: {e}")
            import traceback
            print(traceback.format_exc())
            navegador.close()
            return
    
    tempo_total = time.time() - tempo_inicio
    
    # Preparar e enviar dados
    print(f"\n{'=' * 70}")
    print(f"📊 PREPARANDO DADOS PARA ENVIO")
    print(f"{'=' * 70}")
    
    dados_envio = preparar_dados_para_envio(alunos_dados, tempo_total)
    
    print(f"\n{'=' * 70}")
    print(f"📈 ESTATÍSTICAS DA COLETA")
    print(f"{'=' * 70}")
    print(f"Total de alunos processados: {len(alunos_dados)}")
    print(f"Alunos com dados.........: {dados_envio['metadata']['alunos_com_dados']}")
    print(f"Alunos sem dados.........: {dados_envio['metadata']['alunos_sem_dados']}")
    print(f"Tempo total..............: {tempo_total:.1f}s ({tempo_total/60:.1f} minutos)")
    print(f"Média por aluno..........: {tempo_total/len(alunos_dados):.1f}s")
    print(f"{'=' * 70}")
    
    # Enviar para Google Sheets
    sucesso = enviar_para_sheets(dados_envio)
    
    print(f"\n{'=' * 70}")
    if sucesso:
        print(f"🎉 PROCESSO FINALIZADO COM SUCESSO!")
    else:
        print(f"⚠️ PROCESSO FINALIZADO COM ERROS NO ENVIO")
    print(f"{'=' * 70}")

if __name__ == "__main__":
    main()
