# bk_historicoindividual_melhorado.py
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
import concurrent.futures
from threading import Lock

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_LISTAGEM_ALUNOS = "https://musical.congregacao.org.br/alunos/listagem"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzSMrefJ-RJvjBNLVnqB2iXdBpoxM5WwuDUbl3rKelFplv898DKu9R9oWYXGgVxNjie/exec'

# Lock para thread safety
print_lock = Lock()

def safe_print(*args, **kwargs):
    """Print thread-safe"""
    with print_lock:
        print(*args, **kwargs)

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
        
        data = {
            'draw': '1',
            'start': '0',
            'length': '10000',
            'search[value]': '',
            'search[regex]': 'false'
        }
        
        resp = session.post(URL_LISTAGEM_ALUNOS, data=data, headers=headers, timeout=30)
        
        if resp.status_code == 200:
            dados_json = resp.json()
            alunos = []
            
            for linha in dados_json.get('data', []):
                if len(linha) >= 8:
                    id_aluno = linha[0]
                    nome_info = linha[1]
                    comum_info = linha[2]
                    ministerio = linha[3]
                    instrumento = linha[4]
                    nivel = linha[5]
                    
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
            
            safe_print(f"✅ Encontrados {len(alunos)} alunos")
            return alunos
            
    except Exception as e:
        safe_print(f"❌ Erro ao obter lista de alunos: {e}")
        return []

def extrair_texto_limpo(elemento):
    """Extrai texto limpo de um elemento HTML, preservando informações importantes"""
    if not elemento:
        return ""
    
    # Extrair texto preservando quebras de linha importantes
    texto = elemento.get_text(separator=" ", strip=True)
    
    # Normalizar espaços em branco
    texto = re.sub(r'\s+', ' ', texto)
    
    # Remover texto de botões de ação
    texto = re.sub(r'\s*Apagar\s*', '', texto)
    texto = re.sub(r'\s*Excluir\s*', '', texto)
    
    return texto.strip()

def extrair_dados_tabela_completa_melhorada(soup_secao, secao_nome):
    """Versão melhorada que extrai TODOS os dados de uma tabela"""
    registros = []
    
    # Encontrar todas as tabelas na seção
    tabelas = soup_secao.find_all('table')
    
    for idx_tabela, tabela in enumerate(tabelas):
        # Pular se é tabela de grupo (será processada separadamente)
        table_id = tabela.get('id', '')
        if 'grupo' in table_id.lower():
            continue
            
        # Extrair cabeçalho
        thead = tabela.find('thead')
        if not thead:
            continue
            
        headers = []
        header_row = thead.find('tr')
        if header_row:
            for th in header_row.find_all('th'):
                header_text = extrair_texto_limpo(th)
                if header_text and header_text.lower() not in ['ações', 'acoes']:
                    headers.append(header_text)
        
        if not headers:
            continue
        
        # Extrair dados do corpo da tabela
        tbody = tabela.find('tbody')
        if not tbody:
            continue
            
        for tr_idx, tr in enumerate(tbody.find_all('tr')):
            # Verificar se não é linha de "nenhum registro encontrado"
            if ('dataTables_empty' in str(tr.get('class', [])) or 
                'Nenhum registro encontrado' in tr.get_text() or
                'Oops...' in tr.get_text()):
                continue
                
            linha_dados = []
            tds = tr.find_all('td')
            
            # Processar cada célula, limitando ao número de headers
            for i in range(min(len(headers), len(tds))):
                td = tds[i]
                
                # Extrair texto limpo da célula
                conteudo_celula = extrair_texto_limpo(td)
                
                # Se a célula está vazia, tentar extrair atributos úteis
                if not conteudo_celula:
                    # Verificar se há inputs ou selects com valores
                    inputs = td.find_all(['input', 'select'])
                    for input_elem in inputs:
                        valor = input_elem.get('value', '')
                        if valor:
                            conteudo_celula = valor
                            break
                
                linha_dados.append(conteudo_celula)
            
            # Só adicionar se há dados válidos na linha
            if linha_dados and any(campo.strip() for campo in linha_dados):
                # Extrair ID da linha se existir
                row_id = tr.get('id', '')
                
                registro = {
                    'secao': secao_nome,
                    'tabela_index': idx_tabela,
                    'linha_index': tr_idx,
                    'row_id': row_id,
                    'headers': headers,
                    'dados': linha_dados,
                    'dados_estruturados': dict(zip(headers, linha_dados)),
                    'timestamp_coleta': datetime.now().isoformat()
                }
                registros.append(registro)
    
    return registros

def extrair_dados_grupo_melhorada(soup_secao, secao_nome):
    """Versão melhorada para extrair dados das seções de grupo"""
    registros = []
    
    # Buscar por H3 seguido de tabelas
    h3_elements = soup_secao.find_all('h3')
    
    for h3 in h3_elements:
        h3_text = h3.get_text().lower()
        if 'grupo' in h3_text or 'aulas em grupo' in h3_text:
            # Encontrar próxima tabela após o H3
            next_table = h3.find_next('table')
            if next_table:
                # Extrair cabeçalho
                headers = []
                thead = next_table.find('thead')
                if thead:
                    header_row = thead.find('tr')
                    if header_row:
                        for th in header_row.find_all('th'):
                            header_text = extrair_texto_limpo(th)
                            if header_text:
                                headers.append(header_text)
                
                # Extrair dados
                tbody = next_table.find('tbody')
                if tbody:
                    for tr_idx, tr in enumerate(tbody.find_all('tr')):
                        if ('dataTables_empty' in str(tr.get('class', [])) or
                            'Nenhum registro encontrado' in tr.get_text()):
                            continue
                            
                        linha_dados = []
                        tds = tr.find_all('td')
                        
                        for td in tds:
                            conteudo_celula = extrair_texto_limpo(td)
                            linha_dados.append(conteudo_celula)
                        
                        if linha_dados and any(campo.strip() for campo in linha_dados):
                            registro = {
                                'secao': f"{secao_nome}_grupo",
                                'subsecao': h3.get_text().strip(),
                                'linha_index': tr_idx,
                                'headers': headers,
                                'dados': linha_dados,
                                'dados_estruturados': dict(zip(headers, linha_dados)) if len(headers) == len(linha_dados) else {},
                                'timestamp_coleta': datetime.now().isoformat()
                            }
                            registros.append(registro)
    
    return registros

def processar_secao_completa_melhorada(html_content, secao_id):
    """Versão melhorada que processa uma seção completa"""
    if not html_content:
        return []
    
    soup = BeautifulSoup(html_content, 'html.parser')
    registros_secao = []
    
    # Encontrar a div da aba específica
    tab_pane = soup.find('div', {'id': secao_id, 'class': 'tab-pane'})
    if not tab_pane:
        tab_pane = soup.find('div', id=secao_id)
    
    if tab_pane:
        # Extrair dados das tabelas principais
        registros_principais = extrair_dados_tabela_completa_melhorada(tab_pane, secao_id)
        registros_secao.extend(registros_principais)
        
        # Extrair dados das tabelas de grupo
        registros_grupo = extrair_dados_grupo_melhorada(tab_pane, secao_id)
        registros_secao.extend(registros_grupo)
    
    return registros_secao

def obter_backup_completo_aluno_melhorado(session, aluno_id, aluno_nome=""):
    """Versão melhorada que extrai TODOS os dados disponíveis"""
    try:
        url_historico = f"https://musical.congregacao.org.br/licoes/index/{aluno_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Referer': 'https://musical.congregacao.org.br/alunos/listagem',
            'Connection': 'keep-alive',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        
        resp = session.get(url_historico, headers=headers, timeout=20)
        
        if resp.status_code != 200:
            safe_print(f"      ⚠️ Status HTTP {resp.status_code} para aluno {aluno_id}")
            return {}
        
        # Processar todas as seções
        secoes = ['mts', 'msa', 'provas', 'metodos', 'hinario', 'escalas']
        backup_completo = {
            'aluno_id': aluno_id,
            'aluno_nome': aluno_nome,
            'timestamp_coleta': datetime.now().isoformat(),
            'secoes': {}
        }
        
        total_registros = 0
        
        for secao in secoes:
            registros_secao = processar_secao_completa_melhorada(resp.text, secao)
            backup_completo['secoes'][secao] = registros_secao
            total_registros += len(registros_secao)
        
        if total_registros > 0:
            # Criar resumo detalhado por seção
            resumo_secoes = {}
            for secao, registros in backup_completo['secoes'].items():
                if registros:
                    resumo_secoes[secao] = {
                        'total_registros': len(registros),
                        'headers_encontrados': list(set(str(reg.get('headers', [])) for reg in registros if reg.get('headers'))),
                    }
            
            backup_completo['resumo_secoes'] = resumo_secoes
            safe_print(f"      ✓ {aluno_nome[:30]}... - {total_registros} registros coletados")
        
        return backup_completo
        
    except Exception as e:
        safe_print(f"      ❌ Erro ao processar aluno {aluno_id}: {e}")
        return {}

def processar_lote_backup_completo_melhorado(session, lote_alunos, lote_numero):
    """Processa um lote de alunos para backup completo melhorado"""
    resultado_lote = []
    
    for i, aluno in enumerate(lote_alunos):
        try:
            # Obter backup completo do aluno
            backup_aluno = obter_backup_completo_aluno_melhorado(session, aluno['id'], aluno['nome'])
            
            if backup_aluno:
                # Adicionar informações básicas do aluno
                backup_aluno['info_basica'] = {
                    'nome': aluno['nome'],
                    'id': aluno['id'],
                    'comum': aluno['comum'],
                    'ministerio': aluno['ministerio'],
                    'instrumento': aluno['instrumento'],
                    'nivel': aluno['nivel']
                }
                
                resultado_lote.append(backup_aluno)
            
            # Pausa entre alunos
            time.sleep(0.3)
            
        except Exception as e:
            safe_print(f"      ⚠️ Erro ao processar aluno {aluno['id']}: {e}")
    
    safe_print(f"   📦 Lote {lote_numero} concluído ({len(resultado_lote)} alunos processados)")
    return resultado_lote

def criar_sessoes_multiplas(cookies_dict, num_sessoes=2):
    """Cria múltiplas sessões com os mesmos cookies"""
    sessoes = []
    for i in range(num_sessoes):
        session = requests.Session()
        session.cookies.update(cookies_dict)
        sessoes.append(session)
    return sessoes

def main():
    tempo_inicio = time.time()
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
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
        
        # Criar sessão principal
        cookies_dict = extrair_cookies_playwright(pagina)
        session_principal = requests.Session()
        session_principal.cookies.update(cookies_dict)
        
        # Obter lista de alunos
        print("🔍 Obtendo lista de alunos...")
        alunos = obter_lista_alunos(session_principal)
        
        if not alunos:
            print("❌ Nenhum aluno encontrado.")
            navegador.close()
            return
        
        # Para teste inicial, uncomment para limitar
        # alunos = alunos[:10]  # Teste com 10 alunos primeiro
        
        print(f"📊 Iniciando backup completo melhorado de {len(alunos)} alunos...")
        
        # Dividir alunos em lotes menores
        batch_size = 5
        lotes = [alunos[i:i + batch_size] for i in range(0, len(alunos), batch_size)]
        
        backup_completo_final = {
            'metadata': {
                'timestamp_inicio': datetime.now().isoformat(),
                'total_alunos': len(alunos),
                'versao_script': '3.0_melhorado_completo',
                'fonte': 'musical.congregacao.org.br',
                'descricao': 'Backup completo com extração melhorada de todos os dados das tabelas'
            },
            'alunos': []
        }
        
        # Criar múltiplas sessões para paralelização
        sessoes = criar_sessoes_multiplas(cookies_dict, 2)
        
        # Processar lotes com threading
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = []
            
            for i, lote in enumerate(lotes):
                session_para_lote = sessoes[i % len(sessoes)]
                
                future = executor.submit(
                    processar_lote_backup_completo_melhorado, 
                    session_para_lote, 
                    lote, 
                    i + 1
                )
                futures.append(future)
                
                # Processar em grupos menores para controle
                if len(futures) >= 2:
                    for future in concurrent.futures.as_completed(futures):
                        resultado_lote = future.result()
                        backup_completo_final['alunos'].extend(resultado_lote)
                    
                    futures = []
                    
                    # Status atualizado
                    alunos_processados = len(backup_completo_final['alunos'])
                    progresso = (alunos_processados / len(alunos)) * 100
                    tempo_decorrido = (time.time() - tempo_inicio) / 60
                    print(f"📈 Progresso: {alunos_processados}/{len(alunos)} ({progresso:.1f}%) - {tempo_decorrido:.1f}min")
            
            # Processar futures restantes
            for future in concurrent.futures.as_completed(futures):
                resultado_lote = future.result()
                backup_completo_final['alunos'].extend(resultado_lote)
        
        # Calcular estatísticas detalhadas
        tempo_total = (time.time() - tempo_inicio) / 60
        backup_completo_final['metadata']['timestamp_fim'] = datetime.now().isoformat()
        backup_completo_final['metadata']['tempo_total_minutos'] = tempo_total
        backup_completo_final['metadata']['alunos_processados'] = len(backup_completo_final['alunos'])
        
        # Estatísticas por seção
        total_registros = 0
        stats_secoes = {}
        stats_detalhadas = {}
        
        for aluno_backup in backup_completo_final['alunos']:
            for secao_nome, registros_secao in aluno_backup.get('secoes', {}).items():
                total_registros += len(registros_secao)
                stats_secoes[secao_nome] = stats_secoes.get(secao_nome, 0) + len(registros_secao)
                
                # Estatísticas detalhadas por tipo de dados
                if registros_secao and secao_nome not in stats_detalhadas:
                    stats_detalhadas[secao_nome] = {
                        'headers_exemplo': registros_secao[0].get('headers', []) if registros_secao else [],
                        'estrutura_dados': list(registros_secao[0].get('dados_estruturados', {}).keys()) if registros_secao else []
                    }
        
        backup_completo_final['metadata']['total_registros'] = total_registros
        backup_completo_final['metadata']['stats_secoes'] = stats_secoes
        backup_completo_final['metadata']['stats_detalhadas'] = stats_detalhadas
        
        print(f"\n📊 Backup completo melhorado finalizado!")
        print(f"   🎯 Total de alunos processados: {len(backup_completo_final['alunos'])}")
        print(f"   📋 Total de registros coletados: {total_registros}")
        print(f"   ⏱️ Tempo total: {tempo_total:.1f} minutos")
        print(f"   🚀 Velocidade: {len(backup_completo_final['alunos'])/tempo_total:.1f} alunos/min")
        
        print(f"\n📈 Estatísticas por seção:")
        for secao, count in stats_secoes.items():
            if count > 0:
                print(f"   └─ {secao}: {count} registros")
        
        # Preparar dados para envio ao Apps Script
        body_para_apps_script = {
            "tipo": "backup_completo_melhorado",
            "dados": backup_completo_final,
            "resumo": {
                "total_alunos": len(backup_completo_final['alunos']),
                "total_registros": total_registros,
                "tempo_processamento": f"{tempo_total:.1f} minutos",
                "stats_secoes": stats_secoes,
                "versao": "3.0_melhorado",
                "melhorias": [
                    "Extração completa de conteúdo das células",
                    "Mapeamento estruturado de dados por headers",
                    "Identificação de IDs de registros",
                    "Processamento melhorado de tabelas de grupo",
                    "Limpeza aprimorada de texto"
                ]
            }
        }
        
        # Tentar enviar para Apps Script
        try:
            print("📤 Enviando backup melhorado para Google Sheets...")
            resposta_post = requests.post(URL_APPS_SCRIPT, json=body_para_apps_script, timeout=300)
            print(f"✅ Backup enviado! Status: {resposta_post.status_code}")
            if resposta_post.text:
                print(f"Resposta: {resposta_post.text[:200]}...")
        except Exception as e:
            print(f"❌ Erro ao enviar para Apps Script: {e}")
            
            # Salvar backup local como fallback
            nome_arquivo = f'backup_melhorado_{int(time.time())}.json'
            with open(nome_arquivo, 'w', encoding='utf-8') as f:
                json.dump(backup_completo_final, f, ensure_ascii=False, indent=2)
            print(f"💾 Backup completo salvo localmente: {nome_arquivo}")
            
            # Salvar também um resumo legível
            nome_resumo = f'resumo_backup_{int(time.time())}.txt'
            with open(nome_resumo, 'w', encoding='utf-8') as f:
                f.write("RESUMO DO BACKUP COMPLETO\n")
                f.write("="*50 + "\n\n")
                f.write(f"Total de alunos: {len(backup_completo_final['alunos'])}\n")
                f.write(f"Total de registros: {total_registros}\n")
                f.write(f"Tempo de processamento: {tempo_total:.1f} minutos\n\n")
                f.write("Estatísticas por seção:\n")
                for secao, count in stats_secoes.items():
                    if count > 0:
                        f.write(f"  {secao}: {count} registros\n")
                        if secao in stats_detalhadas:
                            f.write(f"    Headers: {stats_detalhadas[secao]['headers_exemplo']}\n")
                f.write(f"\nVersão do script: 3.0_melhorado_completo\n")
            print(f"📄 Resumo salvo: {nome_resumo}")
        
        navegador.close()

# Função utilitária para testar um único aluno (para debug)
def testar_um_aluno(aluno_id, aluno_nome="Teste"):
    """Função para testar a extração de dados de um único aluno"""
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=False)  # headless=False para debug
        pagina = navegador.new_page()
        
        pagina.goto(URL_INICIAL)
        pagina.fill('input[name="login"]', EMAIL)
        pagina.fill('input[name="password"]', SENHA)
        pagina.click('button[type="submit"]')
        pagina.wait_for_selector("nav", timeout=15000)
        
        cookies_dict = extrair_cookies_playwright(pagina)
        session = requests.Session()
        session.cookies.update(cookies_dict)
        
        print(f"🔍 Testando extração para aluno {aluno_id}...")
        backup = obter_backup_completo_aluno_melhorado(session, aluno_id, aluno_nome)
        
        # Mostrar resultados detalhados
        print(f"\n📊 Resultados para {aluno_nome}:")
        for secao, registros in backup.get('secoes', {}).items():
            if registros:
                print(f"  {secao}: {len(registros)} registros")
                for i, reg in enumerate(registros[:2]):  # Mostrar primeiros 2 registros
                    print(f"    Registro {i+1}: {reg.get('dados', [])}")
        
        # Salvar resultado para análise
        with open(f'teste_aluno_{aluno_id}.json', 'w', encoding='utf-8') as f:
            json.dump(backup, f, ensure_ascii=False, indent=2)
        
        navegador.close()
        return backup

if __name__ == "__main__":
    if not EMAIL or not SENHA:
        print("❌ Erro: LOGIN_MUSICAL ou SENHA_MUSICAL não definidos.")
        exit(1)
    
    # Para testar um aluno específico, uncomment a linha abaixo e comment main()
    # testar_um_aluno("697150", "Adilson Thiago Virtis Dos Santos")
    
    main()
