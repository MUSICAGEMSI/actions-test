from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os, requests, time, json
from datetime import datetime
import concurrent.futures
from typing import List, Set, Dict, Optional
import re
from bs4 import BeautifulSoup

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzJv9YlseCXdvXwi0OOpxh-Q61rmCly2kMUBEtcv5VSyPEKdcKg7MAVvIgDYSM1yWpV/exec'

RANGE_INICIO = 1
RANGE_FIM = 850000
NUM_THREADS = 25

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

def gerar_timestamp():
    """
    Gera timestamp no formato DD_MM_YYYY-HH:MM
    """
    return datetime.now().strftime('%d_%m_%Y-%H:%M')

def buscar_ids_igrejas_hortolandia() -> Set[int]:
    """Busca os IDs das igrejas de Hortolândia do Google Sheets"""
    print("📥 Buscando IDs das igrejas de Hortolândia do Google Sheets...")
    
    try:
        params = {"acao": "listar_ids_hortolandia"}
        response = requests.get(URL_APPS_SCRIPT, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('status') != 'sucesso':
                print(f"⚠️ Erro: {data.get('mensagem', 'Erro desconhecido')}")
                return set()
            
            ids = set(data.get('ids', []))
            print(f"✅ {len(ids)} IDs de igrejas carregados: {sorted(list(ids))}")
            return ids
        else:
            print(f"⚠️ Erro ao buscar IDs: Status {response.status_code}")
            return set()
            
    except Exception as e:
        print(f"❌ Erro ao buscar IDs das igrejas: {e}")
        return set()

def extrair_dados_completos_membro(html_content: str, id_membro: int) -> Optional[Dict]:
    """Extrai TODOS os dados disponíveis do membro do HTML"""
    if not html_content or 'igreja_selecionada' not in html_content:
        return None
    
    soup = BeautifulSoup(html_content, 'html.parser')
    dados = {'id_membro': id_membro}
    
    # Nome
    nome_input = soup.find('input', {'name': 'nome'})
    dados['nome'] = nome_input.get('value', '').strip() if nome_input else ''
    
    # ID da Igreja
    match_igreja = re.search(r'igreja_selecionada\s*\((\d+)\)', html_content)
    dados['id_igreja'] = int(match_igreja.group(1)) if match_igreja else None
    
    # Cargo/Ministério
    cargo_select = soup.find('select', {'name': 'id_cargo', 'id': 'id_cargo'})
    if cargo_select:
        cargo_option = cargo_select.find('option', {'selected': True})
        if cargo_option:
            dados['id_cargo'] = cargo_option.get('value', '')
            dados['cargo_nome'] = cargo_option.text.strip()
        else:
            dados['id_cargo'] = ''
            dados['cargo_nome'] = ''
    
    # Nível
    nivel_select = soup.find('select', {'name': 'id_nivel', 'id': 'id_nivel'})
    if nivel_select:
        nivel_option = nivel_select.find('option', {'selected': True})
        if nivel_option:
            dados['id_nivel'] = nivel_option.get('value', '')
            dados['nivel_nome'] = nivel_option.text.strip()
        else:
            dados['id_nivel'] = ''
            dados['nivel_nome'] = ''
    
    # Instrumento
    instrumento_select = soup.find('select', {'name': 'id_instrumento', 'id': 'id_instrumento'})
    if instrumento_select:
        instrumento_option = instrumento_select.find('option', {'selected': True})
        if instrumento_option:
            dados['id_instrumento'] = instrumento_option.get('value', '')
            dados['instrumento_nome'] = instrumento_option.text.strip()
        else:
            dados['id_instrumento'] = ''
            dados['instrumento_nome'] = ''
    
    # Tonalidade
    tonalidade_select = soup.find('select', {'name': 'id_tonalidade', 'id': 'id_tonalidade'})
    if tonalidade_select:
        tonalidade_option = tonalidade_select.find('option', {'selected': True})
        if tonalidade_option:
            dados['id_tonalidade'] = tonalidade_option.get('value', '')
            dados['tonalidade_nome'] = tonalidade_option.text.strip()
        else:
            dados['id_tonalidade'] = ''
            dados['tonalidade_nome'] = ''
    
    # Status
    fl_tipo_input = soup.find('input', {'name': 'fl_tipo'})
    dados['fl_tipo'] = fl_tipo_input.get('value', '') if fl_tipo_input else ''
    
    status_input = soup.find('input', {'name': 'status'})
    dados['status'] = status_input.get('value', '') if status_input else ''
    
    # Histórico do Registro
    historico_div = soup.find('div', {'id': 'collapseOne'})
    if historico_div:
        cadastro_p = historico_div.find('p', string=re.compile(r'Cadastrado em:'))
        if cadastro_p:
            texto = cadastro_p.get_text()
            match_data = re.search(r'Cadastrado em:\s*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})', texto)
            match_usuario = re.search(r'por:\s*(.+)$', texto)
            
            dados['data_cadastro'] = match_data.group(1).strip() if match_data else ''
            dados['cadastrado_por'] = match_usuario.group(1).strip() if match_usuario else ''
        
        atualizacao_p = historico_div.find('p', string=re.compile(r'Atualizado em:'))
        if atualizacao_p:
            texto = atualizacao_p.get_text()
            match_data = re.search(r'Atualizado em:\s*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})', texto)
            match_usuario = re.search(r'por:\s*(.+)$', texto)
            
            dados['data_atualizacao'] = match_data.group(1).strip() if match_data else ''
            dados['atualizado_por'] = match_usuario.group(1).strip() if match_usuario else ''
    
    # Dados Ministeriais
    form_min = soup.find('form', {'id': 'grp-musical-min'})
    if form_min:
        igreja_min_select = form_min.find('select', {'name': 'id_igreja'})
        if igreja_min_select:
            igreja_min_option = igreja_min_select.find('option', {'selected': True})
            if igreja_min_option:
                dados['id_igreja_ministerial'] = igreja_min_option.get('value', '')
                dados['igreja_ministerial_nome'] = igreja_min_option.text.strip()
    
    return dados

class ColetorAlunosCompleto:
    def __init__(self, session, thread_id: int, ids_igrejas: Set[int]):
        self.session = session
        self.thread_id = thread_id
        self.ids_igrejas = ids_igrejas
        self.membros_encontrados: List[Dict] = []
        self.requisicoes_feitas = 0
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
    
    def coletar_batch_membros(self, ids_batch: List[int]) -> List[Dict]:
        """Verifica um batch de IDs e retorna os membros completos de Hortolândia"""
        for membro_id in ids_batch:
            try:
                url = f"https://musical.congregacao.org.br/grp_musical/editar/{membro_id}"
                
                resp = self.session.get(url, headers=self.headers, timeout=10)
                self.requisicoes_feitas += 1
                
                if resp.status_code == 200:
                    dados_membro = extrair_dados_completos_membro(resp.text, membro_id)
                    
                    if dados_membro and dados_membro['id_igreja'] in self.ids_igrejas:
                        self.membros_encontrados.append(dados_membro)
                        
                        print(f"✅ T{self.thread_id}: ID {membro_id} | Igreja {dados_membro['id_igreja']} | "
                              f"{dados_membro['nome'][:30]} | {dados_membro.get('instrumento_nome', 'N/A')}")
                
                time.sleep(0.08)
                
                if self.requisicoes_feitas % 500 == 0:
                    print(f"📊 T{self.thread_id}: {self.requisicoes_feitas:,} requisições | "
                          f"{len(self.membros_encontrados)} membros encontrados")
                
            except Exception as e:
                if "timeout" in str(e).lower():
                    print(f"⏱️ T{self.thread_id}: Timeout no ID {membro_id}")
                continue
        
        return self.membros_encontrados

def executar_coleta_paralela_membros(session, ids_igrejas: Set[int], range_inicio: int, 
                                     range_fim: int, num_threads: int) -> List[Dict]:
    """Executa coleta paralela de membros de Hortolândia"""
    total_ids = range_fim - range_inicio + 1
    ids_per_thread = total_ids // num_threads
    
    print(f"📈 Dividindo {total_ids:,} IDs em {num_threads} threads ({ids_per_thread:,} IDs/thread)")
    
    thread_ranges = []
    for i in range(num_threads):
        inicio = range_inicio + (i * ids_per_thread)
        fim = inicio + ids_per_thread - 1
        
        if i == num_threads - 1:
            fim = range_fim
            
        thread_ranges.append(list(range(inicio, fim + 1)))
    
    todos_membros = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        coletores = [ColetorAlunosCompleto(session, i, ids_igrejas) for i in range(num_threads)]
        
        futures = []
        for i, ids_thread in enumerate(thread_ranges):
            future = executor.submit(coletores[i].coletar_batch_membros, ids_thread)
            futures.append((future, i))
        
        for future, thread_id in futures:
            try:
                membros_thread = future.result(timeout=3600)
                todos_membros.extend(membros_thread)
                coletor = coletores[thread_id]
                print(f"✅ Thread {thread_id}: {len(membros_thread)} membros | "
                      f"{coletor.requisicoes_feitas:,} requisições")
            except Exception as e:
                print(f"❌ Thread {thread_id}: Erro - {e}")
    
    return todos_membros

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def salvar_membros_em_arquivo(membros: List[Dict], timestamp: str):
    """Salva os dados completos dos membros em arquivo JSON"""
    nome_arquivo = f"membros_{timestamp.replace(':', '-')}.json"
    
    try:
        with open(nome_arquivo, 'w', encoding='utf-8') as f:
            json.dump({
                "membros": membros,
                "total": len(membros),
                "timestamp": timestamp,
                "campos_coletados": list(membros[0].keys()) if membros else []
            }, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Dados salvos em: {nome_arquivo}")
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo: {e}")

def enviar_membros_para_sheets(membros: List[Dict], tempo_execucao: float, ids_igrejas: Set[int], timestamp: str):
    """Envia os dados completos dos membros para nova planilha no Google Sheets"""
    if not membros:
        print("⚠️ Nenhum membro para enviar")
        return False
    
    print(f"\n📤 Criando nova planilha: Membros_{timestamp}")
    print(f"📊 Enviando {len(membros)} membros...")
    
    headers = [
        "ID_MEMBRO", "NOME", "ID_IGREJA", 
        "ID_CARGO", "CARGO_NOME", 
        "ID_NIVEL", "NIVEL_NOME",
        "ID_INSTRUMENTO", "INSTRUMENTO_NOME",
        "ID_TONALIDADE", "TONALIDADE_NOME",
        "FL_TIPO", "STATUS",
        "DATA_CADASTRO", "CADASTRADO_POR",
        "DATA_ATUALIZACAO", "ATUALIZADO_POR",
        "ID_IGREJA_MINISTERIAL", "IGREJA_MINISTERIAL_NOME"
    ]
    
    relatorio = [headers]
    
    for membro in membros:
        linha = [
            str(membro.get('id_membro', '')),
            membro.get('nome', ''),
            str(membro.get('id_igreja', '')),
            str(membro.get('id_cargo', '')),
            membro.get('cargo_nome', ''),
            str(membro.get('id_nivel', '')),
            membro.get('nivel_nome', ''),
            str(membro.get('id_instrumento', '')),
            membro.get('instrumento_nome', ''),
            str(membro.get('id_tonalidade', '')),
            membro.get('tonalidade_nome', ''),
            str(membro.get('fl_tipo', '')),
            str(membro.get('status', '')),
            membro.get('data_cadastro', ''),
            membro.get('cadastrado_por', ''),
            membro.get('data_atualizacao', ''),
            membro.get('atualizado_por', ''),
            str(membro.get('id_igreja_ministerial', '')),
            membro.get('igreja_ministerial_nome', '')
        ]
        relatorio.append(linha)
    
    payload = {
        "tipo": "nova_planilha_membros_completo",
        "timestamp": timestamp,  # IMPORTANTE: Timestamp no formato DD_MM_YYYY-HH:MM
        "relatorio_formatado": relatorio,
        "metadata": {
            "total_membros": len(membros),
            "total_igrejas_monitoradas": len(ids_igrejas),
            "range_inicio": RANGE_INICIO,
            "range_fim": RANGE_FIM,
            "tempo_execucao_min": round(tempo_execucao/60, 2),
            "threads_utilizadas": NUM_THREADS,
            "timestamp": timestamp,
            "ids_igrejas": sorted(list(ids_igrejas))
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=180)
        
        if response.status_code == 200:
            resultado = response.json()
            
            if resultado.get('status') == 'sucesso':
                print(f"\n✅ SUCESSO! Planilha criada com sucesso!")
                print(f"📝 Nome: {resultado['planilha']['nome']}")
                print(f"🔗 URL: {resultado['planilha']['url']}")
                print(f"🆔 ID: {resultado['planilha']['id']}")
                
                # Salvar URL da planilha em arquivo
                with open(f"planilha_url_{timestamp.replace(':', '-')}.txt", 'w') as f:
                    f.write(f"Planilha criada em: {timestamp}\n")
                    f.write(f"Nome: {resultado['planilha']['nome']}\n")
                    f.write(f"URL: {resultado['planilha']['url']}\n")
                    f.write(f"ID: {resultado['planilha']['id']}\n")
                
                return True
            else:
                print(f"\n❌ Erro na resposta: {resultado.get('mensagem', 'Erro desconhecido')}")
                return False
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
    timestamp_execucao = gerar_timestamp()
    
    print("=" * 80)
    print("🎓 COLETOR COMPLETO DE DADOS - ALUNOS DE HORTOLÂNDIA")
    print("=" * 80)
    print(f"📅 Execução: {timestamp_execucao}")
    print(f"📊 Range de busca: {RANGE_INICIO:,} - {RANGE_FIM:,}")
    print(f"🧵 Threads: {NUM_THREADS}")
    print(f"📄 Nova Planilha: Membros_{timestamp_execucao}")
    print("=" * 80)
    
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
            pagina.goto(URL_INICIAL)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado com sucesso!")
            
        except Exception as e:
            print(f"❌ Erro no login: {e}")
            navegador.close()
            return
        
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
    
    session = requests.Session()
    session.cookies.update(cookies_dict)
    
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=NUM_THREADS + 5,
        pool_maxsize=NUM_THREADS + 5,
        max_retries=2
    )
    session.mount('https://', adapter)
    
    print("\n🎓 Iniciando busca COMPLETA de membros de Hortolândia...")
    print(f"🏛️ Monitorando {len(ids_igrejas)} igrejas")
    
    membros_hortolandia = executar_coleta_paralela_membros(session, ids_igrejas, 
                                                           RANGE_INICIO, RANGE_FIM, NUM_THREADS)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'='*80}")
    print(f"🏁 COLETA COMPLETA FINALIZADA!")
    print(f"{'='*80}")
    print(f"🎓 Membros de Hortolândia encontrados: {len(membros_hortolandia)}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    print(f"📈 Range verificado: {RANGE_INICIO:,} - {RANGE_FIM:,} ({RANGE_FIM - RANGE_INICIO + 1:,} IDs)")
    
    if membros_hortolandia:
        print(f"⚡ Velocidade: {(RANGE_FIM - RANGE_INICIO + 1)/tempo_total:.2f} IDs verificados/segundo")
        
        print(f"\n📋 Primeiros 5 membros (amostra de dados):")
        for i, membro in enumerate(membros_hortolandia[:5]):
            print(f"\n   {i+1}. {membro['nome']}")
            print(f"      ID: {membro['id_membro']} | Igreja: {membro['id_igreja']}")
            print(f"      Cargo: {membro.get('cargo_nome', 'N/A')}")
            print(f"      Instrumento: {membro.get('instrumento_nome', 'N/A')} ({membro.get('tonalidade_nome', 'N/A')})")
            print(f"      Nível: {membro.get('nivel_nome', 'N/A')}")
            print(f"      Cadastro: {membro.get('data_cadastro', 'N/A')}")
        
        # Estatísticas por igreja
        print(f"\n📊 Distribuição por igreja:")
        from collections import Counter
        distribuicao = Counter([m['id_igreja'] for m in membros_hortolandia])
        for igreja_id, qtd in distribuicao.most_common():
            print(f"   Igreja {igreja_id}: {qtd} membros")
        
        # Estatísticas por instrumento
        print(f"\n🎵 Distribuição por instrumento:")
        distribuicao_inst = Counter([m.get('instrumento_nome', 'N/A') for m in membros_hortolandia])
        for instrumento, qtd in distribuicao_inst.most_common(10):
            print(f"   {instrumento}: {qtd} membros")
        
        salvar_membros_em_arquivo(membros_hortolandia, timestamp_execucao)
        enviar_membros_para_sheets(membros_hortolandia, tempo_total, ids_igrejas, timestamp_execucao)
    
    else:
        print("⚠️ Nenhum membro de Hortolândia foi encontrado neste range")
    
    print(f"\n📄 Planilha criada: Membros_{timestamp_execucao}")
    print(f"🎯 Processo finalizado!")

if __name__ == "__main__":
    main()
