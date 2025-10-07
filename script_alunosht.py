from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os
import requests
import time
import json
import concurrent.futures
from typing import List, Set, Dict, Tuple
import re
from collections import Counter

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzl1l143sg2_S5a6bOQy6WqWATMDZpSglIyKUp3OVZtycuHXQmGjisOpzffHTW5TvyK/exec'

# Parâmetros da busca híbrida inteligente
ID_MINIMO = 1
ID_MAXIMO = 1000000
TAMANHO_CHUNK_EXPLORACAO = 10000  # Verificar a cada 10k IDs na exploração
AMOSTRA_POR_CHUNK = 20  # Quantos IDs testar por chunk
NUM_THREADS_COLETA = 25

print(f"🧠 COLETOR HÍBRIDO INTELIGENTE - ALUNOS DE HORTOLÂNDIA")
print(f"🔍 Range total: {ID_MINIMO:,} - {ID_MAXIMO:,}")
print(f"⚡ Estratégia: Exploração rápida + Coleta refinada")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

def buscar_ids_igrejas_hortolandia() -> Set[int]:
    """Busca os IDs das igrejas de Hortolândia do Google Sheets"""
    print("\n📥 Buscando IDs das igrejas de Hortolândia do Google Sheets...")
    
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

def extrair_igreja_selecionada(html_content: str) -> int:
    """Extrai o ID da igreja_selecionada do HTML"""
    if not html_content:
        return None
    
    match = re.search(r'igreja_selecionada\s*\((\d+)\)', html_content)
    if match:
        return int(match.group(1))
    
    match = re.search(r'igreja_selecionada\((\d+)\)', html_content)
    if match:
        return int(match.group(1))
    
    return None

def extrair_nome_aluno(html_content: str) -> str:
    """Extrai o nome do aluno do HTML"""
    if not html_content:
        return ""
    
    match = re.search(r'name="nome"[^>]*value="([^"]+)"', html_content)
    if match:
        return match.group(1).strip()
    
    return ""

def verificar_aluno_existe(session, aluno_id: int, ids_igrejas: Set[int]) -> bool:
    """
    Verifica rapidamente se um aluno existe e é de Hortolândia
    Retorna True se for de Hortolândia, False caso contrário
    """
    try:
        url = f"https://musical.congregacao.org.br/grp_musical/editar/{aluno_id}"
        resp = session.get(url, timeout=8)
        
        if resp.status_code == 200 and 'igreja_selecionada' in resp.text:
            igreja_id = extrair_igreja_selecionada(resp.text)
            return igreja_id in ids_igrejas if igreja_id else False
        
        return False
    except:
        return False

def explorar_chunk(session, chunk_inicio: int, chunk_fim: int, ids_igrejas: Set[int]) -> Tuple[int, bool]:
    """
    Explora um chunk verificando uma amostra aleatória de IDs
    Retorna (numero_encontrados, tem_alunos_hortolandia)
    """
    import random
    
    # Gerar IDs de amostra distribuídos pelo chunk
    tamanho_chunk = chunk_fim - chunk_inicio + 1
    if tamanho_chunk <= AMOSTRA_POR_CHUNK:
        ids_amostra = list(range(chunk_inicio, chunk_fim + 1))
    else:
        step = tamanho_chunk // AMOSTRA_POR_CHUNK
        ids_amostra = [chunk_inicio + (i * step) for i in range(AMOSTRA_POR_CHUNK)]
    
    encontrados = 0
    for id_teste in ids_amostra:
        if verificar_aluno_existe(session, id_teste, ids_igrejas):
            encontrados += 1
        time.sleep(0.05)
    
    return encontrados, encontrados > 0

def fase1_exploracao_rapida(session, ids_igrejas: Set[int]) -> List[Tuple[int, int]]:
    """
    FASE 1: Exploração rápida para encontrar chunks com alunos de Hortolândia
    Retorna lista de ranges (inicio, fim) onde foram encontrados alunos
    """
    print(f"\n{'='*70}")
    print(f"🔍 FASE 1: EXPLORAÇÃO RÁPIDA")
    print(f"{'='*70}")
    print(f"📊 Verificando chunks de {TAMANHO_CHUNK_EXPLORACAO:,} IDs")
    print(f"🎯 Testando {AMOSTRA_POR_CHUNK} IDs por chunk")
    
    chunks_com_alunos = []
    total_chunks = (ID_MAXIMO - ID_MINIMO + 1) // TAMANHO_CHUNK_EXPLORACAO
    
    chunk_atual = 0
    for chunk_inicio in range(ID_MINIMO, ID_MAXIMO, TAMANHO_CHUNK_EXPLORACAO):
        chunk_fim = min(chunk_inicio + TAMANHO_CHUNK_EXPLORACAO - 1, ID_MAXIMO)
        chunk_atual += 1
        
        print(f"🔎 Chunk {chunk_atual}/{total_chunks}: {chunk_inicio:,} - {chunk_fim:,}...", end=" ")
        
        encontrados, tem_alunos = explorar_chunk(session, chunk_inicio, chunk_fim, ids_igrejas)
        
        if tem_alunos:
            print(f"✅ {encontrados} alunos encontrados!")
            chunks_com_alunos.append((chunk_inicio, chunk_fim))
        else:
            print(f"⚫ Vazio")
        
        # Pequena pausa entre chunks
        time.sleep(0.3)
    
    print(f"\n✨ Exploração concluída!")
    print(f"📍 {len(chunks_com_alunos)} chunks com alunos de Hortolândia identificados")
    
    return chunks_com_alunos

class ColetorAlunosHortolandia:
    def __init__(self, session, thread_id: int, ids_igrejas: Set[int]):
        self.session = session
        self.thread_id = thread_id
        self.ids_igrejas = ids_igrejas
        self.alunos_encontrados: List[Dict] = []
        self.requisicoes_feitas = 0
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
    
    def coletar_batch_alunos(self, ids_batch: List[int]) -> List[Dict]:
        """Verifica um batch de IDs de alunos e retorna os que são de Hortolândia"""
        for aluno_id in ids_batch:
            try:
                url = f"https://musical.congregacao.org.br/grp_musical/editar/{aluno_id}"
                
                resp = self.session.get(url, headers=self.headers, timeout=10)
                self.requisicoes_feitas += 1
                
                if resp.status_code == 200:
                    html = resp.text
                    
                    if 'igreja_selecionada' in html:
                        igreja_id = extrair_igreja_selecionada(html)
                        
                        if igreja_id and igreja_id in self.ids_igrejas:
                            nome_aluno = extrair_nome_aluno(html)
                            
                            aluno_data = {
                                'id_aluno': aluno_id,
                                'id_igreja': igreja_id,
                                'nome': nome_aluno
                            }
                            
                            self.alunos_encontrados.append(aluno_data)
                            print(f"✅ T{self.thread_id}: ID {aluno_id} | Igreja {igreja_id} | {nome_aluno[:35]}")
                
                time.sleep(0.08)
                
                if self.requisicoes_feitas % 500 == 0:
                    print(f"📊 T{self.thread_id}: {self.requisicoes_feitas:,} requisições | {len(self.alunos_encontrados)} alunos")
                
            except Exception as e:
                if "timeout" in str(e).lower():
                    print(f"⏱️ T{self.thread_id}: Timeout no ID {aluno_id}")
                continue
        
        return self.alunos_encontrados

def fase2_coleta_completa(session, chunks_com_alunos: List[Tuple[int, int]], 
                         ids_igrejas: Set[int]) -> List[Dict]:
    """
    FASE 2: Coleta completa e paralela nos chunks identificados
    """
    print(f"\n{'='*70}")
    print(f"🎓 FASE 2: COLETA COMPLETA E PARALELA")
    print(f"{'='*70}")
    
    if not chunks_com_alunos:
        print("⚠️ Nenhum chunk para coletar")
        return []
    
    # Mesclar chunks adjacentes para otimizar
    chunks_mesclados = []
    chunk_inicio_atual, chunk_fim_atual = chunks_com_alunos[0]
    
    for i in range(1, len(chunks_com_alunos)):
        proximo_inicio, proximo_fim = chunks_com_alunos[i]
        
        # Se os chunks são adjacentes ou muito próximos, mesclar
        if proximo_inicio <= chunk_fim_atual + TAMANHO_CHUNK_EXPLORACAO:
            chunk_fim_atual = proximo_fim
        else:
            chunks_mesclados.append((chunk_inicio_atual, chunk_fim_atual))
            chunk_inicio_atual, chunk_fim_atual = proximo_inicio, proximo_fim
    
    chunks_mesclados.append((chunk_inicio_atual, chunk_fim_atual))
    
    print(f"📦 {len(chunks_mesclados)} ranges otimizados para coleta:")
    total_ids = 0
    for inicio, fim in chunks_mesclados:
        ids_no_range = fim - inicio + 1
        total_ids += ids_no_range
        print(f"   • {inicio:,} - {fim:,} ({ids_no_range:,} IDs)")
    
    print(f"\n📈 Total de IDs a verificar: {total_ids:,}")
    print(f"🎉 Economia: {100 * (1 - total_ids/ID_MAXIMO):.1f}% do range total!")
    print(f"🧵 Usando {NUM_THREADS_COLETA} threads paralelas\n")
    
    todos_alunos = []
    
    for idx, (range_inicio, range_fim) in enumerate(chunks_mesclados, 1):
        print(f"\n{'─'*70}")
        print(f"📦 Coletando range {idx}/{len(chunks_mesclados)}: {range_inicio:,} - {range_fim:,}")
        print(f"{'─'*70}")
        
        alunos_range = executar_coleta_paralela(session, ids_igrejas, range_inicio, 
                                                range_fim, NUM_THREADS_COLETA)
        todos_alunos.extend(alunos_range)
        
        print(f"✅ Range {idx} concluído: {len(alunos_range)} alunos encontrados")
    
    return todos_alunos

def executar_coleta_paralela(session, ids_igrejas: Set[int], range_inicio: int, 
                            range_fim: int, num_threads: int) -> List[Dict]:
    """Executa coleta paralela em um range específico"""
    total_ids = range_fim - range_inicio + 1
    ids_per_thread = total_ids // num_threads
    
    thread_ranges = []
    for i in range(num_threads):
        inicio = range_inicio + (i * ids_per_thread)
        fim = inicio + ids_per_thread - 1
        
        if i == num_threads - 1:
            fim = range_fim
            
        thread_ranges.append(list(range(inicio, fim + 1)))
    
    todos_alunos = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        coletores = [ColetorAlunosHortolandia(session, i, ids_igrejas) for i in range(num_threads)]
        
        futures = []
        for i, ids_thread in enumerate(thread_ranges):
            future = executor.submit(coletores[i].coletar_batch_alunos, ids_thread)
            futures.append((future, i))
        
        for future, thread_id in futures:
            try:
                alunos_thread = future.result(timeout=3600)
                todos_alunos.extend(alunos_thread)
            except Exception as e:
                print(f"❌ Thread {thread_id}: Erro - {e}")
    
    return todos_alunos

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright para requests"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def salvar_alunos_em_arquivo(alunos: List[Dict], nome_arquivo: str = "alunos_hortolandia.json"):
    """Salva os dados dos alunos em arquivo JSON"""
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
    """Envia os dados dos alunos para Google Sheets via Apps Script"""
    if not alunos:
        print("⚠️ Nenhum aluno para enviar")
        return False
    
    print(f"\n📤 Enviando {len(alunos)} alunos para Google Sheets...")
    
    relatorio = [["ID_ALUNO", "ID_IGREJA", "NOME_ALUNO"]]
    
    for aluno in alunos:
        relatorio.append([
            str(aluno['id_aluno']),
            str(aluno['id_igreja']),
            aluno['nome']
        ])
    
    payload = {
        "tipo": "alunos_hortolandia",
        "relatorio_formatado": relatorio,
        "metadata": {
            "total_alunos": len(alunos),
            "total_igrejas_monitoradas": len(ids_igrejas),
            "range_total": f"{ID_MINIMO}-{ID_MAXIMO}",
            "tempo_execucao_min": round(tempo_execucao/60, 2),
            "threads_utilizadas": NUM_THREADS_COLETA,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "ids_igrejas": sorted(list(ids_igrejas))
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=180)
        
        if response.status_code == 200:
            print("✅ Dados enviados com sucesso para Google Sheets!")
            return True
        else:
            print(f"⚠️ Status HTTP: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
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
    
    # Criar sessão requests otimizada
    session = requests.Session()
    session.cookies.update(cookies_dict)
    
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=NUM_THREADS_COLETA + 5,
        pool_maxsize=NUM_THREADS_COLETA + 5,
        max_retries=2
    )
    session.mount('https://', adapter)
    
    # FASE 1: Exploração rápida
    chunks_com_alunos = fase1_exploracao_rapida(session, ids_igrejas)
    
    if not chunks_com_alunos:
        print("\n❌ Nenhum aluno de Hortolândia encontrado em todo o range!")
        return
    
    # FASE 2: Coleta completa
    alunos_hortolandia = fase2_coleta_completa(session, chunks_com_alunos, ids_igrejas)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'='*70}")
    print(f"🏁 PROCESSO FINALIZADO!")
    print(f"{'='*70}")
    print(f"🎓 Total de alunos encontrados: {len(alunos_hortolandia)}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    
    if alunos_hortolandia:
        print(f"\n📋 Primeiros 10 alunos:")
        for i, aluno in enumerate(sorted(alunos_hortolandia, key=lambda x: x['id_aluno'])[:10]):
            print(f"   {i+1}. ID {aluno['id_aluno']} | Igreja {aluno['id_igreja']} | {aluno['nome'][:50]}")
        
        if len(alunos_hortolandia) > 10:
            print(f"   ... e mais {len(alunos_hortolandia) - 10} alunos")
        
        # Estatísticas por igreja
        print(f"\n📊 Distribuição por igreja:")
        distribuicao = Counter([a['id_igreja'] for a in alunos_hortolandia])
        for igreja_id, qtd in distribuicao.most_common():
            print(f"   Igreja {igreja_id}: {qtd} alunos")
        
        # Salvar em arquivo
        salvar_alunos_em_arquivo(alunos_hortolandia)
        
        # Enviar para Google Sheets
        enviar_alunos_para_sheets(alunos_hortolandia, tempo_total, ids_igrejas)
    
    print(f"\n🎯 Processo finalizado com sucesso!")

if __name__ == "__main__":
    main()
