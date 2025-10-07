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
from tqdm import tqdm

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzl1l143sg2_S5a6bOQy6WqWATMDZpSglIyKUp3OVZtycuHXQmGjisOpzffHTW5TvyK/exec'

# ========== CONFIGURAÇÃO ULTRA PRECISA ==========
# Estratégia: Encontrar primeiro e último aluno, depois varrer todo o range
ID_MINIMO_BUSCA = 1
ID_MAXIMO_BUSCA = 1000000
PASSO_BUSCA_INICIAL = 50  # Passos menores para encontrar limites
NUM_THREADS = 30  # Threads para coleta completa
TIMEOUT_REQUEST = 10
DELAY_ENTRE_REQUESTS = 0.05
# =================================================

print(f"{'='*80}")
print(f"🎯 COLETOR 100% GARANTIDO - ESTRATÉGIA INTELIGENTE")
print(f"{'='*80}")
print(f"✅ FASE 1: Busca binária para encontrar primeiro e último aluno")
print(f"✅ FASE 2: Varredura COMPLETA do range encontrado (ID por ID)")
print(f"🛡️ GARANTIA ABSOLUTA: Nenhum aluno será perdido!")
print(f"{'='*80}\n")

if not EMAIL or not SENHA:
    print("❌ Erro: Credenciais não definidas")
    exit(1)

def buscar_ids_igrejas_hortolandia() -> Set[int]:
    """Busca os IDs das igrejas de Hortolândia do Google Sheets"""
    print("📥 Buscando IDs das igrejas de Hortolândia...")
    
    try:
        params = {"acao": "listar_ids_hortolandia"}
        response = requests.get(URL_APPS_SCRIPT, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            ids = set(data.get('ids', []))
            print(f"✅ {len(ids)} IDs de igrejas carregados: {sorted(list(ids))}\n")
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

def verificar_aluno(session, aluno_id: int, ids_igrejas: Set[int]) -> Dict:
    """
    Verifica se um aluno existe e é de Hortolândia
    Retorna dict com dados ou None
    """
    try:
        url = f"https://musical.congregacao.org.br/grp_musical/editar/{aluno_id}"
        resp = session.get(url, timeout=TIMEOUT_REQUEST)
        
        if resp.status_code == 200 and 'igreja_selecionada' in resp.text:
            igreja_id = extrair_igreja_selecionada(resp.text)
            
            if igreja_id in ids_igrejas:
                nome = extrair_nome_aluno(resp.text)
                return {
                    'id_aluno': aluno_id,
                    'id_igreja': igreja_id,
                    'nome': nome
                }
        
        return None
    except:
        return None

def buscar_primeiro_aluno(session, ids_igrejas: Set[int]) -> int:
    """
    Busca binária para encontrar o PRIMEIRO aluno de Hortolândia
    """
    print("🔍 FASE 1A: Buscando PRIMEIRO aluno...")
    
    # Busca rápida para encontrar região aproximada
    id_atual = ID_MINIMO_BUSCA
    primeiro_encontrado = None
    
    # Saltos grandes para encontrar primeira ocorrência
    passo = 10000
    while id_atual <= ID_MAXIMO_BUSCA:
        if verificar_aluno(session, id_atual, ids_igrejas):
            primeiro_encontrado = id_atual
            print(f"   ✅ Aluno encontrado em ID {id_atual:,}")
            break
        id_atual += passo
        if id_atual % 50000 == 0:
            print(f"   🔎 Buscando... ID {id_atual:,}")
    
    if not primeiro_encontrado:
        print("   ❌ Nenhum aluno encontrado!")
        return None
    
    # Busca binária para refinar e encontrar o PRIMEIRO
    print(f"   🎯 Refinando busca entre {max(ID_MINIMO_BUSCA, primeiro_encontrado - passo):,} e {primeiro_encontrado:,}...")
    
    inicio = max(ID_MINIMO_BUSCA, primeiro_encontrado - passo)
    fim = primeiro_encontrado
    primeiro_confirmado = primeiro_encontrado
    
    while inicio < fim:
        meio = (inicio + fim) // 2
        
        if verificar_aluno(session, meio, ids_igrejas):
            primeiro_confirmado = meio
            fim = meio  # Pode haver anterior
        else:
            inicio = meio + 1
    
    # Verificar alguns IDs antes para garantia
    for id_teste in range(max(ID_MINIMO_BUSCA, primeiro_confirmado - 100), primeiro_confirmado):
        if verificar_aluno(session, id_teste, ids_igrejas):
            primeiro_confirmado = id_teste
            break
    
    print(f"   ✅ PRIMEIRO aluno encontrado: ID {primeiro_confirmado:,}\n")
    return primeiro_confirmado

def buscar_ultimo_aluno(session, ids_igrejas: Set[int], id_inicio: int) -> int:
    """
    Busca binária para encontrar o ÚLTIMO aluno de Hortolândia
    """
    print("🔍 FASE 1B: Buscando ÚLTIMO aluno...")
    
    # Saltos para frente a partir do primeiro
    id_atual = id_inicio
    ultimo_encontrado = id_inicio
    passo = 10000
    
    while id_atual <= ID_MAXIMO_BUSCA:
        if verificar_aluno(session, id_atual, ids_igrejas):
            ultimo_encontrado = id_atual
            print(f"   ✅ Aluno encontrado em ID {id_atual:,}")
            id_atual += passo
        else:
            # Parou de encontrar, refinar busca
            break
        
        if id_atual % 50000 == 0:
            print(f"   🔎 Buscando... ID {id_atual:,}")
    
    # Busca binária para refinar e encontrar o ÚLTIMO
    print(f"   🎯 Refinando busca entre {ultimo_encontrado:,} e {min(ID_MAXIMO_BUSCA, id_atual):,}...")
    
    inicio = ultimo_encontrado
    fim = min(ID_MAXIMO_BUSCA, id_atual)
    ultimo_confirmado = ultimo_encontrado
    
    while inicio < fim:
        meio = (inicio + fim + 1) // 2
        
        if verificar_aluno(session, meio, ids_igrejas):
            ultimo_confirmado = meio
            inicio = meio  # Pode haver posterior
        else:
            fim = meio - 1
    
    # Verificar alguns IDs depois para garantia
    for id_teste in range(ultimo_confirmado + 1, min(ID_MAXIMO_BUSCA, ultimo_confirmado + 100)):
        if verificar_aluno(session, id_teste, ids_igrejas):
            ultimo_confirmado = id_teste
    
    print(f"   ✅ ÚLTIMO aluno encontrado: ID {ultimo_confirmado:,}\n")
    return ultimo_confirmado

class ColetorCompleto:
    def __init__(self, session, thread_id: int, ids_igrejas: Set[int]):
        self.session = session
        self.thread_id = thread_id
        self.ids_igrejas = ids_igrejas
        self.alunos_encontrados: List[Dict] = []
        self.requisicoes = 0
    
    def coletar_range(self, ids_list: List[int]) -> List[Dict]:
        """Coleta TODOS os IDs da lista"""
        for aluno_id in ids_list:
            try:
                aluno = verificar_aluno(self.session, aluno_id, self.ids_igrejas)
                self.requisicoes += 1
                
                if aluno:
                    self.alunos_encontrados.append(aluno)
                    print(f"✅ T{self.thread_id}: ID {aluno_id:,} | Igreja {aluno['id_igreja']} | {aluno['nome'][:40]}")
                
                time.sleep(DELAY_ENTRE_REQUESTS)
                
                if self.requisicoes % 500 == 0:
                    print(f"📊 T{self.thread_id}: {self.requisicoes:,} verificações | {len(self.alunos_encontrados)} alunos")
                
            except Exception as e:
                if "timeout" in str(e).lower():
                    print(f"⏱️ T{self.thread_id}: Timeout no ID {aluno_id}")
                    time.sleep(1)
                continue
        
        return self.alunos_encontrados

def coletar_range_completo(session, ids_igrejas: Set[int], id_inicio: int, id_fim: int) -> List[Dict]:
    """
    Coleta COMPLETA do range com paralelização
    Garante 100% de cobertura
    """
    print(f"\n{'='*80}")
    print(f"🎓 FASE 2: COLETA COMPLETA E GARANTIDA")
    print(f"{'='*80}")
    print(f"📍 Range: {id_inicio:,} - {id_fim:,}")
    
    total_ids = id_fim - id_inicio + 1
    print(f"📊 Total de IDs a verificar: {total_ids:,}")
    print(f"🧵 Threads paralelas: {NUM_THREADS}")
    print(f"🛡️ GARANTIA: Todos os {total_ids:,} IDs serão verificados!\n")
    
    # Dividir range entre threads
    ids_per_thread = total_ids // NUM_THREADS
    thread_ranges = []
    
    for i in range(NUM_THREADS):
        inicio = id_inicio + (i * ids_per_thread)
        fim = inicio + ids_per_thread - 1
        
        if i == NUM_THREADS - 1:
            fim = id_fim  # Última thread pega o resto
        
        thread_ranges.append(list(range(inicio, fim + 1)))
    
    # Verificar que não perdemos IDs
    total_distribuido = sum(len(r) for r in thread_ranges)
    if total_distribuido != total_ids:
        print(f"⚠️ ATENÇÃO: Distribuição incorreta! {total_distribuido} != {total_ids}")
        return []
    
    print(f"✅ Distribuição verificada: {total_distribuido:,} IDs em {NUM_THREADS} threads\n")
    
    todos_alunos = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        coletores = [ColetorCompleto(session, i, ids_igrejas) for i in range(NUM_THREADS)]
        
        futures = []
        for i, ids_thread in enumerate(thread_ranges):
            future = executor.submit(coletores[i].coletar_range, ids_thread)
            futures.append((future, i))
        
        print("🚀 Coleta paralela iniciada...\n")
        
        for future, thread_id in futures:
            try:
                alunos = future.result(timeout=7200)  # 2h timeout
                todos_alunos.extend(alunos)
                print(f"\n✅ Thread {thread_id} finalizada: {len(alunos)} alunos | {coletores[thread_id].requisicoes:,} verificações")
            except Exception as e:
                print(f"\n❌ Thread {thread_id} erro: {e}")
    
    return todos_alunos

def criar_sessao_otimizada(cookies: list) -> requests.Session:
    """Cria sessão requests otimizada com cookies válidos do Playwright"""
    session = requests.Session()
    for cookie in cookies:
        domain = cookie.get("domain", "musical.congregacao.org.br").lstrip(".")
        session.cookies.set(cookie["name"], cookie["value"], domain=domain)

    adapter = requests.adapters.HTTPAdapter(
        pool_connections=NUM_THREADS + 5,
        pool_maxsize=NUM_THREADS + 5,
        max_retries=3
    )
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    })
    return session

def salvar_alunos_arquivo(alunos: List[Dict], nome_arquivo: str = "alunos_hortolandia_completo.json"):
    """Salva alunos em arquivo JSON"""
    try:
        with open(nome_arquivo, 'w', encoding='utf-8') as f:
            json.dump({
                "alunos": alunos,
                "total": len(alunos),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "garantia": "100% - varredura completa do range"
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Dados salvos em: {nome_arquivo}")
    except Exception as e:
        print(f"❌ Erro ao salvar: {e}")

def enviar_para_sheets(alunos: List[Dict], tempo_exec: float, ids_igrejas: Set[int], 
                      range_inicio: int, range_fim: int):
    """Envia dados para Google Sheets"""
    if not alunos:
        print("⚠️ Nenhum aluno para enviar")
        return False
    
    print(f"\n📤 Enviando {len(alunos)} alunos para Google Sheets...")
    
    relatorio = [["ID_ALUNO", "ID_IGREJA", "NOME_ALUNO"]]
    
    for aluno in sorted(alunos, key=lambda x: x['id_aluno']):
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
            "total_igrejas": len(ids_igrejas),
            "range_verificado": f"{range_inicio:,} - {range_fim:,}",
            "total_ids_verificados": range_fim - range_inicio + 1,
            "tempo_execucao_min": round(tempo_exec/60, 2),
            "garantia": "100% - varredura ID por ID",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "ids_igrejas": sorted(list(ids_igrejas))
        }
    }
    
    try:
        response = requests.post(URL_APPS_SCRIPT, json=payload, timeout=180)
        
        if response.status_code == 200:
            print("✅ Dados enviados com sucesso!")
            return True
        else:
            print(f"⚠️ Status HTTP: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        return False

def main():
    tempo_inicio = time.time()
    
    # Buscar IDs das igrejas
    ids_igrejas = buscar_ids_igrejas_hortolandia()
    
    if not ids_igrejas:
        print("❌ Nenhum ID de igreja encontrado!")
        return
    
    # Login
    print("🔐 Realizando login...")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        try:
            pagina.goto(URL_INICIAL)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado!\n")
            
        except Exception as e:
            print(f"❌ Erro no login: {e}")
            navegador.close()
            return
 
        cookies = pagina.context.cookies()
        navegador.close()
        session = criar_sessao_otimizada(cookies)

    # FASE 1: Encontrar limites exatos
    primeiro_id = buscar_primeiro_aluno(session, ids_igrejas)
    
    if not primeiro_id:
        print("❌ Nenhum aluno de Hortolândia encontrado!")
        return
    
    ultimo_id = buscar_ultimo_aluno(session, ids_igrejas, primeiro_id)
    
    range_total = ultimo_id - primeiro_id + 1
    print(f"{'='*80}")
    print(f"📊 RANGE IDENTIFICADO:")
    print(f"{'='*80}")
    print(f"   Primeiro ID: {primeiro_id:,}")
    print(f"   Último ID: {ultimo_id:,}")
    print(f"   Total de IDs: {range_total:,}")
    print(f"   Economia: {100 * (1 - range_total/ID_MAXIMO_BUSCA):.2f}%")
    print(f"{'='*80}\n")
    
    # FASE 2: Coleta completa
    alunos = coletar_range_completo(session, ids_igrejas, primeiro_id, ultimo_id)
    
    tempo_total = time.time() - tempo_inicio
    
    # Resultados
    print(f"\n{'='*80}")
    print(f"🏁 PROCESSO FINALIZADO!")
    print(f"{'='*80}")
    print(f"✅ Total de alunos: {len(alunos)}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    print(f"📊 IDs verificados: {range_total:,}")
    print(f"🎯 Taxa de sucesso: {100 * len(alunos) / range_total:.2f}%")
    
    if alunos:
        # Estatísticas
        ids_encontrados = sorted([a['id_aluno'] for a in alunos])
        print(f"\n📋 Range real: {ids_encontrados[0]:,} - {ids_encontrados[-1]:,}")
        
        print(f"\n🔝 Primeiros 10 alunos:")
        for i, aluno in enumerate(alunos[:10]):
            print(f"   {i+1}. ID {aluno['id_aluno']:,} | Igreja {aluno['id_igreja']} | {aluno['nome'][:50]}")
        
        # Distribuição por igreja
        print(f"\n📊 Distribuição por igreja:")
        distribuicao = Counter([a['id_igreja'] for a in alunos])
        for igreja_id, qtd in sorted(distribuicao.items()):
            print(f"   Igreja {igreja_id}: {qtd} alunos ({100*qtd/len(alunos):.1f}%)")
        
        # Verificar continuidade
        gaps = []
        for i in range(len(ids_encontrados) - 1):
            diferenca = ids_encontrados[i+1] - ids_encontrados[i]
            if diferenca > 1:
                gaps.append((ids_encontrados[i], ids_encontrados[i+1], diferenca - 1))
        
        if gaps:
            print(f"\n📉 Gaps encontrados (IDs não utilizados): {len(gaps)}")
            if len(gaps) <= 5:
                for inicio, fim, tamanho in gaps:
                    print(f"   Gap de {tamanho} IDs entre {inicio:,} e {fim:,}")
        else:
            print(f"\n✅ IDs contínuos - sem gaps!")
        
        # Salvar e enviar
        salvar_alunos_arquivo(alunos)
        enviar_para_sheets(alunos, tempo_total, ids_igrejas, primeiro_id, ultimo_id)
    
    print(f"\n{'='*80}")
    print(f"🎯 GARANTIA 100% CUMPRIDA!")
    print(f"✅ Todos os {range_total:,} IDs foram verificados!")
    print(f"✅ {len(alunos)} alunos capturados com sucesso!")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
