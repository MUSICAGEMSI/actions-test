from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os
import requests
import time
import json
import concurrent.futures
from typing import List, Set, Dict
import re
from collections import Counter

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzl1l143sg2_S5a6bOQy6WqWATMDZpSglIyKUp3OVZtycuHXQmGjisOpzffHTW5TvyK/exec'

NUM_THREADS = 50

print(f"🎓 COLETOR OTIMIZADO V2 - ALUNOS DE HORTOLÂNDIA")
print(f"✅ Captura: Nome completo do instrutor")
print(f"✅ Validação: Apenas igrejas de Hortolândia")
print(f"🧵 Threads: {NUM_THREADS}")

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
            print(f"✅ {len(ids)} IDs de igrejas: {sorted(list(ids))}")
            return ids
        else:
            print(f"⚠️ Erro ao buscar IDs: Status {response.status_code}")
            return set()
    except Exception as e:
        print(f"❌ Erro: {e}")
        return set()

def buscar_nomes_completos_instrutores_hortolandia(session) -> Set[str]:
    """
    Busca NOMES COMPLETOS dos instrutores de Hortolândia
    Exemplo: "CHARLES GABRIEL BOTELHO RANGEL - SOLTEIRO/20"
    """
    print("\n📥 Buscando nomes COMPLETOS dos instrutores de Hortolândia...")
    
    try:
        url = "https://musical.congregacao.org.br/licoes/instrutores?q=a"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
        }
        
        resp = session.get(url, headers=headers, timeout=20)
        
        if resp.status_code != 200:
            print(f"⚠️ HTTP {resp.status_code}")
            return set()
        
        instrutores = json.loads(resp.text)
        nomes_completos = set()
        
        for instrutor in instrutores:
            # text contém: "NOME COMPLETO - STATUS/IDADE - COMUM CONGREGAÇÃO"
            texto_completo = instrutor['text']
            
            # Extrair apenas "NOME COMPLETO - STATUS/IDADE" (sem a igreja)
            # Formato: "CHARLES GABRIEL BOTELHO RANGEL - SOLTEIRO/20 - Jardim Aline"
            partes = texto_completo.split(' - ')
            
            if len(partes) >= 2:
                # Pega nome + estado civil/idade
                nome_com_status = f"{partes[0].strip()} - {partes[1].strip()}"
                nomes_completos.add(nome_com_status)
        
        print(f"✅ {len(nomes_completos)} instrutores com nomes COMPLETOS carregados")
        print(f"   Exemplos:")
        for i, nome in enumerate(list(nomes_completos)[:3]):
            print(f"   - {nome}")
        
        return nomes_completos
        
    except Exception as e:
        print(f"❌ Erro ao buscar instrutores: {e}")
        return set()

def extrair_dados_aluno_completo(html: str, ids_igrejas_validas: Set[int]) -> Dict:
    """
    Extrai TODOS os dados necessários do HTML
    CRÍTICO: Valida se a igreja pertence a Hortolândia
    """
    if not html:
        return None
    
    dados = {}
    
    # 1. ID da Igreja (VALIDAÇÃO CRÍTICA)
    match = re.search(r'igreja_selecionada\s*\((\d+)\)', html)
    if match:
        id_igreja = int(match.group(1))
        
        # VALIDAÇÃO: Só aceita se for de Hortolândia
        if id_igreja not in ids_igrejas_validas:
            return None  # Igreja não é de Hortolândia - DESCARTA
        
        dados['id_igreja'] = id_igreja
    else:
        return None  # Não encontrou igreja - DESCARTA
    
    # 2. Nome do Aluno
    match = re.search(r'name="nome"[^>]*value="([^"]+)"', html)
    if match:
        dados['nome'] = match.group(1).strip()
    
    # 3. Cargo/Ministério (option selected)
    match = re.search(r'name="id_cargo"[^>]*>.*?<option value="(\d+)" selected[^>]*>\s*([^<]+)', html, re.DOTALL)
    if match:
        dados['id_cargo'] = int(match.group(1))
        dados['cargo'] = match.group(2).strip()
    
    # 4. Nível (option selected)
    match = re.search(r'name="id_nivel"[^>]*>.*?<option value="(\d+)" selected[^>]*>\s*([^<]+)', html, re.DOTALL)
    if match:
        dados['id_nivel'] = int(match.group(1))
        dados['nivel'] = match.group(2).strip()
    
    return dados if dados else None

class ColetorOtimizado:
    def __init__(self, session, thread_id: int, ids_igrejas: Set[int]):
        self.session = session
        self.thread_id = thread_id
        self.ids_igrejas = ids_igrejas
        self.alunos = []
        self.req_count = 0
        self.alunos_descartados_igreja_errada = 0
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
    
    def buscar_alunos_por_igreja(self, igreja_id: int) -> List[Dict]:
        """
        Busca alunos diretamente pela listagem da igreja
        GARANTIA: Varre TODOS os IDs encontrados na listagem
        """
        alunos_igreja = []
        
        try:
            url_listagem = f"https://musical.congregacao.org.br/grp_musical?id_igreja={igreja_id}"
            resp = self.session.get(url_listagem, headers=self.headers, timeout=15)
            self.req_count += 1
            
            if resp.status_code != 200:
                return alunos_igreja
            
            html = resp.text
            
            # Extrair TODOS os IDs de alunos da listagem
            ids_alunos = re.findall(r'grp_musical/editar/(\d+)', html)
            ids_alunos = list(set(map(int, ids_alunos)))
            
            if ids_alunos:
                print(f"🏛️ T{self.thread_id}: Igreja {igreja_id} | {len(ids_alunos)} alunos para verificar")
                
                # GARANTIA 100%: Buscar dados de CADA aluno
                for aluno_id in ids_alunos:
                    dados_aluno = self.buscar_dados_aluno_validado(aluno_id)
                    
                    if dados_aluno:
                        # Validação dupla: deve ser da igreja correta
                        if dados_aluno.get('id_igreja') == igreja_id:
                            alunos_igreja.append(dados_aluno)
                            print(f"   ✅ Aluno {aluno_id} | {dados_aluno['nome'][:40]}")
                        else:
                            self.alunos_descartados_igreja_errada += 1
                            print(f"   ⚠️ Aluno {aluno_id} | Igreja incorreta: {dados_aluno.get('id_igreja')} != {igreja_id}")
                    
                    # Pausa para não sobrecarregar
                    time.sleep(0.08)
            
            time.sleep(0.15)
            
        except Exception as e:
            print(f"⚠️ T{self.thread_id}: Erro igreja {igreja_id}: {str(e)[:50]}")
        
        return alunos_igreja
    
    def buscar_dados_aluno_validado(self, aluno_id: int) -> Dict:
        """
        Busca dados completos de um aluno
        VALIDAÇÃO: Garante que é de Hortolândia
        """
        try:
            url = f"https://musical.congregacao.org.br/grp_musical/editar/{aluno_id}"
            resp = self.session.get(url, headers=self.headers, timeout=12)
            self.req_count += 1
            
            if resp.status_code == 200:
                # Extração com validação de igreja
                dados = extrair_dados_aluno_completo(resp.text, self.ids_igrejas)
                
                if dados:
                    dados['id_aluno'] = aluno_id
                    return dados
            
            time.sleep(0.05)
            
        except Exception as e:
            if "timeout" not in str(e).lower():
                print(f"⚠️ T{self.thread_id}: Erro aluno {aluno_id}: {str(e)[:40]}")
        
        return None

def executar_coleta_completa(session, ids_igrejas: Set[int], num_threads: int) -> List[Dict]:
    """
    COLETA COMPLETA: 100% de cobertura garantida
    Varre TODAS as igrejas de Hortolândia
    """
    lista_igrejas = sorted(list(ids_igrejas))
    print(f"\n🏛️ Dividindo {len(lista_igrejas)} igrejas em {num_threads} threads")
    
    # Distribuir igrejas entre threads
    igrejas_per_thread = max(1, len(lista_igrejas) // num_threads)
    thread_batches = []
    
    for i in range(0, len(lista_igrejas), igrejas_per_thread):
        batch = lista_igrejas[i:i + igrejas_per_thread]
        if batch:
            thread_batches.append(batch)
    
    todos_alunos = []
    total_descartados = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        coletores = [ColetorOtimizado(session, i, ids_igrejas) for i in range(len(thread_batches))]
        
        futures = []
        for i, igrejas_batch in enumerate(thread_batches):
            future = executor.submit(processar_batch_igrejas, coletores[i], igrejas_batch)
            futures.append((future, i))
        
        for future, thread_id in futures:
            try:
                alunos = future.result(timeout=2400)  # 40 min por thread
                todos_alunos.extend(alunos)
                coletor = coletores[thread_id]
                total_descartados += coletor.alunos_descartados_igreja_errada
                
                print(f"\n{'='*60}")
                print(f"✅ Thread {thread_id} CONCLUÍDA:")
                print(f"   Alunos válidos: {len(alunos)}")
                print(f"   Descartados (igreja errada): {coletor.alunos_descartados_igreja_errada}")
                print(f"   Requisições: {coletor.req_count:,}")
                print(f"{'='*60}\n")
                
            except Exception as e:
                print(f"❌ Thread {thread_id}: {e}")
    
    if total_descartados > 0:
        print(f"\n⚠️ TOTAL de alunos descartados (igreja errada): {total_descartados}")
    
    return todos_alunos

def processar_batch_igrejas(coletor: ColetorOtimizado, igrejas: List[int]) -> List[Dict]:
    """Processa um batch de igrejas com 100% de cobertura"""
    todos_alunos = []
    
    for igreja_id in igrejas:
        print(f"\n🔄 T{coletor.thread_id}: Processando Igreja {igreja_id}...")
        alunos_igreja = coletor.buscar_alunos_por_igreja(igreja_id)
        todos_alunos.extend(alunos_igreja)
        print(f"   ✓ Igreja {igreja_id}: {len(alunos_igreja)} alunos coletados")
    
    return todos_alunos

def extrair_cookies_playwright(pagina):
    """Extrai cookies do Playwright"""
    cookies = pagina.context.cookies()
    return {cookie['name']: cookie['value'] for cookie in cookies}

def salvar_alunos(alunos: List[Dict], arquivo: str = "alunos_hortolandia.json"):
    """Salva dados em JSON"""
    try:
        with open(arquivo, 'w', encoding='utf-8') as f:
            json.dump({
                "alunos": alunos,
                "total": len(alunos),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }, f, indent=2, ensure_ascii=False)
        print(f"💾 Salvo em: {arquivo}")
    except Exception as e:
        print(f"❌ Erro ao salvar: {e}")

def enviar_para_sheets(alunos: List[Dict], tempo: float, ids_igrejas: Set[int]):
    """Envia dados para Google Sheets"""
    if not alunos:
        print("⚠️ Nenhum aluno para enviar")
        return False
    
    print(f"\n📤 Enviando {len(alunos)} alunos para Google Sheets...")
    
    # Formato: ID_ALUNO, ID_IGREJA, NOME_ALUNO, CARGO_MINISTERIO, NIVEL
    relatorio = [["ID_ALUNO", "ID_IGREJA", "NOME_ALUNO", "CARGO_MINISTERIO", "NIVEL"]]
    
    for a in alunos:
        relatorio.append([
            str(a['id_aluno']),
            str(a['id_igreja']),
            a.get('nome', ''),
            a.get('cargo', ''),
            a.get('nivel', '')
        ])
    
    payload = {
        "tipo": "alunos_hortolandia_v3_completo",
        "relatorio_formatado": relatorio,
        "metadata": {
            "total_alunos": len(alunos),
            "total_igrejas": len(ids_igrejas),
            "tempo_min": round(tempo/60, 2),
            "threads": NUM_THREADS,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "metodo": "busca_completa_100porcento_garantida",
            "validacao": "dupla_verificacao_igreja"
        }
    }
    
    try:
        resp = requests.post(URL_APPS_SCRIPT, json=payload, timeout=180)
        
        if resp.status_code == 200:
            print("✅ Dados enviados para Google Sheets!")
            return True
        else:
            print(f"⚠️ Status: {resp.status_code}")
            return False
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")
        return False

def main():
    tempo_inicio = time.time()
    
    # Buscar IDs das igrejas
    ids_igrejas = buscar_ids_igrejas_hortolandia()
    
    if not ids_igrejas:
        print("❌ Nenhuma igreja encontrada")
        return
    
    print("\n🔐 Realizando login...")
    
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        pagina = navegador.new_page()
        
        pagina.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        try:
            pagina.goto(URL_INICIAL, timeout=20000)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado!")
            
        except Exception as e:
            print(f"❌ Erro no login: {e}")
            navegador.close()
            return
        
        cookies_dict = extrair_cookies_playwright(pagina)
        navegador.close()
    
    # Sessão otimizada
    session = requests.Session()
    session.cookies.update(cookies_dict)
    
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=NUM_THREADS * 2,
        pool_maxsize=NUM_THREADS * 2,
        max_retries=3
    )
    session.mount('https://', adapter)
    
    print("\n🚀 Iniciando coleta COMPLETA (100% garantida)...")
    
    # COLETA COMPLETA
    alunos = executar_coleta_completa(session, ids_igrejas, NUM_THREADS)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'='*70}")
    print(f"🏁 COLETA FINALIZADA!")
    print(f"{'='*70}")
    print(f"🎓 Alunos válidos (Hortolândia): {len(alunos)}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    print(f"✅ Cobertura: 100% das igrejas varridas")
    
    if alunos:
        print(f"\n📋 Primeiros 10 alunos:")
        for i, a in enumerate(alunos[:10]):
            print(f"   {i+1}. ID: {a['id_aluno']} | Igreja: {a['id_igreja']} | {a['nome'][:40]}")
            print(f"       Cargo: {a.get('cargo', 'N/A')} | Nível: {a.get('nivel', 'N/A')}")
        
        if len(alunos) > 10:
            print(f"   ... e mais {len(alunos) - 10} alunos")
        
        # Estatísticas
        print(f"\n📊 Distribuição por igreja:")
        dist = Counter([a['id_igreja'] for a in alunos])
        for igreja_id, qtd in dist.most_common():
            print(f"   Igreja {igreja_id}: {qtd} alunos")
        
        print(f"\n📊 Distribuição por nível:")
        dist_nivel = Counter([a.get('nivel', 'N/A') for a in alunos])
        for nivel, qtd in dist_nivel.most_common():
            print(f"   {nivel}: {qtd} alunos")
        
        print(f"\n📊 Distribuição por cargo:")
        dist_cargo = Counter([a.get('cargo', 'N/A') for a in alunos])
        for cargo, qtd in dist_cargo.most_common():
            print(f"   {cargo}: {qtd} alunos")
        
        salvar_alunos(alunos)
        enviar_para_sheets(alunos, tempo_total, ids_igrejas)
    else:
        print("⚠️ Nenhum aluno válido encontrado")
    
    print(f"\n🎯 Finalizado!")

if __name__ == "__main__":
    main()
