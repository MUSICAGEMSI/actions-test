from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from playwright.sync_api import sync_playwright
import os
import sys
import requests
import time
import json
import concurrent.futures
from typing import List, Set, Dict
import re

EMAIL = os.environ.get("LOGIN_MUSICAL")
SENHA = os.environ.get("SENHA_MUSICAL")
URL_INICIAL = "https://musical.congregacao.org.br/"
URL_LISTAGEM = "https://musical.congregacao.org.br/alunos/listagem"
URL_APPS_SCRIPT = 'https://script.google.com/macros/s/AKfycbzl1l143sg2_S5a6bOQy6WqWATMDZpSglIyKUp3OVZtycuHXQmGjisOpzffHTW5TvyK/exec'

NUM_THREADS = 25

print(f"🎓 COLETOR DE IDs - ALUNOS DE HORTOLÂNDIA (DA LISTAGEM)")

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

def extrair_ids_da_listagem(pagina) -> List[int]:
    """
    Extrai todos os IDs de alunos da página de listagem
    Procura por links do tipo: /alunos/editar/ID
    """
    ids_alunos = []
    
    try:
        # Aguardar carregamento da tabela
        pagina.wait_for_selector("table, .table, tbody", timeout=10000)
        
        # Extrair HTML da página
        html = pagina.content()
        
        # Procurar por padrão: /alunos/editar/NUMERO
        matches = re.findall(r'/alunos/editar/(\d+)', html)
        
        if matches:
            ids_alunos = [int(id_str) for id_str in matches]
            # Remover duplicatas mantendo a ordem
            ids_alunos = list(dict.fromkeys(ids_alunos))
            print(f"   📋 {len(ids_alunos)} IDs encontrados nesta página")
        
    except Exception as e:
        print(f"   ⚠️ Erro ao extrair IDs: {e}")
    
    return ids_alunos

def navegar_todas_paginas_listagem(pagina) -> List[int]:
    """
    Navega por todas as páginas da listagem e coleta todos os IDs de alunos
    """
    print("\n📄 Navegando pela listagem de alunos...")
    
    todos_ids = []
    pagina_atual = 1
    
    while True:
        print(f"   🔍 Página {pagina_atual}...")
        
        # Extrair IDs da página atual
        ids_pagina = extrair_ids_da_listagem(pagina)
        todos_ids.extend(ids_pagina)
        
        # Verificar se existe botão "Próxima" ou link para próxima página
        try:
            # Tentar diferentes seletores comuns para paginação
            proximo_seletores = [
                'a:has-text("Próxima")',
                'a:has-text("»")',
                'a:has-text("Next")',
                'a.next',
                'li.next a',
                'a[rel="next"]',
                f'a:has-text("{pagina_atual + 1}")'  # Link com número da próxima página
            ]
            
            botao_proximo = None
            for seletor in proximo_seletores:
                try:
                    elemento = pagina.locator(seletor).first
                    if elemento.is_visible(timeout=2000):
                        botao_proximo = elemento
                        break
                except:
                    continue
            
            if botao_proximo:
                # Clicar no botão de próxima página
                botao_proximo.click()
                time.sleep(1.5)  # Aguardar carregamento
                pagina_atual += 1
            else:
                print(f"   ✅ Última página alcançada (página {pagina_atual})")
                break
                
        except Exception as e:
            print(f"   ✅ Fim da paginação (página {pagina_atual})")
            break
    
    # Remover duplicatas
    todos_ids = list(dict.fromkeys(todos_ids))
    
    print(f"\n✅ Total de IDs únicos coletados: {len(todos_ids)}")
    return todos_ids

def extrair_igreja_selecionada(html_content: str) -> int:
    """
    Extrai o ID da igreja_selecionada do HTML
    """
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
    """
    Extrai o nome do aluno do HTML
    """
    if not html_content:
        return ""
    
    match = re.search(r'name="nome"[^>]*value="([^"]+)"', html_content)
    if match:
        return match.group(1).strip()
    
    return ""

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
        """
        Verifica um batch de IDs de alunos e retorna os que são de Hortolândia
        """
        for aluno_id in ids_batch:
            try:              
                url = f"https://musical.congregacao.org.br/alunos/editar/{aluno_id}"
                
                resp = self.session.get(url, headers=self.headers, timeout=10)
                self.requisicoes_feitas += 1
                
                if resp.status_code == 200:
                    html = resp.text
                    
                    if 'igreja_selecionada' in html:
                        igreja_id = extrair_igreja_selecionada(html)
                        
                        # Verificar se é de Hortolândia
                        if igreja_id and igreja_id in self.ids_igrejas:
                            nome_aluno = extrair_nome_aluno(html)
                            
                            aluno_data = {
                                'id_aluno': aluno_id,
                                'id_igreja': igreja_id,
                                'nome': nome_aluno
                            }
                            
                            self.alunos_encontrados.append(aluno_data)
                            print(f"✅ T{self.thread_id}: Aluno {aluno_id} | Igreja {igreja_id} | {nome_aluno[:40]}")
                
                time.sleep(0.05)
                
                if self.requisicoes_feitas % 100 == 0:
                    print(f"📊 T{self.thread_id}: {self.requisicoes_feitas} requisições | {len(self.alunos_encontrados)} alunos encontrados")
                
            except Exception as e:
                if "timeout" in str(e).lower():
                    print(f"⏱️ T{self.thread_id}: Timeout no ID {aluno_id}")
                continue
        
        return self.alunos_encontrados

def executar_coleta_paralela_alunos(session, ids_igrejas: Set[int], ids_alunos: List[int], num_threads: int) -> List[Dict]:
    """
    Executa coleta paralela de alunos de Hortolândia
    """
    total_ids = len(ids_alunos)
    ids_per_thread = (total_ids + num_threads - 1) // num_threads
    
    print(f"\n📈 Processando {total_ids:,} IDs em {num_threads} threads (~{ids_per_thread} IDs/thread)")
    
    # Dividir IDs em batches por thread
    thread_batches = []
    for i in range(num_threads):
        inicio = i * ids_per_thread
        fim = min(inicio + ids_per_thread, total_ids)
        
        if inicio < total_ids:
            thread_batches.append(ids_alunos[inicio:fim])
    
    todos_alunos = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        coletores = [ColetorAlunosHortolandia(session, i, ids_igrejas) for i in range(len(thread_batches))]
        
        futures = []
        for i, ids_batch in enumerate(thread_batches):
            future = executor.submit(coletores[i].coletar_batch_alunos, ids_batch)
            futures.append((future, i))
        
        for future, thread_id in futures:
            try:
                alunos_thread = future.result(timeout=3600)
                todos_alunos.extend(alunos_thread)
                coletor = coletores[thread_id]
                print(f"✅ Thread {thread_id}: {len(alunos_thread)} alunos | {coletor.requisicoes_feitas} requisições")
            except Exception as e:
                print(f"❌ Thread {thread_id}: Erro - {e}")
    
    return todos_alunos

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

def enviar_alunos_para_sheets(alunos: List[Dict], tempo_execucao: float, ids_igrejas: Set[int], total_ids_processados: int):
    """
    Envia os dados dos alunos para Google Sheets via Apps Script
    """
    if not alunos:
        print("⚠️ Nenhum aluno para enviar")
        return False
    
    print(f"\n📤 Enviando {len(alunos)} alunos para Google Sheets...")
    
    relatorio = [
        ["ID_ALUNO", "ID_IGREJA", "NOME_ALUNO"]
    ]
    
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
            "ids_processados": total_ids_processados,
            "tempo_execucao_min": round(tempo_execucao/60, 2),
            "threads_utilizadas": NUM_THREADS,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "ids_igrejas": sorted(list(ids_igrejas)),
            "fonte": "listagem_alunos"
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
            pagina.goto(URL_INICIAL)
            pagina.fill('input[name="login"]', EMAIL)
            pagina.fill('input[name="password"]', SENHA)
            pagina.click('button[type="submit"]')
            pagina.wait_for_selector("nav", timeout=15000)
            print("✅ Login realizado com sucesso!")
            
            # Navegar para página de listagem
            print(f"\n🔗 Acessando: {URL_LISTAGEM}")
            pagina.goto(URL_LISTAGEM)
            time.sleep(2)
            
            # Coletar todos os IDs da listagem
            ids_alunos = navegar_todas_paginas_listagem(pagina)
            
            if not ids_alunos:
                print("❌ Nenhum ID de aluno encontrado na listagem!")
                navegador.close()
                return
            
            # Extrair cookies para sessão requests
            cookies_dict = extrair_cookies_playwright(pagina)
            navegador.close()
            
        except Exception as e:
            print(f"❌ Erro: {e}")
            navegador.close()
            return
    
    # Criar sessão requests otimizada
    session = requests.Session()
    session.cookies.update(cookies_dict)
    
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=NUM_THREADS + 5,
        pool_maxsize=NUM_THREADS + 5,
        max_retries=2
    )
    session.mount('https://', adapter)
    
    print(f"\n🎓 Iniciando verificação de {len(ids_alunos)} alunos...")
    print(f"🏛️ Monitorando {len(ids_igrejas)} igrejas de Hortolândia")
    
    # Executar coleta paralela
    alunos_hortolandia = executar_coleta_paralela_alunos(session, ids_igrejas, ids_alunos, NUM_THREADS)
    
    tempo_total = time.time() - tempo_inicio
    
    print(f"\n{'='*60}")
    print(f"🏁 COLETA DE ALUNOS FINALIZADA!")
    print(f"{'='*60}")
    print(f"📋 IDs processados da listagem: {len(ids_alunos):,}")
    print(f"🎓 Alunos de Hortolândia encontrados: {len(alunos_hortolandia)}")
    print(f"⏱️ Tempo total: {tempo_total:.1f}s ({tempo_total/60:.1f} min)")
    
    if alunos_hortolandia:
        print(f"⚡ Velocidade: {len(ids_alunos)/tempo_total:.2f} IDs verificados/segundo")
        print(f"📊 Taxa de correspondência: {len(alunos_hortolandia)/len(ids_alunos)*100:.2f}%")
        
        print(f"\n📋 Primeiros 10 alunos encontrados:")
        for i, aluno in enumerate(alunos_hortolandia[:10]):
            print(f"   {i+1}. ID: {aluno['id_aluno']} | Igreja: {aluno['id_igreja']} | {aluno['nome'][:50]}")
        
        if len(alunos_hortolandia) > 10:
            print(f"   ... e mais {len(alunos_hortolandia) - 10} alunos")
        
        # Estatísticas por igreja
        print(f"\n📊 Distribuição por igreja:")
        from collections import Counter
        distribuicao = Counter([a['id_igreja'] for a in alunos_hortolandia])
        for igreja_id, qtd in distribuicao.most_common():
            print(f"   Igreja {igreja_id}: {qtd} alunos")
        
        # Salvar em arquivo
        salvar_alunos_em_arquivo(alunos_hortolandia)
        
        # Enviar para Google Sheets
        enviar_alunos_para_sheets(alunos_hortolandia, tempo_total, ids_igrejas, len(ids_alunos))
    
    else:
        print("⚠️ Nenhum aluno de Hortolândia foi encontrado na listagem")
    
    print(f"\n🎯 Processo finalizado!")

if __name__ == "__main__":
    main()
