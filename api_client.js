/**
 * MULTIPLICA SAM - API CLIENT
 * Cliente JavaScript para consumir dados da API backend
 */

const API_BASE_URL = 'http://localhost:5000/api';  // Alterar para URL de produção quando deployar

class SAM_API {
    /**
     * Faz requisição GET para a API
     */
    static async get(endpoint) {
        try {
            const response = await fetch(`${API_BASE_URL}${endpoint}`);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            const data = await response.json();
            return data;
        } catch (error) {
            console.error(`Erro ao buscar ${endpoint}:`, error);
            throw error;
        }
    }

    /**
     * Retorna todas as localidades com estatísticas
     */
    static async getLocalidades() {
        return await this.get('/localidades');
    }

    /**
     * Retorna dados detalhados de uma localidade específica
     */
    static async getLocalidade(id_igreja) {
        return await this.get(`/localidade/${id_igreja}`);
    }

    /**
     * Retorna dados completos de um aluno
     */
    static async getAluno(id_aluno) {
        return await this.get(`/aluno/${id_aluno}`);
    }

    /**
     * Retorna resumo de todos os alunos (opcionalmente filtrado por igreja)
     */
    static async getResumoAlunos(id_igreja = null) {
        const endpoint = id_igreja 
            ? `/resumo-alunos?id_igreja=${id_igreja}`
            : '/resumo-alunos';
        return await this.get(endpoint);
    }

    /**
     * Retorna logs de execução do scraping
     */
    static async getLogsScraping(limit = 50) {
        return await this.get(`/logs-scraping?limit=${limit}`);
    }

    /**
     * Retorna estatísticas gerais do sistema
     */
    static async getEstatisticasGerais() {
        return await this.get('/estatisticas/geral');
    }

    /**
     * Gera e baixa PDF de uma localidade
     */
    static async downloadPDFLocalidade(id_igreja, nomeLocalidade) {
        try {
            const response = await fetch(`${API_BASE_URL}/pdf/localidade/${id_igreja}`);
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            // Criar blob do PDF
            const blob = await response.blob();
            
            // Criar link temporário para download
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `relatorio_${nomeLocalidade}_${new Date().toISOString().split('T')[0]}.pdf`;
            document.body.appendChild(a);
            a.click();
            
            // Limpar
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
            
            return true;
        } catch (error) {
            console.error('Erro ao baixar PDF:', error);
            throw error;
        }
    }
}

/**
 * Atualiza a interface do index.html com dados reais do banco
 */
async function carregarLocalidadesReais() {
    try {
        console.log('📡 Carregando localidades do banco de dados...');
        
        const response = await SAM_API.getLocalidades();
        
        if (response.success && response.data) {
            const localidades = response.data;
            console.log(`✅ ${localidades.length} localidades carregadas`);
            
            // Atualizar array global de localidades
            window.localidadesDB = localidades.map(loc => ({
                nome: loc.nome_localidade.length > 20 
                    ? loc.nome_localidade.substring(0, 18) + '...'
                    : loc.nome_localidade,
                nomeCompleto: loc.nome_localidade,
                codigo: loc.codigo_localidade,
                id_igreja: loc.id_igreja,
                setor: loc.setor,
                cidade: loc.cidade,
                total_alunos: loc.total_alunos || 0,
                alunos_ativos: loc.alunos_ativos || 0,
                total_instrumentos: loc.total_instrumentos || 0,
                total_mts: loc.total_mts || 0,
                total_msa: loc.total_msa || 0,
                total_provas: loc.total_provas || 0,
                media_geral: loc.media_geral_provas || 0,
                // Manter coordenadas e imagens do array original se existirem
                x: 600, y: 245, // Valores padrão - idealmente viriam do banco
                img: loc.codigo_localidade.toLowerCase().replace(/[^a-z0-9]/g, '-')
            }));
            
            // Renderizar localidades na interface
            renderizarLocalidadesReais();
            
            return window.localidadesDB;
        } else {
            console.error('❌ Erro ao carregar localidades:', response.error);
            return [];
        }
    } catch (error) {
        console.error('❌ Erro fatal ao carregar localidades:', error);
        return [];
    }
}

/**
 * Renderiza localidades na interface com dados reais
 */
function renderizarLocalidadesReais() {
    if (!window.localidadesDB || window.localidadesDB.length === 0) {
        console.warn('⚠️ Nenhuma localidade para renderizar');
        return;
    }
    
    const grid = document.getElementById('locationsGrid');
    if (!grid) {
        console.warn('⚠️ Grid de localidades não encontrado');
        return;
    }
    
    grid.innerHTML = window.localidadesDB.map((loc, index) => `
        <div class="location-card" onclick='selecionarLocalidadeReal(${index})' title="${loc.nomeCompleto}">
            <div class="location-icon-wrapper">
                <img src="assets/fotos/${loc.img}.png" alt="${loc.nomeCompleto}" class="location-icon" onerror="this.style.display='none'">
            </div>
            <div class="location-info">
                <div class="location-name">${loc.nome}</div>
                <div class="location-code">${loc.codigo}</div>
            </div>
        </div>
    `).join('');
    
    console.log(`✅ ${window.localidadesDB.length} localidades renderizadas`);
}

/**
 * Atualiza estatísticas gerais na hero section
 */
async function atualizarEstatisticasGerais() {
    try {
        const response = await SAM_API.getEstatisticasGerais();
        
        if (response.success && response.estatisticas) {
            const stats = response.estatisticas;
            
            // Atualizar valores na interface (se os elementos existirem)
            const statElements = {
                'stat-turmas': stats.total_localidades || 87,  // Fallback para valor fixo
                'stat-matriculados': stats.total_alunos || 478,
                'stat-aulas': `${stats.total_provas || 87}/235`
            };
            
            Object.entries(statElements).forEach(([id, value]) => {
                const element = document.getElementById(id);
                if (element) {
                    element.textContent = value;
                }
            });
            
            console.log('✅ Estatísticas gerais atualizadas:', stats);
        }
    } catch (error) {
        console.error('❌ Erro ao atualizar estatísticas gerais:', error);
    }
}

/**
 * Seleciona localidade e busca dados detalhados
 */
async function selecionarLocalidadeReal(index) {
    const localidade = window.localidadesDB[index];
    
    try {
        console.log(`🎯 Buscando dados detalhados de ${localidade.nomeCompleto}...`);
        
        const response = await SAM_API.getLocalidade(localidade.id_igreja);
        
        if (response.success) {
            // Mesclar dados detalhados com dados básicos
            window.localidadeSelecionada = {
                ...localidade,
                ...response.localidade,
                alunos: response.alunos || [],
                estatisticas: response.estatisticas || {}
            };
            
            console.log(`✅ Dados carregados: ${response.count_alunos} alunos`);
            
            // Chamar função original de seleção (se existir)
            if (typeof selectLocation === 'function') {
                selectLocation(index);
            }
        }
    } catch (error) {
        console.error(`❌ Erro ao carregar dados da localidade:`, error);
        
        // Usar dados básicos em caso de erro
        window.localidadeSelecionada = localidade;
        
        if (typeof selectLocation === 'function') {
            selectLocation(index);
        }
    }
}

/**
 * Atualiza função verDados() para incluir geração de PDF
 */
function verDadosComPDF() {
    if (!window.localidadeSelecionada) {
        alert('⚠️ Nenhuma localidade selecionada');
        return;
    }
    
    const loc = window.localidadeSelecionada;
    
    // Criar modal ou overlay com informações
    const modal = document.createElement('div');
    modal.id = 'modal-dados';
    modal.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        width: 100vw;
        height: 100vh;
        background: rgba(0, 0, 0, 0.9);
        z-index: 9999;
        display: flex;
        align-items: center;
        justify-content: center;
        animation: fadeIn 0.3s ease;
    `;
    
    modal.innerHTML = `
        <div style="
            background: rgba(20, 20, 20, 0.95);
            border: 2px solid rgba(59, 130, 246, 0.5);
            border-radius: 20px;
            padding: 40px;
            max-width: 600px;
            width: 90%;
            backdrop-filter: blur(20px);
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.9);
        ">
            <h2 style="color: #60a5fa; margin-bottom: 10px; font-size: 28px;">
                ${loc.nomeCompleto}
            </h2>
            <p style="color: #a0a0a0; margin-bottom: 30px; font-family: 'Courier New';">
                ${loc.codigo}
            </p>
            
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 30px;">
                <div style="background: rgba(255,255,255,0.05); padding: 15px; border-radius: 10px;">
                    <div style="color: #a0a0a0; font-size: 12px; margin-bottom: 5px;">Total de Alunos</div>
                    <div style="color: #fff; font-size: 24px; font-weight: bold;">${loc.total_alunos || 0}</div>
                </div>
                <div style="background: rgba(255,255,255,0.05); padding: 15px; border-radius: 10px;">
                    <div style="color: #a0a0a0; font-size: 12px; margin-bottom: 5px;">Alunos Ativos</div>
                    <div style="color: #fff; font-size: 24px; font-weight: bold;">${loc.alunos_ativos || 0}</div>
                </div>
                <div style="background: rgba(255,255,255,0.05); padding: 15px; border-radius: 10px;">
                    <div style="color: #a0a0a0; font-size: 12px; margin-bottom: 5px;">Total MTS + MSA</div>
                    <div style="color: #fff; font-size: 24px; font-weight: bold;">${(loc.total_mts || 0) + (loc.total_msa || 0)}</div>
                </div>
                <div style="background: rgba(255,255,255,0.05); padding: 15px; border-radius: 10px;">
                    <div style="color: #a0a0a0; font-size: 12px; margin-bottom: 5px;">Média de Provas</div>
                    <div style="color: #fff; font-size: 24px; font-weight: bold;">${loc.media_geral ? loc.media_geral.toFixed(2) : 'N/A'}</div>
                </div>
            </div>
            
            <button id="btn-gerar-pdf" style="
                width: 100%;
                padding: 18px;
                background: linear-gradient(135deg, #3b82f6, #6366f1);
                border: 2px solid rgba(59, 130, 246, 0.5);
                border-radius: 12px;
                color: #fff;
                cursor: pointer;
                font-size: 16px;
                font-weight: 700;
                margin-bottom: 15px;
                transition: all 0.3s ease;
            " onmouseover="this.style.transform='translateY(-2px)'; this.style.boxShadow='0 8px 35px rgba(59, 130, 246, 0.6)';" 
               onmouseout="this.style.transform='translateY(0)'; this.style.boxShadow='none';">
                📄 GERAR RELATÓRIO PDF
            </button>
            
            <button onclick="document.getElementById('modal-dados').remove()" style="
                width: 100%;
                padding: 15px;
                background: rgba(255, 255, 255, 0.05);
                border: 1px solid rgba(255, 255, 255, 0.2);
                border-radius: 12px;
                color: #fff;
                cursor: pointer;
                font-size: 14px;
                transition: all 0.3s ease;
            " onmouseover="this.style.background='rgba(255, 255, 255, 0.1)';" 
               onmouseout="this.style.background='rgba(255, 255, 255, 0.05)';">
                Fechar
            </button>
        </div>
    `;
    
    document.body.appendChild(modal);
    
    // Event listener para gerar PDF
    document.getElementById('btn-gerar-pdf').addEventListener('click', async () => {
        const btn = document.getElementById('btn-gerar-pdf');
        btn.textContent = '⏳ Gerando PDF...';
        btn.disabled = true;
        
        try {
            await SAM_API.downloadPDFLocalidade(loc.id_igreja, loc.codigo);
            btn.textContent = '✅ PDF Gerado!';
            
            setTimeout(() => {
                btn.textContent = '📄 GERAR RELATÓRIO PDF';
                btn.disabled = false;
            }, 2000);
        } catch (error) {
            btn.textContent = '❌ Erro ao gerar PDF';
            alert('Erro ao gerar PDF. Verifique se a API está rodando.');
            
            setTimeout(() => {
                btn.textContent = '📄 GERAR RELATÓRIO PDF';
                btn.disabled = false;
            }, 2000);
        }
    });
}

/**
 * Inicialização quando a página carregar
 */
window.addEventListener('DOMContentLoaded', async () => {
    console.log('🚀 Iniciando MULTIPLICA SAM...');
    
    // Verificar se API está disponível
    try {
        const health = await SAM_API.get('/health');
        console.log('✅ API conectada:', health);
    } catch (error) {
        console.warn('⚠️ API offline - usando dados estáticos');
    }
    
    // Carregar localidades do banco
    await carregarLocalidadesReais();
    
    // Atualizar estatísticas gerais
    await atualizarEstatisticasGerais();
    
    // Substituir função verDados() global
    window.verDados = verDadosComPDF;
    
    console.log('✅ Sistema iniciado!');
});

// Exportar para uso global
window.SAM_API = SAM_API;
window.carregarLocalidadesReais = carregarLocalidadesReais;
window.verDadosComPDF = verDadosComPDF;
