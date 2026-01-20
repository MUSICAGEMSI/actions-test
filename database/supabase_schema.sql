-- ==========================================
-- MULTIPLICA SAM - ESTRUTURA DO BANCO DE DADOS
-- Supabase PostgreSQL Schema
-- ==========================================

-- ==========================================
-- 1. TABELA DE LOCALIDADES
-- ==========================================
CREATE TABLE IF NOT EXISTS localidades (
    id_igreja INTEGER PRIMARY KEY,
    nome_localidade TEXT NOT NULL,
    nome_completo TEXT,
    setor TEXT,
    cidade TEXT,
    texto_completo TEXT,
    codigo_localidade TEXT UNIQUE,
    coordenada_x INTEGER,
    coordenada_y INTEGER,
    imagem_desenho TEXT,
    imagem_real TEXT,
    data_criacao TIMESTAMP DEFAULT NOW(),
    data_atualizacao TIMESTAMP DEFAULT NOW()
);

-- Índices para otimizar buscas
CREATE INDEX idx_localidades_setor ON localidades(setor);
CREATE INDEX idx_localidades_cidade ON localidades(cidade);
CREATE INDEX idx_localidades_nome ON localidades(nome_localidade);

-- ==========================================
-- 2. TABELA DE ALUNOS
-- ==========================================
CREATE TABLE IF NOT EXISTS alunos (
    id_aluno INTEGER PRIMARY KEY,
    nome TEXT NOT NULL,
    id_igreja INTEGER REFERENCES localidades(id_igreja),
    id_cargo INTEGER,
    cargo_nome TEXT,
    id_nivel INTEGER,
    nivel_nome TEXT,
    id_instrumento INTEGER,
    instrumento_nome TEXT,
    id_tonalidade INTEGER,
    tonalidade_nome TEXT,
    fl_tipo TEXT,
    status TEXT,
    data_cadastro TIMESTAMP,
    cadastrado_por TEXT,
    data_atualizacao TIMESTAMP,
    atualizado_por TEXT,
    ultima_atualizacao_scraping TIMESTAMP DEFAULT NOW()
);

-- Índices
CREATE INDEX idx_alunos_igreja ON alunos(id_igreja);
CREATE INDEX idx_alunos_nome ON alunos(nome);
CREATE INDEX idx_alunos_instrumento ON alunos(instrumento_nome);
CREATE INDEX idx_alunos_status ON alunos(status);

-- ==========================================
-- 3. TABELA MTS INDIVIDUAL
-- ==========================================
CREATE TABLE IF NOT EXISTS mts_individual (
    id SERIAL PRIMARY KEY,
    id_aluno INTEGER REFERENCES alunos(id_aluno),
    nome_aluno TEXT,
    coluna_1 TEXT,
    coluna_2 TEXT,
    coluna_3 TEXT,
    coluna_4 TEXT,
    data_licao DATE,
    coluna_6 TEXT,
    data_conclusao DATE,
    observacoes TEXT,
    data_scraping TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_mts_individual_aluno ON mts_individual(id_aluno);

-- ==========================================
-- 4. TABELA MTS GRUPO
-- ==========================================
CREATE TABLE IF NOT EXISTS mts_grupo (
    id SERIAL PRIMARY KEY,
    id_aluno INTEGER REFERENCES alunos(id_aluno),
    nome_aluno TEXT,
    descricao TEXT,
    observacoes TEXT,
    data_licao DATE,
    data_scraping TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_mts_grupo_aluno ON mts_grupo(id_aluno);

-- ==========================================
-- 5. TABELA MSA INDIVIDUAL
-- ==========================================
CREATE TABLE IF NOT EXISTS msa_individual (
    id SERIAL PRIMARY KEY,
    id_aluno INTEGER REFERENCES alunos(id_aluno),
    nome_aluno TEXT,
    data_inicio DATE,
    fase TEXT,
    pagina TEXT,
    clave TEXT,
    observacoes TEXT,
    data_conclusao DATE,
    data_scraping TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_msa_individual_aluno ON msa_individual(id_aluno);

-- ==========================================
-- 6. TABELA MSA GRUPO
-- ==========================================
CREATE TABLE IF NOT EXISTS msa_grupo (
    id SERIAL PRIMARY KEY,
    id_aluno INTEGER REFERENCES alunos(id_aluno),
    nome_aluno TEXT,
    fase_de TEXT,
    fase_ate TEXT,
    pagina_de TEXT,
    pagina_ate TEXT,
    claves TEXT,
    observacoes TEXT,
    data_licao DATE,
    data_scraping TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_msa_grupo_aluno ON msa_grupo(id_aluno);

-- ==========================================
-- 7. TABELA PROVAS
-- ==========================================
CREATE TABLE IF NOT EXISTS provas (
    id SERIAL PRIMARY KEY,
    id_aluno INTEGER REFERENCES alunos(id_aluno),
    nome_aluno TEXT,
    tipo_prova TEXT,
    descricao TEXT,
    data_prova DATE,
    nota DECIMAL(5,2),
    data_resultado DATE,
    observacoes TEXT,
    data_scraping TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_provas_aluno ON provas(id_aluno);
CREATE INDEX idx_provas_data ON provas(data_prova);

-- ==========================================
-- 8. TABELA HINÁRIO INDIVIDUAL
-- ==========================================
CREATE TABLE IF NOT EXISTS hinario_individual (
    id SERIAL PRIMARY KEY,
    id_aluno INTEGER REFERENCES alunos(id_aluno),
    nome_aluno TEXT,
    numero_hino TEXT,
    data_inicio DATE,
    descricao TEXT,
    data_apresentacao DATE,
    data_aprovacao DATE,
    observacoes TEXT,
    data_scraping TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_hinario_individual_aluno ON hinario_individual(id_aluno);

-- ==========================================
-- 9. TABELA HINÁRIO GRUPO
-- ==========================================
CREATE TABLE IF NOT EXISTS hinario_grupo (
    id SERIAL PRIMARY KEY,
    id_aluno INTEGER REFERENCES alunos(id_aluno),
    nome_aluno TEXT,
    descricao TEXT,
    observacoes TEXT,
    data_licao DATE,
    data_scraping TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_hinario_grupo_aluno ON hinario_grupo(id_aluno);

-- ==========================================
-- 10. TABELA MÉTODOS
-- ==========================================
CREATE TABLE IF NOT EXISTS metodos (
    id SERIAL PRIMARY KEY,
    id_aluno INTEGER REFERENCES alunos(id_aluno),
    nome_aluno TEXT,
    nome_metodo TEXT,
    descricao TEXT,
    pagina TEXT,
    data_inicio DATE,
    observacoes TEXT,
    data_conclusao DATE,
    data_scraping TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_metodos_aluno ON metodos(id_aluno);

-- ==========================================
-- 11. TABELA ESCALAS INDIVIDUAL
-- ==========================================
CREATE TABLE IF NOT EXISTS escalas_individual (
    id SERIAL PRIMARY KEY,
    id_aluno INTEGER REFERENCES alunos(id_aluno),
    nome_aluno TEXT,
    tipo_escala TEXT,
    data_inicio DATE,
    descricao TEXT,
    data_apresentacao DATE,
    data_aprovacao DATE,
    observacoes TEXT,
    data_scraping TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_escalas_individual_aluno ON escalas_individual(id_aluno);

-- ==========================================
-- 12. TABELA ESCALAS GRUPO
-- ==========================================
CREATE TABLE IF NOT EXISTS escalas_grupo (
    id SERIAL PRIMARY KEY,
    id_aluno INTEGER REFERENCES alunos(id_aluno),
    nome_aluno TEXT,
    descricao TEXT,
    observacoes TEXT,
    data_licao DATE,
    data_scraping TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_escalas_grupo_aluno ON escalas_grupo(id_aluno);

-- ==========================================
-- 13. TABELA DE LOG DE SCRAPING
-- ==========================================
CREATE TABLE IF NOT EXISTS log_scraping (
    id SERIAL PRIMARY KEY,
    data_execucao TIMESTAMP DEFAULT NOW(),
    modulo TEXT, -- 'localidades', 'alunos', 'historico'
    status TEXT, -- 'sucesso', 'erro', 'em_andamento'
    registros_processados INTEGER,
    registros_sucesso INTEGER,
    registros_erro INTEGER,
    tempo_execucao_segundos INTEGER,
    mensagem_erro TEXT,
    detalhes JSONB
);

CREATE INDEX idx_log_scraping_data ON log_scraping(data_execucao DESC);
CREATE INDEX idx_log_scraping_modulo ON log_scraping(modulo);

-- ==========================================
-- 14. TABELA DE ESTATÍSTICAS CONSOLIDADAS
-- (Para acelerar consultas da interface)
-- ==========================================
CREATE TABLE IF NOT EXISTS estatisticas_localidades (
    id_igreja INTEGER PRIMARY KEY REFERENCES localidades(id_igreja),
    total_alunos INTEGER DEFAULT 0,
    total_turmas INTEGER DEFAULT 0,
    total_aulas INTEGER DEFAULT 0,
    total_mts INTEGER DEFAULT 0,
    total_msa INTEGER DEFAULT 0,
    total_provas INTEGER DEFAULT 0,
    media_notas DECIMAL(5,2),
    ultima_atualizacao TIMESTAMP DEFAULT NOW()
);

-- ==========================================
-- VIEWS ÚTEIS PARA A INTERFACE
-- ==========================================

-- View: Resumo completo por aluno
CREATE OR REPLACE VIEW vw_resumo_alunos AS
SELECT 
    a.id_aluno,
    a.nome,
    a.id_igreja,
    l.nome_localidade,
    l.codigo_localidade,
    a.instrumento_nome,
    a.nivel_nome,
    a.status,
    COUNT(DISTINCT mts_i.id) as total_mts_individual,
    COUNT(DISTINCT mts_g.id) as total_mts_grupo,
    COUNT(DISTINCT msa_i.id) as total_msa_individual,
    COUNT(DISTINCT msa_g.id) as total_msa_grupo,
    COUNT(DISTINCT p.id) as total_provas,
    AVG(p.nota) as media_provas,
    COUNT(DISTINCT h_i.id) as total_hinario_individual,
    COUNT(DISTINCT h_g.id) as total_hinario_grupo,
    COUNT(DISTINCT m.id) as total_metodos,
    COUNT(DISTINCT e_i.id) as total_escalas_individual,
    COUNT(DISTINCT e_g.id) as total_escalas_grupo
FROM alunos a
LEFT JOIN localidades l ON a.id_igreja = l.id_igreja
LEFT JOIN mts_individual mts_i ON a.id_aluno = mts_i.id_aluno
LEFT JOIN mts_grupo mts_g ON a.id_aluno = mts_g.id_aluno
LEFT JOIN msa_individual msa_i ON a.id_aluno = msa_i.id_aluno
LEFT JOIN msa_grupo msa_g ON a.id_aluno = msa_g.id_aluno
LEFT JOIN provas p ON a.id_aluno = p.id_aluno
LEFT JOIN hinario_individual h_i ON a.id_aluno = h_i.id_aluno
LEFT JOIN hinario_grupo h_g ON a.id_aluno = h_g.id_aluno
LEFT JOIN metodos m ON a.id_aluno = m.id_aluno
LEFT JOIN escalas_individual e_i ON a.id_aluno = e_i.id_aluno
LEFT JOIN escalas_grupo e_g ON a.id_aluno = e_g.id_aluno
GROUP BY a.id_aluno, a.nome, a.id_igreja, l.nome_localidade, 
         l.codigo_localidade, a.instrumento_nome, a.nivel_nome, a.status;

-- View: Resumo por localidade
CREATE OR REPLACE VIEW vw_resumo_localidades AS
SELECT 
    l.id_igreja,
    l.nome_localidade,
    l.codigo_localidade,
    l.setor,
    l.cidade,
    COUNT(DISTINCT a.id_aluno) as total_alunos,
    COUNT(DISTINCT CASE WHEN a.status = 'ativo' THEN a.id_aluno END) as alunos_ativos,
    COUNT(DISTINCT a.instrumento_nome) as total_instrumentos,
    COUNT(DISTINCT mts_i.id) + COUNT(DISTINCT mts_g.id) as total_mts,
    COUNT(DISTINCT msa_i.id) + COUNT(DISTINCT msa_g.id) as total_msa,
    COUNT(DISTINCT p.id) as total_provas,
    AVG(p.nota) as media_geral_provas
FROM localidades l
LEFT JOIN alunos a ON l.id_igreja = a.id_igreja
LEFT JOIN mts_individual mts_i ON a.id_aluno = mts_i.id_aluno
LEFT JOIN mts_grupo mts_g ON a.id_aluno = mts_g.id_aluno
LEFT JOIN msa_individual msa_i ON a.id_aluno = msa_i.id_aluno
LEFT JOIN msa_grupo msa_g ON a.id_aluno = msa_g.id_aluno
LEFT JOIN provas p ON a.id_aluno = p.id_aluno
GROUP BY l.id_igreja, l.nome_localidade, l.codigo_localidade, l.setor, l.cidade;

-- ==========================================
-- FUNÇÕES ÚTEIS
-- ==========================================

-- Função para atualizar estatísticas de localidade
CREATE OR REPLACE FUNCTION atualizar_estatisticas_localidade(p_id_igreja INTEGER)
RETURNS VOID AS $$
BEGIN
    INSERT INTO estatisticas_localidades (
        id_igreja, 
        total_alunos, 
        total_mts, 
        total_msa, 
        total_provas, 
        media_notas,
        ultima_atualizacao
    )
    SELECT 
        l.id_igreja,
        COUNT(DISTINCT a.id_aluno),
        COUNT(DISTINCT mts_i.id) + COUNT(DISTINCT mts_g.id),
        COUNT(DISTINCT msa_i.id) + COUNT(DISTINCT msa_g.id),
        COUNT(DISTINCT p.id),
        AVG(p.nota),
        NOW()
    FROM localidades l
    LEFT JOIN alunos a ON l.id_igreja = a.id_igreja
    LEFT JOIN mts_individual mts_i ON a.id_aluno = mts_i.id_aluno
    LEFT JOIN mts_grupo mts_g ON a.id_aluno = mts_g.id_aluno
    LEFT JOIN msa_individual msa_i ON a.id_aluno = msa_i.id_aluno
    LEFT JOIN msa_grupo msa_g ON a.id_aluno = msa_g.id_aluno
    LEFT JOIN provas p ON a.id_aluno = p.id_aluno
    WHERE l.id_igreja = p_id_igreja
    GROUP BY l.id_igreja
    ON CONFLICT (id_igreja) 
    DO UPDATE SET
        total_alunos = EXCLUDED.total_alunos,
        total_mts = EXCLUDED.total_mts,
        total_msa = EXCLUDED.total_msa,
        total_provas = EXCLUDED.total_provas,
        media_notas = EXCLUDED.media_notas,
        ultima_atualizacao = NOW();
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- TRIGGERS
-- ==========================================

-- Trigger para atualizar data_atualizacao automaticamente
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.data_atualizacao = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_localidades_timestamp
BEFORE UPDATE ON localidades
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();

-- ==========================================
-- COMENTÁRIOS NAS TABELAS
-- ==========================================
COMMENT ON TABLE localidades IS 'Localidades de Hortolândia (igrejas/pontos)';
COMMENT ON TABLE alunos IS 'Alunos cadastrados no sistema musical';
COMMENT ON TABLE mts_individual IS 'Lições individuais do MTS (Método de Teoria e Solfejo)';
COMMENT ON TABLE mts_grupo IS 'Lições em grupo do MTS';
COMMENT ON TABLE msa_individual IS 'Lições individuais do MSA (Método de Solfejo Avançado)';
COMMENT ON TABLE msa_grupo IS 'Lições em grupo do MSA';
COMMENT ON TABLE provas IS 'Provas realizadas pelos alunos';
COMMENT ON TABLE hinario_individual IS 'Hinos estudados individualmente';
COMMENT ON TABLE hinario_grupo IS 'Hinos estudados em grupo';
COMMENT ON TABLE metodos IS 'Métodos de ensino aplicados';
COMMENT ON TABLE escalas_individual IS 'Escalas musicais individuais';
COMMENT ON TABLE escalas_grupo IS 'Escalas musicais em grupo';
COMMENT ON TABLE log_scraping IS 'Registro de execuções do scraping';
COMMENT ON TABLE estatisticas_localidades IS 'Estatísticas consolidadas por localidade (cache)';

-- ==========================================
-- INSERIR LOCALIDADES INICIAIS
-- ==========================================
INSERT INTO localidades (id_igreja, nome_localidade, nome_completo, codigo_localidade, coordenada_x, coordenada_y, imagem_desenho, imagem_real, setor, cidade) VALUES
(1431, 'Jd. Santana - Central', 'Jardim Santana - Central', 'BR-22-1431', 600, 245, 'jardim-santana.png', 'jardim-santana.jpg', 'BR-SP-CAMPINAS', 'HORTOLANDIA'),
(1434, 'Jd. Sta. Izabel', 'Jardim Santa Izabel', 'BR-22-1434', 809, 389, 'jardim-santa-izabel.png', 'jardim-santa-izabel.jpg', 'BR-SP-CAMPINAS', 'HORTOLANDIA'),
(1997, 'Vila Real', 'Vila Real', 'BR-22-1997', 567, 129, 'vila-real.png', 'vila-real.jpg', 'BR-SP-CAMPINAS', 'HORTOLANDIA'),
(2052, 'Jd. Amanda I', 'Jardim Amanda I', 'BR-22-2052', 480, 365, 'amanda-1.png', 'amanda-1.jpg', 'BR-SP-CAMPINAS', 'HORTOLANDIA'),
(2795, 'Jd. Aline', 'Jardim Aline', 'BR-22-2795', 877, 180, 'jardim-aline.png', 'jardim-aline.jpg', 'BR-SP-CAMPINAS', 'HORTOLANDIA'),
(3238, 'Jd. das Colinas', 'Jardim das Colinas', 'BR-22-3238', 517, 136, 'jardim-das-colinas.png', 'jardim-das-colinas.jpg', 'BR-SP-CAMPINAS', 'HORTOLANDIA'),
(3493, 'Jd. Novo Horizonte', 'Jardim Novo Horizonte', 'BR-22-3493', 342, 279, 'jardim-novo-horizonte.png', 'jardim-novo-horizonte.jpg', 'BR-SP-CAMPINAS', 'HORTOLANDIA'),
(3833, 'Jd. Interlagos', 'Jardim Interlagos', 'BR-22-3833', 606, 350, 'jardim-interlagos.png', 'jardim-interlagos.jpg', 'BR-SP-CAMPINAS', 'HORTOLANDIA'),
(5419, 'Jd. Amanda IV', 'Jardim Amanda IV', 'BR-22-5419', 447, 332, 'amanda-4.png', 'amanda-4.jpg', 'BR-SP-CAMPINAS', 'HORTOLANDIA'),
(5420, 'Residencial Bellaville', 'Residencial Bellaville', 'BR-22-5420', 525, 35, 'residencial-bellaville.png', 'residencial-bellaville.jpg', 'BR-SP-CAMPINAS', 'HORTOLANDIA')
ON CONFLICT (id_igreja) DO NOTHING;

-- ==========================================
-- CONFIGURAÇÕES DE PERFORMANCE
-- ==========================================
-- Habilitar auto-vacuum para manter performance
ALTER TABLE localidades SET (autovacuum_enabled = true);
ALTER TABLE alunos SET (autovacuum_enabled = true);
ALTER TABLE provas SET (autovacuum_enabled = true);

-- ==========================================
-- FIM DO SCHEMA
-- ==========================================
