const PLANILHA_ID = '1lnzzToyBao-c5sptw4IcnXA0QCvS4bKFpyiQUcxbA3Q';
const ABA_IDS_HORTOLANDIA = 'IDs Hortolândia';

function doGet(e) {
  try {
    const acao = e.parameter.acao;
    
    if (acao === 'listar_ids_hortolandia') {
      return listarIdsIgrejasHortolandia();
    }
    
    return ContentService.createTextOutput(
      JSON.stringify({ 
        erro: 'Ação não reconhecida',
        acoes_disponiveis: ['listar_ids_hortolandia']
      })
    ).setMimeType(ContentService.MimeType.JSON);
    
  } catch (erro) {
    Logger.log('Erro no doGet: ' + erro);
    return ContentService.createTextOutput(
      JSON.stringify({ erro: erro.toString() })
    ).setMimeType(ContentService.MimeType.JSON);
  }
}

function doPost(e) {
  try {
    const dados = JSON.parse(e.postData.contents);
    const tipo = dados.tipo;
    
    if (tipo === 'localidades_hortolandia' || tipo === 'ids_hortolandia') {
      return salvarIdsIgrejasHortolandia(dados);
    } else if (tipo === 'alunos_hortolandia') {
      return salvarAlunosHortolandia(dados);
    }
    
    return ContentService.createTextOutput(
      JSON.stringify({ erro: 'Tipo não reconhecido: ' + tipo })
    ).setMimeType(ContentService.MimeType.JSON);
    
  } catch (erro) {
    Logger.log('Erro no doPost: ' + erro);
    return ContentService.createTextOutput(
      JSON.stringify({ erro: erro.toString() })
    ).setMimeType(ContentService.MimeType.JSON);
  }
}

/**
 * Lista IDs das igrejas de Hortolândia
 * Busca na aba "IDs Hortolândia" e retorna coluna ID_Igreja
 */
function listarIdsIgrejasHortolandia() {
  try {
    const planilha = SpreadsheetApp.openById(PLANILHA_ID);
    const aba = planilha.getSheetByName(ABA_IDS_HORTOLANDIA);

    if (!aba) {
      Logger.log('Aba não encontrada: ' + ABA_IDS_HORTOLANDIA);
      return ContentService.createTextOutput(
        JSON.stringify({ 
          ids: [], 
          total: 0,
          erro: 'Aba não encontrada: ' + ABA_IDS_HORTOLANDIA
        })
      ).setMimeType(ContentService.MimeType.JSON);
    }

    const ultimaLinha = aba.getLastRow();
    const ultimaColuna = aba.getLastColumn();
    
    Logger.log(`Aba encontrada. Linhas: ${ultimaLinha}, Colunas: ${ultimaColuna}`);
    
    if (ultimaLinha < 2) {
      Logger.log('Aba vazia (menos de 2 linhas)');
      return ContentService.createTextOutput(
        JSON.stringify({ ids: [], total: 0, erro: 'Aba vazia' })
      ).setMimeType(ContentService.MimeType.JSON);
    }

    // Ler cabeçalho e limpar espaços
    const cabecalho = aba.getRange(1, 1, 1, ultimaColuna).getValues()[0]
      .map(c => String(c).trim());
    
    Logger.log('Cabeçalho: ' + JSON.stringify(cabecalho));
    
    // Procurar coluna ID_Igreja (case insensitive e sem espaços extras)
    let indiceColuna = -1;
    for (let i = 0; i < cabecalho.length; i++) {
      const coluna = cabecalho[i].toLowerCase().replace(/\s+/g, '');
      if (coluna === 'id_igreja' || coluna === 'idigreja') {
        indiceColuna = i;
        break;
      }
    }

    if (indiceColuna === -1) {
      Logger.log('Coluna ID_Igreja não encontrada. Cabeçalho: ' + JSON.stringify(cabecalho));
      return ContentService.createTextOutput(
        JSON.stringify({ 
          ids: [], 
          total: 0,
          erro: 'Coluna ID_Igreja não encontrada',
          cabecalho: cabecalho
        })
      ).setMimeType(ContentService.MimeType.JSON);
    }

    Logger.log('Coluna ID_Igreja encontrada no índice: ' + indiceColuna);

    // Ler dados da coluna (pular linhas vazias no final)
    const totalLinhas = ultimaLinha - 1; // Excluir cabeçalho
    const dados = aba.getRange(2, indiceColuna + 1, totalLinhas, 1).getValues();
    
    // Filtrar e converter para números
    const ids = [];
    for (let i = 0; i < dados.length; i++) {
      const valor = dados[i][0];
      
      // Parar se encontrar linha vazia ou "RESUMO:"
      if (!valor || String(valor).trim() === '' || String(valor).toUpperCase().includes('RESUMO')) {
        break;
      }
      
      const numero = Number(valor);
      if (!isNaN(numero) && numero > 0) {
        ids.push(numero);
      }
    }

    Logger.log(`Retornando ${ids.length} IDs: ${ids.slice(0, 10).join(', ')}${ids.length > 10 ? '...' : ''}`);

    return ContentService.createTextOutput(
      JSON.stringify({ 
        ids: ids, 
        total: ids.length,
        timestamp: new Date().toISOString(),
        aba: ABA_IDS_HORTOLANDIA
      })
    ).setMimeType(ContentService.MimeType.JSON);

  } catch (erro) {
    Logger.log('Erro ao listar IDs: ' + erro);
    return ContentService.createTextOutput(
      JSON.stringify({ 
        erro: erro.toString(), 
        ids: [],
        stack: erro.stack
      })
    ).setMimeType(ContentService.MimeType.JSON);
  }
}

/**
 * Salva IDs das igrejas de Hortolândia
 */
function salvarIdsIgrejasHortolandia(dados) {
  try {
    const planilha = SpreadsheetApp.openById(PLANILHA_ID);
    let aba = planilha.getSheetByName(ABA_IDS_HORTOLANDIA);
    
    // Criar aba se não existir
    if (!aba) {
      aba = planilha.insertSheet(ABA_IDS_HORTOLANDIA);
      Logger.log('Aba criada: ' + ABA_IDS_HORTOLANDIA);
    } else {
      // Limpar aba existente
      aba.clear();
      Logger.log('Aba limpa: ' + ABA_IDS_HORTOLANDIA);
    }
    
    // Inserir cabeçalhos
    const headers = dados.headers || ['ID_Igreja', 'Nome_Localidade', 'Setor', 'Cidade', 'Texto_Completo'];
    aba.getRange(1, 1, 1, headers.length)
      .setValues([headers])
      .setFontWeight('bold')
      .setBackground('#673AB7')
      .setFontColor('#FFFFFF');

    // Inserir dados
    if (dados.dados && dados.dados.length > 0) {
      aba.getRange(2, 1, dados.dados.length, headers.length)
        .setValues(dados.dados);

      aba.setFrozenRows(1);
      aba.autoResizeColumns(1, headers.length);
      
      // Criar filtro
      const range = aba.getRange(1, 1, dados.dados.length + 1, headers.length);
      range.createFilter();
    }

    // Inserir resumo
    if (dados.resumo) {
      const ultimaLinha = aba.getLastRow() + 2;
      aba.getRange(ultimaLinha, 1, 1, 2)
        .setValues([['RESUMO:', '']])
        .setFontWeight('bold');
      
      const resumoKeys = Object.keys(dados.resumo);
      const resumoValues = resumoKeys.map(k => [k, dados.resumo[k]]);
      
      aba.getRange(ultimaLinha + 1, 1, resumoValues.length, 2)
        .setValues(resumoValues);
    }

    Logger.log(`IDs salvos: ${dados.dados ? dados.dados.length : 0} registros`);
    
    return ContentService.createTextOutput(
      JSON.stringify({ 
        mensagem: 'IDs de Hortolândia inseridos com sucesso',
        linhas: dados.dados ? dados.dados.length : 0,
        aba: ABA_IDS_HORTOLANDIA
      })
    ).setMimeType(ContentService.MimeType.JSON);
    
  } catch (erro) {
    Logger.log('Erro ao salvar IDs: ' + erro);
    return ContentService.createTextOutput(
      JSON.stringify({ erro: erro.toString() })
    ).setMimeType(ContentService.MimeType.JSON);
  }
}

/**
 * Salva alunos de Hortolândia
 */
function salvarAlunosHortolandia(dados) {
  try {
    const planilha = SpreadsheetApp.openById(PLANILHA_ID);
    const nomeAba = 'Alunos Hortolândia';
    let aba = planilha.getSheetByName(nomeAba);
    
    if (!aba) {
      aba = planilha.insertSheet(nomeAba);
      Logger.log('Aba criada: ' + nomeAba);
    } else {
      aba.clear();
      Logger.log('Aba limpa: ' + nomeAba);
    }
    
    // Inserir cabeçalhos
    const headers = dados.headers || ['ID_Aluno', 'ID_Igreja', 'Nome_Aluno'];
    aba.getRange(1, 1, 1, headers.length)
      .setValues([headers])
      .setFontWeight('bold')
      .setBackground('#FF6D00')
      .setFontColor('#FFFFFF');

    // Inserir dados
    if (dados.dados && dados.dados.length > 0) {
      aba.getRange(2, 1, dados.dados.length, headers.length)
        .setValues(dados.dados);

      aba.setFrozenRows(1);
      aba.autoResizeColumns(1, headers.length);
      
      const range = aba.getRange(1, 1, dados.dados.length + 1, headers.length);
      range.createFilter();
    }

    // Inserir resumo
    if (dados.resumo) {
      const ultimaLinha = aba.getLastRow() + 2;
      aba.getRange(ultimaLinha, 1, 1, 2)
        .setValues([['RESUMO:', '']])
        .setFontWeight('bold');
      
      const resumoKeys = Object.keys(dados.resumo);
      const resumoValues = resumoKeys.map(k => [k, dados.resumo[k]]);
      
      aba.getRange(ultimaLinha + 1, 1, resumoValues.length, 2)
        .setValues(resumoValues);
    }

    Logger.log(`Alunos salvos: ${dados.dados ? dados.dados.length : 0} registros`);
    
    return ContentService.createTextOutput(
      JSON.stringify({ 
        mensagem: 'Alunos de Hortolândia inseridos com sucesso',
        linhas: dados.dados ? dados.dados.length : 0,
        aba: nomeAba
      })
    ).setMimeType(ContentService.MimeType.JSON);
    
  } catch (erro) {
    Logger.log('Erro ao salvar alunos: ' + erro);
    return ContentService.createTextOutput(
      JSON.stringify({ erro: erro.toString() })
    ).setMimeType(ContentService.MimeType.JSON);
  }
}

/**
 * Função de teste - Execute manualmente para testar
 */
function testarListarIds() {
  const resultado = listarIdsIgrejasHortolandia();
  const json = JSON.parse(resultado.getContent());
  Logger.log('Resultado: ' + JSON.stringify(json, null, 2));
}
