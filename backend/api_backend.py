"""
MULTIPLICA SAM - API BACKEND
API Flask para servir dados do Supabase para o frontend
"""

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from supabase import create_client, Client
from datetime import datetime
import os
from typing import List, Dict, Optional
import io
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

app = Flask(__name__)
CORS(app)  # Permitir CORS para todas as rotas

# Configuração Supabase
SUPABASE_URL = "https://esrjodsxipjuiaiawddl.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVzcmpvZHN4aXBqdWlhaWF3ZGRsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzczMjY5NTYsImV4cCI6MjA1MjkwMjk1Nn0.x-eN9CbC-6DgCDbiYeHRpg_Ysbt6M62"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==================== ENDPOINTS ====================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check do servidor"""
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'service': 'MULTIPLICA SAM API'
    })

@app.route('/api/localidades', methods=['GET'])
def get_localidades():
    """Retorna todas as localidades"""
    try:
        response = supabase.table('vw_resumo_localidades').select('*').execute()
        return jsonify({
            'success': True,
            'data': response.data,
            'count': len(response.data)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/localidade/<int:id_igreja>', methods=['GET'])
def get_localidade(id_igreja):
    """Retorna dados detalhados de uma localidade específica"""
    try:
        # Dados da localidade
        localidade = supabase.table('vw_resumo_localidades')\
            .select('*')\
            .eq('id_igreja', id_igreja)\
            .single()\
            .execute()
        
        # Alunos da localidade
        alunos = supabase.table('alunos')\
            .select('*')\
            .eq('id_igreja', id_igreja)\
            .execute()
        
        # Estatísticas consolidadas
        stats = supabase.table('estatisticas_localidades')\
            .select('*')\
            .eq('id_igreja', id_igreja)\
            .single()\
            .execute()
        
        return jsonify({
            'success': True,
            'localidade': localidade.data,
            'alunos': alunos.data,
            'estatisticas': stats.data if stats.data else {},
            'count_alunos': len(alunos.data)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/aluno/<int:id_aluno>', methods=['GET'])
def get_aluno(id_aluno):
    """Retorna dados completos de um aluno"""
    try:
        # Dados básicos do aluno
        aluno = supabase.table('alunos')\
            .select('*')\
            .eq('id_aluno', id_aluno)\
            .single()\
            .execute()
        
        # Histórico completo
        historico = {}
        
        # MTS
        mts_ind = supabase.table('mts_individual')\
            .select('*')\
            .eq('id_aluno', id_aluno)\
            .execute()
        mts_grp = supabase.table('mts_grupo')\
            .select('*')\
            .eq('id_aluno', id_aluno)\
            .execute()
        
        # MSA
        msa_ind = supabase.table('msa_individual')\
            .select('*')\
            .eq('id_aluno', id_aluno)\
            .execute()
        msa_grp = supabase.table('msa_grupo')\
            .select('*')\
            .eq('id_aluno', id_aluno)\
            .execute()
        
        # Provas
        provas = supabase.table('provas')\
            .select('*')\
            .eq('id_aluno', id_aluno)\
            .order('data_prova', desc=True)\
            .execute()
        
        # Hinário
        hinario_ind = supabase.table('hinario_individual')\
            .select('*')\
            .eq('id_aluno', id_aluno)\
            .execute()
        hinario_grp = supabase.table('hinario_grupo')\
            .select('*')\
            .eq('id_aluno', id_aluno)\
            .execute()
        
        # Métodos
        metodos = supabase.table('metodos')\
            .select('*')\
            .eq('id_aluno', id_aluno)\
            .execute()
        
        # Escalas
        escalas_ind = supabase.table('escalas_individual')\
            .select('*')\
            .eq('id_aluno', id_aluno)\
            .execute()
        escalas_grp = supabase.table('escalas_grupo')\
            .select('*')\
            .eq('id_aluno', id_aluno)\
            .execute()
        
        historico = {
            'mts_individual': mts_ind.data,
            'mts_grupo': mts_grp.data,
            'msa_individual': msa_ind.data,
            'msa_grupo': msa_grp.data,
            'provas': provas.data,
            'hinario_individual': hinario_ind.data,
            'hinario_grupo': hinario_grp.data,
            'metodos': metodos.data,
            'escalas_individual': escalas_ind.data,
            'escalas_grupo': escalas_grp.data
        }
        
        return jsonify({
            'success': True,
            'aluno': aluno.data,
            'historico': historico
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/resumo-alunos', methods=['GET'])
def get_resumo_alunos():
    """Retorna resumo de todos os alunos"""
    try:
        id_igreja = request.args.get('id_igreja', type=int)
        
        query = supabase.table('vw_resumo_alunos').select('*')
        
        if id_igreja:
            query = query.eq('id_igreja', id_igreja)
        
        response = query.execute()
        
        return jsonify({
            'success': True,
            'data': response.data,
            'count': len(response.data)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/logs-scraping', methods=['GET'])
def get_logs_scraping():
    """Retorna logs de execução do scraping"""
    try:
        limit = request.args.get('limit', 50, type=int)
        
        response = supabase.table('log_scraping')\
            .select('*')\
            .order('data_execucao', desc=True)\
            .limit(limit)\
            .execute()
        
        return jsonify({
            'success': True,
            'logs': response.data,
            'count': len(response.data)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/estatisticas/geral', methods=['GET'])
def get_estatisticas_geral():
    """Retorna estatísticas gerais do sistema"""
    try:
        # Total de localidades
        localidades = supabase.table('localidades').select('id_igreja', count='exact').execute()
        
        # Total de alunos
        alunos = supabase.table('alunos').select('id_aluno', count='exact').execute()
        
        # Alunos ativos
        alunos_ativos = supabase.table('alunos')\
            .select('id_aluno', count='exact')\
            .eq('status', 'ativo')\
            .execute()
        
        # Total de provas
        provas = supabase.table('provas').select('id', count='exact').execute()
        
        # Média geral de notas
        media_response = supabase.rpc('calcular_media_geral').execute()
        
        return jsonify({
            'success': True,
            'estatisticas': {
                'total_localidades': localidades.count,
                'total_alunos': alunos.count,
                'alunos_ativos': alunos_ativos.count,
                'total_provas': provas.count,
                'media_geral': media_response.data if media_response.data else 0
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# ==================== GERAÇÃO DE PDF ====================

@app.route('/api/pdf/localidade/<int:id_igreja>', methods=['GET'])
def gerar_pdf_localidade(id_igreja):
    """Gera PDF com dados completos de uma localidade"""
    try:
        # Buscar dados
        localidade_response = supabase.table('vw_resumo_localidades')\
            .select('*')\
            .eq('id_igreja', id_igreja)\
            .single()\
            .execute()
        
        if not localidade_response.data:
            return jsonify({'success': False, 'error': 'Localidade não encontrada'}), 404
        
        localidade = localidade_response.data
        
        # Buscar alunos
        alunos_response = supabase.table('vw_resumo_alunos')\
            .select('*')\
            .eq('id_igreja', id_igreja)\
            .execute()
        
        alunos = alunos_response.data
        
        # Criar PDF em memória
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=2*cm, bottomMargin=2*cm)
        
        # Estilos
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#3b82f6'),
            spaceAfter=30,
            alignment=1  # Centro
        )
        
        subtitle_style = ParagraphStyle(
            'CustomSubtitle',
            parent=styles['Heading2'],
            fontSize=14,
            textColor=colors.HexColor('#666666'),
            spaceAfter=20,
            alignment=1
        )
        
        section_style = ParagraphStyle(
            'SectionTitle',
            parent=styles['Heading2'],
            fontSize=16,
            textColor=colors.HexColor('#1a1a1a'),
            spaceAfter=12,
            spaceBefore=20
        )
        
        # Conteúdo
        story = []
        
        # Cabeçalho
        story.append(Paragraph("MULTIPLICA SAM", title_style))
        story.append(Paragraph("Sistema Avançado de Monitoramento", subtitle_style))
        story.append(Spacer(1, 0.5*cm))
        
        # Informações da localidade
        story.append(Paragraph(f"<b>{localidade['nome_localidade']}</b>", section_style))
        
        info_data = [
            ['Código:', localidade.get('codigo_localidade', 'N/A')],
            ['Setor:', localidade.get('setor', 'N/A')],
            ['Cidade:', localidade.get('cidade', 'N/A')],
            ['Total de Alunos:', str(localidade.get('total_alunos', 0))],
            ['Alunos Ativos:', str(localidade.get('alunos_ativos', 0))],
            ['Total de Instrumentos:', str(localidade.get('total_instrumentos', 0))],
        ]
        
        info_table = Table(info_data, colWidths=[5*cm, 10*cm])
        info_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f3f4f6')),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 11),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e5e7eb'))
        ]))
        
        story.append(info_table)
        story.append(Spacer(1, 1*cm))
        
        # Estatísticas de ensino
        story.append(Paragraph("Estatísticas de Ensino", section_style))
        
        stats_data = [
            ['Categoria', 'Quantidade'],
            ['Total MTS', str(localidade.get('total_mts', 0))],
            ['Total MSA', str(localidade.get('total_msa', 0))],
            ['Total de Provas', str(localidade.get('total_provas', 0))],
            ['Média Geral', f"{localidade.get('media_geral_provas', 0):.2f}" if localidade.get('media_geral_provas') else 'N/A'],
        ]
        
        stats_table = Table(stats_data, colWidths=[10*cm, 5*cm])
        stats_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3b82f6')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('ALIGN', (1, 0), (1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 11),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e5e7eb')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9fafb')])
        ]))
        
        story.append(stats_table)
        story.append(PageBreak())
        
        # Lista de alunos
        if alunos:
            story.append(Paragraph("Lista de Alunos", section_style))
            story.append(Spacer(1, 0.5*cm))
            
            alunos_data = [['Nome', 'Instrumento', 'Nível', 'Status']]
            
            for aluno in alunos[:50]:  # Limitar a 50 alunos para não ficar muito longo
                alunos_data.append([
                    aluno.get('nome', 'N/A')[:30],
                    aluno.get('instrumento_nome', 'N/A')[:20],
                    aluno.get('nivel_nome', 'N/A')[:15],
                    aluno.get('status', 'N/A')
                ])
            
            alunos_table = Table(alunos_data, colWidths=[7*cm, 4*cm, 3*cm, 2*cm])
            alunos_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3b82f6')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e5e7eb')),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9fafb')])
            ]))
            
            story.append(alunos_table)
            
            if len(alunos) > 50:
                story.append(Spacer(1, 0.3*cm))
                story.append(Paragraph(
                    f"<i>* Mostrando 50 de {len(alunos)} alunos</i>",
                    styles['Normal']
                ))
        
        # Rodapé
        story.append(Spacer(1, 2*cm))
        footer_style = ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#999999'),
            alignment=1
        )
        story.append(Paragraph(
            f"Relatório gerado em {datetime.now().strftime('%d/%m/%Y às %H:%M')}",
            footer_style
        ))
        
        # Gerar PDF
        doc.build(story)
        
        # Retornar PDF
        buffer.seek(0)
        
        return send_file(
            buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'relatorio_{localidade["codigo_localidade"]}_{datetime.now().strftime("%Y%m%d")}.pdf'
        )
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# ==================== RUNNER ====================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
