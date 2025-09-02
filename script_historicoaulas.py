from dotenv import load_dotenv
load_dotenv(dotenv_path="credencial.env")

from supabase import create_client, Client
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Configurações
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY]):
    print("❌ Erro: Credenciais Supabase não encontradas no arquivo .env")
    exit(1)

# Inicializar Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

class MusicalAnalytics:
    def __init__(self):
        self.supabase = supabase
        print("📊 Musical Analytics - Sistema de Análise de Dados")
        print("=" * 50)
    
    def obter_estatisticas_gerais(self):
        """Obtém estatísticas gerais do banco de dados"""
        try:
            # Usar a função SQL personalizada
            result = self.supabase.rpc('get_estatisticas_gerais').execute()
            
            if result.data:
                stats = result.data[0]
                print("📈 ESTATÍSTICAS GERAIS - 2º Semestre 2025")
                print("-" * 45)
                print(f"🎯 Total de Aulas: {stats['total_aulas']:,}")
                print(f"📝 Aulas com ATA: {stats['aulas_com_ata']:,}")
                print(f"👥 Total de Registros de Frequência: {stats['total_frequencias']:,}")
                print(f"✅ Presenças: {stats['total_presentes']:,}")
                print(f"❌ Ausências: {stats['total_ausentes']:,}")
                print(f"📊 Percentual de Presença: {stats['percentual_presenca']:.1f}%")
                print(f"🏛️ Congregações Ativas: {stats['congregacoes_ativas']}")
                print(f"🎵 Cursos Ativos: {stats['cursos_ativos']}")
                
                return stats
            else:
                print("⚠️ Nenhum dado encontrado")
                return None
                
        except Exception as e:
            print(f"❌ Erro ao obter estatísticas: {e}")
            return None
    
    def relatorio_por_congregacao(self, limite=10):
        """Relatório detalhado por congregação"""
        try:
            result = self.supabase.table('view_estatisticas_congregacao').select('*').limit(limite).execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                
                print(f"\n🏛️ TOP {len(df)} CONGREGAÇÕES - Atividade Musical")
                print("-" * 60)
                
                for idx, row in df.iterrows():
                    print(f"{idx+1:2d}. {row['congregacao']}")
                    print(f"    📚 Aulas: {row['total_aulas']} | ATA: {row['aulas_com_ata']}")
                    print(f"    👥 Frequências: {row['total_frequencias']} | Presença: {row['percentual_presenca_geral']:.1f}%")
                    print()
                
                return df
            else:
                print("⚠️ Nenhum dado de congregação encontrado")
                return None
                
        except Exception as e:
            print(f"❌ Erro no relatório por congregação: {e}")
            return None
    
    def relatorio_por_curso(self, limite=15):
        """Relatório detalhado por curso"""
        try:
            result = self.supabase.table('view_estatisticas_curso').select('*').order('total_aulas', desc=True).limit(limite).execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                
                print(f"\n🎵 TOP {len(df)} CURSOS MAIS ATIVOS")
                print("-" * 50)
                
                for idx, row in df.iterrows():
                    print(f"{idx+1:2d}. {row['curso']} - {row['congregacao']}")
                    print(f"    📚 Aulas: {row['total_aulas']} | ATA: {row['aulas_com_ata']}")
                    print(f"    👥 Alunos: {row['total_frequencias']} | Presença: {row['percentual_presenca']:.1f}%")
                    print()
                
                return df
            else:
                print("⚠️ Nenhum dado de curso encontrado")
                return None
                
        except Exception as e:
            print(f"❌ Erro no relatório por curso: {e}")
            return None
    
    def evolucao_mensal(self):
        """Análise da evolução mensal"""
        try:
            result = self.supabase.table('view_estatisticas_mensais').select('*').order('mes').execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                
                print(f"\n📅 EVOLUÇÃO MENSAL - 2º Semestre 2025")
                print("-" * 45)
                
                for idx, row in df.iterrows():
                    mes_nome = self.obter_nome_mes(row['mes_texto'])
                    print(f"📅 {mes_nome}:")
                    print(f"   📚 Aulas: {row['total_aulas']} | ATA: {row['aulas_com_ata']}")
                    print(f"   👥 Frequências: {row['total_frequencias']}")
                    print(f"   📊 Presença: {row['percentual_presenca']:.1f}%")
                    print()
                
                return df
            else:
                print("⚠️ Nenhum dado mensal encontrado")
                return None
                
        except Exception as e:
            print(f"❌ Erro na evolução mensal: {e}")
            return None
    
    def obter_nome_mes(self, mes_codigo):
        """Converte código do mês para nome"""
        meses = {
            '2025-07': 'Julho 2025',
            '2025-08': 'Agosto 2025', 
            '2025-09': 'Setembro 2025',
            '2025-10': 'Outubro 2025',
            '2025-11': 'Novembro 2025',
            '2025-12': 'Dezembro 2025'
        }
        return meses.get(mes_codigo, mes_codigo)
    
    def identificar_problemas(self):
        """Identifica possíveis problemas nos dados"""
        print(f"\n🔍 ANÁLISE DE QUALIDADE DOS DADOS")
        print("-" * 40)
        
        try:
            # Aulas sem frequência
            result = self.supabase.table('aulas').select('*').eq('status_frequencia', 'VAZIA').gte('data_aula', '2025-07-04').lte('data_aula', '2025-12-31').execute()
            aulas_sem_freq = len(result.data) if result.data else 0
            
            # Aulas sem ATA
            result = self.supabase.table('aulas').select('*').eq('tem_ata', False).gte('data_aula', '2025-07-04').lte('data_aula', '2025-12-31').execute()
            aulas_sem_ata = len(result.data) if result.data else 0
            
            # Aulas com erro
            result = self.supabase.table('aulas').select('*').eq('status_frequencia', 'ERRO').gte('data_aula', '2025-07-04').lte('data_aula', '2025-12-31').execute()
            aulas_com_erro = len(result.data) if result.data else 0
            
            print(f"⚠️  Aulas sem frequência registrada: {aulas_sem_freq}")
            print(f"📝 Aulas sem ATA: {aulas_sem_ata}")
            print(f"❌ Aulas com erro na coleta: {aulas_com_erro}")
            
            # Congregações com baixa atividade
            result = self.supabase.table('view_estatisticas_congregacao').select('*').lt('total_aulas', 5).execute()
            cong_baixa_atividade = len(result.data) if result.data else 0
            
            print(f"📉 Congregações com menos de 5 aulas: {cong_baixa_atividade}")
            
            return {
                'aulas_sem_frequencia': aulas_sem_freq,
                'aulas_sem_ata': aulas_sem_ata,
                'aulas_com_erro': aulas_com_erro,
                'congregacoes_baixa_atividade': cong_baixa_atividade
            }
            
        except Exception as e:
            print(f"❌ Erro na análise de qualidade: {e}")
            return None
    
    def exportar_dados_excel(self, arquivo="dados_musical_2sem2025.xlsx"):
        """Exporta todos os dados para Excel"""
        try:
            print(f"\n📤 EXPORTANDO DADOS PARA {arquivo}")
            print("-" * 40)
            
            with pd.ExcelWriter(arquivo, engine='openpyxl') as writer:
                
                # Aba 1: Estatísticas Gerais
                stats_result = self.supabase.rpc('get_estatisticas_gerais').execute()
                if stats_result.data:
                    stats = stats_result.data[0]
                    df_stats = pd.DataFrame([stats])
                    df_stats.to_excel(writer, sheet_name='Estatísticas Gerais', index=False)
                    print("✅ Estatísticas gerais exportadas")
                
                # Aba 2: Por Congregação
                result = self.supabase.table('view_estatisticas_congregacao').select('*').execute()
                if result.data:
                    df_cong = pd.DataFrame(result.data)
                    df_cong.to_excel(writer, sheet_name='Por Congregação', index=False)
                    print("✅ Dados por congregação exportados")
                
                # Aba 3: Por Curso
                result = self.supabase.table('view_estatisticas_curso').select('*').execute()
                if result.data:
                    df_curso = pd.DataFrame(result.data)
                    df_curso.to_excel(writer, sheet_name='Por Curso', index=False)
                    print("✅ Dados por curso exportados")
                
                # Aba 4: Evolução Mensal
                result = self.supabase.table('view_estatisticas_mensais').select('*').execute()
                if result.data:
                    df_mensal = pd.DataFrame(result.data)
                    df_mensal.to_excel(writer, sheet_name='Evolução Mensal', index=False)
                    print("✅ Evolução mensal exportada")
                
                # Aba 5: Detalhes das Aulas
                result = self.supabase.table('view_estatisticas_aulas').select('*').order('data_aula', desc=True).limit(1000).execute()
                if result.data:
                    df_aulas = pd.DataFrame(result.data)
                    df_aulas.to_excel(writer, sheet_name='Detalhes Aulas', index=False)
                    print("✅ Detalhes das aulas exportados")
            
            print(f"🎉 Exportação concluída: {arquivo}")
            return arquivo
            
        except Exception as e:
            print(f"❌ Erro na exportação: {e}")
            return None
    
    def gerar_graficos(self):
        """Gera gráficos de análise"""
        try:
            print(f"\n📊 GERANDO GRÁFICOS DE ANÁLISE")
            print("-" * 40)
            
            # Configurar estilo dos gráficos
            plt.style.use('seaborn-v0_8')
            sns.set_palette("husl")
            
            # Criar figura com subplots
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle('Análise Musical - 2º Semestre 2025', fontsize=16, fontweight='bold')
            
            # Gráfico 1: Top 10 Congregações por Aulas
            result = self.supabase.table('view_estatisticas_congregacao').select('*').order('total_aulas', desc=True).limit(10).execute()
            if result.data:
                df_cong = pd.DataFrame(result.data)
                axes[0,0].barh(df_cong['congregacao'][::-1], df_cong['total_aulas'][::-1])
                axes[0,0].set_title('Top 10 Congregações - Total de Aulas')
                axes[0,0].set_xlabel('Total de Aulas')
            
            # Gráfico 2: Evolução Mensal de Aulas
            result = self.supabase.table('view_estatisticas_mensais').select('*').order('mes').execute()
            if result.data:
                df_mensal = pd.DataFrame(result.data)
                meses = [self.obter_nome_mes(m).split()[0] for m in df_mensal['mes_texto']]
                axes[0,1].plot(meses, df_mensal['total_aulas'], marker='o', linewidth=2, markersize=8)
                axes[0,1].set_title('Evolução Mensal - Total de Aulas')
                axes[0,1].set_ylabel('Total de Aulas')
                axes[0,1].tick_params(axis='x', rotation=45)
            
            # Gráfico 3: Top 10 Cursos por Presença
            result = self.supabase.table('view_estatisticas_curso').select('*').order('percentual_presenca', desc=True).limit(10).execute()
            if result.data:
                df_curso = pd.DataFrame(result.data)
                curso_labels = [f"{row['curso'][:15]}..." if len(row['curso']) > 15 else row['curso'] for _, row in df_curso.iterrows()]
                axes[1,0].bar(range(len(curso_labels)), df_curso['percentual_presenca'])
                axes[1,0].set_title('Top 10 Cursos - % Presença')
                axes[1,0].set_ylabel('% Presença')
                axes[1,0].set_xticks(range(len(curso_labels)))
                axes[1,0].set_xticklabels(curso_labels, rotation=45, ha='right')
            
            # Gráfico 4: Distribuição de Presença por Mês
            result = self.supabase.table('view_estatisticas_mensais').select('*').order('mes').execute()
            if result.data:
                df_mensal = pd.DataFrame(result.data)
                meses = [self.obter_nome_mes(m).split()[0] for m in df_mensal['mes_texto']]
                axes[1,1].bar(meses, df_mensal['percentual_presenca'], color='green', alpha=0.7)
                axes[1,1].set_title('% Presença por Mês')
                axes[1,1].set_ylabel('% Presença')
                axes[1,1].tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            
            # Salvar gráfico
            arquivo_grafico = f"graficos_musical_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            plt.savefig(arquivo_grafico, dpi=300, bbox_inches='tight')
            print(f"✅ Gráficos salvos em: {arquivo_grafico}")
            
            # Mostrar gráfico
            plt.show()
            
            return arquivo_grafico
            
        except Exception as e:
            print(f"❌ Erro ao gerar gráficos: {e}")
            return None
    
    def buscar_congregacao_especifica(self):
        """Busca dados de uma congregação específica"""
        try:
            # Listar congregações disponíveis
            result = self.supabase.table('view_estatisticas_congregacao').select('congregacao').execute()
            if result.data:
                congregacoes = [item['congregacao'] for item in result.data]
                
                print(f"\n🏛️ CONGREGAÇÕES DISPONÍVEIS:")
                print("-" * 40)
                for i, cong in enumerate(congregacoes[:20], 1):  # Mostrar apenas as 20 primeiras
                    print(f"{i:2d}. {cong}")
                
                if len(congregacoes) > 20:
                    print(f"... e mais {len(congregacoes) - 20} congregações")
                
                print(f"\nTotal: {len(congregacoes)} congregações")
                
                # Solicitar entrada do usuário
                congregacao = input("\nDigite o nome da congregação (ou parte dele): ").strip()
                
                if congregacao:
                    # Buscar congregação
                    result = self.supabase.table('view_estatisticas_congregacao').select('*').ilike('congregacao', f'%{congregacao}%').execute()
                    
                    if result.data:
                        print(f"\n🔍 RESULTADOS PARA: '{congregacao}'")
                        print("-" * 50)
                        
                        for cong in result.data:
                            print(f"🏛️ {cong['congregacao']}")
                            print(f"   📚 Total de Aulas: {cong['total_aulas']}")
                            print(f"   📝 Aulas com ATA: {cong['aulas_com_ata']}")
                            print(f"   👥 Total Frequências: {cong['total_frequencias']}")
                            print(f"   📊 % Presença: {cong['percentual_presenca_geral']:.1f}%")
                            print()
                    else:
                        print(f"⚠️ Nenhuma congregação encontrada com '{congregacao}'")
                        
        except Exception as e:
            print(f"❌ Erro na busca: {e}")
    
    def gerar_relatorio_completo(self):
        """Gera relatório completo de análise"""
        print(f"\n🎯 RELATÓRIO COMPLETO - MUSICAL 2º SEMESTRE 2025")
        print(f"📅 Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        print("=" * 60)
        
        # 1. Estatísticas Gerais
        stats = self.obter_estatisticas_gerais()
        
        # 2. Top Congregações
        self.relatorio_por_congregacao(10)
        
        # 3. Top Cursos
        self.relatorio_por_curso(10)
        
        # 4. Evolução Mensal
        self.evolucao_mensal()
        
        # 5. Análise de Qualidade
        problemas = self.identificar_problemas()
        
        # 6. Exportar dados
        arquivo = self.exportar_dados_excel()
        
        # 7. Gerar gráficos
        arquivo_grafico = self.gerar_graficos()
        
        print(f"\n✅ RELATÓRIO COMPLETO FINALIZADO!")
        print(f"📊 Dados exportados para: {arquivo}")
        print(f"📈 Gráficos salvos em: {arquivo_grafico}")
        print("-" * 60)
        
        return {
            'estatisticas': stats,
            'problemas': problemas,
            'arquivo_excel': arquivo,
            'arquivo_grafico': arquivo_grafico
        }

def main():
    """Função principal"""
    analytics = MusicalAnalytics()
    
    # Menu interativo
    while True:
        print(f"\n📊 MENU - MUSICAL ANALYTICS")
        print("-" * 30)
        print("1. 📈 Estatísticas Gerais")
        print("2. 🏛️  Relatório por Congregação")
        print("3. 🎵 Relatório por Curso")
        print("4. 📅 Evolução Mensal")
        print("5. 🔍 Análise de Qualidade")
        print("6. 📤 Exportar para Excel")
        print("7. 📊 Gerar Gráficos")
        print("8. 🔍 Buscar Congregação")
        print("9. 🎯 Relatório Completo")
        print("0. 🚪 Sair")
        
        try:
            opcao = input("\nEscolha uma opção (0-9): ").strip()
            
            if opcao == "1":
                analytics.obter_estatisticas_gerais()
            elif opcao == "2":
                analytics.relatorio_por_congregacao()
            elif opcao == "3":
                analytics.relatorio_por_curso()
            elif opcao == "4":
                analytics.evolucao_mensal()
            elif opcao == "5":
                analytics.identificar_problemas()
            elif opcao == "6":
                analytics.exportar_dados_excel()
            elif opcao == "7":
                analytics.gerar_graficos()
            elif opcao == "8":
                analytics.buscar_congregacao_especifica()
            elif opcao == "9":
                analytics.gerar_relatorio_completo()
            elif opcao == "0":
                print("\n👋 Obrigado por usar o Musical Analytics!")
                print("📊 Sistema desenvolvido para análise de dados musicais")
                break
            else:
                print("❌ Opção inválida! Escolha uma opção de 0 a 9.")
                
        except KeyboardInterrupt:
            print("\n\n👋 Sistema interrompido pelo usuário. Até logo!")
            break
        except Exception as e:
            print(f"❌ Erro inesperado: {e}")
            print("🔄 Retornando ao menu principal...")
        
        # Aguardar antes de mostrar o menu novamente
        input("\n⏸️  Pressione Enter para continuar...")

if __name__ == "__main__":
    main()
