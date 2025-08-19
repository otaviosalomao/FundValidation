"""
Comparador de CSVs - compara rentabilidade da API com banco de dados
"""

import pandas as pd
import logging
from typing import Dict, List, Any
from config import OUTPUT_FILENAME_API, OUTPUT_FILENAME_BANCO, PERIODO_DESCRICOES
from datetime import datetime

logger = logging.getLogger(__name__)


class CSVComparator:
    """Compara dados de rentabilidade entre API e banco de dados"""
    
    def __init__(self):
        self.tolerance = 0.01  # 1% de tolerância (0.01 em escala decimal)
    
    def load_csv_data(self, filename: str) -> pd.DataFrame:
        """
        Carrega dados de um arquivo CSV
        
        Args:
            filename: Nome do arquivo CSV
            
        Returns:
            DataFrame com os dados carregados
        """
        try:
            df = pd.read_csv(filename)
            logger.info(f"CSV carregado: {filename} - {len(df)} registros")
            return df
        except Exception as e:
            logger.error(f"Erro ao carregar CSV {filename}: {e}")
            return pd.DataFrame()
    
    def get_all_combinations(self, api_df: pd.DataFrame, bank_df: pd.DataFrame) -> pd.DataFrame:
        """
        Gera todas as combinações possíveis de ID e Período para garantir validação completa
        
        Args:
            api_df: DataFrame com dados da API
            bank_df: DataFrame com dados do banco
            
        Returns:
            DataFrame com todas as combinações
        """
        try:
            # Obter todos os IDs únicos de ambas as fontes
            api_ids = set(api_df['Id'].unique())
            bank_ids = set(bank_df['FinancialInstrumentId'].unique())
            all_ids = list(api_ids.union(bank_ids))
            
            # Obter todos os períodos únicos de ambas as fontes
            api_periodos = set(api_df['PeriodoSelecionado'].unique())
            bank_periodos = set(bank_df['PeriodoSelecionado'].unique())
            all_periodos = list(api_periodos.union(bank_periodos))
            
            # Criar todas as combinações possíveis
            combinations = []
            for id_val in all_ids:
                for periodo in all_periodos:
                    combinations.append({
                        'Id': id_val,
                        'PeriodoSelecionado': periodo,
                        'DescricaoPeriodo': PERIODO_DESCRICOES.get(periodo, "Desconhecido")
                    })
            
            combinations_df = pd.DataFrame(combinations)
            logger.info(f"Geradas {len(combinations_df)} combinações de ID/Período para validação")
            return combinations_df
            
        except Exception as e:
            logger.error(f"Erro ao gerar combinações: {e}")
            return pd.DataFrame()
    
    def get_api_value(self, api_df: pd.DataFrame, id_val: int, periodo: int) -> float:
        """
        Obtém o valor da API para um ID e período específicos
        
        Args:
            api_df: DataFrame com dados da API
            id_val: ID do instrumento
            periodo: Período selecionado
            
        Returns:
            Valor da API ou 0 se não encontrado
        """
        try:
            # Filtrar por ID e período
            filtered = api_df[(api_df['Id'] == id_val) & (api_df['PeriodoSelecionado'] == periodo)]
            
            if filtered.empty:
                return 0.0
            
            # Converter para numérico e pegar o último valor
            filtered['PercentualAcumulado'] = pd.to_numeric(filtered['PercentualAcumulado'], errors='coerce')
            last_value = filtered['PercentualAcumulado'].iloc[-1]
            
            return float(last_value) if pd.notna(last_value) else 0.0
            
        except Exception as e:
            logger.warning(f"Erro ao obter valor da API para ID={id_val}, Período={periodo}: {e}")
            return 0.0
    
    def get_bank_value(self, bank_df: pd.DataFrame, id_val: int, periodo: int) -> float:
        """
        Obtém o valor do banco para um ID e período específicos
        
        Args:
            bank_df: DataFrame com dados do banco
            id_val: ID do instrumento
            periodo: Período selecionado
            
        Returns:
            Valor do banco ou 0 se não encontrado
        """
        try:
            # Filtrar por FinancialInstrumentId e período
            filtered = bank_df[(bank_df['FinancialInstrumentId'] == id_val) & (bank_df['PeriodoSelecionado'] == periodo)]
            
            if filtered.empty:
                return 0.0
            
            # Converter para numérico e pegar o último valor
            filtered['PorcentagemRentabilidadeAcumulada'] = pd.to_numeric(filtered['PorcentagemRentabilidadeAcumulada'], errors='coerce')
            last_value = filtered['PorcentagemRentabilidadeAcumulada'].iloc[-1]
            
            return float(last_value) if pd.notna(last_value) else 0.0
            
        except Exception as e:
            logger.warning(f"Erro ao obter valor do banco para ID={id_val}, Período={periodo}: {e}")
            return 0.0
    
    def compare_all_combinations(self, api_df: pd.DataFrame, bank_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compara todos os registros individualmente para garantir validação completa
        Considera correspondência exata entre PositionDate do banco e DataFinal da API
        Usa operações vetorizadas para melhor performance
        
        Args:
            api_df: DataFrame com dados da API
            bank_df: DataFrame com dados do banco
            
        Returns:
            DataFrame com comparação completa incluindo todos os registros individuais
        """
        try:
            logger.info("Iniciando comparação otimizada...")
            
            # Preparar dados para merge
            # Converter DataFinal da API para formato compatível com PositionDate do banco
            api_df_clean = api_df.copy()
            api_df_clean['DataFinal_clean'] = api_df_clean['DataFinal'].apply(self._normalize_date)
            
            bank_df_clean = bank_df.copy()
            bank_df_clean['PositionDate_clean'] = bank_df_clean['PositionDate'].apply(self._normalize_date)
            
            # Fazer merge baseado em Id, PeriodoSelecionado e data
            logger.info("Fazendo merge dos dados...")
            merged_df = pd.merge(
                bank_df_clean,
                api_df_clean,
                left_on=['FinancialInstrumentId', 'PeriodoSelecionado', 'PositionDate_clean'],
                right_on=['Id', 'PeriodoSelecionado', 'DataFinal_clean'],
                how='left',
                suffixes=('_banco', '_api')
            )
            
            logger.info(f"Merge concluído: {len(merged_df)} registros")
            
            # Processar resultados
            results = []
            
            for _, row in merged_df.iterrows():
                # Dados do banco
                id_val = row['FinancialInstrumentId']
                periodo = row['PeriodoSelecionado']
                position_date = row['PositionDate']
                bank_value = row['PorcentagemRentabilidadeAcumulada']
                
                # Dados da API
                if pd.notna(row['PercentualAcumulado']):
                    api_value = row['PercentualAcumulado']
                    api_data_final = row['DataFinal']
                    tipo_registro = "Ambas (Data exata)"
                else:
                    # Buscar valor da API para o mesmo ID e período (sem correspondência de data)
                    api_filtered = api_df[(api_df['Id'] == id_val) & (api_df['PeriodoSelecionado'] == periodo)]
                    if not api_filtered.empty:
                        api_value = api_filtered['PercentualAcumulado'].iloc[-1]
                        api_data_final = api_filtered['DataFinal'].iloc[-1]
                        tipo_registro = "Ambas (Data não exata)"
                    else:
                        api_value = 0.0
                        api_data_final = ""
                        tipo_registro = "Só Banco"
                
                # Calcular diferença
                diferenca = abs(api_value - bank_value)
                
                # Determinar status (tolerância de 1%)
                status = 'OK' if diferenca < self.tolerance else 'ERRO'
                
                # Adicionar resultado
                results.append({
                    'Id': id_val,
                    'Periodo': periodo,
                    'DescricaoPeriodo': PERIODO_DESCRICOES.get(periodo, "Desconhecido"),
                    'PositionDate_Banco': position_date,
                    'DataFinal_API': api_data_final,
                    'QuotaValue': row.get('QuotaValue', ''),
                    'Rentabilidade': row.get('Rentabilidade', ''),
                    'RentabilidadeAcumulada': row.get('RentabilidadeAcumulada', ''),
                    'PercentualAcumulado_API': api_value,
                    'PercentualAcumulado_Banco': bank_value,
                    'Diferenca': diferenca,
                    'Status': status,
                    'TipoRegistro': tipo_registro
                })
            
            # Adicionar registros da API que não têm correspondente no banco
            logger.info("Adicionando registros só da API...")
            api_used = set(merged_df['DataFinal'].dropna().unique())
            
            for _, api_record in api_df.iterrows():
                id_val = api_record['Id']
                periodo = api_record['PeriodoSelecionado']
                api_data_final = api_record.get('DataFinal', '')
                
                # Verificar se já foi processado no merge
                if api_data_final not in api_used:
                    # Verificar se existe no banco para o mesmo ID e período
                    bank_filtered = bank_df[(bank_df['FinancialInstrumentId'] == id_val) & 
                                           (bank_df['PeriodoSelecionado'] == periodo)]
                    
                    if bank_filtered.empty:
                        # Registro só existe na API
                        results.append({
                            'Id': id_val,
                            'Periodo': periodo,
                            'DescricaoPeriodo': PERIODO_DESCRICOES.get(periodo, "Desconhecido"),
                            'PositionDate_Banco': '',
                            'DataFinal_API': api_data_final,
                            'QuotaValue': '',
                            'Rentabilidade': '',
                            'RentabilidadeAcumulada': '',
                            'PercentualAcumulado_API': api_record['PercentualAcumulado'],
                            'PercentualAcumulado_Banco': 0.0,
                            'Diferenca': api_record['PercentualAcumulado'],
                            'Status': 'ERRO',
                            'TipoRegistro': 'Só API'
                        })
            
            # Criar DataFrame de resultados
            result_df = pd.DataFrame(results)
            
            # Ordenar por ID, Período e PositionDate
            result_df = result_df.sort_values(['Id', 'Periodo', 'PositionDate_Banco'])
            
            logger.info(f"Comparação registro a registro concluída: {len(result_df)} registros")
            
            # Estatísticas
            total_ok = len(result_df[result_df['Status'] == 'OK'])
            total_erro = len(result_df[result_df['Status'] == 'ERRO'])
            
            logger.info(f"Registros OK: {total_ok}")
            logger.info(f"Registros com ERRO: {total_erro}")
            logger.info(f"Taxa de sucesso: {(total_ok / len(result_df) * 100):.1f}%")
            
            # Estatísticas por tipo de registro
            for tipo in result_df['TipoRegistro'].unique():
                count = len(result_df[result_df['TipoRegistro'] == tipo])
                logger.info(f"Tipo '{tipo}': {count} registros")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Erro na comparação registro a registro: {e}")
            return pd.DataFrame()
    
    def _normalize_date(self, date_str: str) -> str:
        """
        Normaliza uma data para formato padrão para comparação
        
        Args:
            date_str: String de data em qualquer formato
            
        Returns:
            Data normalizada no formato YYYY-MM-DD
        """
        try:
            if pd.isna(date_str) or date_str == '':
                return ''
            
            if isinstance(date_str, str):
                # Tentar diferentes formatos
                for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        return dt.strftime('%Y-%m-%d')
                    except ValueError:
                        continue
            
            return str(date_str)
            
        except Exception as e:
            logger.warning(f"Erro ao normalizar data '{date_str}': {e}")
            return str(date_str)
    
    def export_comparison(self, comparison_df: pd.DataFrame, filename: str = "comparacao_rentabilidade.csv") -> str:
        """
        Exporta resultado da comparação para CSV
        
        Args:
            comparison_df: DataFrame com comparação
            filename: Nome do arquivo de saída
            
        Returns:
            Caminho do arquivo gerado
        """
        try:
            comparison_df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"Comparação exportada: {filename}")
            return filename
        except Exception as e:
            logger.error(f"Erro ao exportar comparação: {e}")
            return ""
    
    def run_comparison(self) -> str:
        """
        Executa comparação completa entre CSVs da API e banco
        
        Returns:
            Caminho do arquivo de comparação gerado
        """
        try:
            logger.info("=== INICIANDO COMPARAÇÃO ENTRE CSVs ===")
            
            # Carregar dados
            api_df = self.load_csv_data(OUTPUT_FILENAME_API)
            bank_df = self.load_csv_data(OUTPUT_FILENAME_BANCO)
            
            if api_df.empty:
                logger.error("Dados da API não encontrados")
                return ""
            
            if bank_df.empty:
                logger.error("Dados do banco não encontrados")
                return ""
            
            # Fazer comparação registro a registro
            comparison = self.compare_all_combinations(api_df, bank_df)
            
            if comparison.empty:
                logger.error("Falha na comparação dos dados")
                return ""
            
            # Exportar resultado
            output_file = self.export_comparison(comparison)
            
            logger.info("=== COMPARAÇÃO CONCLUÍDA ===")
            return output_file
            
        except Exception as e:
            logger.error(f"Erro durante comparação: {e}")
            return ""
