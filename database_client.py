"""
Cliente para conexão com banco de dados SQL Server
"""

import pyodbc
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from config import DB_CONNECTION_STRING, PERIODO_MAPPING, CACHE_ENABLED, CACHE_TTL_HOURS_DB
from cache_manager import CacheManager

logger = logging.getLogger(__name__)


class DatabaseClient:
    """Cliente para conexão com banco de dados SQL Server"""
    
    def __init__(self):
        self.connection_string = DB_CONNECTION_STRING
        self.connection = None
        self.cache_manager = CacheManager() if CACHE_ENABLED else None
        
    def connect(self) -> bool:
        """
        Estabelece conexão com o banco de dados
        
        Returns:
            True se conectou com sucesso, False caso contrário
        """
        try:
            self.connection = pyodbc.connect(self.connection_string)
            logger.info("✅ Conexão com banco de dados estabelecida com sucesso")
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao conectar com banco de dados: {e}")
            return False
    
    def disconnect(self):
        """Fecha a conexão com o banco de dados"""
        if self.connection:
            self.connection.close()
            logger.info("Conexão com banco de dados fechada")
    
    def get_period_dates(self, periodo: int, reference_date: datetime = None) -> tuple:
        """
        Calcula as datas de início e fim baseado no período selecionado
        
        Args:
            periodo: Período do enum EPeriodo
            reference_date: Data de referência (padrão: hoje)
            
        Returns:
            Tupla com (data_inicio, data_fim)
        """
        if reference_date is None:
            reference_date = datetime.now()
        
        if periodo == 0:  # SemanaAtual
            start_date = reference_date - timedelta(days=reference_date.weekday())
            end_date = start_date + timedelta(days=6)
        elif periodo == 1:  # MesAtual
            start_date = reference_date.replace(day=1)
            if reference_date.month == 12:
                end_date = reference_date.replace(year=reference_date.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                end_date = reference_date.replace(month=reference_date.month + 1, day=1) - timedelta(days=1)
        elif periodo == 2:  # AnoAtual
            start_date = reference_date.replace(month=1, day=1)
            end_date = reference_date.replace(month=12, day=31)
        elif periodo == 3:  # DozeMeses
            start_date = reference_date - timedelta(days=365)
            end_date = reference_date
        elif periodo == 4:  # TresAnos
            start_date = reference_date - timedelta(days=3*365)
            end_date = reference_date
        elif periodo == 5:  # DesdeDoisMilDezenove
            start_date = datetime(2019, 1, 1)
            end_date = reference_date
        elif periodo == 6:  # DoisAnos
            start_date = reference_date - timedelta(days=2*365)
            end_date = reference_date
        elif periodo == 7:  # TrintaDias
            start_date = reference_date - timedelta(days=30)
            end_date = reference_date
        elif periodo == 8:  # Inicio
            start_date = datetime(1900, 1, 1)
            end_date = reference_date
        else:
            # Período padrão: mês atual
            start_date = reference_date.replace(day=1)
            if reference_date.month == 12:
                end_date = reference_date.replace(year=reference_date.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                end_date = reference_date.replace(month=reference_date.month + 1, day=1) - timedelta(days=1)
        
        return start_date, end_date
    
    def _get_period_description(self, periodo: int) -> str:
        """
        Retorna a descrição do período baseado no enum EPeriodo
        
        Args:
            periodo: Número do período (0-8)
            
        Returns:
            Descrição do período
        """
        period_descriptions = {
            0: "SemanaAtual",
            1: "MesAtual", 
            2: "AnoAtual",
            3: "DozeMeses",
            4: "TresAnos",
            5: "DesdeDoisMilDezenove",
            6: "DoisAnos",
            7: "TrintaDias",
            8: "Inicio"
        }
        
        return period_descriptions.get(periodo, "Desconhecido")
    
    def execute_fund_value_query(self, financial_instrument_id: int, periodo: int) -> List[Dict[str, Any]]:
        """
        Executa a query de histórico de valores dos fundos
        
        Args:
            financial_instrument_id: ID do instrumento financeiro
            periodo: Período selecionado (0-8)
            
        Returns:
            Lista de registros retornados pela query
        """
        # Verificar cache primeiro (incluindo informação da âncora na chave)
        if self.cache_manager:
            cached_data = self.cache_manager.get(
                "db_with_anchor", 
                financial_instrument_id=financial_instrument_id, 
                periodo=periodo
            )
            if cached_data is not None:
                logger.info(f"📦 Dados do banco recuperados do cache para ID={financial_instrument_id}, Período={periodo}")
                return cached_data
        
        if not self.connection:
            if not self.connect():
                return []
        
        try:
            # Calcular datas baseado no período
            start_date, end_date = self.get_period_dates(periodo)
            
            # Ajustar data inicial para incluir o dia anterior como âncora
            # Isso garante que tenhamos o valor base para calcular a rentabilidade do primeiro dia
            anchor_date = start_date - timedelta(days=1)
            logger.info(f"Data de âncora: {anchor_date.strftime('%Y-%m-%d')} (dia anterior ao início do período)")
            
            # Query SQL
            query = """
            SELECT
                FVH.[FinancialInstrumentFundValueHistoryId]
                ,FVH.[FinancialInstrumentId]
                ,FVH.[QuotaValue]
                ,FI.Name
                ,FVH.PositionDate
            FROM 
                [dbo].[FinancialInstrumentFundValueHistory] FVH
                INNER JOIN FinancialInstrument FI ON FI.FinancialInstrumentId = FVH.FinancialInstrumentId
                inner join (
                    SELECT
                        a.FinancialInstrumentId,
                        coalesce(MAX(b.PositionDate), MIN(a.PositionDate)) DT_INI
                    FROM 
                        FinancialInstrumentFundValueHistory a
                        left join FinancialInstrumentFundValueHistory b
                                    on b.FinancialInstrumentID = a.FinancialInstrumentID
                                    AND b.PositionDate < ?
                        WHERE
                            a.FinancialInstrumentID = ?                                        
                            AND a.PositionDate >= ?
                        GROUP BY
                            a.FinancialInstrumentId, b.FinancialInstrumentId
                ) SEL_MIN
                ON FVH.FinancialInstrumentID = SEL_MIN.FinancialInstrumentId
                inner join (
                    SELECT
                        FinancialInstrumentId,
                        MAX(PositionDate) DT_FIM
                    FROM 
                        FinancialInstrumentFundValueHistory
                    WHERE
                        FinancialInstrumentID = ?
                        AND PositionDate < ?
                    GROUP BY
                        FinancialInstrumentId
                ) SEL_MAX
                ON FVH.FinancialInstrumentID = SEL_MAX.FinancialInstrumentId
            WHERE FVH.[FinancialInstrumentId] = ?
            and fvh.PositionDate >= sel_min.dt_ini and fvh.PositionDate <= sel_max.DT_FIM
            ORDER BY FVH.PositionDate
            """
            
            # Parâmetros da query (usar anchor_date em vez de start_date para buscar dados)
            params = [
                anchor_date,  # Usar data de âncora para buscar desde o dia anterior
                financial_instrument_id,
                anchor_date,  # Usar data de âncora
                financial_instrument_id,
                end_date,
                financial_instrument_id
            ]
            
            logger.info(f"Executando query para ID={financial_instrument_id}, Período={periodo}")
            logger.info(f"Período: {start_date.strftime('%Y-%m-%d')} até {end_date.strftime('%Y-%m-%d')}")
            logger.info(f"Buscando dados desde: {anchor_date.strftime('%Y-%m-%d')} (inclui âncora)")
            
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            
            # Obter nomes das colunas
            columns = [column[0] for column in cursor.description]
            
            # Converter resultados para lista de dicionários
            results = []
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                # Adicionar informações do período
                record['PeriodoSelecionado'] = periodo
                record['DescricaoPeriodo'] = self._get_period_description(periodo)
                record['DataInicioPeriodo'] = start_date.strftime('%Y-%m-%d')
                record['DataFimPeriodo'] = end_date.strftime('%Y-%m-%d')
                results.append(record)
            
            cursor.close()
            
            logger.info(f"✅ Query executada com sucesso: {len(results)} registros retornados")
            
            # Salvar no cache (usar chave diferente para indicar que inclui âncora)
            if self.cache_manager:
                self.cache_manager.set(
                    "db_with_anchor", 
                    results, 
                    ttl_hours=CACHE_TTL_HOURS_DB,
                    financial_instrument_id=financial_instrument_id, 
                    periodo=periodo
                )
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Erro ao executar query para ID={financial_instrument_id}, Período={periodo}: {e}")
            return []
    
    def get_all_fund_values(self, financial_instrument_ids: List[int], periodos: List[int]) -> List[Dict[str, Any]]:
        """
        Executa a query para todos os IDs e períodos
        
        Args:
            financial_instrument_ids: Lista de IDs dos instrumentos financeiros
            periodos: Lista de períodos para consultar
            
        Returns:
            Lista de todos os registros retornados
        """
        all_results = []
        
        for instrument_id in financial_instrument_ids:
            for periodo in periodos:
                results = self.execute_fund_value_query(instrument_id, periodo)
                all_results.extend(results)
                
                # Pequena pausa para não sobrecarregar o banco
                import time
                time.sleep(0.1)
        
        logger.info(f"Total de registros retornados do banco: {len(all_results)}")
        return all_results
    
    def close(self):
        """
        Fecha a conexão com o banco de dados
        """
        if hasattr(self, 'connection') and self.connection:
            try:
                self.connection.close()
                logger.info("Conexão com banco de dados fechada")
            except Exception as e:
                logger.warning(f"Erro ao fechar conexão: {e}")
        else:
            logger.info("Nenhuma conexão ativa para fechar")
