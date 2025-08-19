"""
Busca dados do banco de dados e exporta CSVs separados
"""

import logging
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from database_client import DatabaseClient
from csv_exporter import CSVExporter
from config import OUTPUT_FILENAME_API, OUTPUT_FILENAME_BANCO

logger = logging.getLogger(__name__)


class DataCombiner:
    """Busca dados do banco de dados e exporta CSVs separados"""

    def __init__(self):
        self.db_client = DatabaseClient()
        self.csv_exporter = CSVExporter()

    def get_database_data(self, api_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Busca dados do banco de dados para os IDs e períodos da API

        Args:
            api_records: Lista de registros da API

        Returns:
            Lista de registros do banco de dados
        """
        try:
            logger.info("Conectando ao banco de dados...")
            if not self.db_client.connect():
                logger.error("❌ Falha ao conectar com banco de dados")
                return []

            logger.info("✅ Conectado ao banco de dados com sucesso")

            # Obter IDs únicos dos registros da API
            api_ids = list(set(record["Id"] for record in api_records))
            api_periodos = list(set(record["PeriodoSelecionado"] for record in api_records))

            logger.info(f"Buscando dados do banco para {len(api_ids)} IDs e {len(api_periodos)} períodos...")

            # Buscar dados do banco
            db_records = self.db_client.get_all_fund_values(api_ids, api_periodos)

            if not db_records:
                logger.warning("⚠️ Nenhum dado do banco foi retornado")
                return []

            logger.info(f"✅ {len(db_records)} registros retornados do banco de dados")

            # Calcular rentabilidade para cada registro
            db_records_with_rentabilidade = self._calculate_rentabilidade(db_records)

            return db_records_with_rentabilidade

        except Exception as e:
            logger.error(f"❌ Erro ao buscar dados do banco: {e}")
            return []
        finally:
            self.db_client.close()

    def _calculate_rentabilidade(self, db_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calcula rentabilidade, rentabilidade acumulada e porcentagem para cada registro
        Usa o dia anterior ao início do período como âncora para cálculos

        Args:
            db_records: Lista de registros do banco

        Returns:
            Lista de registros com colunas de rentabilidade calculadas
        """
        if not db_records:
            return []

        # Agrupar por FinancialInstrumentId e PeriodoSelecionado
        grouped_records = {}
        for record in db_records:
            key = (record["FinancialInstrumentId"], record["PeriodoSelecionado"])
            if key not in grouped_records:
                grouped_records[key] = []
            grouped_records[key].append(record)

        # Calcular rentabilidade para cada grupo
        records_with_rentabilidade = []
        for key, records in grouped_records.items():
            # Ordenar por PositionDate
            sorted_records = sorted(records, key=lambda x: x["PositionDate"])

            if len(sorted_records) < 1:
                continue

            # Separar registros âncora dos registros do período
            # Âncora é o registro com data anterior ao início do período
            anchor_records = []
            period_records = []
            
            for record in sorted_records:
                position_date = record['PositionDate']
                data_inicio = record['DataInicioPeriodo']
                
                # Converter para datetime se necessário
                if isinstance(position_date, str):
                    # Tentar primeiro com formato completo (data + hora)
                    try:
                        position_date = datetime.strptime(position_date, '%Y-%m-%d %H:%M:%S').date()
                    except ValueError:
                        # Se falhar, tentar apenas com data
                        position_date = datetime.strptime(position_date, '%Y-%m-%d').date()
                elif hasattr(position_date, 'date'):
                    position_date = position_date.date()
                
                if isinstance(data_inicio, str):
                    data_inicio = datetime.strptime(data_inicio, '%Y-%m-%d').date()
                elif hasattr(data_inicio, 'date'):
                    data_inicio = data_inicio.date()
                
                # Se a data é anterior ao início do período, é âncora
                if position_date < data_inicio:
                    anchor_records.append(record)
                else:
                    period_records.append(record)
            
            # Obter valor de âncora
            anchor_quota = None
            if anchor_records:
                anchor_quota = float(anchor_records[0]["QuotaValue"])
                logger.debug(f"Âncora encontrada para ID={key[0]}, Período={key[1]}: {anchor_quota}")
            
            # Calcular rentabilidade para cada registro do período
            rentabilidade_acumulada_anterior = 0.0  # Inicializar rentabilidade acumulada
            
            for i in range(len(period_records)):
                record = period_records[i].copy()
                current_quota = float(record["QuotaValue"])

                if i == 0:
                    # Primeiro dia do período: usar quota de âncora (dia anterior ao início)
                    if anchor_quota is not None:
                        # Fórmula: (Valor atual / Valor âncora) - 1
                        rentabilidade = (current_quota / anchor_quota) - 1
                        rentabilidade_acumulada = rentabilidade
                    else:
                        # Se não há âncora, rentabilidade é 0
                        rentabilidade = 0.0
                        rentabilidade_acumulada = 0.0
                        logger.warning(f"Âncora não encontrada para ID={key[0]}, Período={key[1]}")
                else:
                    # Demais dias: sempre usar quota do dia anterior
                    previous_quota = float(period_records[i-1]["QuotaValue"])
                    # Fórmula: (Valor atual / Valor anterior) - 1
                    rentabilidade = (current_quota / previous_quota) - 1
                    
                    # Rentabilidade acumulada: ((rentabilidade acumulada anterior + 1) * (rentabilidade atual + 1)) - 1
                    rentabilidade_acumulada = ((rentabilidade_acumulada_anterior + 1) * (rentabilidade + 1)) - 1

                # Porcentagem da rentabilidade acumulada (rentabilidade acumulada * 100)
                porcentagem_rentabilidade_acumulada = rentabilidade_acumulada * 100

                record.update({
                    "Rentabilidade": rentabilidade,
                    "RentabilidadeAcumulada": rentabilidade_acumulada,
                    "PorcentagemRentabilidadeAcumulada": porcentagem_rentabilidade_acumulada
                })

                records_with_rentabilidade.append(record)
                
                # Atualizar rentabilidade acumulada anterior para próxima iteração
                rentabilidade_acumulada_anterior = rentabilidade_acumulada

        return records_with_rentabilidade



    def export_separate_csvs(self, api_records: List[Dict[str, Any]], db_records: List[Dict[str, Any]]) -> Tuple[str, str]:
        """
        Exporta dados da API e do banco em CSVs separados

        Args:
            api_records: Dados apenas da API
            db_records: Dados apenas do banco

        Returns:
            Tuple com (caminho_csv_api, caminho_csv_banco)
        """
        try:
            # Exportar dados da API
            api_csv_path = self.csv_exporter.export_to_csv(
                api_records,
                OUTPUT_FILENAME_API
            )

            # Exportar dados do banco
            db_csv_path = self.csv_exporter.export_to_csv(
                db_records,
                OUTPUT_FILENAME_BANCO
            )

            logger.info(f"✅ CSV da API exportado: {api_csv_path}")
            logger.info(f"✅ CSV do banco exportado: {db_csv_path}")

            return api_csv_path, db_csv_path

        except Exception as e:
            logger.error(f"❌ Erro ao exportar CSVs separados: {e}")
            return "", ""
