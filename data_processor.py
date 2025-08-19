"""
Processador de dados para estruturar as respostas da API
"""

import logging
from typing import Dict, Any, List, Optional
from config import IDS, PERIODOS, PERIODO_DESCRICOES

logger = logging.getLogger(__name__)


class DataProcessor:
    """Processa e estrutura os dados recebidos da API"""
    
    @staticmethod
    def flatten_response_data(response_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Aplaina a estrutura de dados da resposta da API de rentabilidade
        
        Args:
            response_data: Dados da resposta da API
            
        Returns:
            Lista de registros aplainados com campos específicos de rentabilidade
        """
        try:
            id_value = response_data.get("id")
            periodo = response_data.get("periodo")
            data = response_data.get("data", {})
            
            if not data:
                logger.warning(f"Dados vazios para ID={id_value}, Período={periodo}")
                return []
            
            # Verificar se os dados contêm a estrutura de rentabilidade esperada
            if not isinstance(data, dict) or "Rentabilidades" not in data:
                logger.warning(f"Estrutura de dados inesperada para ID={id_value}, Período={periodo}")
                return []
            
            rentabilidades = data.get("Rentabilidades", [])
            if not isinstance(rentabilidades, list):
                logger.warning(f"Campo 'Rentabilidades' não é uma lista para ID={id_value}, Período={periodo}")
                return []
            
            records = []
            for rentabilidade in rentabilidades:
                if isinstance(rentabilidade, dict):
                    # Extrair apenas os campos específicos solicitados
                    record = {
                        "Id": id_value,
                        "PeriodoSelecionado": periodo,
                        "DescricaoPeriodo": PERIODO_DESCRICOES.get(periodo, f"Período {periodo}"),
                        "DataInicial": rentabilidade.get("DataInicial", ""),
                        "DataFinal": rentabilidade.get("DataFinal", ""),
                        "PercentualSobreBenchmark": rentabilidade.get("PercentualSobreBenchmark", ""),
                        "PercentualAcumuladoBenchmark": rentabilidade.get("PercentualAcumuladoBenchmark", ""),
                        "PercentualAcumulado": rentabilidade.get("PercentualAcumulado", ""),
                        "NominalAcumulado": rentabilidade.get("NominalAcumulado", "")
                    }
                    records.append(record)
                else:
                    logger.warning(f"Item de rentabilidade inválido para ID={id_value}, Período={periodo}")
            
            logger.info(f"Processados {len(records)} registros de rentabilidade para ID={id_value}, Período={periodo}")
            return records
                
        except Exception as e:
            logger.error(f"Erro ao processar dados para ID={response_data.get('id')}, Período={response_data.get('periodo')}: {e}")
            return []
    
    @staticmethod
    def process_all_responses(responses: List[Optional[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        Processa todas as respostas da API
        
        Args:
            responses: Lista de respostas da API
            
        Returns:
            Lista de todos os registros processados
        """
        all_records = []
        
        for response in responses:
            if response is not None:
                records = DataProcessor.flatten_response_data(response)
                all_records.extend(records)
            else:
                logger.warning("Resposta vazia encontrada")
        
        logger.info(f"Total de {len(all_records)} registros processados")
        return all_records
    
    @staticmethod
    def sort_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Ordena os registros por Id e PeriodoSelecionado
        
        Args:
            records: Lista de registros para ordenar
            
        Returns:
            Lista ordenada
        """
        try:
            # Ordenar primeiro por Id, depois por PeriodoSelecionado
            sorted_records = sorted(records, key=lambda x: (x.get("Id", 0), x.get("PeriodoSelecionado", 0)))
            logger.info("Registros ordenados com sucesso")
            return sorted_records
        except Exception as e:
            logger.error(f"Erro ao ordenar registros: {e}")
            return records
