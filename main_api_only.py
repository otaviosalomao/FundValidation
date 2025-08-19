"""
Aplicação principal para coleta de dados de rentabilidade (APENAS API)
Esta versão funciona sem conexão com banco de dados
"""

import logging
import time
from typing import List, Optional, Dict, Any
from config import IDS, PERIODOS, OUTPUT_FILENAME
from api_client import APIClient
from data_processor import DataProcessor
from csv_exporter import CSVExporter

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RentabilidadeAppAPIOnly:
    """Aplicação principal para coleta de dados de rentabilidade (apenas API)"""
    
    def __init__(self):
        self.api_client = APIClient()
        self.data_processor = DataProcessor()
        self.csv_exporter = CSVExporter()
        
    def collect_all_data(self) -> List[Optional[Dict[str, Any]]]:
        """
        Coleta todos os dados da API para todas as combinações de ID e período
        
        Returns:
            Lista de todas as respostas da API
        """
        responses = []
        total_requests = len(IDS) * len(PERIODOS)
        current_request = 0
        
        logger.info(f"Iniciando coleta de dados para {len(IDS)} IDs e {len(PERIODOS)} períodos")
        logger.info(f"Total de requisições: {total_requests}")
        
        for id_value in IDS:
            for periodo in PERIODOS:
                current_request += 1
                logger.info(f"Progresso: {current_request}/{total_requests} - ID: {id_value}, Período: {periodo}")
                
                response = self.api_client.get_rentabilidade_data(id_value, periodo)
                responses.append(response)
                
                # Pequena pausa para não sobrecarregar a API
                time.sleep(0.1)
        
        logger.info("Coleta de dados concluída")
        return responses
    
    def run(self) -> bool:
        """
        Executa a aplicação completa
        
        Returns:
            True se executou com sucesso, False caso contrário
        """
        try:
            logger.info("=== INICIANDO APLICAÇÃO DE COLETA DE DADOS (APENAS API) ===")
            
            # Coletar dados
            responses = self.collect_all_data()
            
            # Contar respostas bem-sucedidas
            successful_responses = sum(1 for r in responses if r is not None)
            logger.info(f"Respostas bem-sucedidas: {successful_responses}/{len(responses)}")
            
            # Processar dados da API
            api_records = self.data_processor.process_all_responses(responses)
            
            if not api_records:
                logger.error("Nenhum dado da API foi processado com sucesso")
                return False
            
            # Ordenar registros
            sorted_records = self.data_processor.sort_records(api_records)
            
            # Exportar para CSV
            csv_path = self.csv_exporter.export_to_csv(sorted_records, "dados_rentabilidade_api_only.csv")
            
            if csv_path:
                # Gerar resumo
                csv_summary = self.csv_exporter.get_csv_summary(sorted_records)
                
                logger.info("Resumo da exportação:")
                logger.info(f"  Total de registros: {csv_summary.get('total_registros', 0)}")
                logger.info(f"  IDs únicos: {csv_summary.get('ids_unicos', 0)}")
                logger.info(f"  Períodos únicos: {csv_summary.get('periodos_unicos', 0)}")
                
                logger.info(f"=== APLICAÇÃO EXECUTADA COM SUCESSO ===")
                logger.info(f"Arquivo CSV criado: {csv_path}")
                return True
            else:
                logger.error("=== FALHA NA EXPORTAÇÃO ===")
                return False
                
        except Exception as e:
            logger.error(f"Erro durante execução da aplicação: {e}")
            return False
        
        finally:
            # Sempre fechar o cliente HTTP
            self.api_client.close()
            logger.info("Cliente HTTP fechado")


def main():
    """Função principal"""
    app = RentabilidadeAppAPIOnly()
    success = app.run()
    
    if success:
        print(f"\n✅ Aplicação executada com sucesso!")
        print(f"📁 Arquivo CSV criado: dados_rentabilidade_api_only.csv")
        print(f"📊 Verifique o arquivo de log 'app.log' para detalhes")
    else:
        print(f"\n❌ Falha na execução da aplicação")
        print(f"📊 Verifique o arquivo de log 'app.log' para detalhes")
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
