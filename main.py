"""
Aplicação principal para coleta de dados de rentabilidade
"""

import logging
import time
from typing import List, Optional, Dict, Any
from config import IDS, PERIODOS, OUTPUT_FILENAME
from api_client import APIClient
from data_processor import DataProcessor
from data_combiner import DataCombiner
from csv_exporter import CSVExporter
from csv_comparator import CSVComparator

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


class RentabilidadeApp:
    """Aplicação principal para coleta de dados de rentabilidade"""
    
    def __init__(self):
        self.api_client = APIClient()
        self.data_processor = DataProcessor()
        self.data_combiner = DataCombiner()
        self.csv_exporter = CSVExporter()
        self.csv_comparator = CSVComparator()
        
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
    
    def process_and_export_data(self, responses: List[Optional[Dict[str, Any]]]) -> str:
        """
        Processa e exporta os dados para CSV
        
        Args:
            responses: Lista de respostas da API
            
        Returns:
            Caminho do arquivo CSV criado
        """
        logger.info("Iniciando processamento dos dados")
        
        # Processar todas as respostas
        records = self.data_processor.process_all_responses(responses)
        
        if not records:
            logger.error("Nenhum dado foi processado com sucesso")
            return ""
        
        # Ordenar registros
        sorted_records = self.data_processor.sort_records(records)
        
        # Exportar para CSV
        csv_path = self.csv_exporter.export_to_csv(sorted_records)
        
        if csv_path:
            # Gerar resumos
            csv_summary = self.csv_exporter.get_csv_summary(sorted_records)
            combined_summary = self.data_combiner.get_combined_summary(sorted_records)
            
            logger.info("Resumo da exportação:")
            logger.info(f"  Total de registros: {csv_summary.get('total_registros', 0)}")
            logger.info(f"  IDs únicos: {csv_summary.get('ids_unicos', 0)}")
            logger.info(f"  Períodos únicos: {csv_summary.get('periodos_unicos', 0)}")
            
            logger.info("Resumo da combinação com banco de dados:")
            logger.info(f"  Registros com dados do banco: {combined_summary.get('registros_com_dados_banco', 0)}")
            logger.info(f"  Registros sem dados do banco: {combined_summary.get('registros_sem_dados_banco', 0)}")
            logger.info(f"  Total de valores de quota: {combined_summary.get('total_valores_quota', 0)}")
            if combined_summary.get('quota_valor_media'):
                logger.info(f"  Média dos valores de quota: {combined_summary.get('quota_valor_media', 0):.4f}")
        
        return csv_path
    
    def run(self) -> bool:
        try:
            logger.info("=== INICIANDO APLICAÇÃO DE COLETA DE DADOS ===")
            
            # 1. COLETAR DADOS DA API
            logger.info("=== ETAPA 1: COLETANDO DADOS DA API ===")
            responses = self.collect_all_data()
            successful_responses = sum(1 for r in responses if r is not None)
            logger.info(f"Respostas bem-sucedidas: {successful_responses}/{len(responses)}")

            api_records = self.data_processor.process_all_responses(responses)
            if not api_records:
                logger.error("Nenhum dado da API foi processado com sucesso")
                return False

            # Ordenar registros da API
            sorted_api_records = self.data_processor.sort_records(api_records)
            
            # 2. EXPORTAR CSV DA API
            logger.info("=== ETAPA 2: EXPORTANDO CSV DA API ===")
            api_csv_path = self.csv_exporter.export_to_csv(sorted_api_records, "dados_rentabilidade_api.csv")
            if not api_csv_path:
                logger.error("Falha ao exportar CSV da API")
                return False
            
            logger.info(f"✅ CSV da API exportado: {api_csv_path}")
            logger.info(f"📊 Total de registros da API: {len(sorted_api_records)}")

            # 3. BUSCAR DADOS DO BANCO
            logger.info("=== ETAPA 3: BUSCANDO DADOS DO BANCO ===")
            db_records = self.data_combiner.get_database_data(sorted_api_records)
            if not db_records:
                logger.warning("⚠️ Nenhum dado do banco foi retornado")
                # Mesmo sem dados do banco, a aplicação foi bem-sucedida
                logger.info("=== APLICAÇÃO EXECUTADA COM SUCESSO ===")
                logger.info(f"📁 CSV da API: {api_csv_path}")
                logger.info("📁 CSV do banco: Não gerado (sem dados)")
                return True

            # Ordenar registros do banco
            sorted_db_records = self.data_processor.sort_records(db_records)
            
            # 4. EXPORTAR CSV DO BANCO
            logger.info("=== ETAPA 4: EXPORTANDO CSV DO BANCO ===")
            db_csv_path = self.csv_exporter.export_to_csv(sorted_db_records, "dados_rentabilidade_banco.csv")
            if not db_csv_path:
                logger.error("Falha ao exportar CSV do banco")
                return False
            
            logger.info(f"✅ CSV do banco exportado: {db_csv_path}")
            logger.info(f"📊 Total de registros do banco: {len(sorted_db_records)}")

            # 5. RESUMO FINAL
            logger.info("=== RESUMO FINAL ===")
            logger.info(f"📊 Total de registros da API: {len(sorted_api_records)}")
            logger.info(f"📊 Total de registros do banco: {len(sorted_db_records)}")
            logger.info(f"📊 IDs únicos da API: {len(set(record['Id'] for record in sorted_api_records))}")
            logger.info(f"📊 Períodos únicos da API: {len(set(record['PeriodoSelecionado'] for record in sorted_api_records))}")
            
            logger.info("=== ARQUIVOS GERADOS ===")
            logger.info(f"📁 CSV da API: {api_csv_path}")
            logger.info(f"📁 CSV do banco: {db_csv_path}")

            # 5. COMPARAÇÃO ENTRE CSVs
            logger.info("=== ETAPA 5: COMPARANDO RENTABILIDADE API vs BANCO ===")
            comparison_csv_path = self.csv_comparator.run_comparison()
            
            if comparison_csv_path:
                logger.info(f"✅ Comparação exportada: {comparison_csv_path}")
                logger.info("=== ARQUIVOS FINAIS GERADOS ===")
                logger.info(f"📁 CSV da API: {api_csv_path}")
                logger.info(f"📁 CSV do banco: {db_csv_path}")
                logger.info(f"📁 CSV comparação: {comparison_csv_path}")
            else:
                logger.warning("⚠️ Falha na geração da comparação")
            
            logger.info("=== APLICAÇÃO EXECUTADA COM SUCESSO ===")
            return True
            
        except Exception as e:
            logger.error(f"Erro durante execução da aplicação: {e}")
            return False
        finally:
            self.api_client.close()
            logger.info("Cliente HTTP fechado")


def main():
    """Função principal"""
    app = RentabilidadeApp()
    success = app.run()
    
    if success:
        print(f"\n✅ Aplicação executada com sucesso!")
        print(f"📁 Arquivo CSV criado: {OUTPUT_FILENAME}")
        print(f"📊 Verifique o arquivo de log 'app.log' para detalhes")
    else:
        print(f"\n❌ Falha na execução da aplicação")
        print(f"📊 Verifique o arquivo de log 'app.log' para detalhes")
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
