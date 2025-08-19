"""
Script para testar a conexão com o banco de dados SQL Server
"""

import logging
from config import DB_CONNECTION_STRING, IDS, PERIODO_MAPPING
from database_client import DatabaseClient

# Configurar logging para teste
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_database_connection():
    """Testa a conexão com o banco de dados"""
    logger.info("=== TESTE DE CONEXÃO COM BANCO DE DADOS ===")
    
    try:
        db_client = DatabaseClient()
        
        # Testar conexão
        if db_client.connect():
            logger.info("✅ Conexão com banco de dados estabelecida com sucesso")
            
            # Testar uma query simples
            test_id = IDS[0]  # Primeiro ID da lista
            test_periodo = list(PERIODO_MAPPING.keys())[0]  # Primeiro período
            
            logger.info(f"Testando query para ID={test_id}, Período={test_periodo}")
            
            results = db_client.execute_fund_value_query(test_id, PERIODO_MAPPING[test_periodo])
            
            if results:
                logger.info(f"✅ Query executada com sucesso: {len(results)} registros retornados")
                
                # Mostrar primeiro resultado
                first_result = results[0]
                logger.info("Primeiro resultado:")
                for key, value in first_result.items():
                    logger.info(f"  {key}: {value}")
                
                # Mostrar estatísticas
                quota_values = [r.get("QuotaValue") for r in results if r.get("QuotaValue")]
                if quota_values:
                    logger.info(f"Estatísticas dos valores de quota:")
                    logger.info(f"  Total de valores: {len(quota_values)}")
                    logger.info(f"  Valor mínimo: {min(quota_values)}")
                    logger.info(f"  Valor máximo: {max(quota_values)}")
                    logger.info(f"  Valor médio: {sum(quota_values) / len(quota_values):.4f}")
                
            else:
                logger.warning("⚠️ Query executada mas nenhum resultado retornado")
            
            # Fechar conexão
            db_client.disconnect()
            return True
            
        else:
            logger.error("❌ Falha ao conectar com banco de dados")
            return False
            
    except Exception as e:
        logger.error(f"❌ Erro durante teste de banco: {e}")
        return False


def test_period_mapping():
    """Testa o mapeamento de períodos"""
    logger.info("\n=== TESTE DE MAPEAMENTO DE PERÍODOS ===")
    
    logger.info("Mapeamento configurado:")
    for api_period, db_period in PERIODO_MAPPING.items():
        logger.info(f"  Período API {api_period} → Período Banco {db_period}")
    
    # Verificar se todos os períodos estão mapeados
    api_periods = set(range(1, 9))  # 1 a 8
    mapped_periods = set(PERIODO_MAPPING.keys())
    
    missing_periods = api_periods - mapped_periods
    if missing_periods:
        logger.warning(f"⚠️ Períodos não mapeados: {missing_periods}")
    else:
        logger.info("✅ Todos os períodos da API estão mapeados")
    
    return len(missing_periods) == 0


def test_connection_string():
    """Testa se a string de conexão está configurada"""
    logger.info("\n=== TESTE DE STRING DE CONEXÃO ===")
    
    if not DB_CONNECTION_STRING:
        logger.error("❌ String de conexão não configurada")
        return False
    
    logger.info("✅ String de conexão configurada")
    logger.info(f"Servidor: {DB_CONNECTION_STRING.split('Server=')[1].split(';')[0] if 'Server=' in DB_CONNECTION_STRING else 'N/A'}")
    logger.info(f"Database: {DB_CONNECTION_STRING.split('Database=')[1].split(';')[0] if 'Database=' in DB_CONNECTION_STRING else 'N/A'}")
    
    return True


def main():
    """Função principal do teste"""
    logger.info("Iniciando testes de banco de dados...\n")
    
    # Testar string de conexão
    if not test_connection_string():
        logger.error("❌ Falha na validação da string de conexão")
        return False
    
    # Testar mapeamento de períodos
    if not test_period_mapping():
        logger.error("❌ Falha na validação do mapeamento de períodos")
        return False
    
    # Testar conexão com banco
    if not test_database_connection():
        logger.error("❌ Falha no teste de conexão com banco de dados")
        return False
    
    logger.info("\n🎉 Todos os testes de banco de dados passaram com sucesso!")
    logger.info("A aplicação está pronta para combinar dados da API com dados do banco.")
    
    return True


if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 Testes de banco de dados concluídos com sucesso!")
        print("Execute 'python main.py' para iniciar a coleta completa de dados.")
    else:
        print("\n💥 Testes de banco de dados falharam!")
        print("Verifique as configurações de banco antes de executar a aplicação principal.")
    
    exit(0 if success else 1)
