"""
Script de teste para validar a aplicação antes da execução completa
"""

import logging
from config import IDS, PERIODOS, BASE_URL, FIXED_PARAMS
from api_client import APIClient

# Configurar logging para teste
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_single_request():
    """Testa uma única requisição para validar a conectividade"""
    logger.info("=== TESTE DE CONECTIVIDADE ===")
    
    # Testar com o primeiro ID e período
    test_id = IDS[0]
    test_periodo = PERIODOS[0]
    
    logger.info(f"Testando requisição para ID={test_id}, Período={test_periodo}")
    logger.info(f"URL: {BASE_URL}")
    logger.info(f"Parâmetros: {FIXED_PARAMS}")
    
    try:
        client = APIClient()
        
        # Fazer uma requisição de teste
        response = client.get_rentabilidade_data(test_id, test_periodo)
        
        if response:
            logger.info("✅ Teste de conectividade bem-sucedido!")
            logger.info(f"Status Code: {response.get('status_code')}")
            logger.info(f"Dados recebidos: {type(response.get('data'))}")
            
            # Mostrar estrutura dos dados
            data = response.get('data')
            if isinstance(data, dict):
                logger.info(f"Chaves disponíveis: {list(data.keys())}")
                
                # Verificar se contém a estrutura de rentabilidade esperada
                if "Rentabilidades" in data:
                    rentabilidades = data.get("Rentabilidades", [])
                    if isinstance(rentabilidades, list):
                        logger.info(f"✅ Estrutura de rentabilidade encontrada com {len(rentabilidades)} registros")
                        
                        # Mostrar exemplo do primeiro registro
                        if rentabilidades:
                            primeiro = rentabilidades[0]
                            logger.info(f"Exemplo de campos: {list(primeiro.keys())}")
                    else:
                        logger.warning("Campo 'Rentabilidades' não é uma lista")
                else:
                    logger.warning("Campo 'Rentabilidades' não encontrado na resposta")
                    
            elif isinstance(data, list):
                logger.info(f"Lista com {len(data)} itens")
            else:
                logger.info(f"Tipo de dado: {type(data)}")
                
        else:
            logger.error("❌ Falha no teste de conectividade")
            return False
            
    except Exception as e:
        logger.error(f"❌ Erro durante teste: {e}")
        return False
    
    finally:
        client.close()
    
    return True


def test_configuration():
    """Testa se as configurações estão corretas"""
    logger.info("=== VALIDAÇÃO DE CONFIGURAÇÃO ===")
    
    # Verificar se todos os IDs são números
    if not all(isinstance(id_val, int) for id_val in IDS):
        logger.error("❌ IDs devem ser números inteiros")
        return False
    
    # Verificar se todos os períodos são números de 1 a 8
    if not all(1 <= p <= 8 for p in PERIODOS):
        logger.error("❌ Períodos devem estar entre 1 e 8")
        return False
    
    # Verificar se a URL está configurada
    if not BASE_URL:
        logger.error("❌ URL base não configurada")
        return False
    
    # Verificar parâmetros fixos
    required_params = ["IdContaCorrente", "ProdutoSelecionado", "BenchmarkSelecionado", "MaximoPontosRetornados"]
    for param in required_params:
        if param not in FIXED_PARAMS:
            logger.error(f"❌ Parâmetro obrigatório ausente: {param}")
            return False
    
    logger.info("✅ Configuração válida")
    logger.info(f"Total de IDs: {len(IDS)}")
    logger.info(f"Total de períodos: {len(PERIODOS)}")
    logger.info(f"Total de requisições: {len(IDS) * len(PERIODOS)}")
    
    return True


def main():
    """Função principal do teste"""
    logger.info("Iniciando testes da aplicação...")
    
    # Testar configuração
    if not test_configuration():
        logger.error("❌ Falha na validação de configuração")
        return False
    
    # Testar conectividade
    if not test_single_request():
        logger.error("❌ Falha no teste de conectividade")
        return False
    
    logger.info("✅ Todos os testes passaram com sucesso!")
    logger.info("A aplicação está pronta para execução completa.")
    
    return True


if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 Testes concluídos com sucesso!")
        print("Execute 'python main.py' para iniciar a coleta completa de dados.")
    else:
        print("\n💥 Testes falharam!")
        print("Verifique as configurações e conectividade antes de executar a aplicação principal.")
    
    exit(0 if success else 1)
