#!/usr/bin/env python3
"""
Script para gerenciar o cache da aplicação
"""

import argparse
import sys
import logging
from cache_manager import CacheManager
from config import CACHE_DIR

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Função principal do script de gerenciamento de cache"""
    parser = argparse.ArgumentParser(description='Gerenciar cache da aplicação')
    parser.add_argument(
        'action',
        choices=['info', 'clear', 'clear-expired', 'clear-api', 'clear-db'],
        help='Ação a ser executada'
    )
    parser.add_argument(
        '--id',
        type=int,
        help='ID específico para invalidar cache (usado com clear-api ou clear-db)'
    )
    parser.add_argument(
        '--periodo',
        type=int,
        help='Período específico para invalidar cache (usado com clear-api ou clear-db)'
    )

    args = parser.parse_args()

    try:
        cache_manager = CacheManager(cache_dir=CACHE_DIR)

        if args.action == 'info':
            show_cache_info(cache_manager)
        
        elif args.action == 'clear':
            clear_all_cache(cache_manager)
        
        elif args.action == 'clear-expired':
            clear_expired_cache(cache_manager)
        
        elif args.action == 'clear-api':
            clear_api_cache(cache_manager, args.id, args.periodo)
        
        elif args.action == 'clear-db':
            clear_db_cache(cache_manager, args.id, args.periodo)

    except Exception as e:
        logger.error(f"Erro ao executar ação '{args.action}': {e}")
        sys.exit(1)


def show_cache_info(cache_manager: CacheManager):
    """Mostra informações sobre o cache"""
    logger.info("=== INFORMAÇÕES DO CACHE ===")
    
    cache_info = cache_manager.get_cache_info()
    
    if not cache_info:
        logger.warning("Não foi possível obter informações do cache")
        return
    
    print(f"📁 Diretório: {cache_info['cache_dir']}")
    print(f"📊 Total de arquivos: {cache_info['total_files']}")
    print(f"✅ Arquivos válidos: {cache_info['valid_files']}")
    print(f"⏰ Arquivos expirados: {cache_info['expired_files']}")
    print(f"💾 Tamanho total: {cache_info['total_size_mb']} MB")
    
    if cache_info['total_files'] > 0:
        success_rate = (cache_info['valid_files'] / cache_info['total_files']) * 100
        print(f"📈 Taxa de cache válido: {success_rate:.1f}%")


def clear_all_cache(cache_manager: CacheManager):
    """Remove todos os arquivos de cache"""
    logger.info("=== LIMPANDO TODO O CACHE ===")
    
    removed_count = cache_manager.clear_all()
    
    if removed_count > 0:
        logger.info(f"✅ {removed_count} arquivos de cache removidos")
    else:
        logger.info("ℹ️ Nenhum arquivo de cache encontrado")


def clear_expired_cache(cache_manager: CacheManager):
    """Remove apenas arquivos de cache expirados"""
    logger.info("=== LIMPANDO CACHE EXPIRADO ===")
    
    removed_count = cache_manager.clear_expired()
    
    if removed_count > 0:
        logger.info(f"✅ {removed_count} arquivos expirados removidos")
    else:
        logger.info("ℹ️ Nenhum arquivo expirado encontrado")


def clear_api_cache(cache_manager: CacheManager, id_value: int = None, periodo: int = None):
    """Remove cache específico da API"""
    logger.info("=== LIMPANDO CACHE DA API ===")
    
    if id_value is not None and periodo is not None:
        # Cache específico
        success = cache_manager.invalidate("api", id=id_value, periodo=periodo)
        if success:
            logger.info(f"✅ Cache da API removido para ID={id_value}, Período={periodo}")
        else:
            logger.info(f"ℹ️ Cache da API não encontrado para ID={id_value}, Período={periodo}")
    else:
        # Todos os caches da API (implementação mais complexa seria necessária)
        logger.warning("⚠️ Para limpar todo cache da API, use 'clear' ou especifique --id e --periodo")


def clear_db_cache(cache_manager: CacheManager, id_value: int = None, periodo: int = None):
    """Remove cache específico do banco de dados"""
    logger.info("=== LIMPANDO CACHE DO BANCO ===")
    
    if id_value is not None and periodo is not None:
        # Cache específico
        success = cache_manager.invalidate("db", financial_instrument_id=id_value, periodo=periodo)
        if success:
            logger.info(f"✅ Cache do banco removido para ID={id_value}, Período={periodo}")
        else:
            logger.info(f"ℹ️ Cache do banco não encontrado para ID={id_value}, Período={periodo}")
    else:
        # Todos os caches do banco (implementação mais complexa seria necessária)
        logger.warning("⚠️ Para limpar todo cache do banco, use 'clear' ou especifique --id e --periodo")


if __name__ == "__main__":
    main()
