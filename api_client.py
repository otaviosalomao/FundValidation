"""
Cliente para fazer requisições HTTP à API de rentabilidade
"""

import requests
import time
import logging
from typing import Dict, Any, Optional
from urllib3.exceptions import InsecureRequestWarning
from config import BASE_URL, FIXED_PARAMS, REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY, CACHE_ENABLED, CACHE_TTL_HOURS_API
from cache_manager import CacheManager

# Suprimir avisos de certificado SSL para localhost
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class APIClient:
    """Cliente para fazer requisições à API de rentabilidade"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False  # Desabilitar verificação SSL para localhost
        self.cache_manager = CacheManager() if CACHE_ENABLED else None
        
    def get_rentabilidade_data(self, id_value: int, periodo: int) -> Optional[Dict[str, Any]]:
        """
        Faz requisição GET para obter dados de rentabilidade
        
        Args:
            id_value: ID do produto
            periodo: Período selecionado (1-8)
            
        Returns:
            Dados da resposta ou None se houver erro
        """
        # Verificar cache primeiro
        if self.cache_manager:
            cached_data = self.cache_manager.get("api", id=id_value, periodo=periodo)
            if cached_data is not None:
                logger.info(f"📦 Dados recuperados do cache para ID={id_value}, Período={periodo}")
                return cached_data
        
        params = FIXED_PARAMS.copy()
        params.update({
            "Id": str(id_value),
            "PeriodoSelecionado": str(periodo)
        })
        
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Fazendo requisição para ID={id_value}, Período={periodo} (tentativa {attempt + 1})")
                
                response = self.session.get(
                    BASE_URL,
                    params=params,
                    timeout=REQUEST_TIMEOUT
                )
                
                if response.status_code == 200:
                    logger.info(f"Sucesso para ID={id_value}, Período={periodo}")
                    result = {
                        "id": id_value,
                        "periodo": periodo,
                        "data": response.json(),
                        "status_code": response.status_code
                    }
                    
                    # Salvar no cache
                    if self.cache_manager:
                        self.cache_manager.set(
                            "api", 
                            result, 
                            ttl_hours=CACHE_TTL_HOURS_API,
                            id=id_value, 
                            periodo=periodo
                        )
                    
                    return result
                else:
                    logger.warning(f"Status {response.status_code} para ID={id_value}, Período={periodo}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro na requisição para ID={id_value}, Período={periodo}: {e}")
                
            except Exception as e:
                logger.error(f"Erro inesperado para ID={id_value}, Período={periodo}: {e}")
            
            # Aguardar antes da próxima tentativa
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
        
        logger.error(f"Falha após {MAX_RETRIES} tentativas para ID={id_value}, Período={periodo}")
        return None
    
    def close(self):
        """Fecha a sessão HTTP"""
        self.session.close()
