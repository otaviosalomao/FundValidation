"""
Gerenciador de cache para requisições da API e banco de dados
"""

import json
import hashlib
import os
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Union
from pathlib import Path

logger = logging.getLogger(__name__)


class CacheManager:
    """Gerenciador de cache usando arquivos JSON"""

    def __init__(self, cache_dir: str = "cache", default_ttl_hours: int = 24):
        """
        Inicializa o gerenciador de cache

        Args:
            cache_dir: Diretório onde os arquivos de cache serão armazenados
            default_ttl_hours: TTL padrão em horas para os itens de cache
        """
        self.cache_dir = Path(cache_dir)
        self.default_ttl_hours = default_ttl_hours
        self._ensure_cache_dir()

    def _ensure_cache_dir(self):
        """Garante que o diretório de cache existe"""
        try:
            self.cache_dir.mkdir(exist_ok=True)
            logger.info(f"Diretório de cache configurado: {self.cache_dir}")
        except Exception as e:
            logger.error(f"Erro ao criar diretório de cache: {e}")
            raise

    def _generate_cache_key(self, prefix: str, **kwargs) -> str:
        """
        Gera uma chave única para o cache baseada nos parâmetros

        Args:
            prefix: Prefixo da chave (ex: 'api', 'db')
            **kwargs: Parâmetros para gerar a chave

        Returns:
            Chave única para o cache
        """
        # Criar string com todos os parâmetros ordenados
        params_str = "_".join([f"{k}={v}" for k, v in sorted(kwargs.items())])
        # Gerar hash MD5 para evitar nomes de arquivo muito longos
        hash_obj = hashlib.md5(params_str.encode())
        return f"{prefix}_{hash_obj.hexdigest()}"

    def _get_cache_file_path(self, cache_key: str) -> Path:
        """
        Retorna o caminho do arquivo de cache

        Args:
            cache_key: Chave do cache

        Returns:
            Caminho do arquivo de cache
        """
        return self.cache_dir / f"{cache_key}.json"

    def _is_cache_valid(self, cache_data: Dict[str, Any]) -> bool:
        """
        Verifica se o cache ainda é válido baseado no TTL

        Args:
            cache_data: Dados do cache

        Returns:
            True se o cache é válido, False caso contrário
        """
        try:
            created_at = datetime.fromisoformat(cache_data.get("created_at", ""))
            ttl_hours = cache_data.get("ttl_hours", self.default_ttl_hours)
            expires_at = created_at + timedelta(hours=ttl_hours)
            return datetime.now() < expires_at
        except (ValueError, TypeError) as e:
            logger.warning(f"Erro ao verificar validade do cache: {e}")
            return False

    def get(self, prefix: str, **kwargs) -> Optional[Any]:
        """
        Recupera dados do cache

        Args:
            prefix: Prefixo da chave
            **kwargs: Parâmetros para gerar a chave

        Returns:
            Dados do cache ou None se não encontrado/expirado
        """
        try:
            cache_key = self._generate_cache_key(prefix, **kwargs)
            cache_file = self._get_cache_file_path(cache_key)

            if not cache_file.exists():
                logger.debug(f"Cache não encontrado: {cache_key}")
                return None

            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)

            if not self._is_cache_valid(cache_data):
                logger.debug(f"Cache expirado: {cache_key}")
                self._delete_cache_file(cache_file)
                return None

            logger.info(f"✅ Cache encontrado: {cache_key}")
            return cache_data.get("data")

        except Exception as e:
            logger.warning(f"Erro ao recuperar cache: {e}")
            return None

    def set(self, prefix: str, data: Any, ttl_hours: Optional[int] = None, **kwargs) -> bool:
        """
        Armazena dados no cache

        Args:
            prefix: Prefixo da chave
            data: Dados a serem armazenados
            ttl_hours: TTL em horas (opcional, usa padrão se None)
            **kwargs: Parâmetros para gerar a chave

        Returns:
            True se bem-sucedido, False caso contrário
        """
        try:
            cache_key = self._generate_cache_key(prefix, **kwargs)
            cache_file = self._get_cache_file_path(cache_key)

            cache_data = {
                "created_at": datetime.now().isoformat(),
                "ttl_hours": ttl_hours or self.default_ttl_hours,
                "cache_key": cache_key,
                "data": data
            }

            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False, default=str)

            logger.info(f"💾 Cache salvo: {cache_key}")
            return True

        except Exception as e:
            logger.error(f"Erro ao salvar cache: {e}")
            return False

    def _delete_cache_file(self, cache_file: Path):
        """
        Remove arquivo de cache

        Args:
            cache_file: Caminho do arquivo de cache
        """
        try:
            cache_file.unlink()
            logger.debug(f"Cache removido: {cache_file.name}")
        except Exception as e:
            logger.warning(f"Erro ao remover cache: {e}")

    def invalidate(self, prefix: str, **kwargs) -> bool:
        """
        Invalida um item específico do cache

        Args:
            prefix: Prefixo da chave
            **kwargs: Parâmetros para gerar a chave

        Returns:
            True se removido com sucesso, False caso contrário
        """
        try:
            cache_key = self._generate_cache_key(prefix, **kwargs)
            cache_file = self._get_cache_file_path(cache_key)

            if cache_file.exists():
                self._delete_cache_file(cache_file)
                logger.info(f"🗑️ Cache invalidado: {cache_key}")
                return True
            else:
                logger.debug(f"Cache não encontrado para invalidação: {cache_key}")
                return False

        except Exception as e:
            logger.error(f"Erro ao invalidar cache: {e}")
            return False

    def clear_all(self) -> int:
        """
        Remove todos os arquivos de cache

        Returns:
            Número de arquivos removidos
        """
        try:
            cache_files = list(self.cache_dir.glob("*.json"))
            removed_count = 0

            for cache_file in cache_files:
                try:
                    cache_file.unlink()
                    removed_count += 1
                except Exception as e:
                    logger.warning(f"Erro ao remover {cache_file}: {e}")

            logger.info(f"🧹 Cache limpo: {removed_count} arquivos removidos")
            return removed_count

        except Exception as e:
            logger.error(f"Erro ao limpar cache: {e}")
            return 0

    def clear_expired(self) -> int:
        """
        Remove apenas os arquivos de cache expirados

        Returns:
            Número de arquivos expirados removidos
        """
        try:
            cache_files = list(self.cache_dir.glob("*.json"))
            removed_count = 0

            for cache_file in cache_files:
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)

                    if not self._is_cache_valid(cache_data):
                        cache_file.unlink()
                        removed_count += 1

                except Exception as e:
                    logger.warning(f"Erro ao verificar/remover {cache_file}: {e}")

            logger.info(f"🧹 Cache expirado removido: {removed_count} arquivos")
            return removed_count

        except Exception as e:
            logger.error(f"Erro ao limpar cache expirado: {e}")
            return 0

    def get_cache_info(self) -> Dict[str, Any]:
        """
        Retorna informações sobre o cache

        Returns:
            Dicionário com informações do cache
        """
        try:
            cache_files = list(self.cache_dir.glob("*.json"))
            total_files = len(cache_files)
            valid_files = 0
            expired_files = 0
            total_size = 0

            for cache_file in cache_files:
                try:
                    file_size = cache_file.stat().st_size
                    total_size += file_size

                    with open(cache_file, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)

                    if self._is_cache_valid(cache_data):
                        valid_files += 1
                    else:
                        expired_files += 1

                except Exception:
                    expired_files += 1

            return {
                "cache_dir": str(self.cache_dir),
                "total_files": total_files,
                "valid_files": valid_files,
                "expired_files": expired_files,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2)
            }

        except Exception as e:
            logger.error(f"Erro ao obter informações do cache: {e}")
            return {}
