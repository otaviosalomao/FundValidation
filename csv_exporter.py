"""
Exportador de dados para CSV
"""

import csv
import logging
from typing import List, Dict, Any
from pathlib import Path
from config import OUTPUT_FILENAME

logger = logging.getLogger(__name__)


class CSVExporter:
    """Exporta dados processados para arquivo CSV"""
    
    @staticmethod
    def export_to_csv(records: List[Dict[str, Any]], filename: str = None) -> str:
        """
        Exporta os registros para um arquivo CSV
        
        Args:
            records: Lista de registros para exportar
            filename: Nome do arquivo (opcional)
            
        Returns:
            Caminho do arquivo CSV criado
        """
        if not records:
            logger.warning("Nenhum registro para exportar")
            return ""
        
        # Usar nome padrão se não fornecido
        if filename is None:
            filename = OUTPUT_FILENAME
        
        try:
            # Criar diretório se não existir
            output_path = Path(filename)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Obter todas as chaves únicas dos registros
            fieldnames = set()
            for record in records:
                fieldnames.update(record.keys())
            
            # Ordenar as chaves para consistência, garantindo que PorcentagemRentabilidadeAcumulada seja a última
            fieldnames = list(fieldnames)
            
            # Se PorcentagemRentabilidadeAcumulada existir, movê-la para o final
            if "PorcentagemRentabilidadeAcumulada" in fieldnames:
                fieldnames.remove("PorcentagemRentabilidadeAcumulada")
                fieldnames = sorted(fieldnames) + ["PorcentagemRentabilidadeAcumulada"]
            else:
                fieldnames = sorted(fieldnames)
            
            # Escrever CSV
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Escrever cabeçalho
                writer.writeheader()
                
                # Escrever registros
                for record in records:
                    # Preencher campos ausentes com string vazia
                    row = {field: record.get(field, "") for field in fieldnames}
                    writer.writerow(row)
            
            logger.info(f"CSV exportado com sucesso: {output_path}")
            logger.info(f"Total de registros exportados: {len(records)}")
            logger.info(f"Colunas: {', '.join(fieldnames)}")
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Erro ao exportar CSV: {e}")
            return ""
    
    @staticmethod
    def get_csv_summary(records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Gera um resumo dos dados exportados
        
        Args:
            records: Lista de registros exportados
            
        Returns:
            Dicionário com resumo dos dados
        """
        if not records:
            return {}
        
        # Contar registros por ID
        id_counts = {}
        for record in records:
            id_value = record.get("Id", "N/A")
            id_counts[id_value] = id_counts.get(id_value, 0) + 1
        
        # Contar registros por período
        periodo_counts = {}
        for record in records:
            periodo = record.get("PeriodoSelecionado", "N/A")
            periodo_counts[periodo] = periodo_counts.get(periodo, 0) + 1
        
        summary = {
            "total_registros": len(records),
            "ids_unicos": len(id_counts),
            "periodos_unicos": len(periodo_counts),
            "contagem_por_id": id_counts,
            "contagem_por_periodo": periodo_counts
        }
        
        return summary
