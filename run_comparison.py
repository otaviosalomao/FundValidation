#!/usr/bin/env python3
"""
Script para executar a comparação entre dados da API e do banco
"""

from csv_comparator import CSVComparator

if __name__ == "__main__":
    print("=== INICIANDO COMPARAÇÃO ENTRE CSVs ===")
    
    comparator = CSVComparator()
    output_file = comparator.run_comparison()
    
    if output_file:
        print(f"✅ Comparação concluída: {output_file}")
    else:
        print("❌ Erro na comparação")
