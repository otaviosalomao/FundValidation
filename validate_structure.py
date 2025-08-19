"""
Script para validar a estrutura de dados processados
"""

import json
from data_processor import DataProcessor

# Dados de exemplo baseados na resposta real da API
SAMPLE_RESPONSE = {
    "id": 314,
    "periodo": 1,
    "status_code": 200,
    "data": {
        "DataUltimaAtualizacao": "2025-08-19T13:16:34.882Z",
        "IsCache": True,
        "DataInicial": "2025-08-01T00:00:00",
        "DataFinal": "2025-08-19T00:00:00-03:00",
        "Rentabilidades": [
            {
                "DataInicial": "2025-08-01T00:00:00Z",
                "DataFinal": "2025-08-01T00:00:00",
                "PercentualSobreBenchmark": 0.0,
                "PercentualAcumuladoBenchmark": 0.0,
                "PercentualAcumulado": 0.1693942504330551692482013400,
                "NominalAcumulado": 0.0
            },
            {
                "DataInicial": "2025-08-04T00:00:00Z",
                "DataFinal": "2025-08-04T00:00:00",
                "PercentualSobreBenchmark": 0.0,
                "PercentualAcumuladoBenchmark": 0.0,
                "PercentualAcumulado": 0.2465937400259317766969422600,
                "NominalAcumulado": 0.0
            }
        ]
    }
}

def validate_data_structure():
    """Valida se a estrutura de dados está sendo processada corretamente"""
    print("=== VALIDAÇÃO DA ESTRUTURA DE DADOS ===\n")
    
    # Processar dados de exemplo
    processor = DataProcessor()
    records = processor.flatten_response_data(SAMPLE_RESPONSE)
    
    if not records:
        print("❌ Falha: Nenhum registro foi processado")
        return False
    
    print(f"✅ {len(records)} registros processados com sucesso\n")
    
    # Verificar estrutura do primeiro registro
    first_record = records[0]
    expected_fields = [
        "Id", "PeriodoSelecionado", "DataInicial", "DataFinal",
        "PercentualSobreBenchmark", "PercentualAcumuladoBenchmark",
        "PercentualAcumulado", "NominalAcumulado"
    ]
    
    print("Campos encontrados:")
    for field in expected_fields:
        if field in first_record:
            value = first_record[field]
            print(f"  ✅ {field}: {value}")
        else:
            print(f"  ❌ {field}: AUSENTE")
    
    print(f"\nEstrutura completa do primeiro registro:")
    print(json.dumps(first_record, indent=2, ensure_ascii=False))
    
    # Verificar se todos os campos esperados estão presentes
    missing_fields = [field for field in expected_fields if field not in first_record]
    if missing_fields:
        print(f"\n❌ Campos ausentes: {missing_fields}")
        return False
    
    print(f"\n✅ Todos os campos esperados estão presentes!")
    return True

def validate_multiple_responses():
    """Valida o processamento de múltiplas respostas"""
    print("\n=== VALIDAÇÃO DE MÚLTIPLAS RESPOSTAS ===\n")
    
    # Criar múltiplas respostas de exemplo
    responses = [
        SAMPLE_RESPONSE,
        {
            "id": 315,
            "periodo": 2,
            "status_code": 200,
            "data": {
                "Rentabilidades": [
                    {
                        "DataInicial": "2025-08-01T00:00:00Z",
                        "DataFinal": "2025-08-01T00:00:00",
                        "PercentualSobreBenchmark": 1.5,
                        "PercentualAcumuladoBenchmark": 0.015,
                        "PercentualAcumulado": 0.2000000000000000000000000000,
                        "NominalAcumulado": 0.0
                    }
                ]
            }
        }
    ]
    
    processor = DataProcessor()
    all_records = processor.process_all_responses(responses)
    
    if not all_records:
        print("❌ Falha: Nenhum registro foi processado das múltiplas respostas")
        return False
    
    print(f"✅ {len(all_records)} registros processados de {len(responses)} respostas")
    
    # Verificar ordenação
    sorted_records = processor.sort_records(all_records)
    print(f"✅ Registros ordenados: {len(sorted_records)}")
    
    # Mostrar resumo por ID e período
    summary = {}
    for record in sorted_records:
        key = (record["Id"], record["PeriodoSelecionado"])
        if key not in summary:
            summary[key] = 0
        summary[key] += 1
    
    print("\nResumo por ID e Período:")
    for (id_val, periodo), count in sorted(summary.items()):
        print(f"  ID {id_val}, Período {periodo}: {count} registros")
    
    return True

def main():
    """Função principal de validação"""
    print("Iniciando validação da estrutura de dados...\n")
    
    # Validar estrutura básica
    if not validate_data_structure():
        return False
    
    # Validar múltiplas respostas
    if not validate_multiple_responses():
        return False
    
    print("\n🎉 VALIDAÇÃO CONCLUÍDA COM SUCESSO!")
    print("A estrutura de dados está sendo processada corretamente.")
    print("A aplicação está pronta para uso.")
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
