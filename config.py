"""
Configurações da aplicação para coleta de dados de rentabilidade
"""

# URL base da API
BASE_URL = "https://localhost:7278/carteira/rentabilidade/grafico"

# Parâmetros fixos
FIXED_PARAMS = {
    "IdContaCorrente": "5177807",
    "ProdutoSelecionado": "4",
    "BenchmarkSelecionado": "0",
    "MaximoPontosRetornados": "365"
}

# IDs para consulta
IDS = [314, 315, 316, 771, 1103, 1104, 1105, 1136, 1138, 1142, 1143, 1144, 1145, 1232, 1246, 1292]

# Períodos disponíveis (1 a 8)
PERIODOS = list(range(1, 9))

# Configurações de requisição
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
RETRY_DELAY = 1  # segundos

# Configurações de saída
OUTPUT_FILENAME = "dados_rentabilidade.csv"
OUTPUT_FILENAME_API = "dados_rentabilidade_api.csv"
OUTPUT_FILENAME_BANCO = "dados_rentabilidade_banco.csv"

# Configurações de cache
CACHE_ENABLED = True
CACHE_DIR = "cache"
CACHE_TTL_HOURS_API = 24  # TTL para cache da API em horas
CACHE_TTL_HOURS_DB = 12   # TTL para cache do banco em horas

# String de conexão com o banco de dados
DB_CONNECTION_STRING = "Driver={ODBC Driver 18 for SQL Server};Server=SRVSQL02SP.gf.int\\GORILA,1460;Database=Platform;UID=tema_gorila;PWD=reKc6lHlCiAO3cjlHfKF;Connection Timeout=300;TrustServerCertificate=yes;"

# Mapeamento de períodos para o enum EPeriodo
PERIODO_MAPPING = {
    1: 0,  # SemanaAtual
    2: 1,  # MesAtual  
    3: 2,  # AnoAtual
    4: 3,  # DozeMeses
    5: 4,  # TresAnos
    6: 5,  # DesdeDoisMilDezenove
    7: 6,  # DoisAnos
    8: 7,  # TrintaDias
}

# Configurações de comparação
COMPARISON_TOLERANCE = 0.10  # 5% de tolerância para comparação de valores

# Descrições dos períodos baseadas no enum EPeriodo
PERIODO_DESCRICOES = {
    1: "Na semana atual",
    2: "No mês atual",
    3: "No ano atual", 
    4: "Nos últimos 12 meses",
    5: "Nos últimos 3 anos",
    6: "Desde 2019",
    7: "Nos últimos 2 anos",
    8: "Nos últimos 30 dias",
}
