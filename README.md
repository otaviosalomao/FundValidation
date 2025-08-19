# Aplicação de Coleta de Dados de Rentabilidade

Esta aplicação Python coleta dados de rentabilidade de uma API REST e os exporta para um arquivo CSV ordenado.

## Funcionalidades

- ✅ Faz requisições GET para a API de rentabilidade
- ✅ Coleta dados para 16 IDs diferentes (314, 315, 316, 771, 1103, 1104, 1105, 1136, 1138, 1142, 1143, 1144, 1145, 1232, 1246, 1292)
- ✅ Processa dados para 8 períodos diferentes (1 a 8)
- ✅ Total de 128 requisições (16 IDs × 8 períodos)
- ✅ **NOVO**: Conecta ao banco de dados SQL Server
- ✅ **NOVO**: Executa query de histórico de valores dos fundos
- ✅ **NOVO**: Combina dados da API com dados do banco
- ✅ **NOVO**: Calcula estatísticas dos valores de quota (min, max, média)
- ✅ Exporta dados ordenados por ID e Período para CSV
- ✅ Sistema de retry automático para falhas de rede
- ✅ Logging completo para monitoramento
- ✅ Tratamento de erros robusto

## Estrutura do Projeto

```
FundValidation/
├── main.py              # Arquivo principal da aplicação
├── config.py            # Configurações e constantes
├── api_client.py        # Cliente HTTP para a API
├── data_processor.py    # Processamento e estruturação dos dados
├── csv_exporter.py      # Exportação para CSV
├── requirements.txt     # Dependências Python
├── README.md           # Esta documentação
├── exemplo_csv.csv     # Exemplo da estrutura do CSV de saída
├── test_app.py         # Script de teste e validação
├── validate_structure.py # Script para validar estrutura de dados
├── database_client.py  # Cliente para conexão com SQL Server
├── data_combiner.py    # Módulo para combinar dados da API com banco
├── test_database.py    # Script para testar conexão com banco
├── run.bat             # Script de execução para Windows
└── dados_rentabilidade.csv  # Arquivo de saída (gerado após execução)
```

## Instalação

1. **Instalar dependências:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Verificar configurações:**
   - Edite `config.py` se necessário para ajustar parâmetros
   - A URL padrão é: `https://localhost:7278/carteira/rentabilidade/grafico`

## Uso

### Execução Simples
```bash
python main.py
```

### Execução com Log Detalhado
```bash
python main.py 2>&1 | tee execution.log
```

### Scripts de Validação

**Teste de Conectividade:**
```bash
python test_app.py
```

**Validação da Estrutura de Dados:**
```bash
python validate_structure.py
```

**Teste de Conexão com Banco de Dados:**
```bash
python test_database.py
```

## Parâmetros da API

### Parâmetros Fixos
- `IdContaCorrente`: 5177807
- `ProdutoSelecionado`: 4
- `BenchmarkSelecionado`: 0
- `MaximoPontosRetornados`: 365

### Parâmetros Variáveis
- `Id`: 314, 315, 316, 771, 1103, 1104, 1105, 1136, 1138, 1142, 1143, 1144, 1145, 1232, 1246, 1292
- `PeriodoSelecionado`: 1, 2, 3, 4, 5, 6, 7, 8

## Estrutura da Resposta da API

A API retorna dados no seguinte formato:

```json
{
  "DataUltimaAtualizacao": "2025-08-19T13:16:34.882Z",
  "IsCache": true,
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
    }
  ]
}
```

A aplicação extrai apenas os campos relevantes do array `Rentabilidades` e adiciona os parâmetros `Id` e `PeriodoSelecionado` para identificação.

## Banco de Dados SQL Server

### Conexão
A aplicação se conecta ao banco de dados SQL Server usando a string de conexão configurada em `config.py`:

```
Server=SRVSQL02SP.gf.int\GORILA,1460;Database=Platform;User Id=tema_gorila;Password=reKc6lHlCiAO3cjlHfKF;Connection Timeout=300;pooling='true';
```

### Mapeamento de Períodos
Os períodos da API são mapeados para o enum `EPeriodo` do banco de dados:

| Período API | Enum Banco | Descrição |
|-------------|------------|-----------|
| 1 | 0 | SemanaAtual |
| 2 | 1 | MesAtual |
| 3 | 2 | AnoAtual |
| 4 | 3 | DozeMeses |
| 5 | 4 | TresAnos |
| 6 | 5 | DesdeDoisMilDezenove |
| 7 | 6 | DoisAnos |
| 8 | 7 | TrintaDias |

### Query Executada
A aplicação executa a seguinte query para cada combinação de ID e período:

```sql
SELECT
    FVH.[FinancialInstrumentFundValueHistoryId]
    ,FVH.[FinancialInstrumentId]
    ,FVH.[QuotaValue]
    ,FI.Name
    ,FVH.PositionDate
FROM 
    [dbo].[FinancialInstrumentFundValueHistory] FVH
    INNER JOIN FinancialInstrument FI ON FI.FinancialInstrumentId = FVH.FinancialInstrumentId
    -- ... (subqueries para cálculo de datas)
WHERE FVH.[FinancialInstrumentId] = @FinancialInstrumentId
    and fvh.PositionDate >= sel_min.dt_ini and fvh.PositionDate <= sel_max.DT_FIM
ORDER BY FVH.PositionDate
```

## Estrutura do CSV de Saída

O arquivo CSV gerado contém as seguintes colunas específicas de rentabilidade **combinadas com dados do banco de dados**:

### Colunas da API de Rentabilidade
| Coluna | Descrição |
|--------|-----------|
| `Id` | ID do produto (parâmetro enviado no request) |
| `PeriodoSelecionado` | Período selecionado (1-8) |
| `DataInicial` | Data inicial do período de rentabilidade |
| `DataFinal` | Data final do período de rentabilidade |
| `PercentualSobreBenchmark` | Percentual sobre benchmark |
| `PercentualAcumuladoBenchmark` | Percentual acumulado do benchmark |
| `PercentualAcumulado` | Percentual acumulado da rentabilidade |
| `NominalAcumulado` | Valor nominal acumulado |

### Colunas do Banco de Dados
| Coluna | Descrição |
|--------|-----------|
| `FinancialInstrumentFundValueHistoryId` | ID do histórico de valor do fundo |
| `FinancialInstrumentId` | ID do instrumento financeiro |
| `QuotaValue` | Valor da quota na data específica |
| `FinancialInstrumentName` | Nome do instrumento financeiro |
| `PositionDate` | Data da posição |
| `DataInicioPeriodo` | Data de início do período calculado |
| `DataFimPeriodo` | Data de fim do período calculado |
| `TotalRegistrosBanco` | Total de registros encontrados no banco |
| `QuotaValueMin` | Valor mínimo da quota no período |
| `QuotaValueMax` | Valor máximo da quota no período |
| `QuotaValueAvg` | Valor médio da quota no período |

## Configurações

### Arquivo `config.py`
- **URL da API**: `BASE_URL`
- **Parâmetros fixos**: `FIXED_PARAMS`
- **IDs para consulta**: `IDS`
- **Períodos**: `PERIODOS`
- **Timeout**: `REQUEST_TIMEOUT`
- **Tentativas**: `MAX_RETRIES`
- **Arquivo de saída**: `OUTPUT_FILENAME`

### Personalização
Para modificar a aplicação:

1. **Adicionar novos IDs**: Edite a lista `IDS` em `config.py`
2. **Alterar períodos**: Modifique a lista `PERIODOS` em `config.py`
3. **Mudar parâmetros fixos**: Ajuste `FIXED_PARAMS` em `config.py`
4. **Configurar retry**: Modifique `MAX_RETRIES` e `RETRY_DELAY` em `config.py`

## Logs

A aplicação gera logs detalhados em:
- **Console**: Saída em tempo real
- **Arquivo**: `app.log` com histórico completo

### Níveis de Log
- **INFO**: Operações normais e progresso
- **WARNING**: Avisos sobre dados vazios ou status HTTP não-200
- **ERROR**: Erros de rede, processamento ou exportação

## Tratamento de Erros

### Sistema de Retry
- Máximo de 3 tentativas por requisição
- Delay de 1 segundo entre tentativas
- Timeout de 30 segundos por requisição

### Validação de Dados
- Verificação de respostas vazias
- Tratamento de diferentes estruturas de dados (lista, dicionário, valor simples)
- Preenchimento de campos ausentes com strings vazias

## Performance

- **Requisições sequenciais** para evitar sobrecarga da API
- **Pausa de 0.1 segundos** entre requisições
- **Processamento em memória** para melhor performance
- **Exportação otimizada** para CSV

## Monitoramento

### Métricas Coletadas
- Total de requisições feitas
- Respostas bem-sucedidas vs. falhas
- Tempo de execução
- Contagem de registros por ID e período

### Resumo de Execução
Ao final da execução, a aplicação exibe:
- ✅ Status de sucesso/falha
- 📁 Caminho do arquivo CSV gerado
- 📊 Localização do arquivo de log

## Troubleshooting

### Problemas Comuns

1. **Erro de SSL/Certificado**
   - A aplicação desabilita verificação SSL para localhost
   - Se necessário, ajuste em `api_client.py`

2. **Timeout de Requisições**
   - Aumente `REQUEST_TIMEOUT` em `config.py`
   - Verifique conectividade de rede

3. **Falhas de API**
   - Verifique se a API está rodando em `localhost:7278`
   - Confirme parâmetros em `config.py`

4. **Erro de Permissão de Arquivo**
   - Verifique permissões de escrita no diretório
   - A aplicação cria diretórios automaticamente se necessário

### Logs de Debug
Para debug detalhado, modifique o nível de log em `main.py`:
```python
logging.basicConfig(level=logging.DEBUG, ...)
```

## Próximos Passos

### Melhorias Sugeridas
1. **Paralelização**: Implementar requisições concorrentes para melhor performance
2. **Cache**: Adicionar cache para evitar requisições duplicadas
3. **Validação**: Implementar validação de esquema de dados
4. **Métricas**: Adicionar métricas de performance e qualidade dos dados
5. **Configuração**: Interface de linha de comando para parâmetros
6. **Notificações**: Sistema de notificação para falhas críticas

### Extensões
- **Banco de Dados**: Exportar para PostgreSQL, MySQL, etc.
- **APIs**: Suporte para outras APIs de rentabilidade
- **Formato**: Exportar para Excel, JSON, XML
- **Scheduling**: Execução automática via cron/agendador

## Suporte

Para dúvidas ou problemas:
1. Verifique os logs em `app.log`
2. Confirme as configurações em `config.py`
3. Teste a conectividade com a API
4. Verifique as permissões de arquivo

## Licença

Este projeto é de uso interno para coleta de dados de rentabilidade.
