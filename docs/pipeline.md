# Pipeline — Jobs & Pipelines

## Notebooks e Ordem de Execução

| Ordem | Notebook | Descrição |
|---|---|---|
| 1 | `01_landing_extracao` | Extrai dados das fontes e grava em Landing |
| 2 | `02_bronze_ingestao` | Lê Landing e grava em Bronze (Delta Lake) |
| 3 | `03_silver_data_quality` | Aplica Data Quality e grava em Silver |
| 4 | `04_gold_modelagem` | Modela dimensionalmente e grava em Gold |

## Configuração do Job no Databricks

1. Acesse **Workflows → Jobs → Create Job**
2. Adicione cada notebook como uma Task
3. Configure as dependências (cada task depende da anterior)
4. Execute o Job e monitore via **Job Runs**

## Dependências entre Tasks

```
Task 1: landing_extracao
    └── Task 2: bronze_ingestao
            └── Task 3: silver_data_quality
                    └── Task 4: gold_modelagem
```
