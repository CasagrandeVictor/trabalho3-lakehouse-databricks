# Trabalho 3 — Lakehouse com Databricks e Arquitetura Medalhão

Pipeline de dados no Databricks implementando a arquitetura Medalhão (Landing → Bronze → Silver → Gold) com Jobs & Pipelines.

## Tecnologias

- Databricks Free Edition
- Delta Lake
- CSV / JSON
- Jobs & Pipelines
- MongoDB Atlas (fonte não relacional)
- PostgreSQL / Supabase (fonte relacional)

## Arquitetura Medalhão

```
[Fonte de Dados]
      │
      ▼
  LANDING/DADOS   ← Dados brutos extraídos (CSV/JSON)
      │
      ▼
   BRONZE         ← Dados brutos no formato Delta Lake
      │
      ▼
   SILVER         ← Dados tratados com Data Quality aplicado
      │
      ▼
    GOLD          ← Tabelas dimensionais (Ralph Kimball)
```

## Estrutura do Repositório

```
trabalho3-lakehouse-databricks/
├── notebooks/
│   ├── 01_landing_extracao.py       # Extração das fontes → LANDING/DADOS
│   ├── 02_bronze_ingestao.py        # LANDING/DADOS → BRONZE (Delta Lake)
│   ├── 03_silver_data_quality.py    # BRONZE → SILVER (Data Quality)
│   └── 04_gold_modelagem.py         # SILVER → GOLD (Modelagem Dimensional)
├── docs/
│   ├── index.md
│   ├── arquitetura.md
│   └── pipeline.md
├── data/
│   └── samples/                     # Amostras de dados para testes
├── mkdocs.yml
└── README.md
```

## Como Executar

1. Importe os notebooks para o Databricks workspace
2. Configure as credenciais das fontes de dados nos secrets do Databricks
3. Crie um Job no Databricks vinculando os notebooks em ordem sequencial:
   - Task 1: `01_landing_extracao`
   - Task 2: `02_bronze_ingestao` (depende da Task 1)
   - Task 3: `03_silver_data_quality` (depende da Task 2)
   - Task 4: `04_gold_modelagem` (depende da Task 3)
4. Execute o Job

## Documentação

Gerada com MkDocs: `mkdocs serve`
