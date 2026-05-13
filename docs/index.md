# Lakehouse com Databricks — Arquitetura Medalhão

Trabalho 3 da disciplina de Engenharia de Dados.

## Objetivo

Construir um pipeline de dados no Databricks implementando a arquitetura Medalhão (experiência PaaS), percorrendo as camadas Landing → Bronze → Silver → Gold com automação via Jobs & Pipelines.

## Tecnologias Utilizadas

| Tecnologia | Papel |
|---|---|
| Databricks Free Edition | Plataforma de processamento |
| Delta Lake | Formato de armazenamento nas camadas Bronze/Silver/Gold |
| MongoDB Atlas | Fonte de dados não relacional (JSON) |
| PostgreSQL / Supabase | Fonte de dados relacional (CSV) |
| Jobs & Pipelines | Orquestração sequencial dos notebooks |
