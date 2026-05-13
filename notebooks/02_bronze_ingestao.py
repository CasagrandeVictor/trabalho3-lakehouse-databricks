# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Bronze — Ingestão
# MAGIC Lê os arquivos CSV/JSON do schema `landing` e grava no formato **Delta Lake** no schema `bronze`.

# COMMAND ----------

CATALOG = "main"
SCHEMA_LANDING = "landing"
SCHEMA_BRONZE   = "bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")

LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA_LANDING}/dados"

# COMMAND ----------

# MAGIC %md ## Ingestão — Arquivos CSV (fontes relacionais)

# COMMAND ----------

tabelas_csv = ["clientes", "pedidos", "produtos"]  # mesmo que no notebook 01

for tabela in tabelas_csv:
    df = spark.read.csv(f"{LANDING_PATH}/{tabela}", header=True, inferSchema=True)

    (df.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.{tabela}"))

    print(f"[BRONZE] '{tabela}' gravado como Delta Lake — {df.count()} registros")

# COMMAND ----------

# MAGIC %md ## Ingestão — Arquivos JSON (fontes não relacionais)

# COMMAND ----------

colecoes_json = ["colecao1", "colecao2"]

for colecao in colecoes_json:
    df = spark.read.json(f"{LANDING_PATH}/{colecao}")

    (df.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.{colecao}"))

    print(f"[BRONZE] '{colecao}' gravado como Delta Lake — {df.count()} registros")

# COMMAND ----------

print("[BRONZE] Ingestão concluída com sucesso.")
