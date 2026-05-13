# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Bronze — Ingestão (Delta Lake)
# MAGIC Lê os CSVs da camada Landing e grava no formato **Delta Lake** no schema `bronze`, preservando os dados brutos.

# COMMAND ----------

CATALOG        = "main"
SCHEMA_LANDING = "landing"
SCHEMA_BRONZE  = "bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")

LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA_LANDING}/dados"

# COMMAND ----------

tabelas = [
    "regiao",
    "estado",
    "municipio",
    "marca",
    "modelo",
    "cliente",
    "endereco",
    "telefone",
    "carro",
    "apolice",
    "sinistro",
]

for tabela in tabelas:
    df = spark.read.csv(f"{LANDING_PATH}/{tabela}", header=True, inferSchema=True)

    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.{tabela}"))

    print(f"[BRONZE] '{tabela}' → Delta Lake | {df.count()} registros")

# COMMAND ----------

print("[BRONZE] Ingestão concluída com sucesso.")
