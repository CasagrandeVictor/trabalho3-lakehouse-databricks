# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver — Data Quality
# MAGIC Lê os dados do schema `bronze`, aplica regras de Data Quality e grava no schema `silver`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG        = "main"
SCHEMA_BRONZE  = "bronze"
SCHEMA_SILVER  = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}")

# COMMAND ----------

# MAGIC %md ## Funções de Data Quality

# COMMAND ----------

def remover_duplicatas(df, subset=None):
    antes = df.count()
    df = df.dropDuplicates(subset)
    print(f"  Duplicatas removidas: {antes - df.count()}")
    return df

def remover_nulos_criticos(df, colunas_criticas):
    antes = df.count()
    df = df.dropna(subset=colunas_criticas)
    print(f"  Linhas com nulos críticos removidas: {antes - df.count()}")
    return df

def padronizar_strings(df, colunas):
    for col in colunas:
        if col in df.columns:
            df = df.withColumn(col, F.upper(F.trim(F.col(col))))
    return df

def validar_dq(df, tabela):
    total = df.count()
    print(f"\n[SILVER] DQ — {tabela}: {total} registros após limpeza")
    return df

# COMMAND ----------

# MAGIC %md ## Processamento — Clientes

# COMMAND ----------

df_clientes = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.clientes")

df_clientes = remover_duplicatas(df_clientes)
df_clientes = remover_nulos_criticos(df_clientes, colunas_criticas=["id", "nome"])
df_clientes = padronizar_strings(df_clientes, ["nome", "cidade", "estado"])
df_clientes = validar_dq(df_clientes, "clientes")

(df_clientes.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.clientes"))

# COMMAND ----------

# MAGIC %md ## Processamento — Pedidos

# COMMAND ----------

df_pedidos = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.pedidos")

df_pedidos = remover_duplicatas(df_pedidos)
df_pedidos = remover_nulos_criticos(df_pedidos, colunas_criticas=["id", "id_cliente", "data_pedido"])

# Garantir tipo correto na data
if "data_pedido" in df_pedidos.columns:
    df_pedidos = df_pedidos.withColumn("data_pedido", F.to_date(F.col("data_pedido")))

# Remover valores negativos em valor_total
if "valor_total" in df_pedidos.columns:
    df_pedidos = df_pedidos.filter(F.col("valor_total") >= 0)

df_pedidos = validar_dq(df_pedidos, "pedidos")

(df_pedidos.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.pedidos"))

# COMMAND ----------

# MAGIC %md ## Processamento — Produtos

# COMMAND ----------

df_produtos = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.produtos")

df_produtos = remover_duplicatas(df_produtos)
df_produtos = remover_nulos_criticos(df_produtos, colunas_criticas=["id", "nome"])
df_produtos = padronizar_strings(df_produtos, ["nome", "categoria"])
df_produtos = validar_dq(df_produtos, "produtos")

(df_produtos.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.produtos"))

# COMMAND ----------

print("[SILVER] Data Quality aplicado e dados gravados com sucesso.")
