# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold — Modelagem Dimensional (Ralph Kimball)
# MAGIC Lê os dados do schema `silver` e alimenta as tabelas dimensionais e fato no schema `gold`.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "main"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}")

# COMMAND ----------

# MAGIC %md ## Leitura da camada Silver

# COMMAND ----------

df_clientes = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.clientes")
df_pedidos  = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.pedidos")
df_produtos = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.produtos")

# COMMAND ----------

# MAGIC %md ## dim_cliente

# COMMAND ----------

dim_cliente = (df_clientes
    .select(
        F.col("id").alias("sk_cliente"),
        F.col("nome"),
        F.col("email"),
        F.col("cidade"),
        F.col("estado")
    )
    .dropDuplicates(["sk_cliente"]))

(dim_cliente.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_cliente"))

print(f"[GOLD] dim_cliente: {dim_cliente.count()} registros")

# COMMAND ----------

# MAGIC %md ## dim_produto

# COMMAND ----------

dim_produto = (df_produtos
    .select(
        F.col("id").alias("sk_produto"),
        F.col("nome"),
        F.col("categoria"),
        F.col("preco")
    )
    .dropDuplicates(["sk_produto"]))

(dim_produto.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_produto"))

print(f"[GOLD] dim_produto: {dim_produto.count()} registros")

# COMMAND ----------

# MAGIC %md ## dim_data

# COMMAND ----------

dim_data = (df_pedidos
    .select(F.col("data_pedido").alias("data"))
    .dropDuplicates()
    .withColumn("sk_data",   F.date_format(F.col("data"), "yyyyMMdd").cast("int"))
    .withColumn("ano",       F.year(F.col("data")))
    .withColumn("mes",       F.month(F.col("data")))
    .withColumn("dia",       F.dayofmonth(F.col("data")))
    .withColumn("trimestre", F.quarter(F.col("data")))
    .withColumn("nome_mes",  F.date_format(F.col("data"), "MMMM"))
    .select("sk_data", "data", "ano", "mes", "dia", "trimestre", "nome_mes"))

(dim_data.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_data"))

print(f"[GOLD] dim_data: {dim_data.count()} registros")

# COMMAND ----------

# MAGIC %md ## fato_pedidos

# COMMAND ----------

fato_pedidos = (df_pedidos
    .withColumn("sk_data", F.date_format(F.col("data_pedido"), "yyyyMMdd").cast("int"))
    .select(
        F.col("id").alias("sk_pedido"),
        F.col("id_cliente").alias("sk_cliente"),
        F.col("id_produto").alias("sk_produto"),
        F.col("sk_data"),
        F.col("quantidade"),
        F.col("valor_total")
    ))

(fato_pedidos.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.fato_pedidos"))

print(f"[GOLD] fato_pedidos: {fato_pedidos.count()} registros")

# COMMAND ----------

# MAGIC %md ## Verificação Final

# COMMAND ----------

spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA_GOLD}").show()

print("[GOLD] Modelagem dimensional concluída com sucesso.")
