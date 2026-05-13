# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Landing — Extração de Dados
# MAGIC Extrai dados das fontes originais e grava no schema `landing` em formato CSV (relacional) e JSON (não relacional).

# COMMAND ----------

# MAGIC %md ## 1. Configuração

# COMMAND ----------

CATALOG = "main"
SCHEMA_LANDING = "landing"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_LANDING}")

LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA_LANDING}/dados"

# COMMAND ----------

# MAGIC %md ## 2. Extração — Fonte Relacional (PostgreSQL / Supabase)

# COMMAND ----------

# Configurar credenciais via Databricks Secrets
# dbutils.secrets.get(scope="trabalho3", key="pg_host")

pg_host     = "<SEU_HOST>"
pg_port     = "5432"
pg_database = "<SEU_DATABASE>"
pg_user     = "<SEU_USER>"
pg_password = "<SUA_SENHA>"

jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"

jdbc_properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

# Lista de tabelas a extrair
tabelas_relacionais = ["clientes", "pedidos", "produtos"]  # ajuste conforme seu banco

for tabela in tabelas_relacionais:
    df = spark.read.jdbc(url=jdbc_url, table=tabela, properties=jdbc_properties)
    df.write.mode("overwrite").csv(f"{LANDING_PATH}/{tabela}", header=True)
    print(f"[LANDING] Tabela '{tabela}' extraída → {LANDING_PATH}/{tabela}")

# COMMAND ----------

# MAGIC %md ## 3. Extração — Fonte Não Relacional (MongoDB Atlas)

# COMMAND ----------

# Requer conector MongoDB Spark — instale via cluster libraries:
# maven: org.mongodb.spark:mongo-spark-connector_2.12:10.2.0

mongo_uri = "mongodb+srv://<USER>:<SENHA>@<CLUSTER>.mongodb.net/<DATABASE>"

colecoes_mongo = ["colecao1", "colecao2"]  # ajuste conforme seu banco

for colecao in colecoes_mongo:
    df = (spark.read
          .format("mongodb")
          .option("spark.mongodb.read.connection.uri", mongo_uri)
          .option("spark.mongodb.read.database", "<DATABASE>")
          .option("spark.mongodb.read.collection", colecao)
          .load())

    df.write.mode("overwrite").json(f"{LANDING_PATH}/{colecao}")
    print(f"[LANDING] Coleção '{colecao}' extraída → {LANDING_PATH}/{colecao}")

# COMMAND ----------

print("[LANDING] Extração concluída com sucesso.")
