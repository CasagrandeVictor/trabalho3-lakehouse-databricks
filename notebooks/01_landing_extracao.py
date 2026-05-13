# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Landing — Extração do SeguroDB (SQL Server)
# MAGIC Extrai todas as tabelas do banco SeguroDB e grava no schema `landing` em formato CSV.

# COMMAND ----------

# MAGIC %md ## 1. Configuração

# COMMAND ----------

CATALOG        = "main"
SCHEMA_LANDING = "landing"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_LANDING}")

LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA_LANDING}/dados"

# COMMAND ----------

# MAGIC %md ## 2. Conexão com SQL Server (SeguroDB)

# COMMAND ----------

# Credenciais — use Databricks Secrets em produção
# dbutils.secrets.get(scope="trabalho3", key="sqlserver_host")

sql_host     = "<HOST_SQL_SERVER>"   # ex: localhost ou IP do container
sql_port     = "1433"
sql_database = "SeguroDB"
sql_user     = "SA"
sql_password = "SqlServer@2022!"

jdbc_url = f"jdbc:sqlserver://{sql_host}:{sql_port};databaseName={sql_database};encrypt=false"

jdbc_properties = {
    "user":     sql_user,
    "password": sql_password,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# MAGIC %md ## 3. Extração de todas as tabelas do SeguroDB

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
    df = spark.read.jdbc(url=jdbc_url, table=tabela, properties=jdbc_properties)
    df.write.mode("overwrite").csv(f"{LANDING_PATH}/{tabela}", header=True)
    print(f"[LANDING] '{tabela}' extraída → {df.count()} registros")

# COMMAND ----------

print("[LANDING] Extração concluída com sucesso.")
