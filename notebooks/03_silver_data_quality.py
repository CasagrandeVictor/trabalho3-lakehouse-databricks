# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver — Data Quality
# MAGIC Lê os dados do schema `bronze`, aplica regras de qualidade e grava no schema `silver`.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "main"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}")

# COMMAND ----------

# MAGIC %md ## Funções de Data Quality

# COMMAND ----------

def dq_remover_duplicatas(df, subset=None):
    antes = df.count()
    df = df.dropDuplicates(subset)
    removidos = antes - df.count()
    if removidos > 0:
        print(f"  Duplicatas removidas: {removidos}")
    return df

def dq_remover_nulos(df, colunas):
    antes = df.count()
    df = df.dropna(subset=colunas)
    removidos = antes - df.count()
    if removidos > 0:
        print(f"  Linhas com nulos em {colunas} removidas: {removidos}")
    return df

def dq_padronizar_strings(df, colunas):
    for col in colunas:
        if col in df.columns:
            df = df.withColumn(col, F.upper(F.trim(F.col(col))))
    return df

def salvar_silver(df, tabela):
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.{tabela}"))
    print(f"[SILVER] '{tabela}' gravado — {df.count()} registros")

# COMMAND ----------

# MAGIC %md ## regiao

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.regiao")
df = dq_remover_duplicatas(df, ["id_regiao"])
df = dq_remover_nulos(df, ["id_regiao", "nome_regiao"])
df = dq_padronizar_strings(df, ["nome_regiao"])
salvar_silver(df, "regiao")

# COMMAND ----------

# MAGIC %md ## estado

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.estado")
df = dq_remover_duplicatas(df, ["id_estado"])
df = dq_remover_nulos(df, ["id_estado", "nome_estado", "uf"])
df = dq_padronizar_strings(df, ["nome_estado", "uf"])
salvar_silver(df, "estado")

# COMMAND ----------

# MAGIC %md ## municipio

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.municipio")
df = dq_remover_duplicatas(df, ["id_municipio"])
df = dq_remover_nulos(df, ["id_municipio", "nome_municipio"])
df = dq_padronizar_strings(df, ["nome_municipio"])
salvar_silver(df, "municipio")

# COMMAND ----------

# MAGIC %md ## marca

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.marca")
df = dq_remover_duplicatas(df, ["id_marca"])
df = dq_remover_nulos(df, ["id_marca", "nome_marca"])
df = dq_padronizar_strings(df, ["nome_marca", "pais_origem"])
salvar_silver(df, "marca")

# COMMAND ----------

# MAGIC %md ## modelo

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.modelo")
df = dq_remover_duplicatas(df, ["id_modelo"])
df = dq_remover_nulos(df, ["id_modelo", "nome_modelo"])
df = dq_padronizar_strings(df, ["nome_modelo", "categoria"])
salvar_silver(df, "modelo")

# COMMAND ----------

# MAGIC %md ## cliente

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.cliente")
df = dq_remover_duplicatas(df, ["id_cliente"])
df = dq_remover_nulos(df, ["id_cliente", "nome", "cpf"])
# CPF: remove caracteres não numéricos e valida comprimento
df = df.withColumn("cpf", F.regexp_replace(F.col("cpf"), r"[.\-]", ""))
df = df.filter(F.length(F.col("cpf")) == 11)
df = dq_padronizar_strings(df, ["nome"])
# Email lowercase
if "email" in df.columns:
    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))
salvar_silver(df, "cliente")

# COMMAND ----------

# MAGIC %md ## endereco

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.endereco")
df = dq_remover_duplicatas(df, ["id_endereco"])
df = dq_remover_nulos(df, ["id_endereco", "id_cliente", "logradouro"])
# CEP: mantém apenas dígitos
if "cep" in df.columns:
    df = df.withColumn("cep", F.regexp_replace(F.col("cep"), r"[.\-]", ""))
df = dq_padronizar_strings(df, ["logradouro", "bairro"])
salvar_silver(df, "endereco")

# COMMAND ----------

# MAGIC %md ## telefone

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.telefone")
df = dq_remover_duplicatas(df, ["id_telefone"])
df = dq_remover_nulos(df, ["id_telefone", "id_cliente", "ddd", "numero"])
# Número: mantém apenas dígitos
df = df.withColumn("numero", F.regexp_replace(F.col("numero"), r"[^\d]", ""))
salvar_silver(df, "telefone")

# COMMAND ----------

# MAGIC %md ## carro

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.carro")
df = dq_remover_duplicatas(df, ["id_carro"])
df = dq_remover_nulos(df, ["id_carro", "id_modelo", "placa", "chassi"])
df = dq_padronizar_strings(df, ["placa", "cor", "combustivel"])
# Ano de fabricação: deve ser razoável
if "ano_fabricacao" in df.columns:
    df = df.filter(F.col("ano_fabricacao").between(1950, 2025))
salvar_silver(df, "carro")

# COMMAND ----------

# MAGIC %md ## apolice

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.apolice")
df = dq_remover_duplicatas(df, ["id_apolice"])
df = dq_remover_nulos(df, ["id_apolice", "id_cliente", "id_carro", "numero_apolice", "data_inicio", "data_fim"])
df = df.withColumn("data_inicio", F.to_date(F.col("data_inicio")))
df = df.withColumn("data_fim",    F.to_date(F.col("data_fim")))
# Data fim deve ser posterior à data início
df = df.filter(F.col("data_fim") > F.col("data_inicio"))
# Valores monetários não negativos
df = df.filter(F.col("valor_cobertura") >= 0)
df = df.filter(F.col("valor_franquia")  >= 0)
df = dq_padronizar_strings(df, ["status"])
salvar_silver(df, "apolice")

# COMMAND ----------

# MAGIC %md ## sinistro

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.sinistro")
df = dq_remover_duplicatas(df, ["id_sinistro"])
df = dq_remover_nulos(df, ["id_sinistro", "id_apolice", "data_ocorrencia"])
df = df.withColumn("data_ocorrencia", F.to_date(F.col("data_ocorrencia")))
df = df.filter(F.col("valor_prejuizo") >= 0)
df = dq_padronizar_strings(df, ["tipo_sinistro", "status"])
salvar_silver(df, "sinistro")

# COMMAND ----------

print("[SILVER] Data Quality aplicado em todas as tabelas com sucesso.")
