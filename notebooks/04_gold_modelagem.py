# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold — Modelagem Dimensional (Ralph Kimball)
# MAGIC Lê os dados do schema `silver` e alimenta as tabelas dimensionais e fato no schema `gold`.
# MAGIC
# MAGIC **Modelo estrela:**
# MAGIC - `dim_cliente`
# MAGIC - `dim_carro`
# MAGIC - `dim_data`
# MAGIC - `fato_apolice`
# MAGIC - `fato_sinistro`

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "main"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}")

# COMMAND ----------

# MAGIC %md ## Leitura da camada Silver

# COMMAND ----------

cliente   = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.cliente")
endereco  = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.endereco")
municipio = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.municipio")
estado    = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.estado")
regiao    = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.regiao")
carro     = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.carro")
modelo    = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.modelo")
marca     = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.marca")
apolice   = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.apolice")
sinistro  = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.sinistro")

# COMMAND ----------

# MAGIC %md ## dim_cliente
# MAGIC Consolida cliente + endereço + município + estado + região.

# COMMAND ----------

dim_cliente = (
    cliente
    .join(endereco,  "id_cliente",  "left")
    .join(municipio, "id_municipio","left")
    .join(estado,    "id_estado",   "left")
    .join(regiao,    "id_regiao",   "left")
    .select(
        cliente["id_cliente"].alias("sk_cliente"),
        cliente["nome"],
        cliente["cpf"],
        cliente["email"],
        cliente["data_nascimento"],
        cliente["sexo"],
        endereco["logradouro"],
        endereco["bairro"],
        endereco["cep"],
        municipio["nome_municipio"].alias("municipio"),
        estado["uf"],
        estado["nome_estado"].alias("estado"),
        regiao["nome_regiao"].alias("regiao"),
    )
    .dropDuplicates(["sk_cliente"])
)

(dim_cliente.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_cliente"))

print(f"[GOLD] dim_cliente: {dim_cliente.count()} registros")

# COMMAND ----------

# MAGIC %md ## dim_carro
# MAGIC Consolida carro + modelo + marca.

# COMMAND ----------

dim_carro = (
    carro
    .join(modelo, "id_modelo", "left")
    .join(marca,  "id_marca",  "left")
    .select(
        carro["id_carro"].alias("sk_carro"),
        carro["placa"],
        carro["chassi"],
        carro["ano_fabricacao"],
        carro["cor"],
        carro["combustivel"],
        modelo["nome_modelo"].alias("modelo"),
        modelo["categoria"].alias("categoria_modelo"),
        marca["nome_marca"].alias("marca"),
        marca["pais_origem"],
    )
    .dropDuplicates(["sk_carro"])
)

(dim_carro.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_carro"))

print(f"[GOLD] dim_carro: {dim_carro.count()} registros")

# COMMAND ----------

# MAGIC %md ## dim_data
# MAGIC Gerada a partir das datas presentes em apólices e sinistros.

# COMMAND ----------

datas_apolice  = apolice.select(F.col("data_inicio").alias("data")).union(
                 apolice.select(F.col("data_fim").alias("data")))
datas_sinistro = sinistro.select(F.col("data_ocorrencia").alias("data"))

dim_data = (
    datas_apolice.union(datas_sinistro)
    .dropDuplicates()
    .withColumn("sk_data",      F.date_format(F.col("data"), "yyyyMMdd").cast("int"))
    .withColumn("ano",          F.year("data"))
    .withColumn("mes",          F.month("data"))
    .withColumn("dia",          F.dayofmonth("data"))
    .withColumn("trimestre",    F.quarter("data"))
    .withColumn("semana",       F.weekofyear("data"))
    .withColumn("dia_semana",   F.dayofweek("data"))
    .withColumn("nome_mes",     F.date_format("data", "MMMM"))
    .select("sk_data", "data", "ano", "mes", "dia", "trimestre", "semana", "dia_semana", "nome_mes")
)

(dim_data.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_data"))

print(f"[GOLD] dim_data: {dim_data.count()} registros")

# COMMAND ----------

# MAGIC %md ## fato_apolice

# COMMAND ----------

fato_apolice = (
    apolice
    .withColumn("sk_data_inicio", F.date_format("data_inicio", "yyyyMMdd").cast("int"))
    .withColumn("sk_data_fim",    F.date_format("data_fim",    "yyyyMMdd").cast("int"))
    .withColumn("vigencia_dias",  F.datediff("data_fim", "data_inicio"))
    .select(
        F.col("id_apolice").alias("sk_apolice"),
        F.col("numero_apolice"),
        F.col("id_cliente").alias("sk_cliente"),
        F.col("id_carro").alias("sk_carro"),
        F.col("sk_data_inicio"),
        F.col("sk_data_fim"),
        F.col("vigencia_dias"),
        F.col("valor_cobertura"),
        F.col("valor_franquia"),
        F.col("status"),
    )
)

(fato_apolice.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.fato_apolice"))

print(f"[GOLD] fato_apolice: {fato_apolice.count()} registros")

# COMMAND ----------

# MAGIC %md ## fato_sinistro

# COMMAND ----------

fato_sinistro = (
    sinistro
    .join(apolice.select("id_apolice", "id_cliente", "id_carro"), "id_apolice", "left")
    .withColumn("sk_data", F.date_format("data_ocorrencia", "yyyyMMdd").cast("int"))
    .select(
        F.col("id_sinistro").alias("sk_sinistro"),
        F.col("id_apolice").alias("sk_apolice"),
        F.col("id_cliente").alias("sk_cliente"),
        F.col("id_carro").alias("sk_carro"),
        F.col("sk_data"),
        F.col("tipo_sinistro"),
        F.col("descricao"),
        F.col("valor_prejuizo"),
        F.col("status"),
    )
)

(fato_sinistro.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.fato_sinistro"))

print(f"[GOLD] fato_sinistro: {fato_sinistro.count()} registros")

# COMMAND ----------

# MAGIC %md ## Verificação Final

# COMMAND ----------

spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA_GOLD}").show()

print("[GOLD] Modelagem dimensional concluída com sucesso.")
