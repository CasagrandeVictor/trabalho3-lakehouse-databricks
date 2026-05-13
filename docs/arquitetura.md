# Arquitetura Medalhão

## Camadas

### LANDING / DADOS
Dados brutos extraídos diretamente das fontes originais e gravados no schema `landing` do Databricks.

- Fontes relacionais → formato **CSV**
- Fontes não relacionais → formato **JSON**

### BRONZE
Leitura dos arquivos da camada Landing e gravação no formato **Delta Lake** no schema `bronze`.  
Sem transformações: dados brutos preservados.

### SILVER
Aplicação de regras de **Data Quality** sobre os dados Bronze:

- Remoção de duplicatas
- Tratamento de nulos
- Padronização de tipos
- Validações de negócio

Resultado gravado no schema `silver` em formato Delta Lake.

### GOLD
Modelagem dimensional (Ralph Kimball) sobre os dados Silver:

- Tabelas de dimensão (`dim_*`)
- Tabelas fato (`fato_*`)

Gravado no schema `gold` para consumo analítico.

## Fluxo

```
Fonte Relacional (PostgreSQL/Supabase)  ──┐
                                          ├──► LANDING ──► BRONZE ──► SILVER ──► GOLD
Fonte Não Relacional (MongoDB Atlas)    ──┘
```
