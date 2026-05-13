# Arquitetura Medalhão — SeguroDB

## Fonte de Dados

**SeguroDB** — banco de dados SQL Server com 11 tabelas do domínio de seguros veiculares:

| Tabela | Descrição |
|---|---|
| `regiao` | 5 regiões do Brasil |
| `estado` | 27 estados + DF |
| `municipio` | 30 municípios |
| `marca` | Marcas de veículos |
| `modelo` | Modelos de veículos |
| `cliente` | Dados cadastrais dos segurados |
| `endereco` | Endereços dos clientes |
| `telefone` | Telefones dos clientes |
| `carro` | Veículos segurados |
| `apolice` | Apólices de seguro |
| `sinistro` | Sinistros registrados |

## Camadas

### LANDING / DADOS
Extração via JDBC do SQL Server → arquivos **CSV** gravados no schema `landing`.

### BRONZE
Leitura dos CSVs → gravação em **Delta Lake** no schema `bronze`. Dados brutos preservados.

### SILVER
Regras de Data Quality aplicadas tabela a tabela:

- Remoção de duplicatas por chave primária
- Remoção de nulos em campos obrigatórios
- Padronização de strings (UPPER + TRIM)
- Limpeza de CPF e CEP (só dígitos)
- Validação de datas e valores monetários

### GOLD — Modelo Estrela (Ralph Kimball)

```
         dim_cliente
              │
dim_data ─── fato_apolice ─── dim_carro
              │
         fato_sinistro
```

| Tabela | Tipo | Descrição |
|---|---|---|
| `dim_cliente` | Dimensão | Cliente + endereço + município + estado + região |
| `dim_carro` | Dimensão | Carro + modelo + marca |
| `dim_data` | Dimensão | Calendário gerado das datas de apólices e sinistros |
| `fato_apolice` | Fato | Apólices com métricas de cobertura, franquia e vigência |
| `fato_sinistro` | Fato | Sinistros com valor de prejuízo e tipo |
