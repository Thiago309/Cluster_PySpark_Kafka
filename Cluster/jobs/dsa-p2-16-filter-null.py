# DSA Projeto 2 - Script 16 - Filtro de Nulos

# Imports
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Ambiente
os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nDSA Projeto 2 - Script 16 - Filtro de Nulos:\n')

# Cria uma sessão Spark com um nome específico para a aplicação
spark = SparkSession.builder.appName('DSAProjeto2-Script16').getOrCreate()

# Cria uma lista de tuplas com dados, onde alguns valores estão ausentes (None)
dados_dsa = [("Bruno", None, "M"), ("Laura", "Fortaleza", "F"), ("Vagner", None, None)]

# Define os nomes das colunas para o DataFrame
colunas = ["nome", "estado", "genero"]

# Cria um DataFrame com os dados e nomes de colunas especificados
df = spark.createDataFrame(dados_dsa, colunas)

print("Schema")
# Exibe o esquema do DataFrame, mostrando os tipos de dados e estrutura
df.printSchema()

print("DataFrame")
# Exibe os dados do DataFrame
df.show()

print("Filtra as linhas onde o valor da coluna 'estado' é nulo")
# Filtra as linhas onde o valor da coluna 'estado' é nulo usando sintaxe SQL
df.filter("estado is NULL").show()

print("Filtra as linhas onde o valor da coluna 'estado' é nulo usando função do DataFrame")
# Filtra as linhas onde o valor da coluna 'estado' é nulo usando função do DataFrame
df.filter(df.estado.isNull()).show()

print("Filtra as linhas onde o valor da coluna 'estado' é nulo usando função 'col' para referenciar a coluna")
# Filtra as linhas onde o valor da coluna 'estado' é nulo usando função 'col' para referenciar a coluna
df.filter(col("estado").isNull()).show()

print("Filtra as linhas onde tanto 'estado' quanto 'genero' são nulos")
# Filtra as linhas onde tanto 'estado' quanto 'genero' são nulos usando sintaxe SQL
df.filter("estado IS NULL AND genero IS NULL").show()

print("Filtra as linhas onde tanto 'estado' quanto 'genero' são nulos usando funções do DataFrame")
# Filtra as linhas onde tanto 'estado' quanto 'genero' são nulos usando funções do DataFrame
df.filter(df.estado.isNull() & df.genero.isNull()).show()

print("Filtra as linhas onde o valor da coluna 'estado' não é nulo usando diferentes abordagens")
# Filtra as linhas onde o valor da coluna 'estado' não é nulo usando diferentes abordagens
df.filter("estado is not NULL").show()
df.filter("NOT estado is NULL").show()
df.filter(df.estado.isNotNull()).show()
df.filter(col("estado").isNotNull()).show()

print("Remove as linhas onde o valor da coluna 'estado' é nulo")
# Remove as linhas onde o valor da coluna 'estado' é nulo
df.na.drop(subset=["estado"]).show()

# Cria ou substitui uma visualização temporária com o nome 'DATA' para o DataFrame atual
df.createOrReplaceTempView("DATA")

print("Executa consultas SQL sobre a visualização 'DATA', filtrando linhas com base na presença de valores nulos em 'ESTADO' e/ou 'GENERO'")
# Executa consultas SQL sobre a visualização 'DATA', filtrando linhas com base na presença de valores nulos em 'ESTADO' e/ou 'GENERO'
spark.sql("SELECT * FROM DATA where ESTADO IS NULL").show()
spark.sql("SELECT * FROM DATA where ESTADO IS NULL AND GENERO IS NULL").show()
spark.sql("SELECT * FROM DATA where ESTADO IS NOT NULL").show()