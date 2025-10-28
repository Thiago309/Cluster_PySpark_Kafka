# DSA Projeto 2 - Script 07 - Dicionário e Dataframe

# Imports
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, MapType, IntegerType
from pyspark.sql.functions import explode, map_keys, col

# Ambiente
os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nDSA Projeto 2 - Script 07 - Dicionário e Dataframe:\n')

# Cria uma sessão Spark com um nome específico para a aplicação
spark = SparkSession.builder.appName('DSAProjeto2-Script07').getOrCreate()

# Define os dados de entrada, contendo nomes e classificações em formato de dicionário
dados_dsa = [('Eduardo', {'categoria':'black','nivel':'brown'}),
             ('Sofia', {'categoria':'brown','nivel':None}),
             ('Gabriel', {'categoria':'red','nivel':'black'}),
             ('Fernanda', {'categoria':'grey','nivel':'grey'}),
             ('Carlos', {'categoria':'brown','nivel':''})]

# Cria um DataFrame com os dados fornecidos e esquema inicial simples
df = spark.createDataFrame(data = dados_dsa, schema = ['nome','classificacao'])

# Exibe o esquema do DataFrame para visualização
df.printSchema()

# Mostra o DataFrame completo sem truncar os dados
df.show(truncate=False)

# Define um esquema com tipos específicos, incluindo MapType(Tipo: Dicionario)
schema = StructType([
    # Define a coluna nome como tipo String e define que é possivel receber valores nulos.
    StructField('nome', StringType(), True),
    # Define a coluna classificação como tipo Dicionario(map) e define as chaves(key= nivel & categoria) como tipo String e valores(values= brown, black e etc...) como tipo valor.
    StructField('classificacao', MapType(StringType(),StringType()),True)
])

# Cria um novo DataFrame com os dados fornecidos e o esquema definido
df2 = spark.createDataFrame(data = dados_dsa, schema = schema)

# Exibe o esquema do novo DataFrame para visualização
df2.printSchema()

# Mostra o novo DataFrame completo sem truncar os dados
df2.show(truncate=False)


# ------------ Primeiro método (Fácil) de Como Separar as Informações de um Dicionario -----------

# Transforma o DataFrame para separar as informações de classificação em colunas distintas
df3 = df.rdd.map(lambda x: \
      (x.nome, x.classificacao["categoria"], x.classificacao["nivel"])) \
      .toDF(["nome","categoria","nivel"])

# Exibe o esquema do DataFrame transformado
df3.printSchema()

# Mostra o DataFrame transformado
df3.show()


# -------------- Segundo método (Intermediario) de Como Separar as Informações de um Dicionario -------------

# Adiciona colunas 'categoria' e 'nivel' ao DataFrame original, extraindo valores do mapa de classificação, 
# e remove a coluna original de classificação
df.withColumn("categoria", df.classificacao.getItem("categoria")) \
  .withColumn("nivel", df.classificacao.getItem("nivel")) \
  .drop("classificacao") \
  .show()


# -------------- Terceiro método (Bem Fácil) de Como Separar as Informações de um Dicionario -------------

# Não há diferença de performance entre o método anterior e esse. Porem o ".getItem", ajuda na legibilidade do código no momento de manutenção.
# Outra maneira de adicionar colunas 'categoria' e 'nivel', similar à anterior, usando sintaxe 
# de acesso direto ao dicionário
df.withColumn("categoria", df.classificacao["categoria"]) \
  .withColumn("nivel", df.classificacao["nivel"]) \
  .drop("classificacao") \
  .show()


# -------------- Quarto método (Dificil) de Como Separar as Informações de um Dicionario -------------

# Tem como beneficio executar cada tarefa de foma separada. Isso para um grande volume de dados é extremamente útil.

# 1. Seleciona e mostra distintas chaves do mapa de classificação, preparando para a extração de colunas
niveisDF = df.select(explode(map_keys(df.classificacao))).distinct()

# 2. Converte o DataFrame de chaves para uma lista
niveisList = niveisDF.rdd.map(lambda x:x[0]).collect()

# 3. Prepara uma lista de colunas, criando colunas dinamicamente com base nas chaves do mapa
niveisCols = list(map(lambda x: col("classificacao").getItem(x).alias(str(x)), niveisList))

# 4. Seleciona o nome e as colunas dinâmicas criadas a partir das chaves do mapa de classificação
df.select(df.nome, *niveisCols).show()