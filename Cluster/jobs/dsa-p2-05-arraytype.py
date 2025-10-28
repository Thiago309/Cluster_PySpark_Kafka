# DSA Projeto 2 - Script 05 - Array Type e StructType

# Imports
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import array
from pyspark.sql.functions import array_contains

# Ambiente
os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nDSA Projeto 2 - Script 05 - Array Type e StructType:\n')

# Cria uma sessão Spark com um nome específico para a aplicação
spark = SparkSession.builder.appName('DSAProjeto2-Script05').getOrCreate()

# Define dados de exemplo para o DataFrame
dados_dsa = [("Patricia,Freitas",["Python","Rust","C++"],["Scala","Ruby"],"RJ","SP"),
             ("Fernanda,Oliveira,",["Java","Python","C++"],["PHP","Perl"],"MG","RS"),
             ("Carlos,Souza",["PHP","Java"],["Ruby","Python"],"ES","SC")]

# Define um tipo de coluna Array com Strings, sem aceitar valores nulos (exemplo)
arrayCol = ArrayType(StringType(),False)

# Define o esquema (estrutura) do DataFrame
# É indicado utilizar o schema com o StructType para definir o tipo do dado que desejo utilizar.
# Caso não, o Spark decide utilizar o tipo do dado, mas as vezes ele não acerta o tipo ideal.
# Sendo assim, é bom deixar explicito o tipo do dado que desejo utilizar.
schema = StructType([ 
    StructField("nome", StringType(),True), 
    StructField("linguagemMaisUsada", ArrayType(StringType()),True), 
    StructField("linguagemMenosUsada", ArrayType(StringType()),True), 
    StructField("estadoAnterior", StringType(), True), 
    StructField("estadoAtual", StringType(), True) 
  ])

# Cria um DataFrame com os dados e esquema especificados
df = spark.createDataFrame(data = dados_dsa, schema = schema)

# Exibe o esquema do DataFrame
df.printSchema()

# Mostra o DataFrame
df.show()

# Seleciona e mostra o nome e explode a coluna linguagemMaisUsada para linhas individuais
df.select(df.nome, explode(df.linguagemMaisUsada)).show()

# Seleciona e mostra a coluna nome dividida em um array
# Convertendo o tipo da coluna nome para um array
df.select(split(df.nome,",").alias("nomeAsArray")).show()

# Seleciona e mostra o nome e um array dos estados anterior e atual
df.select(df.nome, array(df.estadoAnterior,df.estadoAtual).alias("estadoAsArray")).show()

# Seleciona e mostra se a coluna linguagemMaisUsada contém "Python"
df.select(df.nome, array_contains(df.linguagemMaisUsada,"Python").alias("usa_python")).show()