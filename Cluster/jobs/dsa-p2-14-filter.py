# DSA Projeto 2 - Script 14 - Filtros

# Imports
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col

# Ambiente
os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nDSA Projeto 2 - Script 14 - Filtros:\n')

# Cria uma sessão Spark com um nome específico para a aplicação
spark = SparkSession.builder.appName('DSAProjeto2-Script14').getOrCreate()

# Cria dados de exemplo
dados_dsa = [
        (("Jonas","","Soares"),["Java","Scala","C++"],"SP","M"),
        (("Clara","Rose",""),["Spark","Java","C++"],"RJ","F"),
        (("Marcela","","Teixeira"),["Rust","Go"],"SP","F"),
        (("Roberto","Vasconcelos","Souza"),["Rust","Go"],"RJ","M"),
        (("Alice","Telles","Morgan"),["Rust","Go"],"RJ","M"),
        (("Luiz","Duarte","Oliveira"),["Python","Go"],"SP","M")]
        
# Define o schema de Struct Aninhada
schema_dados_dsa = StructType([
        StructField('nome', StructType([
             StructField('primeironome', StringType(), True),
             StructField('nomedomeio', StringType(), True),
             StructField('sobrenome', StringType(), True)
             ])),
         StructField('linguagens', ArrayType(StringType()), True),
         StructField('estado', StringType(), True),
         StructField('genero', StringType(), True)
         ])

# Cria o dataframe com a definição do schema
df = spark.createDataFrame(data = dados_dsa, schema = schema_dados_dsa)

# Exibe o esquema do DataFrame para verificar os tipos de dados das colunas
df.printSchema()

# Mostra o DataFrame sem truncar os valores (para visualização completa)
print("Dataframe Completo:")
df.show(truncate=False)

# -- Relação de igualdade --
# Sintax PySpark
print("Filtros de Igualdade:")
df.filter(df.estado == "SP").show(truncate=False)

# Essa linha do script é mais indicada para automações. Já que afunção col, deixa explicito que o acesso deve ser em uma coluna do dataFrame.
df.filter(col("estado") == "SP").show(truncate=False) 

# Sintax SQL
# Indicado para querys completas.
df.filter("genero == 'M'").show(truncate=False)         


# -- Relação de desigualdade --
# Sintax PySpark
print("Filtros de Desigualdade:")
df.filter(~(df.estado == "SP")).show(truncate=False) # Evitar esse script, pois afeta a legibilidade.
df.filter(df.estado != "SP").show(truncate=False)    # Esse é o mais indicado. Pois utiliza direto o operador de comparação.

# Sintax SQL
df.filter("genero <> 'M'").show(truncate=False)      # Essa Sintax é mais propensa a erros.