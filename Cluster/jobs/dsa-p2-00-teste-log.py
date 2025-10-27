# DSA Projeto 2 - Script 00 - Testando o Nível de Log

# Imports
import os
from pyspark.sql import SparkSession

# Ambiente
# Definindo dentro do conteiner pyspark o 'xterm' como variavel de ambiente do terminal correspondente ao linux ou MacOS.
os.environ['TERM'] = 'xterm'
# Executa limpeza do terminal a cada execução do Script 00
os.system('clear')

# Comando Python para exibir o titulo do Script.
print('\nDSA Projeto 2 - Script 00 - Testando o Nível de Log:\n')

# Cria uma sessão Spark para interagir com o conteiner
spark = SparkSession.builder.appName('DSAProjeto2-Script00').getOrCreate()

# Cria os dados
dados = [("FCD", "Data Science", 6), ("FED", "Engenharia de Dados", 5), ("FADA", "Analytics", 4)]

# Colunas
colunas = ["nome", "categoria", "num_cursos"]

# Cria o dataframe
df = spark.createDataFrame(data = dados, schema = colunas)

# Mostra a tabela
df.show(truncate=False)
