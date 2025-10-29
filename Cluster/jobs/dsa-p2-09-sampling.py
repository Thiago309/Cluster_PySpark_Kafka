# DSA Projeto 2 - Script 09 - Sampling

# Imports
import os
from pyspark.sql import SparkSession

# Ambiente
os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nDSA Projeto 2 - Script 09 - Sampling:\n')

# Cria uma sessão Spark com um nome específico para a aplicação
spark = SparkSession.builder.appName('DSAProjeto2-Script09').getOrCreate()

# Cria um DataFrame com 100 números, de 0 a 99
df = spark.range(100)

print("Exemplos com sample()")
print()

# Imprime uma amostra de aproximadamente 6% dos dados
print(df.sample(0.06).collect())
print()

# Imprime uma amostra de aproximadamente 10% dos dados, com uma semente específica para reprodução
print(df.sample(0.1,123).collect())
print()

# Repete a amostragem anterior para demonstrar a consistência com a mesma semente
print(df.sample(0.1,123).collect())
print()

# Imprime outra amostra de 10%, mas com uma semente diferente, resultando em uma amostra diferente
print(df.sample(0.1,456).collect())
print()

print("Exemplos com Substituição")
print()

# Amostra com substituição, aproximadamente 30% dos dados, com semente especificada
print(df.sample(True,0.3,123).collect())
print()

# Amostra sem substituição, aproximadamente 30% dos dados, com semente especificada
print(df.sample(0.3,123).collect())
print()

print("Exemplos com sampleBy")
print()

# Cria um novo DataFrame adicionando uma coluna 'key', calculada como o resto da divisão do id por 3
df2 = df.select((df.id % 3).alias("key"))

# Realiza uma amostragem estratificada com diferentes frações para cada valor da chave, sem semente especificada
print(df2.sampleBy("key", {0: 0.1, 1: 0.2},0).collect())
print()

print("Exemplos com RDD")
print()

# Cria um RDD de números de 0 a 99
rdd = spark.sparkContext.range(0,100)

# Imprime uma amostra sem substituição de aproximadamente 10% dos dados do RDD, com semente especificada
print(rdd.sample(False,0.1,0).collect())
print()

# Imprime uma amostra com substituição de aproximadamente 30% dos dados do RDD, com semente especificada
print(rdd.sample(True,0.3,123).collect())
print()

# Realiza uma amostragem sem substituição, selecionando 10 elementos aleatórios do RDD, com semente especificada
print(rdd.takeSample(False,10,0))
print()

# Realiza uma amostragem com substituição, selecionando 30 elementos aleatórios do RDD, com semente especificada
print(rdd.takeSample(True,30,123))
print()

# A semente (seed) em operações de amostragem, como as realizadas em processos de seleção aleatória de dados, 
# serve para garantir a reprodutibilidade dos resultados. Quando você especifica uma semente para um algoritmo 
# de amostragem ou qualquer outro tipo de operação que envolve aleatoriedade, você está essencialmente fixando 
# o ponto de partida do gerador de números aleatórios. Isso significa que cada vez que você executar o mesmo código, 
# com a mesma semente, nos mesmos dados, você obterá exatamente o mesmo conjunto de resultados.

"""
Amostragem: é a técnica de selecionar um subconjunto (uma "amostra") de dados de um conjunto muito maior (a "população") 
para análise. O objetivo é que essa pequena amostra represente com precisão as características e tendências do conjunto 
de dados completo.

Analogia: Imagine que você está cozinhando um enorme caldeirão de sopa (a população de dados). Para saber se o tempero está bom,
você não precisa beber o caldeirão inteiro. Você pega uma colher (a amostra), prova, e assume que o sabor daquela colher
representa o sabor de todo o caldeirão. Essa colherada é a sua amostragem.


Por que essa pratica é tão Essencial?

No mundo do Big Data, você lida com volumes massivos de informação (petabytes). Analisar todos os dados o tempo todo é 
muitas vezes impraticável. A amostragem é usada por três motivos principais:

1. Economia de Tempo e Custo Computacional:

Análise: Rodar uma consulta em 10 bilhões de linhas de um data warehouse pode levar horas e custar caro. Rodar a mesma 
consulta em uma amostra de 1% (100 milhões de linhas) pode levar segundos e dar uma resposta 99% precisa.

Machine Learning: Treinar um modelo de machine learning em um conjunto de dados gigantesco pode exigir hardware 
extremamente caro e dias de processamento. Treinar em uma amostra bem-feita é muito mais rápido e, muitas vezes, 
resulta em um modelo quase tão bom quanto.

2. Viabilidade Prática:

Às vezes, é impossível coletar todos os dados. Em pesquisas de opinião, você não pergunta a todos os cidadãos de um país;
você entrevista uma amostra de 1.000 pessoas. Em controle de qualidade, para testar a durabilidade de uma lâmpada, você 
precisa destruí-la; você não pode testar todas as lâmpadas, ou não sobraria nenhuma para vender.

3. Agilidade na Análise (Exploração de Dados):

Quando um analista de dados recebe um novo dataset gigante, o primeiro passo é explorá-lo. Em vez de esperar horas por 
consultas, o analista pode carregar uma pequena amostra na memória do seu computador (em ferramentas como Pandas/Python) 
para entender rapidamente a estrutura, os tipos de dados e as tendências iniciais.


A amostra precisa ser "Justa" (Representativa)

Se você pegar a colherada de sopa apenas do topo do caldeirão, e todo o sal estiver no fundo, sua amostra será ruim (enviesada) 
e sua conclusão estará errada ("a sopa está sem sal!"). Para evitar esse cenário, a amostragem deve ser feita usando métodos estatísticos.


Tipos Comuns de Amostragem

Existem várias "receitas" para coletar a amostra. As mais comuns são:

1. Amostragem Aleatória Simples (Simple Random Sampling):

O que é: Cada ponto de dado tem exatamente a mesma chance de ser escolhido.
Analogia: Colocar o nome de todos os clientes em um chapéu e sortear 100 deles.


2. Amostragem Estratificada (Stratified Sampling):

O que é: Usada quando a população tem subgrupos importantes. Você divide a população em "estratos" (ex: clientes 
"premium", "básicos" e "novos") e depois sorteia aleatoriamente dentro de cada grupo.

Por que usar? Garante que grupos menores e importantes (como os clientes "premium") não sejam deixados de fora do
sorteio por puro azar.


3. Amostragem Sistemática (Systematic Sampling):

O que é: Você organiza seus dados em uma lista, escolhe um ponto de partida aleatório e, em seguida, seleciona cada 
"N-ésimo" item (ex: a cada 100 clientes na lista).

Por que usar? É mais simples de implementar do que um sorteio puramente aleatório.

"""