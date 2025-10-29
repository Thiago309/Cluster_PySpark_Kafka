# DSA Projeto 2 - Script 11 - Aggregate e Stats

# Imports
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, collect_list
from pyspark.sql.functions import collect_set, sum, avg, min, max, countDistinct, count, sum_distinct
from pyspark.sql.functions import first, last, kurtosis, mean, skewness 
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop
from pyspark.sql.functions import variance, var_samp, var_pop

# Ambiente
os.environ['TERM'] = 'xterm'
os.system('clear')

print('\nDSA Projeto 2 - Script 11 - Aggregate e Stats:\n')

# Cria uma sessão Spark com um nome específico para a aplicação
spark = SparkSession.builder.appName('DSAProjeto2-Script11').getOrCreate()

# Define os dados e o esquema para o DataFrame
dados_dsa = [("Ana", "Vendas", 3000),
             ("Carlos", "Vendas", 4600),
             ("Maria", "Vendas", 4100),
             ("Gabriel", "Contabilidade", 3000),
             ("Eduardo", "Vendas", 3000),
             ("Mariano", "Contabilidade", 3300),
             ("Gustavo", "Contabilidade", 3900),
             ("Pedro", "Marketing", 3000),
             ("Leonardo", "Marketing", 2000),
             ("Wesley", "Vendas", 4100)]

# Define o esquema para o DataFrame
schema = ["nome", "departmento", "salario"]
  
# Cria um DataFrame com os dados e esquema fornecidos
df = spark.createDataFrame(data = dados_dsa, schema = schema)

# Exibe o esquema do DataFrame
df.printSchema()

# Mostra os dados do DataFrame sem truncar os valores
df.show(truncate=False)

# Calcula e imprime a contagem distinta dos salários
print("Contagem Distinta: " + str(df.select(approx_count_distinct("salario")).collect()[0][0]))
print()

# Calcula e imprime a média dos salários
print("Média: " + str(df.select(avg("salario")).collect()[0][0]))
print()

# Coleta e mostra a lista de todos os salários (com repetições)
df.select(collect_list("salario")).show(truncate=False)

# Coleta e mostra o conjunto de salários (sem repetições)
df.select(collect_set("salario")).show(truncate=False)

# Seleciona a contagem distinta dos departamentos e salários
df2 = df.select(countDistinct("departmento", "salario"))

# Mostra o resultado da contagem distinta
df2.show(truncate=False)

# Imprime a contagem distinta de departamentos e salários
print("Contagem Distinta de Departamento e Salario: "+str(df2.collect()[0][0]))
print()

# Calcula e imprime a contagem total dos salários
print("Contagem: "+str(df.select(count("salario")).collect()[0]))
print()

# Seleciona e mostra o primeiro salário da lista
df.select(first("salario")).show(truncate=False)

# Seleciona e mostra o último salário da lista
df.select(last("salario")).show(truncate=False)

# Calcula e mostra a curtose dos salários
df.select(kurtosis("salario")).show(truncate=False)

# Encontra e mostra o maior salário
df.select(max("salario")).show(truncate=False)

# Encontra e mostra o menor salário
df.select(min("salario")).show(truncate=False)

# Calcula e mostra a média dos salários
df.select(mean("salario")).show(truncate=False)

# Calcula e mostra a assimetria dos salários
df.select(skewness("salario")).show(truncate=False)

# Calcula e mostra o desvio padrão amostral e populacional dos salários
df.select(stddev("salario"), stddev_samp("salario"), stddev_pop("salario")).show(truncate=False)

# Calcula e mostra a soma dos salários
df.select(sum("salario")).show(truncate=False)

# Calcula e mostra a soma dos salários distintos
df.select(sum_distinct("salario")).show(truncate=False)

# Calcula e mostra a variância amostral e populacional dos salários
df.select(variance("salario"),var_samp("salario"),var_pop("salario")).show(truncate=False)



"""

Aggregate e Stats

Conceitos fundamentais na análise e processamento de dados, especialmente em ferramentas como PySpark e Pandas.
Embora parecidos, eles têm focos ligeiramente diferentes:

1. Aggregate (Agregação): Foca em resumir os dados através de agrupamentos.

2. Stats (Estatísticas): Foca em descrever a distribuição e as propriedades dos dados.


Aggregate (Agregação)

O que é? Agregação é o processo de pegar muitas linhas de dados e "agregá-las" (ou resumi-las) num conjunto menor de 
linhas, com base num critério de agrupamento.

A analogia mais simples é a de um carrinho de compras. Os itens individuais no seu carrinho são os dados brutos. A 
agregação é o talão de caixa, que não lista cada item individualmente, mas sim o total (soma) a pagar.

No contexto do PySpark (ou SQL), isto é quase sempre sinónimo da operação groupBy().
Como funciona: Você primeiro agrupa os dados por uma ou mais colunas e depois aplica uma função de agregação para 
calcular um valor resumido para esse grupo.

Funções de Agregação Comuns:

count(): Contar o número de linhas no grupo.

sum(): Somar os valores de uma coluna no grupo.

avg() (ou mean()): Calcular a média dos valores no grupo.

min(): Encontrar o valor mínimo no grupo.

max(): Encontrar o valor máximo no grupo.

--------------------------------------------------------------------------------------------------------------------------

Stats (Estatísticas)

O que é? Estatísticas (ou estatísticas descritivas) são cálculos usados para descrever as características principais de
um conjunto de dados, geralmente de uma coluna numérica.

Não se trata de agrupar, mas sim de entender a distribuição e a qualidade dos seus dados.

Analogia: Se os seus dados são os jogadores de uma equipa de futebol, as estatísticas são as "medidas" da equipa: qual é
a altura média? Qual é o jogador mais alto e o mais baixo? Quão "espalhada" está a idade dos jogadores (desvio padrão)?

No PySpark, a forma mais fácil de ver isto é com o comando .describe().

Métricas Estatísticas Comuns:

count: Contagem total de registos.

mean: A média (o valor "típico").

stddev: O desvio padrão (o quão "espalhados" os dados estão em relação à média).

min: O valor mínimo.

max: O valor máximo.

Percentis (ex: 25%, 50% - a mediana, 75%): Descrevem a distribuição.


Diferença Principal (Resumida)


ConceitoPergunta que RespondePrincipal Comando PySpark

Aggregate: Qual é o total/média por grupo? --> df.groupBy(...).agg(...)

Stats: Qual é a natureza dos meus dados (média, desvio, etc.)? --> df.describe()


---------------------------------------------------------------------------------------------------------------------

Curtose (Kurtose)

É uma medida estatística que descreve a forma de uma distribuição de dados, especificamente o quão "pesadas" são as
suas "caudas" (tails). Em termos mais práticos, a curtose nos diz se os seus dados têm mais ou menos valores extremos 
(outliers) do que uma distribuição normal (aquela clássica forma de "sino").

O Ponto de Referência: A Distribuição Normal
Tudo sobre a curtose é medido em comparação com a distribuição normal.

Importante: A maioria dos softwares (incluindo PySpark e Pandas) calcula o "excesso de curtose" (excess kurtosis), que 
é Curtose - 3.

Neste modelo, uma distribuição normal perfeita tem um excesso de curtose igual a 0.

Usaremos esse valor (0) como nosso ponto de referência.


1. Mesocúrtica (Excesso de Curtose ≈ 0)

O que é: É a distribuição normal.

Forma: A forma de sino clássica.

O que significa: A quantidade de valores extremos (outliers) é exatamente a que seria esperada em uma distribuição 
normal. Seus dados se comportam de maneira "padrão".


2. Leptocúrtica (Excesso de Curtose > 0, Positiva)

O que é: Caudas pesadas (Lepto = magro).

Forma: O pico central é mais alto e "magro" (pontudo), e as caudas são mais longas e grossas.

O que significa: Seus dados têm MAIS outliers do que o esperado. Há uma probabilidade maior de ocorrerem valores muito 
extremos (tanto positivos quanto negativos).

Exemplo: Retornos do mercado de ações. A maioria dos dias tem pouca mudança (pico alto), mas há mais dias de "crash" ou
"boom" (caudas pesadas) do que um modelo normal preveria.


3. Platicúrtica (Excesso de Curtose < 0, Negativa)

O que é: Caudas leves (Platy = achatado).

Forma: O pico central é mais baixo e "achatado", e as caudas são mais curtas e finas.

O que significa: Seus dados têm MENOS outliers do que o esperado. Os valores estão mais concentrados em torno da média, 
e valores extremos são muito raros.

Exemplo: Uma distribuição uniforme (onde todos os resultados têm a mesma probabilidade, como rolar um dado) é um exemplo 
de distribuição platicúrtica.

------------------------------------------------------------------------------------------------------------------------

Skewnewss (Assimetria ou Obliquidade)

É outro conceito estatístico fundamental, parceiro da Curtose, que também descreve a forma de uma distribuição de dados.
Mede o grau de falta de simetria de uma distribuição de dados. Informa se a curva dos seus dados é simétrica (como um 
sino perfeito) ou se ela está "puxada" ou "esticada" para um dos lados.

O Ponto de Referência: A Distribuição Simétrica

Novamente, o nosso ponto de referência é a distribuição normal, que é perfeitamente simétrica. Uma distribuição
perfeitamente simétrica tem um Skewness = 0. Nela, a Média, a Mediana (o valor do meio) e a Moda (o valor mais ]
frequente) estão todas no mesmo ponto central. O Skewness nos mostra o quanto a sua distribuição se desvia desse
equilíbrio perfeito.

Os 3 Tipos de Skewness (Assimetria)

Existem três possibilidades:


1. Simétrica (Skewness ≈ 0)
O que é: A distribuição é balanceada. Os dados estão distribuídos de forma igual em ambos os lados do centro.

Cauda: As caudas esquerda e direita são do mesmo tamanho.

Média vs. Mediana: Média ≈ Mediana ≈ Moda.

Exemplo: Altura de pessoas em uma população grande, peso de produtos industrializados.


2. Assimetria Positiva (Positive Skew ou Right-Skewed) (Skewness > 0)
O que é: A distribuição tem uma cauda longa à direita.

Forma: A maioria dos seus dados está concentrada à esquerda (valores mais baixos), mas alguns poucos valores muito altos
(outliers) "puxam" a distribuição para a direita.

Média vs. Mediana: Média > Mediana. Os valores altos (outliers) "inflacionam" a média, puxando-a para a direita, enquanto
a mediana permanece mais resistente.

Exemplo Clássico: Salários. A maioria das pessoas em uma empresa ganha um salário "normal" (à esquerda), mas alguns 
poucos executivos com salários astronômicos criam uma longa cauda à direita.


3. Assimetria Negativa (Negative Skew ou Left-Skewed) (Skewness < 0)
O que é: A distribuição tem uma cauda longa à esquerda.

Forma: A maioria dos seus dados está concentrada à direita (valores mais altos), mas alguns poucos valores muito baixos 
(outliers) "puxam" a distribuição para a esquerda.

Média vs. Mediana: Média < Mediana. Os valores baixos (outliers) "puxam" a média para a esquerda.

Exemplo Clássico: Notas de uma prova muito fácil. A maioria dos alunos tirou notas altas (8, 9, 10), concentrando 
os dados à direita, mas alguns poucos alunos que não estudaram tiraram notas muito baixas (1, 2), criando uma cauda 
à esquerda.

---------------------------------------------------------------------------------------------------------------------------

RESUMO:

Skewness (Assimetria) informa sobre o equilíbrio da sua distribuição, e Kurtosis (Curtose) informa sobre os 
extremos (outliers). Ambos são essenciais para entender a verdadeira forma dos seus dados.

"""