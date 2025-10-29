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

Por que isso é Importante para Análise de Dados?

1. Escolha da Medida Central: Em dados com alta assimetria (positiva ou negativa), a Média se torna uma medida enganosa, 
pois é fortemente influenciada pelos outliers. Nesses casos, a Mediana (o valor que está exatamente no meio) é uma 
representação muito mais honesta e robusta do "centro" dos dados.

2. Detecção de Outliers: O Skewness lhe diz não apenas se existem outliers (como a Curtose), mas também a direção deles 
(se são valores extremos altos ou baixos).

3. Modelagem de Machine Learning: Muitos modelos de machine learning (especialmente os de regressão) funcionam melhor 
(ou só funcionam corretamente) se os dados tiverem uma distribuição próxima da normal (Skewness ≈ 0). Se seus dados 
forem muito assimétricos, você talvez precise aplicar uma transformação (como log(x)) para "normalizá-los" antes de treinar o modelo.


RESUMO:

Skewness (Assimetria) informa sobre o equilíbrio da sua distribuição, e Kurtosis (Curtose) informa sobre os 
extremos (outliers). Ambos são essenciais para entender a verdadeira forma dos seus dados.

-------------------------------------------------------------------------------------------------------------------------

Desvio Padrão - Standard Deviation (stddev)

É um número que mede o quão "espalhados" os seus dados estão em relação à média (ao valor médio).

(Alta vs. Baixa)

Desvio Padrão BAIXO: Significa que os pontos de dados tendem a estar muito próximos da média. Os dados são consistentes 
e "agrupados".

Desvio Padrão ALTO: Significa que os pontos de dados estão espalhados por uma ampla gama de valores, longe da média. 
Os dados são inconsistentes e "dispersos".


Analogia Mais Simples: Notas da Turma

Imagine duas turmas que fizeram a mesma prova e tiveram a mesma nota média: 7.

Turma A (Baixo stddev): As notas foram 6.5, 7, 7, 7.5, 7.
Todos os alunos tiveram um desempenho muito similar e próximo da média. O "espalhamento" é pequeno.

Turma B (Alto stddev): As notas foram 1, 5, 8, 10, 11.
O desempenho foi totalmente inconsistente. Há notas muito baixas e muito altas, bem longe da média 7. O "espalhamento" 
é enorme.

Embora ambas as turmas tenham a mesma média (7), o desvio padrão contaria uma história completamente diferente sobre 
elas. A Turma A é previsível, a Turma B é caótica.


Por que isso é Importante para Análise de Dados?

O desvio padrão é o seu principal indicador de variabilidade e risco.

1. Entendimento dos Dados: Ele complementa a média. Dizer "a venda média é de R$ 100" não significa muito. Dizer "a 
venda média é R$ 100 com um desvio padrão de R$ 5" (um stddev baixo) significa que a maioria das vendas fica entre R$ 95 
e R$ 105. Dizer "a venda média é R$ 100 com um desvio padrão de R$ 50" (um stddev alto) significa que as vendas estão por
toda parte, talvez de R$ 10 a R$ 200.

2. Detecção de Anomalias (Outliers): O desvio padrão é a régua que usamos para medir o quão "estranho" um ponto de dado é. 
Na estatística, uma regra comum (a "regra 68-95-99.7") diz que em uma distribuição normal, quase todos os dados (99.7%)
devem estar dentro de 3 desvios padrão da média. Se você encontra um ponto de dado que está a 5 desvios padrão de 
distância, é quase certo que seja uma anomalia ou um erro de medição.

3. Modelagem de Machine Learning: Muitas técnicas de pré-processamento, como a "Padronização" (StandardScaler), usam o 
desvio padrão para reescalonar os dados, garantindo que todos os recursos (features) tenham a mesma escala de variação, 
o que melhora o desempenho de muitos algoritmos.


RESUMO

stddev (Desvio Padrão) é o "número-régua" que lhe diz, em média, a que distância seus dados estão da média.

------------------------------------------------------------------------------------------------------------------------

Variância (Variance)

A Variância também é uma medida que quantifica o quão "espalhados" os dados estão em relação à média.


A Relação Fundamental: Variância vs. Desvio Padrão

-> A Variância é o Desvio Padrão ao quadrado. [Variance = (stddev)²]

-> O Desvio Padrão é a raiz quadrada da Variância. [stddev = √Variance]

Como a Variância é Calculada (Conceitualmente)?


A variância é, literalmente, "a média das distâncias ao quadrado da média".

Vamos usar a mesma analogia das notas da Turma B:

Notas: 1, 5, 8, 10, 11

Média: 7

1. Calcule a distância de cada nota até a média:

1 - 7 = -6
5 - 7 = -2
8 - 7 = 1
10 - 7 = 3
11 - 7 = 4

2. Eleve todas essas distâncias ao quadrado (Isso elimina os números negativos e penaliza mais os valores distantes):

(-6)² = 36

(-2)² = 4

(1)² = 1

(3)² = 9

(4)² = 16

3. Calcule a média desses quadrados:

(36 + 4 + 1 + 9 + 16) / 5 = 66 / 5 = 13.2

A Variância da Turma B é 13.2.


A Interpretação

O que significa "13.2"? Nada!

Se as notas eram "pontos", a variância é "pontos ao quadrado". Se estivéssemos medindo preços em Reais (R$), a variância
 seria em "Reais ao quadrado". Ninguém consegue interpretar uma "nota ao quadrado" ou um "Real ao quadrado".

É por isso que quase sempre damos um passo final:

4. Tire a raiz quadrada da Variância para obter o Desvio Padrão:

√13.2 ≈ 3.63

Agora sim temos um número interpretável! O Desvio Padrão é 3.63 pontos. Isso nos diz que, em média, as notas da Turma B 
estão a cerca de 3.63 pontos de distância da média 7.


Então, por que a Variância Existe?

Se a variância é difícil de interpretar, por que nos importamos com ela?

A resposta é que a Variância é muito mais fácil de usar em cálculos estatísticos e matemáticos complexos.

Trabalhar com raízes quadradas (como no Desvio Padrão) torna as fórmulas matemáticas (como em provas estatísticas ou em 
algoritmos de machine learning) muito mais complicadas.

A Variância, por não ter a raiz quadrada, tem propriedades matemáticas "mais limpas" que a tornam a medida preferida para
 os cálculos internos da estatística.

 
Sendo assim, os estatísticos e os algoritmos usam a Variância para fazer as contas. Os, analistas e engenheiros de dados, 
olham o Desvio Padrão para entender o resultado.
"""