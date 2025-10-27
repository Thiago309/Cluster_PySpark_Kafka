# Cria Imagem PySpark

# Imagem do SO usada como base
FROM python:3.11-bullseye as spark-base
# Argumentos para facilitar a atualização do download da versão do pyspark
ARG PYSPARK_VERSION=3.5.0

# Atualiza o SO e instala pacotes
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      tree \
      nano \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Variáveis de ambiente
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

# Cria as pastas
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download do arquivo de binários do Spark.
RUN curl https://archive.apache.org/dist/spark/spark-${PYSPARK_VERSION}/spark-${PYSPARK_VERSION}-bin-hadoop3.tgz -o spark-${PYSPARK_VERSION}-bin-hadoop3.tgz \
# Extrai o arquivo baixado na pasta indicada na maquina virtual.
 && tar xvzf spark-${PYSPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
# Remove o arquivo logo a pós a instalação evitando disperdicio de armazenamento.
 && rm -rf spark-${PYSPARK_VERSION}-bin-hadoop3.tgz

# Prepara o ambiente com PySpark
FROM spark-base as pyspark

# Instala as dependências Python. Copia a pasta local de requirements.txt para o diretorio raiz dentro do container o SPARK_HOME:-"/opt/spark"
COPY requirements/requirements.txt .
# Instala as dependencias python do arquivo requirements.txt
RUN pip3 install -r requirements.txt

# Mais variáveis de ambiente
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# Copia o arquivo de configuração do Spark para a imagem
COPY conf/spark-defaults.conf "$SPARK_HOME/conf"
# Copia o arquivo de manipulação de log dos clusters Spark
COPY conf/log4j2.properties "$SPARK_HOME/conf"
# Ajuste de permissões dos binarios
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

# Variável PYTHONPATH
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Copia o script de inicialização dos serviços para a imagem no diretorio raiz do conteiner
COPY entrypoint.sh .

# Ajusta o privilégio
RUN chmod +x entrypoint.sh

# Executa o script cada vez que inicializar um container
ENTRYPOINT ["./entrypoint.sh"]