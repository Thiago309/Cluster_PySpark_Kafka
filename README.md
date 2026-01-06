![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Linux](https://img.shields.io/badge/Linux-FCC624?style=for-the-badge&logo=linux&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Engenharia de Dados](https://img.shields.io/badge/Engenharia%20de%20Dados-orange?style=for-the-badge)

## Cluster_PySpark_Kafka
Implementação prática de um Cluster PySpark integrado ao Kafka para processamento de Big Data. O projeto reúne 50 scripts de automação e transformação de dados, além da documentação completa do ambiente configurado com Docker e WSL2.

---
## 📂 Estrutura do Projeto

```bash
Cluster/
├── conf/
│   ├── log4j2.properties         # Configuração de logs do Spark
│   └── spark-defaults.conf       # Configurações padrão do ambiente Spark
├── dados/                        # Diretório mapeado (Input/Output de dados)
│   ├── coalesce2/
│   ├── dsa_partition1/
│   ├── partition*/               # Diretórios demonstrando estratégias de particionamento
│   ├── range-partition/
│   ├── re-partition/
│   ├── zipcodes-estado*/         # Dados geográficos particionados
│   ├── dataset1.txt
│   ├── dataset2.csv
│   ├── sqlite-jdbc-3.50.3.0.jar  # Driver JDBC para conexão com banco SQLite
│   ├── usuarios.db               # Banco de dados SQLite
│   └── usuarios.json
├── jobs/                         # Scripts de Processamento (PySpark)
│   ├── dsa-p2-00-teste-log.py    # Script inicial de teste
│   ├── ...                       # (Scripts 01 ao 49 cobrindo RDDs, DataFrames, SQL, etc.)
│   ├── dsa-p2-50-window-functions.py
│   └── projeto1.py               # Projeto prático consolidado
├── requirements/
│   └── requirements.txt          # Lista de dependências Python (pip)
├── .env.spark                    # Variáveis de ambiente para o Spark
├── .gitattributes
├── .gitignore
├── Dockerfile                    # Definição da imagem Docker do Cluster
├── LEIAME.txt                    # Instruções rápidas
├── LICENSE
├── README.md                     # Documentação oficial
├── docker-compose.yml            # Orquestração dos containers (Master/Workers)
└── entrypoint.sh                 # Script de inicialização do container
```
---
## 📝 Autor 

![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)

Desenvolvido por [Thiago Vinicius](https://www.linkedin.com/in/thiagoviniciusbsantos/).
