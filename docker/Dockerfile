FROM apache/airflow:3.2.1

USER root

# Instala o JDK (necessário para o PySpark/Spark rodar a JVM)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Descobre dinamicamente o caminho do Java instalado e exporta o JAVA_HOME
RUN JAVA_PATH=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "export JAVA_HOME=$JAVA_PATH" >> /etc/profile.d/java_home.sh && \
    ln -sfn $JAVA_PATH /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# Volta para o usuário airflow para instalar os pacotes Python com segurança
USER airflow

# Instala as dependências de dados direto na imagem para acelerar o bootstrap dos containers
RUN pip install --no-cache-dir kaggle boto3 botocore python-dotenv pyspark