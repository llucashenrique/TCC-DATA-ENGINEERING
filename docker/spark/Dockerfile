FROM apache/spark:3.5.1

USER root

# Instalar dependências básicas
RUN apt-get update && \
    apt-get install -y curl ca-certificates openssl && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Baixar dependências necessárias para S3A (compatível com Hadoop 3.3.4)
RUN curl -k -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -k -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Adiciona driver JDBC PostgreSQL
RUN curl -k -L https://jdbc.postgresql.org/download/postgresql-42.6.0.jar \
    -o /opt/spark/jars/postgresql-42.6.0.jar

# Voltar para o usuário padrão do Spark
USER spark