FROM apache/spark:3.5.1

USER root

# Instalar dependências básicas + pip
RUN apt-get update && \
    apt-get install -y curl ca-certificates openssl python3-pip && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# S3A jars
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# JDBC PostgreSQL
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.6.0.jar \
    -o /opt/spark/jars/postgresql-42.6.0.jar

# Python libs
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$JAVA_HOME/bin:/opt/spark/bin:$PATH

USER spark