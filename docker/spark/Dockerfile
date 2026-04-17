FROM apache/spark:3.5.1

USER root

# Dependências básicas
RUN apt-get update && \
    apt-get install -y curl ca-certificates openssl python3-pip && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark/jars

# Integração S3/MinIO
RUN curl -f -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -f -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# JDBC PostgreSQL
RUN curl -f -L -o /opt/spark/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Dependências transitivas do hadoop-common
RUN curl -f -L -o /opt/spark/jars/woodstox-core-6.4.0.jar \
    https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/6.4.0/woodstox-core-6.4.0.jar && \
    curl -f -L -o /opt/spark/jars/stax2-api-4.2.1.jar \
    https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar && \
    curl -f -L -o /opt/spark/jars/commons-configuration2-2.8.0.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.8.0/commons-configuration2-2.8.0.jar && \
    curl -f -L -o /opt/spark/jars/commons-logging-1.1.3.jar \
    https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar && \
    curl -f -L -o /opt/spark/jars/re2j-1.1.jar \
    https://repo1.maven.org/maven2/com/google/re2j/re2j/1.1/re2j-1.1.jar

# Python libs
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$JAVA_HOME/bin:/opt/spark/bin:$PATH

RUN chmod -R 777 /opt/spark/jars

USER spark