FROM apache/airflow:2.9.3-python3.11

USER root

# Java is required by PySpark (JVM under the hood)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Connecteur S3A (+ SDK AWS), requis par PySpark pour lire/ecrire sur MinIO.
# Versions alignees sur le Hadoop 3.3.4 embarque par pyspark==3.5.1.
USER root
RUN mkdir -p /opt/spark-jars \
    && curl -L -o /opt/spark-jars/hadoop-aws-3.3.4.jar \
       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -L -o /opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar \
       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
USER airflow

# Make our pipeline code importable from DAGs
ENV PYTHONPATH="/opt/airflow/src:${PYTHONPATH}"
