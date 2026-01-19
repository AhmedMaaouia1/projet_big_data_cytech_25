# ============================================================================
# DOCKERFILE AIRFLOW + SPARK
# ============================================================================
# Cette image étend l'image officielle Airflow en ajoutant:
#   1. Apache Spark (pour spark-submit)
#   2. Java (requis par Spark)
#   3. Le provider Airflow pour Spark
#   4. Les dépendances Python pour le ML (PySpark)
#
# Pourquoi cette image custom?
#   - SparkSubmitOperator nécessite spark-submit accessible
#   - L'image Airflow officielle n'a pas Spark
#   - On veut éviter docker exec (anti-pattern)
# ============================================================================

FROM apache/airflow:2.8.1-python3.10

# ============================================================================
# ÉTAPE 1: Installation en tant que root
# ============================================================================
USER root

# Installer Java (requis par Spark) et PostgreSQL client (pour EX03)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Définir JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ============================================================================
# ÉTAPE 2: Installation de Spark
# ============================================================================
# On télécharge et installe Spark 3.5.1 (même version que le cluster)
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && chmod -R 755 ${SPARK_HOME}

# Ajouter Spark au PATH
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# ============================================================================
# ÉTAPE 3: Installation des JARs nécessaires (S3/MinIO, PostgreSQL)
# ============================================================================
# Ces JARs permettent à Spark de communiquer avec MinIO et PostgreSQL
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar \
    && curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar \
    && curl -fsSL https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
    -o ${SPARK_HOME}/jars/postgresql-42.7.4.jar

# ============================================================================
# ÉTAPE 4: Installation des packages Python (en tant que airflow)
# ============================================================================
USER airflow

# Installer le provider Apache Spark pour Airflow
# C'est ce package qui fournit SparkSubmitOperator
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    pyspark==${SPARK_VERSION}

# ============================================================================
# ÉTAPE 5: Configuration Spark pour S3/MinIO
# ============================================================================
# NOTE: Les credentials S3A sont passés via variables d'environnement
# au runtime (docker-compose.yml) et non hardcodés ici.
# Seuls les paramètres non-sensibles sont dans spark-defaults.conf
# ============================================================================
USER root

# Créer le fichier de configuration Spark (paramètres non-sensibles uniquement)
# Les credentials (access.key, secret.key) sont passés via SPARK_CONF dans le DAG
RUN echo 'spark.hadoop.fs.s3a.endpoint=minio:9000\n\
spark.hadoop.fs.s3a.path.style.access=true\n\
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem\n\
spark.hadoop.fs.s3a.connection.ssl.enabled=false' > ${SPARK_HOME}/conf/spark-defaults.conf

# Revenir à l'utilisateur airflow
USER airflow

# ============================================================================
# RÉSUMÉ DE CE QUE CETTE IMAGE CONTIENT:
# ============================================================================
# - Apache Airflow 2.8.1
# - Java 17 (OpenJDK)
# - Apache Spark 3.5.1
# - Provider apache-airflow-providers-apache-spark
# - JARs: hadoop-aws, aws-java-sdk, postgresql
# - Configuration S3A pour MinIO
# ============================================================================
