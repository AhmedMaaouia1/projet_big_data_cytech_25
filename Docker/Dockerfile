FROM eclipse-temurin:8-jre-jammy

ARG SPARK_VERSION=3.5.1
ARG HADOOP_PROFILE=hadoop3
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-distutils \
    python3-pip \
    curl \
    bash \
    ca-certificates \
    tini \
    procps \
 && rm -rf /var/lib/apt/lists/*

# Ensure python command exists
RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir \
    numpy \
    pandas \
    flake8 \
    pytest \
    pyment \
    autopep8

# Install Spark
RUN mkdir -p /opt \
 && curl -fSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz -o /tmp/spark.tgz \
 && tar -xzf /tmp/spark.tgz -C /opt \
 && mv /opt/${SPARK_PACKAGE} ${SPARK_HOME} \
 && rm /tmp/spark.tgz

# Add Hadoop AWS + AWS SDK bundle (for s3a://)
# Spark 3.5.x (hadoop3 build) is typically aligned with Hadoop 3.3.x.
# We'll use 3.3.4 which is a common match for Spark hadoop3 distributions.
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar \
 && curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar

# PostgreSQL JDBC driver
RUN curl -fSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
      -o ${SPARK_HOME}/jars/postgresql-42.7.4.jar

# Default Spark conf
RUN mkdir -p ${SPARK_HOME}/conf
COPY Docker/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

# Entrypoint
COPY Docker/start-spark.sh /start-spark.sh
RUN chmod +x /start-spark.sh

WORKDIR /opt/workdir
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/start-spark.sh"]
