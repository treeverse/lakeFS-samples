FROM jupyter/pyspark-notebook:notebook-6.4.11

ENV HADOOP_AWS_VERSION=3.3.1
ENV AWS_SDK_VERSION=1.11.901
ENV AIRFLOW_VERSION=2.3.3
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.3.3/constraints-3.10.txt"

USER root

RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -O ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -O ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar
RUN pip install lakefs-client==0.70.2
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
RUN pip install airflow-provider-lakefs==0.44.0
RUN pip install apache-airflow-providers-apache-spark==3.0.0
RUN mkdir -p /root/airflow/dags

USER $NB_UID
