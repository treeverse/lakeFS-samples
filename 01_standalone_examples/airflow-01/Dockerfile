FROM jupyter/pyspark-notebook:notebook-6.4.11

ENV HADOOP_AWS_VERSION=3.3.1
ENV AWS_SDK_VERSION=1.11.901
ENV AIRFLOW_VERSION=2.5.1
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.5.1/constraints-3.10.txt"
ENV _AIRFLOW_DB_UPGRADE='true'
ENV _AIRFLOW_WWW_USER_CREATE='true'
ENV _AIRFLOW_WWW_USER_USERNAME='airflow'
ENV _AIRFLOW_WWW_USER_PASSWORD='airflow'
ENV AIRFLOW__API__AUTH_BACKENDS='airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'
ENV AIRFLOW__CORE__FERNET_KEY='Ty-eEUIQQdUjarNo7kEbKza1AeKv4zMxE3q07MafCEs='
ENV SQLALCHEMY_SILENCE_UBER_WARNING=1

USER root

RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -O ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -O ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar

RUN pip install lakefs==0.2.1
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
RUN pip install airflow-provider-lakefs==0.48.0
RUN pip install apache-airflow-providers-apache-spark==4.0.0

USER $NB_UID
