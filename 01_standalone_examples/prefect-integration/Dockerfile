FROM jupyter/pyspark-notebook:notebook-6.4.11

ENV HADOOP_AWS_VERSION=3.3.1
ENV AWS_SDK_VERSION=1.11.901

USER root

RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -O ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -O ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar

RUN pip install lakefs==0.2.1

RUN pip install pydantic==1.10.11
RUN pip install -U prefect
#RUN prefect config set PREFECT_API_URL="http://host.docker.internal:4200/api"

USER $NB_UID
