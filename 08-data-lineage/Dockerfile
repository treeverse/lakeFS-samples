FROM jupyter/pyspark-notebook:notebook-6.4.11

ENV HADOOP_AWS_VERSION=3.3.1
ENV AWS_SDK_VERSION=1.11.901

USER root

RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -O ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -O ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar
RUN wget https://repo1.maven.org/maven2/io/lakefs/hadoop-lakefs-assembly/0.1.12/hadoop-lakefs-assembly-0.1.12.jar -O ${SPARK_HOME}/jars/hadoop-lakefs-assembly-0.1.12.jar
RUN wget https://repo1.maven.org/maven2/io/lakefs/lakefs-spark-client-312-hadoop3_2.12/0.5.1/lakefs-spark-client-312-hadoop3_2.12-0.5.1.jar -O ${SPARK_HOME}/jars/lakefs-spark-client-312-hadoop3_2.12-0.5.1.jar

RUN pip install lakefs-client==0.96.1

USER $NB_UID
