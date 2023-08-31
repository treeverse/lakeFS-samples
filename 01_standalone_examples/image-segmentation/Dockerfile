FROM jupyter/all-spark-notebook:notebook-6.5.3

ENV HADOOP_AWS_VERSION=3.3.1
ENV AWS_SDK_VERSION=1.11.901
ENV LAKEFS_VERSION=0.107.0

USER root

RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -O ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -O ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar
RUN wget https://github.com/treeverse/lakeFS/releases/download/v${LAKEFS_VERSION}/lakeFS_${LAKEFS_VERSION}_Linux_x86_64.tar.gz -O /usr/local/bin/lakeFS_${LAKEFS_VERSION}_Linux_x86_64.tar.gz
RUN tar -xvzf /usr/local/bin/lakeFS_${LAKEFS_VERSION}_Linux_x86_64.tar.gz -C /usr/local/bin/ && rm /usr/local/bin/lakeFS_${LAKEFS_VERSION}_Linux_x86_64.tar.gz

RUN pip install lakefs-client==${LAKEFS_VERSION}
RUN pip install boto3==1.21.32
RUN pip install tabulate
RUN pip install petastorm==0.12.1
RUN pip install mlflow==2.6.0
RUN pip install hyperopt==0.2.7
RUN pip install pydantic==1.10.11
RUN pip install torch==1.13.1
RUN pip install torchvision==0.14.1
RUN pip install segmentation-models-pytorch==0.3.3
RUN pip install pytorch-lightning==1.5.4

USER $NB_UID

# Disable the "Would you like to receive official Jupyter news?" popup
RUN jupyter labextension disable "@jupyterlab/apputils-extension:announcements"
