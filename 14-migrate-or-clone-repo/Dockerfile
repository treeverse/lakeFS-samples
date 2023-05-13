FROM jupyter/pyspark-notebook:notebook-6.4.11

USER root

RUN wget https://aka.ms/downloadazcopy-v10-linux -O /usr/local/bin/downloadazcopy-v10-linux && \
    cd /usr/local/bin && \
    tar -vxf downloadazcopy-v10-linux && \
    cd azcopy_linux_amd64_10.* && \
    chmod 755 azcopy && mv azcopy .. && rm NOTICE.txt && cd .. && \
    rmdir azcopy_linux_amd64_10.*/ && \
    rm downloadazcopy-v10-linux
    
RUN pip install lakefs-client==0.100.0
RUN pip install awscli
RUN pip install awscliv2

USER $NB_UID
