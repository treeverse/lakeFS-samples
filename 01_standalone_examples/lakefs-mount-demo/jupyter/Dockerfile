FROM jupyter/tensorflow-notebook:notebook-7.0.6

USER root

RUN apt-get update && apt install -y fuse

RUN pip install lakefs==0.11.1
RUN pip install lakefs-spec==0.11.1
RUN pip install datasets==3.2.0
RUN pip install 'datasets[vision]'
RUN pip install torchvision==0.17.2

# Disable the "Would you like to receive official Jupyter news?" popup
RUN jupyter labextension disable "@jupyterlab/apputils-extension:announcements"

