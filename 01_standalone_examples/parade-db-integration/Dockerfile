FROM jupyter/base-notebook:notebook-7.0.6

USER root

RUN apt-get update
RUN apt install -y postgresql-client

RUN pip install lakefs==0.7.1

USER $NB_UID

# Disable the "Would you like to receive official Jupyter news?" popup
RUN jupyter labextension disable "@jupyterlab/apputils-extension:announcements"
