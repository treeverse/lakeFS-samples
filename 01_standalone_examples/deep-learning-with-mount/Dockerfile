FROM jupyter/tensorflow-notebook:notebook-7.0.6

USER root

RUN pip install lakefs==0.7.1
RUN pip install numpy

# Disable the "Would you like to receive official Jupyter news?" popup
RUN jupyter labextension disable "@jupyterlab/apputils-extension:announcements"
