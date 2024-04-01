FROM jupyter/minimal-notebook:notebook-7.0.6

USER root

# These commands install the cv2 dependencies that are normally present on the local machine
RUN apt-get update && apt-get install ffmpeg libsm6 libxext6  -y

RUN pip install lakefs==0.4.1

# Used openai 0.28.1 (old version) for langchain v0.0.331 compatibility
RUN pip install langchain==0.0.331 unstructured[pdf]==0.10.29 openai==0.28.1 tiktoken==0.4.0

RUN conda install -y -c pytorch faiss-cpu=1.7.4 mkl=2021 blas=1.0=mkl
RUN pip install deltalake==0.13.0
RUN pip install ipywidgets==8.1.1

USER $NB_UID

# Disable the "Would you like to receive official Jupyter news?" popup
RUN jupyter labextension disable "@jupyterlab/apputils-extension:announcements"
