FROM jupyter/minimal-notebook:notebook-7.0.6

# lakefs                 - high-level Python client used for repositories, branches and commits
# lakefs-enterprise-sdk  - adds the Datasets API; pinned to match treeverse/lakefs-enterprise
# boto3                  - used only to show the objects landing in Backblaze B2 directly
RUN pip install --no-cache-dir \
    lakefs==0.16.0 \
    lakefs-enterprise-sdk==1.100.0 \
    boto3==1.35.36 \
    pandas==2.2.3

# Disable the "Would you like to receive official Jupyter news?" popup
RUN jupyter labextension disable "@jupyterlab/apputils-extension:announcements"
