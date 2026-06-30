FROM jupyter/base-notebook:notebook-7.0.6

# Where to install the dvc-to-lakefs package from. Defaults to the published
# PyPI release. For local development against an unpublished checkout, build
# with:
#   --build-arg DVC_TO_LAKEFS_SOURCE="-e /home/jovyan/dvc-to-lakefs-pkg"
# and mount the checkout at that path (see docker-compose.override.yml.example).
ARG DVC_TO_LAKEFS_SOURCE="dvc-to-lakefs>=0.1.0"

USER root
# git is required: dvc-to-lakefs only imports git-backed DVC repositories.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
USER ${NB_UID}

# DVC with S3 support (talks to MinIO) and the lakeFS Python SDK (used for
# validation).
RUN pip install --no-cache-dir \
        "dvc[s3]>=3.0" \
        "lakefs>=0.16.0"

# Install the migration package. Skipped when the build arg is empty, which
# lets a local-dev override install it from a mounted checkout at runtime
# instead (see docker-compose.override.yml.example).
RUN if [ -n "${DVC_TO_LAKEFS_SOURCE}" ]; then \
        pip install --no-cache-dir "${DVC_TO_LAKEFS_SOURCE}"; \
    fi

# Disable the "Would you like to receive official Jupyter news?" popup.
RUN jupyter labextension disable "@jupyterlab/apputils-extension:announcements"
