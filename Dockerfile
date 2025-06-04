FROM apache/airflow:2.11.0

ENV AIRFLOW_HOME=/opt/airflow

# Install Spark provider package
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark

# Install rest Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Switch to root to install system packages
USER root

# Install useful system tools
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
    vim wget unzip curl bash gosu wget gcc git libffi-dev libssl-dev libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME env for Spark
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

# Google Cloud SDK installation
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=470.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk
ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME

# Copy and prep scripts
COPY scripts scripts
#RUN chmod +x scripts/*

# Set user back to Airflow default
USER $AIRFLOW_UID
