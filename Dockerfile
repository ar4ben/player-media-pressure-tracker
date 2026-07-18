ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        openjdk-17-jre-headless \
    && curl --fail --silent --show-error \
        https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor --output /usr/share/keyrings/google-cloud.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/google-cloud.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update \
    && apt-get install --yes --no-install-recommends google-cloud-cli \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY --chown=airflow:root pyproject.toml /tmp/media-pressure/pyproject.toml
COPY --chown=airflow:root src /tmp/media-pressure/src

RUN pip install --no-cache-dir \
    /tmp/media-pressure \
    pytest==9.0.3 && pip check
