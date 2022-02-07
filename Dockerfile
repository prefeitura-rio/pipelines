# Build arguments
ARG PYTHON_VERSION=3.9-slim

# Get linkerd-await and Oracle Instant Client
FROM curlimages/curl:7.81.0 as curl-step
ARG LINKERD_AWAIT_VERSION=v0.2.4
ARG ORACLE_INSTANT_CLIENT_URL=https://download.oracle.com/otn_software/linux/instantclient/215000/instantclient-basic-linux.x64-21.5.0.0.0dbru.zip
RUN curl -sSLo /tmp/linkerd-await https://github.com/linkerd/linkerd-await/releases/download/release%2F${LINKERD_AWAIT_VERSION}/linkerd-await-${LINKERD_AWAIT_VERSION}-amd64 && \
    chmod 755 /tmp/linkerd-await && \
    curl -sSLo /tmp/instantclient.zip $ORACLE_INSTANT_CLIENT_URL

# Unzip Oracle Instant Client
FROM ubuntu:18.04 as unzip-step
COPY --from=curl-step /tmp/instantclient.zip /tmp/instantclient.zip
RUN apt-get update && \
    apt-get install --no-install-recommends -y unzip && \
    rm -rf /var/lib/apt/lists/* && \
    unzip /tmp/instantclient.zip -d /tmp

# Python version: 3.9
FROM python:${PYTHON_VERSION}

# Setting environment with prefect version
ARG PREFECT_VERSION=0.15.9
ENV PREFECT_VERSION $PREFECT_VERSION

# Setting environment with Oracle Instant Client URL

# Setup Oracle Instant Client
WORKDIR /opt/oracle
COPY --from=unzip-step /tmp/instantclient_21_5 /opt/oracle/instantclient_21_5
RUN apt-get update && \
    apt-get install --no-install-recommends -y libaio1 && \
    rm -rf /var/lib/apt/lists/* && \
    sh -c "echo /opt/oracle/instantclient_21_5 > /etc/ld.so.conf.d/oracle-instantclient.conf" && \
    ldconfig

# Setup virtual environment and prefect
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN python3 -m pip install --no-cache-dir -U "pip>=21.2.4" "prefect==$PREFECT_VERSION"

# Install requirements
WORKDIR /app
COPY . .
RUN python3 -m pip install --prefer-binary --no-cache-dir -U . && \
    mkdir -p /opt/prefect/app/bases && \
    mkdir -p /root/.basedosdados/templates && \
    mkdir -p /root/.basedosdados/credentials/

# Copy linkerd-await and setup entrypoint
COPY --from=curl-step /tmp/linkerd-await /linkerd-await
ENTRYPOINT ["/linkerd-await", "--shutdown", "--"]