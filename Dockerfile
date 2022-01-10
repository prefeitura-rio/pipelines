# Build arguments
ARG PYTHON_VERSION=3.9-slim

# Get linkerd-await
FROM curlimages/curl:7.81.0 as linkerd
ARG LINKERD_AWAIT_VERSION=v0.2.4
RUN curl -sSLo /tmp/linkerd-await https://github.com/linkerd/linkerd-await/releases/download/release%2F${LINKERD_AWAIT_VERSION}/linkerd-await-${LINKERD_AWAIT_VERSION}-amd64 && \
    chmod 755 /tmp/linkerd-await

# Python version: 3.9
FROM python:${PYTHON_VERSION}

# Setting environment with prefect version
ARG PREFECT_VERSION=0.15.9
ENV PREFECT_VERSION $PREFECT_VERSION

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
COPY --from=linkerd /tmp/linkerd-await /linkerd-await
ENTRYPOINT ["/linkerd-await", "--shutdown", "--"]