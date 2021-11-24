# Build arguments
ARG PYTHON_VERSION=3.9-slim

# Python version: 3.9
FROM python:${PYTHON_VERSION}

# Setting environment with prefect version
ARG PREFECT_VERSION=0.15.7
ENV PREFECT_VERSION $PREFECT_VERSION

# Setup virtual environment and prefect
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN python3 -m pip install --no-cache-dir -U "pip>=21.2.4" "prefect==$PREFECT_VERSION"

# Install requirements
WORKDIR /app
COPY requirements-docker.txt .
RUN python3 -m pip install --no-cache-dir -U -r requirements-docker.txt