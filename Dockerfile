FROM python:3-slim

ARG DOLT_VERSION=1.59.19

RUN apt update -y && \
    apt install -y \
        curl \
        tini \
        git \
        jq \
        build-essential \
        libleveldb-dev \
        ca-certificates && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

# we install dolt with the install.sh script, which will determine the platform/arch of the container
# and install the proper dolt binary
RUN bash -c 'curl -L https://github.com/dolthub/dolt/releases/download/v${DOLT_VERSION}/install.sh | bash'
RUN /usr/local/bin/dolt version

COPY src/ /src/
RUN pip install -e "/src[leveldb]"

ENV DA_DOLT_DIR="/repo/dolt" \
    DA_SPAWN_DOLT_SERVER=false \
    DA_DOLT_DB="dolt" \
    DA_EMAIL="anonymous@localhost" \
    DA_NAME="anonymous" \
    DA_ANNEX_COMMIT_MESSAGE="update dolt-annex"

RUN mkdir /repo && mkdir /repo/db && mkdir /repo/filestore

VOLUME [ "/repo/" ]

COPY scripts/ /scripts/

# Copy example configuration files for testing
COPY example_config/ /example_config/

WORKDIR /repo

ENTRYPOINT ["tini", "--", "/scripts/setup.sh"]
