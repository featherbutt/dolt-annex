FROM python:3-slim AS build

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

COPY src/ /src/
RUN pip install -e "/src[leveldb]"

RUN mkdir /repo && mkdir /repo/db && mkdir /repo/filestore

VOLUME [ "/repo/" ]

COPY scripts/ /scripts/

# Copy example configuration files for testing
COPY example_config/ /example_config/

WORKDIR /repo

FROM build AS packaged

WORKDIR /

RUN pyinstaller /src/dolt_annex.spec

ENTRYPOINT ["tini", "--"]

CMD ["/dist/dolt_annex/dolt_annex"]

FROM build AS with_dolt

ARG DOLT_VERSION=2.3.3

# we install dolt with the install.sh script, which will determine the platform/arch of the container
# and install the proper dolt binary
RUN bash -c 'curl -L https://github.com/dolthub/dolt/releases/download/v${DOLT_VERSION}/install.sh | bash'
RUN /usr/local/bin/dolt version

ENTRYPOINT ["tini", "--", "/scripts/setup.sh"]