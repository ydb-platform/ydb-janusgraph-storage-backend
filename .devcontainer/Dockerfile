FROM maven:3.9-eclipse-temurin-11

# JanusGraph master builds with Java 8/11 (compiler target 1.8, CI on 8 and 11),
# so JDK 11 is the safe development JDK for a storage backend module.

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        curl \
        ca-certificates \
        less \
        vim-tiny \
    && rm -rf /var/lib/apt/lists/*

# YDB CLI for inspecting the local cluster (scheme ls, ad-hoc queries).
RUN curl -fsSL https://install.ydb.tech/cli | bash \
    && ln -s /root/ydb/bin/ydb /usr/local/bin/ydb

WORKDIR /workspace
