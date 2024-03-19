FROM dockerhub.qingcloud.com/kafka_arm/base as base
ENV PATH /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-17-openjdk-arm64/bin:/opt/openssl/bin
ENV LD_LIBRARY_PATH /opt/openssl/lib/
ENV OPENSSL_CONF /opt/openssl/ssl/openssl.cnf
SHELL ["/bin/bash", "-c"]

# Add strimzi user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN useradd -r -m -u 1001 -g 0 strimzi

ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}
ENV STRIMZI_HOME=/opt/strimzi
RUN mkdir -p ${STRIMZI_HOME}/bin
WORKDIR ${STRIMZI_HOME}

COPY scripts/ tmp/bin/ bin/

COPY tmp/lib/ lib/

USER 1001

FROM scratch

COPY --from=base / /
ENV PATH /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-17-openjdk-arm64/bin:/opt/openssl/bin
ENV LD_LIBRARY_PATH /opt/openssl/lib/
ENV OPENSSL_CONF /opt/openssl/ssl/openssl.cnf
ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}
ENV STRIMZI_HOME=/opt/strimzi
WORKDIR ${STRIMZI_HOME}
USER 1001
