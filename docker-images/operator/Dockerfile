FROM dockerhub.qingcloud.com/kafka_arm/base as base

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

RUN echo "keystore.pkcs12.macIterationCount = 15000" >> /etc/java/java-11-openjdk/java-11-openjdk-11.0.19.0.7-4.el8.aarch64/conf/security/java.security
RUN echo "keystore.pkcs12.keyPbeIterationCount = 15000" >> /etc/java/java-11-openjdk/java-11-openjdk-11.0.19.0.7-4.el8.aarch64/conf/security/java.security
RUN echo "keystore.pkcs12.certPbeIterationCount = 15000" >> /etc/java/java-11-openjdk/java-11-openjdk-11.0.19.0.7-4.el8.aarch64/conf/security/java.security

USER 1001

FROM scratch

COPY --from=base / /
ARG strimzi_version=1.0-SNAPSHOT
ENV STRIMZI_VERSION ${strimzi_version}
ENV STRIMZI_HOME=/opt/strimzi
WORKDIR ${STRIMZI_HOME}
USER 1001
