FROM dockerhub.qingcloud.com/kafka_arm/distroless:java17_nonroot_debian12-arm64 as base
ARG JAVA_VERSION=17
ARG TARGETPLATFORM
ENV PATH /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-17-openjdk-arm64/bin:/opt/openssl/bin
ENV LD_LIBRARY_PATH /opt/openssl/lib/
SHELL ["/bin/bash", "-c"]

USER root

#RUN microdnf update \
#    && microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install java-${JAVA_VERSION}-openjdk-headless openssl shadow-utils \
#    && microdnf clean all
#RUN apt update \
#    && apt install openjdk-${JAVA_VERSION}-jdk openssl tini -y \
#    && apt clean all

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64

#####
# Add Tini
#####
#ENV TINI_VERSION v0.19.0
#ENV TINI_SHA256_AMD64=93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c
#ENV TINI_SHA256_ARM64=07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81
#ENV TINI_SHA256_PPC64LE=3f658420974768e40810001a038c29d003728c5fe86da211cff5059e48cfdfde
#ENV TINI_SHA256_S390X=931b70a182af879ca249ae9de87ef68423121b38d235c78997fafc680ceab32d
#
#RUN echo $TARGETPLATFORM
#RUN set -ex; \
#    if [[ ${TARGETPLATFORM} = "linux/ppc64le" ]]; then \
#        curl -s -L
#RUN set -ex; \
#    if [[ ${TARGETPLATFORM} = "linux/ppc64le" ]]; then \
#        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-ppc64le -o /usr/bin/tini; \
#        echo "${TINI_SHA256_PPC64LE} */usr/bin/tini" | sha256sum -c; \
#        chmod +x /usr/bin/tini; \
#    elif [[ ${TARGETPLATFORM} = "linux/arm64" ]]; then \
#        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-arm64 -o /usr/bin/tini; \
#        echo "${TINI_SHA256_ARM64} */usr/bin/tini" | sha256sum -c; \
#        chmod +x /usr/bin/tini; \
#    elif [[ ${TARGETPLATFORM} = "linux/s390x" ]]; then \
#        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-s390x -o /usr/bin/tini; \
#        echo "${TINI_SHA256_S390X} */usr/bin/tini" | sha256sum -c; \
#        chmod +x /usr/bin/tini; \
#    else \
#        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /usr/bin/tini; \
#        echo "${TINI_SHA256_AMD64} */usr/bin/tini" | sha256sum -c; \
#        chmod +x /usr/bin/tini; \
#    fi
#RUN rm -f /usr/bin/id /usr/bin/gdbm_dump /usr/bin/gdbm_load /usr/bin/gdbmtool /usr/bin/lua /usr/bin/luac 
#RUN rm -rf /usr/bin/id /usr/share/gdb
#RUN rpm -e --nodeps gdbm-1.18-2.el8.aarch64 lua-5.3.4-12.el8.aarch64
#COPY openssl /usr/bin/openssl
#RUN chmod +x /usr/bin/openssl

COPY bin/ /usr/bin/
COPY bin/true /bin/true
RUN chmod +x /usr/bin/decrypt

COPY openssl /opt/openssl

RUN echo "keystore.pkcs12.macIterationCount = 15000" >> /etc/java-17-openjdk/security/java.security
RUN echo "keystore.pkcs12.keyPbeIterationCount = 15000" >> /etc/java-17-openjdk/security/java.security
RUN echo "keystore.pkcs12.certPbeIterationCount = 15000" >> /etc/java-17-openjdk/security/java.security



FROM scratch
COPY --from=base / /
ENV PATH /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-17-openjdk-arm64/bin:/opt/openssl/bin
ENV LD_LIBRARY_PATH /opt/openssl/lib/
ENV OPENSSL_CONF /opt/openssl/ssl/openssl.cnf
ENTRYPOINT ["/bin/bash", "-c"]
