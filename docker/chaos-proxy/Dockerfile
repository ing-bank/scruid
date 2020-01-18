ARG ARG_OPENJDK_IMAGE=openjdk:8-jre-alpine
ARG ARG_BUILD_IMAGE=maven:3.6.3-jdk-8-slim

# image for building clusterf-chaos-proxy
FROM $ARG_BUILD_IMAGE

ARG ARG_CHAOS_PROXY_VERSION

RUN set -ex; \
    apt-get update; \
    apt-get -y install git openssh-client

RUN set -ex; \
    cd /tmp; \
    git clone https://github.com/clusterfk/chaos-proxy.git; \
    cd chaos-proxy; \
    git reset --hard ${ARG_CHAOS_PROXY_VERSION}; \
    mvn clean package

# clusterf-chaos-proxy image
FROM $ARG_OPENJDK_IMAGE

COPY --from=0 /tmp/chaos-proxy/target/clusterf-chaos-proxy.jar /clusterf-chaos-proxy.jar

ENV JAVA_OPTS=""

ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /clusterf-chaos-proxy.jar" ]