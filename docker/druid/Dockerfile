ARG ARG_OPENJDK_IMAGE=openjdk:8-jre-alpine

FROM $ARG_OPENJDK_IMAGE

ARG ARG_DRUID_VERSION
ARG ARG_ZOOKEEPER_VERSION

ENV DRUID_VERSION=$ARG_DRUID_VERSION
ENV ZOOKEEPER_VERSION=$ARG_ZOOKEEPER_VERSION
ENV LANG C.UTF-8

RUN set -ex; apk add --no-cache bash perl python curl

# druid
RUN set -ex; \
    curl -fsLS "https://www.apache.org/dyn/closer.cgi?filename=/incubator/druid/$DRUID_VERSION/apache-druid-$DRUID_VERSION-bin.tar.gz&action=download" | gunzip | tar x -C /opt; \
    mv /opt/apache-druid-${DRUID_VERSION} /opt/druid

# zookeeper
RUN set -ex; \
    curl -fsLS "https://www.apache.org/dyn/closer.cgi?filename=/zookeeper/zookeeper-$ZOOKEEPER_VERSION/apache-zookeeper-$ZOOKEEPER_VERSION.tar.gz&action=download" | gunzip | tar x -C /opt; \
    mv /opt/apache-zookeeper-${ZOOKEEPER_VERSION} /opt/druid/zk

WORKDIR /opt/druid/

ADD config/common.runtime.properties conf/druid/single-server/micro-quickstart/_common/common.runtime.properties

RUN set -ex; \
    bash -c "./bin/start-micro-quickstart &"; \
    ./bin/post-index-task --file quickstart/tutorial/wikipedia-index.json --url http://localhost:8081 --submit-timeout 600

# Expose ports:
# - 8888: HTTP (router)
# - 8081: HTTP (coordinator)
# - 8082: HTTP (broker)
# - 8083: HTTP (historical)
# - 8090: HTTP (overlord)
# - 2181 2888 3888: ZooKeeper
EXPOSE 8888
EXPOSE 8081
EXPOSE 8082
EXPOSE 8083
EXPOSE 8090
EXPOSE 2181 2888 3888

ENTRYPOINT ./bin/start-micro-quickstart