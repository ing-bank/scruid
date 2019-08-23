#
# Parameters that can also be overridden from CLI
#
#   For example, to override the version of Druid to `0.15.1-incubating`:
#
#   make DRUID_VERSION=0.15.1-incubating image
#

ZOOKEEPER_VERSION ?= 3.4.14
DRUID_VERSION ?= 0.16.0-incubating
OPENRESTY_VERSION ?= 1.15.8.2-1
# ClusterF chaos proxy version is defined by the corresponding git commit hash id
CHAOS_PROXY_VERSION ?= f935b60

#
# Constants
#

# Registry of the docker images --- TODO: should change
REGISTRY=docker.pkg.github.com/anskarl
# Base docker image with openjdk jre
OPENJDK_IMAGE=openjdk:8-jre-alpine

#
# Derived tags
# 
DRUID_VER_TAG=$(REGISTRY)/scruid/druid:$(DRUID_VERSION)
DRUID_LATEST_TAG=$(REGISTRY)/scruid/druid:latest
OPENRESTY_VER_TAG=$(REGISTRY)/scruid/openresty:$(OPENRESTY_VERSION)-alpine
OPENRESTY_LATEST_TAG=$(REGISTRY)/scruid/openresty:latest
CHAOS_PROXY_VER_TAG=$(REGISTRY)/scruid/clusterf-chaos-proxy:$(CHAOS_PROXY_VERSION)
CHAOS_PROXY_LATEST_TAG=$(REGISTRY)/scruid/clusterf-chaos-proxy:latest


.DEFAULT_GOAL := image

CURRENT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

druid-image:
	@echo "Creating image with tags '$(DRUID_VER_TAG)' and '$(DRUID_LATEST_TAG)'"
	docker build \
	    --build-arg ARG_OPENJDK_IMAGE=$(OPENJDK_IMAGE) \
	    --build-arg ARG_DRUID_VERSION=$(DRUID_VERSION) \
	    --build-arg ARG_ZOOKEEPER_VERSION=$(ZOOKEEPER_VERSION) \
	    -t $(DRUID_VER_TAG)  \
	    -f "$(CURRENT_DIR)/druid/Dockerfile" "$(CURRENT_DIR)/druid/"
	docker tag $(DRUID_VER_TAG) $(DRUID_LATEST_TAG)

druid-push:
	@echo "Pushing image with tags '$(DRUID_VER_TAG)' and '$(DRUID_LATEST_TAG)'"
	docker push $(DRUID_VER_TAG) && docker push $(DRUID_LATEST_TAG)

druid-clean:
	@echo "Deleting image with tags '$(DRUID_VER_TAG)' and '$(DRUID_LATEST_TAG)'"
	docker rmi $(DRUID_VER_TAG)
	docker rmi $(DRUID_LATEST_TAG)

openresty-image:
	@echo "Creating image with tags '$(OPENRESTY_VER_TAG)' and '$(OPENRESTY_LATEST_TAG)'"
	docker build \
	    --build-arg ARG_OPENRESTY_VERSION=$(OPENRESTY_VERSION) \
	    -t $(OPENRESTY_VER_TAG) \
	    -f "$(CURRENT_DIR)/openresty/Dockerfile" "$(CURRENT_DIR)/openresty/"
	docker tag $(OPENRESTY_VER_TAG) $(OPENRESTY_LATEST_TAG)

openresty-push:
	@echo "Pushing image with tags '$(OPENRESTY_VER_TAG)' and '$(OPENRESTY_LATEST_TAG)'"
	docker push $(OPENRESTY_VER_TAG) && docker push $(OPENRESTY_LATEST_TAG)

openresty-clean:
	@echo "Deleting image with tags '$(OPENRESTY_VER_TAG)' and '$(OPENRESTY_LATEST_TAG)'"
	docker rmi $(OPENRESTY_VER_TAG)
	docker rmi $(OPENRESTY_LATEST_TAG)


chaos-proxy-image:
	@echo "Creating image with tags '$(CHAOS_PROXY_VER_TAG)' and '$(CHAOS_PROXY_LATEST_TAG)'"
	docker build \
		--build-arg ARG_OPENJDK_IMAGE=$(OPENJDK_IMAGE) \
	    --build-arg ARG_CHAOS_PROXY_VERSION=$(CHAOS_PROXY_VERSION) \
	    -t $(CHAOS_PROXY_VER_TAG) \
	    -f "$(CURRENT_DIR)/chaos-proxy/Dockerfile" "$(CURRENT_DIR)/chaos-proxy/"

	docker tag $(CHAOS_PROXY_VER_TAG) $(CHAOS_PROXY_LATEST_TAG)

chaos-proxy-push:
	@echo "Pushing image with tags '$(CHAOS_PROXY_VER_TAG)' and '$(CHAOS_PROXY_LATEST_TAG)'"
	docker push $(CHAOS_PROXY_VER_TAG) && docker push $(CHAOS_PROXY_LATEST_TAG)

chaos-proxy-clean:
	@echo "Deleting image with tags '$(CHAOS_PROXY_VER_TAG)' and '$(CHAOS_PROXY_LATEST_TAG)'"
	docker rmi $(CHAOS_PROXY_VER_TAG)
	docker rmi $(CHAOS_PROXY_LATEST_TAG)	


image: druid-image openresty-image chaos-proxy-image

push: druid-push openresty-push chaos-proxy-push

clean: druid-clean openresty-clean chaos-proxy-clean