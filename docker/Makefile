# determine the absolute path where this Makefile exists
CURRENT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
#!make
# include the contents of .env as variables, in order to get DRUID_VERSION, OPENRESTY_VERSION, etc
include $(CURRENT_DIR)/../.env
export



# Registry of the docker images
REGISTRY=docker.pkg.github.com
REPOSITORY_OWNER ?=${USER}/scruid
# Base docker image with openjdk jre
OPENJDK_IMAGE=openjdk:8-jre-alpine

#
# Derived tags
# 
DRUID_VER_TAG=$(REGISTRY)/$(REPOSITORY_OWNER)/druid:$(DRUID_VERSION)
OPENRESTY_VER_TAG=$(REGISTRY)/$(REPOSITORY_OWNER)/openresty:$(OPENRESTY_VERSION)-alpine
CHAOS_PROXY_VER_TAG=$(REGISTRY)/$(REPOSITORY_OWNER)/clusterf-chaos-proxy:$(CHAOS_PROXY_VERSION)

.DEFAULT_GOAL := image


druid-image:
	@echo "Creating image with tag '$(DRUID_VER_TAG)'"
	docker build \
	    --build-arg ARG_OPENJDK_IMAGE=$(OPENJDK_IMAGE) \
	    --build-arg ARG_DRUID_VERSION=$(DRUID_VERSION) \
	    --build-arg ARG_ZOOKEEPER_VERSION=$(ZOOKEEPER_VERSION) \
	    -t $(DRUID_VER_TAG)  \
	    -f "$(CURRENT_DIR)/druid/Dockerfile" "$(CURRENT_DIR)/druid/"

druid-push:
	@echo "Pushing image with tag '$(DRUID_VER_TAG)'"
	docker push $(DRUID_VER_TAG)

druid-clean:
	@echo "Deleting image with tag '$(DRUID_VER_TAG)'"
	docker rmi $(DRUID_VER_TAG)

druid-pull:
	@echo "Pulling image with tag '$(DRUID_VER_TAG)'"
	docker pull $(DRUID_VER_TAG)

openresty-image:
	@echo "Creating image with tag '$(OPENRESTY_VER_TAG)'"
	docker build \
	    --build-arg ARG_OPENRESTY_VERSION=$(OPENRESTY_VERSION) \
	    -t $(OPENRESTY_VER_TAG) \
	    -f "$(CURRENT_DIR)/openresty/Dockerfile" "$(CURRENT_DIR)/openresty/"

openresty-push:
	@echo "Pushing image with tag '$(OPENRESTY_VER_TAG)'"
	docker push $(OPENRESTY_VER_TAG)

openresty-clean:
	@echo "Deleting image with tag '$(OPENRESTY_VER_TAG)'"
	docker rmi $(OPENRESTY_VER_TAG)

openresty-pull:
	@echo "Pulling image with tag '$(OPENRESTY_VER_TAG)'"
	docker pull $(OPENRESTY_VER_TAG)

chaos-proxy-image:
	@echo "Creating image with tag '$(CHAOS_PROXY_VER_TAG)'"
	docker build \
		--build-arg ARG_OPENJDK_IMAGE=$(OPENJDK_IMAGE) \
	    --build-arg ARG_CHAOS_PROXY_VERSION=$(CHAOS_PROXY_VERSION) \
	    -t $(CHAOS_PROXY_VER_TAG) \
	    -f "$(CURRENT_DIR)/chaos-proxy/Dockerfile" "$(CURRENT_DIR)/chaos-proxy/"

chaos-proxy-push:
	@echo "Pushing image with tag '$(CHAOS_PROXY_VER_TAG)'"
	docker push $(CHAOS_PROXY_VER_TAG)

chaos-proxy-clean:
	@echo "Deleting image with tag '$(CHAOS_PROXY_VER_TAG)'"
	docker rmi $(CHAOS_PROXY_VER_TAG)

chaos-proxy-pull:
	@echo "Pulling image with tag '$(CHAOS_PROXY_VER_TAG)'"
	docker pull $(CHAOS_PROXY_VER_TAG)

image: druid-image openresty-image chaos-proxy-image

push: druid-push openresty-push chaos-proxy-push

clean: druid-clean openresty-clean chaos-proxy-clean

pull: druid-pull openresty-pull chaos-proxy-pull