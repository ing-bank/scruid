#!/usr/bin/env bash

: ${TRY_LOOP:="20"}
: ${DRUID_HOST:="localhost"}
: ${DRUID_PORT:="8082"}
: ${REPOSITORY_OWNER:="${USER}/scruid"}

set -e

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [[ ${j} -ge ${TRY_LOOP} ]]; then
      echo >&2 "INFO $(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "INFO $(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

build_images(){
  (cd docker; REPOSITORY_OWNER=${REPOSITORY_OWNER} make image)
}


usage() {
  echo "Usage $(basename ${0}) <command>"
  echo "  start         : start container instances for testing."
  echo "  stop          : stop container instances."
  echo "  restart       : restart container instances."
  echo "  down          : delete container instances and their volumes."
  echo "  status        : list container instances."
  echo "  build         : build container images."
  echo "  help or usage : print usage instructions."
}

case "$1" in
  start)
    # pull images if they exist, otherwise try to build any missing locally
    REPOSITORY_OWNER=${REPOSITORY_OWNER} docker-compose pull || build_images
    REPOSITORY_OWNER=${REPOSITORY_OWNER} docker-compose up -d
    wait_for_port "druid" ${DRUID_HOST} ${DRUID_PORT}
  ;;
  stop)
    REPOSITORY_OWNER=${REPOSITORY_OWNER} docker-compose stop
  ;;
  restart)
    REPOSITORY_OWNER=${REPOSITORY_OWNER} docker-compose restart
    wait_for_port "druid" ${DRUID_HOST} ${DRUID_PORT} 
  ;;    
  down)
    REPOSITORY_OWNER=${REPOSITORY_OWNER} docker-compose down -v
  ;;
  status)
    REPOSITORY_OWNER=${REPOSITORY_OWNER} docker-compose ps
  ;;
  build)
    build_images
  ;;
  help|usage)
    usage
  ;;
  *)
    echo "ERROR $(date) - unknown command '$1'" 
    usage 
  ;;  
esac
