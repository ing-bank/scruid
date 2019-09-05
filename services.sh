#!/usr/bin/env bash

: ${TRY_LOOP:="20"}
: ${DRUID_HOST:="localhost"}
: ${DRUID_PORT:="8082"}

set -e

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "INFO $(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "INFO $(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}


usage() {
  echo "Usage $(basename ${0}) <command>"
  echo "  start         : start container instances for testing."
  echo "  stop          : stop container instances."
  echo "  restart       : restart container instances."
  echo "  down          : delete container instances and their volumes."
  echo "  status        : list container instances."
  echo "  help or usage : print usage instructions."
}

case "$1" in
  start)
    docker-compose up -d --build
    wait_for_port "druid" ${DRUID_HOST} ${DRUID_PORT}
  ;;
  stop)
    docker-compose stop
  ;;
  restart)
    docker-compose restart 
    wait_for_port "druid" ${DRUID_HOST} ${DRUID_PORT} 
  ;;    
  down)
    docker-compose down -v
  ;;
  status)
    docker-compose ps
  ;;
  help|usage)
    usage
  ;;
  *)
    echo "ERROR $(date) - unknown command '$1'" 
    usage 
  ;;  
esac
