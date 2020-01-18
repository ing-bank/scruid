ARG ARG_OPENRESTY_VERSION=1.15.8.2-6

FROM openresty/openresty:${ARG_OPENRESTY_VERSION}-alpine

ADD nginx.conf.d/* /etc/nginx/conf.d/

EXPOSE 8080
EXPOSE 8081
