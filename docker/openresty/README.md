# OpenResty container

Generates an HTTP 502 response, and sends the response body really slowly, so that we can test with
a response read timeout.

Can be expanded with additional NGINX configuration files if future test cases require other
OpenResty functions.

## References:

- Docker Hub: [openresty/openresty](https://hub.docker.com/r/openresty/openresty)
- OpenResty [NGINX echo module](https://github.com/openresty/echo-nginx-module) 
