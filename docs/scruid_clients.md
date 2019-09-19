# Scruid Clients

Scruid provides the following two implementations of HTTP Clients:

 - (Default) **DruidHttpClient**: uses the [connection-level client-side API](https://doc.akka.io/docs/akka-http/10.1.5/client-side/connection-level.html). 
 - **DruidAdvancedHttpClient** with the following characteristics:
   
   1. Uses Akka HTTP to manage [cached connection-pool](https://doc.akka.io/docs/akka-http/10.1.5/client-side/host-level.html#host-level-client-side-api) 
   to Druid host, therefore it can control the number of active connections to Druid, as well as to reuse the connections.
   
   2. Uses a queue to issue requests to the connection-pool and receive responses when they are available 
   (based on [Akka HTTP - Using the host-level API with a queue](https://doc.akka.io/docs/akka-http/10.1.5/client-side/host-level.html#using-the-host-level-api-with-a-queue)). 
   By having a queue in front of cached connection-pool, when the queue overflows it can be selected which strategy
   should be followed (Backpressure, Fail, DropHead, etc., for details see [OverflowStrategy in Akka documentation](https://doc.akka.io/api/akka/2.5.23/akka/stream/OverflowStrategy$.html))
   
   3. Can connect to more than one Druid Broker nodes and load balance the requests. In that case, the client 
   creates a separate cached connection-pool per Broker and adds a [Balancer](https://doc.akka.io/api/akka/2.5.23/akka/stream/scaladsl/Balance.html) 
   in front of them.

   4. Supports authentication by means of the `RequestInterceptor` system. At present, only HTTP Basic authentication is implemented.

## Configuration of DruidAdvancedHttpClient

To enable `DruidAdvancedHttpClient` you should specify as a `client-backend` following class name in the configuration:

```
 client-backend = "ing.wbaa.druid.client.DruidAdvancedHttpClient"
```

An example of `DruidAdvancedHttpClient` configuration can be found below:

```
client-backend = "ing.wbaa.druid.client.DruidAdvancedHttpClient"

client-config = {

 druid-advanced-http-client ={

     queue-size = 32768
     queue-overflow-strategy = "Backpressure"
     query-retries = 5
     query-retry-delay = 10 ms

     host-connection-pool = {
         max-connections = 32
         min-connections = 0
         max-open-requests = 128
         max-connection-lifetime = 15 min
     }

     request-interceptor = "ing.wbaa.druid.auth.basic.BasicAuthenticationExtension"

     request-interceptor-config = {
         username = "scruid-user"
         password = "${SCRUID_PASSWORD}"
     }
 }

}
```

A description of the configuration parameters is given below:

  - `queue-size`: size of the buffer that holds queries that are going to be sent to Druid
  - `queue-overflow-strategy`: the strategy to follow when incoming queries cannot fit inside the buffer
  - `query-retries`: the number of retry attempts of a query when its has been failed for some reason, 
  e.g., due to a temporary internal server error in a Druid query node
  - `query-retry-delay`: the duration to wait before retrying a failed query
  - `host-connection-pool`: contains any Akka Http [Host Connection Pool settings](https://doc.akka.io/docs/akka-http/current/client-side/host-level.html#configuring-a-host-connection-pool) 
  to override for the host connection pools
  - `request-interceptor`: contains the fully-qualified name of an object that implements the `RequestInterceptorBuilder` trait.
  - `request-interceptor-config`: this configuration object is passed as-is to the specified `request-interceptor` builder.

### Programmatically override DruidAdvancedHttpClient configuration

Similar to Scruid main configuration `ing.wbaa.druid.DruidConfig`, the configuration of `DruidAdvancedHttpClient`
can be programmatically overridden. This can be performed using the builder in `ing.wbaa.druid.client.DruidAdvancedHttpClient.ConfigBuilder`.

Consider for example that we would like to setup `DruidAdvancedHttpClient` as a client backend and override some of
its settings:

```scala
val advancedDruidClientConf = DruidAdvancedHttpClient.ConfigBuilder()
    .withQueryRetries(10)
    .withQueueSize(8096)
    .withQueueOverflowStrategy("DropNew")
    .withQueryRetryDelay(1.second)
    .withHostConnectionPoolParams(
      Map("max-connections" -> "8", "max-connection-lifetime" -> "5 minutes")
    )
    .build()

implicit val druidConfig = DruidConfig(
    clientBackend = classOf[DruidAdvancedHttpClient],
    clientConfig = advancedDruidClientConf
)

```

The above example configuration overrides the following parameters:
  
  - `query-retries` is set to `10`
  - `queue-size` is set to `8096`
  - `queue-overflow-strategy` is set to `DropNew`
  - `query-retry-delay` is set to one second
  - `max-connections` and `max-connection-lifetime` of the connection pool settings 
  are set to `8` and `5 minutes`, respectively. 
  
Please note that any parameter that is not explicitly overridden will remain the same as it is specified in
the configuration parameters in `reference.conf` or `application.conf`.

## Custom client implementation

Scruid provides an API to implement a custom client by extending the trait `ing.wbaa.druid.client.DruidClient`, 
which defines the functionality that should be implemented in order to be directly used by the rest of the 
library. In order to be also used by the configuration of Scruid (i.e., `ing.wbaa.druid.DruidConfig`),  
the client should be constructed by its companion object which should implement the trait 
`ing.wbaa.druid.client.DruidClientBuilder`. 

To load the custom client, the configuration parameter `client-backend` should be set to point to the
fully qualified name of the corresponding implementation `DruidClientBuilder`. 

To pass custom configuration options it is advised to be provided inside the `client-config` configuration path 
(as it has been presented in the `DruidAdvancedHttpClient` configuration example).
