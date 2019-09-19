/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ing.wbaa.druid.client

import akka.NotUsed
import akka.actor.{ ActorSystem, Scheduler }
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.stream._
import akka.stream.scaladsl._
import com.typesafe.config.{ Config, ConfigException, ConfigFactory, ConfigValueFactory }
import ing.wbaa.druid.{ DruidConfig, DruidQuery, DruidResponse, DruidResult, QueryHost }
import akka.pattern.retry
import scala.reflect.runtime.universe

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor, Future, Promise }
import scala.util.{ Failure, Success, Try }

class DruidAdvancedHttpClient private (
    connectionFlow: DruidAdvancedHttpClient.ConnectionFlow,
    brokerConnections: Map[QueryHost, DruidAdvancedHttpClient.ConnectionFlow],
    responseParsingTimeout: FiniteDuration,
    url: String,
    bufferSize: Int,
    bufferOverflowStrategy: OverflowStrategy,
    queryRetries: Int,
    queryRetryDelay: FiniteDuration,
    requestInterceptor: RequestInterceptor
)(implicit val system: ActorSystem)
    extends DruidClient
    with DruidResponseHandler {

  private implicit val materializer: ActorMaterializer = ActorMaterializer()
  private implicit val ec: ExecutionContextExecutor    = system.dispatcher
  private implicit val scheduler: Scheduler            = system.scheduler

  private val queue: SourceQueueWithComplete[DruidAdvancedHttpClient.ConnectionIn] =
    Source
      .queue[DruidAdvancedHttpClient.ConnectionIn](bufferSize, bufferOverflowStrategy)
      .named("druid-client-queue")
      .via(connectionFlow)
      .async
      .toMat(Sink.foreach {
        case (Success(r), responsePromise) => responsePromise.success(r)
        case (Failure(e), responsePromise) => responsePromise.failure(e)
      })(Keep.left)
      .run()

  override def actorSystem: ActorSystem = system

  override def actorMaterializer: ActorMaterializer = materializer

  override def isHealthy()(implicit druidConfig: DruidConfig): Future[Boolean] =
    healthCheck.map(_.forall { case (_, isHealthyBroker) => isHealthyBroker })

  override def healthCheck(implicit druidConfig: DruidConfig): Future[Map[QueryHost, Boolean]] = {

    val request = HttpRequest(HttpMethods.GET, uri = druidConfig.healthEndpoint)

    val checksF = Future.sequence {
      brokerConnections.map {
        case (queryHost, flow) =>
          val responsePromise = Promise[HttpResponse]()

          Source
            .single(request -> responsePromise)
            .via(flow)
            .runWith(Sink.head)
            .flatMap {
              case (Success(response), _) =>
                Future {
                  response.discardEntityBytes()
                  queryHost -> (response.status == StatusCodes.OK)
                }
              case (Failure(ex), _) => Future.failed(ex)
            }
            .recover { case _ => queryHost -> false }
      }
    }

    checksF.map(_.toMap)
  }

  override def doQuery(
      query: DruidQuery
  )(implicit druidConfig: DruidConfig): Future[DruidResponse] =
    Marshal(query)
      .to[RequestEntity]
      .flatMap { entity =>
        val request =
          HttpRequest(HttpMethods.POST, url).withEntity(entity.withContentType(`application/json`))
        retry(
          () =>
            executeRequest(request).flatMap { response =>
              handleResponse(response, query.queryType, responseParsingTimeout)
          },
          queryRetries,
          queryRetryDelay
        )
      }

  override def doQueryAsStream(
      query: DruidQuery
  )(implicit druidConfig: DruidConfig): Source[DruidResult, NotUsed] = {
    val responsePromise = Promise[HttpResponse]()

    Source
      .fromFuture(
        createHttpRequest(query).map(request => request -> responsePromise)
      )
      .via(connectionFlow)
      .flatMapConcat {

        case (Success(response), _) =>
          responsePromise.future.flatMap(Future.successful)
          handleResponseAsStream(response)

        case (Failure(ex), _) =>
          responsePromise.future.flatMap(_ => Future.failed(ex))
          Source.failed(ex)
      }
  }

  override def shutdown(): Future[Unit] =
    Http(system)
      .shutdownAllConnectionPools()
      .andThen {
        case _ =>
          materializer.shutdown()
          system.terminate()
      }

  /**
    * Adds the specified HttpRequest to the queue in order to be executed to some Druid host
    *
    * @param request the Http request for Druid
    *
    * @return a future with the corresponding Http response from Druid
    */
  private def executeRequest(request: HttpRequest): Future[HttpResponse] = {
    logger.debug(
      s"Executing api ${request.method} request to ${request.uri} with entity: ${request.entity}"
    )

    val responsePromise = Promise[HttpResponse]()

    queue
      .offer(requestInterceptor.interceptRequest(request) -> responsePromise)
      .flatMap {
        case QueueOfferResult.Enqueued =>
          requestInterceptor.interceptResponse(request, responsePromise.future, this.executeRequest)
        case QueueOfferResult.Dropped =>
          Future.failed[HttpResponse](new RuntimeException("Queue overflowed. Try again later."))
        case QueueOfferResult.Failure(ex) =>
          Future.failed[HttpResponse](ex)
        case QueueOfferResult.QueueClosed =>
          Future.failed[HttpResponse](
            new RuntimeException(
              "Queue was closed (pool shut down) while running the request. Try again later."
            )
          )
      }
  }

  private def createHttpRequest(
      query: DruidQuery
  )(implicit druidConfig: DruidConfig): Future[HttpRequest] =
    Marshal(query)
      .to[RequestEntity]
      .map { entity =>
        HttpRequest(HttpMethods.POST, uri = druidConfig.url)
          .withEntity(entity.withContentType(`application/json`))
      }
}

object DruidAdvancedHttpClient extends DruidClientBuilder {

  object Parameters {
    final val DruidAdvancedHttpClient    = "druid-advanced-http-client"
    final val ConnectionPoolSettings     = "host-connection-pool"
    final val QueueSize                  = "queue-size"
    final val QueueOverflowStrategy      = "queue-overflow-strategy"
    final val QueryRetries               = "query-retries"
    final val QueryRetryDelay            = "query-retry-delay"
    final val AkkaHttpHostConnectionPool = "akka.http.host-connection-pool"
    final val RequestInterceptor         = "request-interceptor"
    final val RequestInterceptorConfig   = "request-interceptor-config"
  }

  class ConfigBuilder {

    private var queueSize: Option[Int]                                = None
    private var queueOverflowStrategy: Option[String]                 = None
    private var queryRetries: Option[Int]                             = None
    private var queryRetryDelay: Option[java.time.Duration]           = None
    private var hostConnectionPoolParams: Option[Map[String, String]] = None
    private var requestInterceptor: Option[RequestInterceptor]        = None

    def withQueueSize(v: Int): this.type = {
      queueSize = Option(v)
      this
    }

    def withQueueOverflowStrategy(v: String): this.type = {
      queueOverflowStrategy = Option(v)
      this
    }

    def withQueryRetries(v: Int): this.type = {
      queryRetries = Option(v)
      this
    }
    def withQueryRetryDelay(v: FiniteDuration): this.type = {
      queryRetryDelay = Option(java.time.Duration.ofNanos(v.toNanos))
      this
    }

    def withHostConnectionPoolParams(v: Map[String, String]): this.type = {
      hostConnectionPoolParams = Option(v)
      this
    }

    def withRequestInterceptor(v: RequestInterceptor): this.type = {
      requestInterceptor = Option(v)
      this
    }

    def build(): Config = {
      import scala.collection.JavaConverters._

      val initialConfig = hostConnectionPoolParams
        .foldLeft(ConfigFactory.empty().root())(
          (initialConf, settingsMap) =>
            initialConf.withValue(Parameters.ConnectionPoolSettings,
                                  ConfigValueFactory.fromMap(settingsMap.asJava))
        )

      val requestInterceptorClass  = requestInterceptor.map(_.getClass.getName)
      val requestInterceptorConfig = requestInterceptor.map(_.exportConfig.root())

      val params = Seq(
        Parameters.QueueSize                -> queueSize,
        Parameters.QueueOverflowStrategy    -> queueOverflowStrategy,
        Parameters.QueryRetries             -> queryRetries,
        Parameters.QueryRetryDelay          -> queryRetryDelay,
        Parameters.RequestInterceptor       -> requestInterceptorClass,
        Parameters.RequestInterceptorConfig -> requestInterceptorConfig
      )

      params
        .foldLeft(initialConfig) {
          case (conf, (name, valueOpt)) =>
            valueOpt
              .map(value => conf.withValue(name, ConfigValueFactory.fromAnyRef(value)))
              .getOrElse(conf)
        }
        .toConfig
        .atPath(Parameters.DruidAdvancedHttpClient)
        .withFallback(DruidConfig.DefaultConfig.clientConfig)
    }
  }
  object ConfigBuilder {
    def apply(): ConfigBuilder = new ConfigBuilder()
  }

  type ConnectionIn   = (HttpRequest, Promise[HttpResponse])
  type ConnectionOut  = (Try[HttpResponse], Promise[HttpResponse])
  type ConnectionFlow = Flow[ConnectionIn, ConnectionOut, NotUsed]
  override val supportsMultipleBrokers: Boolean = true

  override def apply(druidConfig: DruidConfig): DruidClient = {
    implicit val system: ActorSystem          = druidConfig.system
    implicit val materializer: Materializer   = ActorMaterializer()
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    val clientConfig = druidConfig.clientConfig
      .getConfig(Parameters.DruidAdvancedHttpClient)

    val poolConfig = loadPoolConfig(clientConfig)

    val maxRetries = clientConfig.getInt(Parameters.QueryRetries)

    val retryDelay =
      FiniteDuration(clientConfig.getDuration(Parameters.QueryRetryDelay).getNano, NANOSECONDS)

    val brokerFlows = createConnectionFlows(druidConfig.hosts,
                                            druidConfig.secure,
                                            druidConfig.responseParsingTimeout,
                                            poolConfig)

    val connectionFlow =
      if (brokerFlows.size > 1) balancer(brokerFlows) else brokerFlows.values.head

    val bufferSize = clientConfig.getInt(Parameters.QueueSize)

    val bufferOverflowStrategy: OverflowStrategy = parseOverflowStrategy(
      clientConfig.getString(Parameters.QueueOverflowStrategy)
    )

    val requestInterceptor = loadRequestInterceptor(clientConfig)

    new DruidAdvancedHttpClient(
      connectionFlow,
      brokerFlows,
      druidConfig.responseParsingTimeout,
      druidConfig.url,
      bufferSize,
      bufferOverflowStrategy,
      maxRetries,
      retryDelay,
      requestInterceptor
    )
  }

  /**
    * Loads host connection pool settings from 'druid.client-config.druid-advanced-http-client.host-connection-pool',
    * in order to override the corresponding Akka host connection pool settings.
    *
    * @param clientConfig client configuration
    * @return host connection pool configuration
    */
  private def loadPoolConfig(clientConfig: Config): Config = {
    // settings from akka host connection pool
    val akkaConf = ConfigFactory.load(Parameters.AkkaHttpHostConnectionPool)

    // Load host connection pool settings from 'druid-advanced-http-client.host-connection-pool'
    // and use akka host connection pool as fallback.
    Try(clientConfig.getConfig(Parameters.ConnectionPoolSettings))
      .map(conf => conf.atPath("akka.http.host-connection-pool").withFallback(akkaConf))
      .getOrElse(akkaConf)
  }

  private def loadRequestInterceptor(clientConfig: Config): RequestInterceptor = {

    val interceptorType: Class[_ <: RequestInterceptor] = Class
      .forName(clientConfig.getString(Parameters.RequestInterceptor))
      .asInstanceOf[Class[RequestInterceptor]]

    val runtimeMirror     = universe.runtimeMirror(getClass.getClassLoader)
    val module            = runtimeMirror.staticModule(interceptorType.getName)
    val obj               = runtimeMirror.reflectModule(module)
    val clientConstructor = obj.instance.asInstanceOf[RequestInterceptorBuilder]

    val configuration =
      Option(clientConfig.getConfig(Parameters.RequestInterceptorConfig))
        .getOrElse(ConfigFactory.empty())

    clientConstructor(configuration)
  }

  /**
    * Creates an instance of OverflowStrategy from the specified strategy name
    *
    * @param strategyName the name of the overflow strategy
    *
    * @return the corresponding OverflowStrategy
    *
    * @see [[akka.stream.OverflowStrategy]]
    * @throws com.typesafe.config.ConfigException.Generic when the given overflow strategy does not matches
    *                                                     with any known strategy name
    */
  private def parseOverflowStrategy(strategyName: String): OverflowStrategy =
    strategyName match {
      case "DropHead"     => OverflowStrategy.dropHead
      case "DropTail"     => OverflowStrategy.dropTail
      case "DropBuffer"   => OverflowStrategy.dropBuffer
      case "DropNew"      => OverflowStrategy.dropNew
      case "Fail"         => OverflowStrategy.fail
      case "Backpressure" => OverflowStrategy.backpressure
      case name =>
        throw new ConfigException.Generic(
          s"Unknown overflow strategy ($name) for client config parameter '${Parameters.QueueOverflowStrategy}'"
        )
    }

  /**
    * Creates a balancer over the specified Druid brokers
    *
    * @param brokers a map of Query Host info along with its corresponding connection flow
    *
    * @return a flow that balances the queries to multiple brokers
    */
  private def balancer(
      brokers: Map[QueryHost, Flow[ConnectionIn, ConnectionOut, Any]]
  ): ConnectionFlow =
    Flow.fromGraph(GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._

      val balancer = b.add(Balance[ConnectionIn](outputPorts = brokers.size))
      val merge    = b.add(Merge[ConnectionOut](inputPorts = brokers.size))

      brokers.values.foreach { brokerConnectionFlow ⇒
        balancer ~> brokerConnectionFlow ~> merge
      }
      FlowShape(balancer.in, merge.out)
    })

  /**
    * Creates cached connection flows for the specified Druid Brokers
    *
    * @param brokers the Druid Brokers to perform queries
    * @param secureConnection specify if the connection is secure or not (i.e., HTTPS or HTTP)
    * @param connectionPoolConfig the Akka settings for the connection pool
    * @param system the actor system to use
    *
    * @return a map of druid query hosts associated with their corresponding cached connection pool
    */
  private def createConnectionFlows(
      brokers: Seq[QueryHost],
      secureConnection: Boolean,
      responseParsingTimeout: FiniteDuration,
      connectionPoolConfig: Config
  )(implicit system: ActorSystem,
    materializer: Materializer,
    ec: ExecutionContext): Map[QueryHost, ConnectionFlow] = {

    require(brokers.nonEmpty)

    val settings: ConnectionPoolSettings = ConnectionPoolSettings(connectionPoolConfig)
    val parallelism                      = settings.pipeliningLimit * settings.maxConnections
    val log: LoggingAdapter              = system.log

    brokers.map { queryHost ⇒
      val flow = Flow[ConnectionIn]
        .log("scruid-load-balancer", _ => s"Sending query to ${queryHost.host}:${queryHost.port}")
        .via {
          if (secureConnection) {
            Http().cachedHostConnectionPoolHttps[Promise[HttpResponse]](
              host = queryHost.host,
              port = queryHost.port,
              settings = settings,
              log = log
            )
          } else {
            Http().cachedHostConnectionPool[Promise[HttpResponse]](
              host = queryHost.host,
              port = queryHost.port,
              settings = settings,
              log = log
            )
          }
        }
        .mapAsyncUnordered(parallelism) {
          // consider any response with HTTP Code different from StatusCodes.OK as a failure
          case (triedResponse, responsePromise) =>
            triedResponse match {
              case Success(response) if response.status != StatusCodes.OK =>
                response.entity
                  .toStrict(responseParsingTimeout)
                  .map {
                    // Future.success(entity) => Future.success(Try.success(entity))
                    Success(_)
                  }
                  .recover {
                    // Future.failure(throwable) => Future.success(Try.failure(throwable))
                    case t: Throwable => Failure(t)
                  }
                  .map { entity =>
                    // Future.success(Try(entity|throwable)) => Future.success(failure(HSE(try)), promise)
                    val failure = Failure(
                      new HttpStatusException(response.status,
                                              response.protocol,
                                              response.headers,
                                              entity)
                    )
                    (failure, responsePromise)
                  }
              case _ => Future.successful((triedResponse, responsePromise))
            }
        }

      queryHost -> flow.async
    }.toMap
  }
}
