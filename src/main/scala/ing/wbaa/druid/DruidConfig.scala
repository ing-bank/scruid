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

package ing.wbaa.druid

import java.net.URI

import akka.actor.ActorSystem
import com.typesafe.config.{ Config, ConfigException, ConfigFactory }
import ing.wbaa.druid.client.{ DruidClient, DruidClientBuilder }

import scala.annotation.switch
import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions
import scala.reflect.runtime.universe

/*
 * Druid API Config Immutable
 */
class DruidConfig(val hosts: Seq[QueryHost],
                  val secure: Boolean,
                  val url: String,
                  val healthEndpoint: String,
                  val datasource: String,
                  val responseParsingTimeout: FiniteDuration,
                  val clientBackend: Class[_ <: DruidClient],
                  val clientConfig: Config,
                  val system: ActorSystem) {
  def copy(
      hosts: Seq[QueryHost] = this.hosts,
      secure: Boolean = this.secure,
      url: String = this.url,
      healthEndpoint: String = this.healthEndpoint,
      datasource: String = this.datasource,
      responseParsingTimeout: FiniteDuration = this.responseParsingTimeout,
      clientBackend: Class[_ <: DruidClient] = this.clientBackend,
      clientConfig: Config = this.clientConfig
  ): DruidConfig =
    new DruidConfig(hosts,
                    secure,
                    url,
                    healthEndpoint,
                    datasource,
                    responseParsingTimeout,
                    clientBackend,
                    clientConfig,
                    system)

  lazy val client: DruidClient = {
    val runtimeMirror     = universe.runtimeMirror(getClass.getClassLoader)
    val module            = runtimeMirror.staticModule(clientBackend.getName)
    val obj               = runtimeMirror.reflectModule(module)
    val clientConstructor = obj.instance.asInstanceOf[DruidClientBuilder]

    if (hosts.size > 1 && !clientConstructor.supportsMultipleBrokers)
      throw new IllegalStateException(
        s"The specified Druid client '${clientBackend.getName}' does not support multiple query nodes"
      )

    clientConstructor(this)
  }
}

case class QueryHost(host: String, port: Int)

object DruidConfig {
  private final val URISchemeSepPattern = "://".r

  private val config = ConfigFactory.load()

  private val druidConfig = config.getConfig("druid")

  implicit def asFiniteDuration(d: java.time.Duration): FiniteDuration =
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)

  implicit val DefaultConfig: DruidConfig = apply()

  def apply(
      hosts: Seq[QueryHost] = extractHostsFromConfig,
      secure: Boolean = druidConfig.getBoolean("secure"),
      url: String = druidConfig.getString("url"),
      healthEndpoint: String = druidConfig.getString("health-endpoint"),
      datasource: String = druidConfig.getString("datasource"),
      responseParsingTimeout: FiniteDuration = druidConfig.getDuration("response-parsing-timeout"),
      clientBackend: Class[_ <: DruidClient] =
        Class.forName(druidConfig.getString("client-backend")).asInstanceOf[Class[DruidClient]],
      clientConfig: Config = druidConfig.getConfig("client-config"),
      system: ActorSystem = ActorSystem("scruid-actor-system")
  ): DruidConfig =
    new DruidConfig(hosts,
                    secure,
                    url,
                    healthEndpoint,
                    datasource,
                    responseParsingTimeout,
                    clientBackend,
                    clientConfig,
                    system)

  /**
    * Extract query node hosts with their ports from the specified configuration
    *
    * @throws ConfigException.Generic when the 'hosts' parameter in configuration is empty, null or invalid
    *
    * @return a sequence of query node hosts
    */
  private def extractHostsFromConfig: Seq[QueryHost] = {
    val hosts = druidConfig.getString("hosts").trim

    if (hosts.isEmpty)
      throw new ConfigException.Generic("Empty configuration parameter 'hosts'")

    val hostWithPortsValues = hosts.split(',').map(_.trim).toIndexedSeq

    if (hostWithPortsValues.exists(_.isEmpty))
      throw new ConfigException.Generic("Empty host:port value in configuration parameter 'hosts'")

    hostWithPortsValues.map { hostPortStr =>
      val countSchemeSeparators = URISchemeSepPattern.findAllIn(hostPortStr).size

      // `hostPortStr` should contain at most one definition of a URI scheme
      val uri = (countSchemeSeparators: @switch) match {
        case 0 =>
          new URI("druid://" + hostPortStr) // adding 'druid://' scheme to avoid URISyntaxException
        case 1 => new URI(hostPortStr)
        case _ =>
          throw new ConfigException.Generic(
            s"Invalid host port definition in configuration parameter 'hosts', failed to parse '$hostPortStr'"
          )
      }

      // Get the host address (ip v4 or v6) or hostname
      val host = Option(uri.getHost).getOrElse {
        throw new ConfigException.Generic(
          s"Invalid host port definition in configuration parameter 'hosts', failed to read host address or name from '$hostPortStr'"
        )
      }

      // Get the port number
      val port = Option(uri.getPort).find(_ > 0).getOrElse {
        throw new ConfigException.Generic(
          s"Invalid host port definition in configuration parameter 'hosts', failed to read port number from '$hostPortStr'"
        )
      }

      QueryHost(host, port)
    }
  }

}
