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

import ing.wbaa.druid.client.DruidAdvancedHttpClient
import org.scalatest.{ Matchers, WordSpec }

import scala.concurrent.duration._

class ConfigOverridesSpec extends WordSpec with Matchers {

  def asFiniteDuration(d: java.time.Duration): FiniteDuration =
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)

  "DruidConfig" should {

    "be overridden for hosts" in {
      val overriddenHosts = Seq(
        QueryHost("localhost", 8082),
        QueryHost("localhost", 8083),
        QueryHost("localhost", 8084)
      )

      val overriddenConfig = DruidConfig(hosts = overriddenHosts)

      overriddenConfig.hosts shouldEqual overriddenHosts
    }

    "be overridden for client implementation" in {
      val classOfDruidClient = classOf[DruidAdvancedHttpClient]

      val overriddenConfig = DruidConfig(clientBackend = classOfDruidClient)

      overriddenConfig.clientBackend shouldEqual classOfDruidClient
    }

    "be overridden for DruidAdvancedHttpClient configuration" in {
      import DruidAdvancedHttpClient.Parameters

      val classOfDruidClient = classOf[DruidAdvancedHttpClient]

      val HostConnectionPoolParamsOverrides =
        Map("max-connections" -> "8", "max-connection-lifetime" -> "5 minutes")

      val advancedDruidClientConf = DruidAdvancedHttpClient
        .ConfigBuilder()
        .withQueryRetries(10)
        .withQueueSize(8096)
        .withQueueOverflowStrategy("DropNew")
        .withQueryRetryDelay(1.second)
        .withHostConnectionPoolParams(HostConnectionPoolParamsOverrides)
        .build()

      val overriddenConfig = DruidConfig(
        clientBackend = classOfDruidClient,
        clientConfig = advancedDruidClientConf
      )

      val overriddenClientConfig =
        overriddenConfig.clientConfig.getConfig(Parameters.DruidAdvancedHttpClient)

      overriddenConfig.clientBackend shouldEqual classOfDruidClient

      overriddenClientConfig.getInt(Parameters.QueryRetries) shouldEqual 10
      overriddenClientConfig.getInt(Parameters.QueueSize) shouldEqual 8096
      overriddenClientConfig.getString(Parameters.QueueOverflowStrategy) shouldEqual "DropNew"
      asFiniteDuration(overriddenClientConfig.getDuration(Parameters.QueryRetryDelay)) shouldEqual 1.second

      val overriddenHostConnectionPool = overriddenClientConfig
        .getConfig(Parameters.ConnectionPoolSettings)

      overriddenHostConnectionPool.getInt("max-connections") shouldEqual 8
      asFiniteDuration(overriddenHostConnectionPool.getDuration("max-connection-lifetime")) shouldEqual 5.minutes

    }

  }
}
