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

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

/*
 * Druid API Config Immutable
 */
class DruidConfig(val host: String,
                  val port: Int,
                  val secure: Boolean,
                  val url: String,
                  val datasource: String,
                  val responseParsingTimeout: FiniteDuration)

object DruidConfig {

  private val config      = ConfigFactory.load()
  private val druidConfig = config.getConfig("druid")

  implicit def asFiniteDuration(d: java.time.Duration): FiniteDuration =
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)

  implicit val DefaultConfig: DruidConfig = apply()

  def apply(host: String = druidConfig.getString("host"),
            port: Int = druidConfig.getInt("port"),
            secure: Boolean = druidConfig.getBoolean("secure"),
            url: String = druidConfig.getString("url"),
            datasource: String = druidConfig.getString("datasource"),
            responseParsingTimeout: FiniteDuration =
              druidConfig.getDuration("response-parsing-timeout")): DruidConfig =
    new DruidConfig(host, port, secure, url, datasource, responseParsingTimeout)
}
