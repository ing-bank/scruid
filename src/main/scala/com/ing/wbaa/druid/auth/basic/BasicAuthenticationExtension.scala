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
package com.ing.wbaa.druid.auth.basic

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.{ Authorization, BasicHttpCredentials }
import com.ing.wbaa.druid.client.{
  NoopRequestInterceptor,
  RequestInterceptor,
  RequestInterceptorBuilder
}
import com.typesafe.config.{ Config, ConfigFactory, ConfigValueFactory }
import com.typesafe.scalalogging.LazyLogging

/**
  * Adds a basic authentication header with static credentials to every outgoing request. Does not
  * modify the response.
  *
  * @param username the username
  * @param password the password
  */
class BasicAuthenticationExtension(username: String, password: String)
    extends NoopRequestInterceptor {
  override def interceptRequest(request: HttpRequest): HttpRequest =
    request.withHeaders(Authorization(BasicHttpCredentials(username, password)))

  override def exportConfig: Config =
    ConfigFactory
      .empty()
      .withValue("username", ConfigValueFactory.fromAnyRef(username))
      .withValue("password", ConfigValueFactory.fromAnyRef(password))
}

object BasicAuthenticationExtension extends RequestInterceptorBuilder with LazyLogging {

  override def apply(config: Config): RequestInterceptor = {

    val username =
      Option(config.getString("username")).getOrElse {
        throw new IllegalStateException(
          "BasicAuthenticationExtension requires 'username' configuration parameter to be specified"
        )
      }

    val password =
      Option(config.getString("password")).getOrElse {
        throw new IllegalStateException(
          "BasicAuthenticationExtension requires 'password' configuration parameter to be specified"
        )
      }

    logger.info(s"BasicAuthenticationExtension[username=$username] created")
    new BasicAuthenticationExtension(username, password)
  }
}
