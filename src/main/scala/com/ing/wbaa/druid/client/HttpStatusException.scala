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
package com.ing.wbaa.druid.client

import scala.collection.immutable.Seq
import scala.util.Try

import akka.http.scaladsl.model.{ HttpEntity, HttpHeader, HttpProtocol, HttpResponse, StatusCode }

/**
  * Indicates that Druid returned a non-OK HTTP status code.
  *
  * Note: the response entity is eagerly loaded (`HttpEntity.Strict`). I chose to do so because
  * having to drain or discard a stream on an exception object violates the Principle of Least
  * Surprise. This class does not simply embed the `HttpResponse` because I found no way to
  * expose the (eager loaded) fields without requiring an execution context.
  *
  * This class extends IllegalStateException to maintain backwards compatibility.
  *
  * @param status the response status.
  * @param protocol the response protocol.
  * @param headers the response headers.
  * @param entity the response entity.
  */
class HttpStatusException(val status: StatusCode,
                          val protocol: HttpProtocol,
                          val headers: Seq[HttpHeader],
                          val entity: Try[HttpEntity.Strict])
    extends IllegalStateException(s"Received response with HTTP status code $status") {

  /**
    * Returns an `HttpResponse` object equivalent to the original response received from Druid.
    *
    * Caution: this method throws an exception if `this.entity` is a failure.
    * @return an HTTP response object.
    * @throws Throwable if the response entity `toStrict` failed.
    */
  def response: HttpResponse = HttpResponse(status, headers, entity.get, protocol)
}
