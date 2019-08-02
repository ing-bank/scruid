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

import akka.http.scaladsl.model.{ HttpEntity, StatusCode }

/**
  * Indicates that Druid returned a non-OK HTTP status code.
  *
  * Note: the response entity is eagerly loaded (`HttpEntity.Strict`). I chose to do so because
  * having to drain or discard a stream on an exception object violates the Principle of Least
  * Surprise.
  *
  * This class extends IllegalStateException to maintain backwards compatibility.
  *
  * @param status the response status.
  * @param entity the response entity.
  */
class HttpStatusException(val status: StatusCode, val entity: Option[HttpEntity.Strict])
    extends IllegalStateException(s"Received response with HTTP status code $status") {}
