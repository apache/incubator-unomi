/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package unomi

import java.util.UUID

import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.http.Predef._

/**
 * Created by toto on 07/04/15.
 */
class PrepareIndices extends Simulation {

  val httpProtocol = http
    .baseURL("http://localhost:8181")
    .inferHtmlResources(WhiteList("""http://localhost:8181/.*"""), BlackList())
    .acceptHeader("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
    .acceptEncodingHeader("gzip, deflate")
    .acceptLanguageHeader("en-US,en;q=0.5")
    .connection("keep-alive")
    .contentTypeHeader("text/plain;charset=UTF-8")
    .userAgentHeader("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:24.0) Gecko/20100101 Firefox/24.0")

  val headers = Map(
    "Origin" -> "http://localhost:8080",
    "Pragma" -> "no-cache"
  )

  val urllistFeed = csv("urllist.txt").random

  val format = new java.text.SimpleDateFormat("yyyy-MM")
  val minTime: Long = format.parse("2014-01").getTime()
  val maxTime: Long = format.parse("2015-12").getTime()

  val seqSessionsFeed = (minTime to maxTime by 86400000).map { seq =>
    Map(
      "sessionId" -> UUID.randomUUID().toString,
      "timestamp" -> seq,
      "previousURL" -> ""
    )
  }

  val loadContext = feed(seqSessionsFeed).feed(urllistFeed).exec(http("LoadContext").post("/cxs/context.js?sessionId=${sessionId}&timestamp=${timestamp}")
    .headers(headers)
    .body(ELFileBody("ContextLoad_request_0.json")))

  val prepare = scenario("PrepareIndices").repeat((minTime to maxTime by 86400000).size) { loadContext }

  setUp(prepare.inject(atOnceUsers(1))).protocols(httpProtocol)

}
