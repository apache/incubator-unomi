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
import java.util.concurrent.TimeUnit

import io.gatling.core.Predef._
import io.gatling.http.Predef._

/**
 * User scenario
 */
object UserScenario {
  val r = scala.util.Random

  val headers = Map(
    "Origin" -> "http://localhost:8080",
    "Pragma" -> "no-cache",
    "User-Agent" -> "${userAgent}"
  )

  // Feeds

  val usersFeed = Iterator.continually {
    Map(
      "numberOfSessions" -> Parameters.numberOfSessionsPerUser
    )
  }

  val sessionsFeed = Iterator.continually {
    Map(
      "sessionId" -> UUID.randomUUID().toString,
      "sessionSize" -> Math.max(2, Math.round(Parameters.sessionSizeAverage + Parameters.sessionSizeStdDev * r.nextGaussian()).toInt),
      "timestamp" -> (Parameters.minTime + r.nextInt(((Parameters.maxTime - Parameters.minTime) / 1000).toInt).toLong * 1000),
      "previousURL" -> "",
      "gender" -> (if (r.nextBoolean()) "male" else "female"),
      "age" -> (15 + r.nextInt(60)),
      "income" -> (10000 * r.nextInt(2000)),
      "faceBookId" -> (if (r.nextInt(10) > 7) "facebook" + Integer.toString(r.nextInt(10000)) else ""),
      "twitterId" -> (if (r.nextInt(10) > 7) "twitter" + Integer.toString(r.nextInt(10000)) else ""),
      "email" -> (if (r.nextInt(10) > 7) "user" + Integer.toString(r.nextInt(10000000)) + "@test.com" else ""),
      "phoneNumber" -> (if (r.nextInt(10) > 7) "001-202-555-" + Integer.toString(1000 + r.nextInt(10000)) else ""),
      "leadAssignedTo" -> (if (r.nextInt(10) > 7) "account_manager" + Integer.toString(r.nextInt(10)) + "@test.com" else "")
    )
  }

  val requestsFeed = Iterator.continually {
    Map(
      "pauseTime" -> Math.round((Parameters.delayAverage + Parameters.delayStdDev * r.nextGaussian()) * 1000).asInstanceOf[Int],
      "requestTemplate" -> r.nextInt(2)
    )
  }

  val ipListFeed = csv("ipList.txt").random
  val linklist = separatedValues("linklist.txt", ' ').random
  val urllistFeed = csv("urllist.txt").random
  val userAgentFeed = csv("userAgent.txt").random
  val wordsFeed = csv("words.txt").random

  val flagNewUser = exec(session => {
    session.set("flag", "New user")
  })

  val flagNewSession = exec(session => {
    if (session.attributes.get("flag").get == "") session.set("flag", "New session") else session
  })

  val unflag = exec(session => {
    session.set("flag", "")
  })

  val updatePreviousURL = exec(session => {
    session.set("previousURL", session.attributes.get("destinationURL").get)
  })

  val pauseAndUpdateTimestamp = pause("${pauseTime}", TimeUnit.MILLISECONDS)
    .exec(session => {
    session.set("timestamp", session.attributes.get("timestamp").get.asInstanceOf[Long] + session.attributes.get("pauseTime").get.asInstanceOf[Int])
  })

  // Browsing requests and scenario

  val loadContext = feed(requestsFeed).feed(urllistFeed).exec(http("LoadContext ${requestTemplate} ${flag}").post("/cxs/context.js?sessionId=${sessionId}&timestamp=${timestamp}&remoteAddr=${ip}")
    .headers(headers)
    .body(ELFileBody("ContextLoad_request_${requestTemplate}.json")))
    .exec(updatePreviousURL)
    .exec(pauseAndUpdateTimestamp)

  val userLogin = feed(requestsFeed).exec(http("UserLogin").post("/cxs/eventcollector?sessionId=${sessionId}&timestamp=${timestamp}&remoteAddr=${ip}")
    .headers(headers)
    .body(ELFileBody("UserLogin_request.json")))
    .exec(pauseAndUpdateTimestamp)

  val formEvent = feed(requestsFeed).exec(http("Form").post("/cxs/eventcollector?sessionId=${sessionId}&timestamp=${timestamp}&remoteAddr=${ip}")
    .headers(headers)
    .body(ELFileBody("Form_request.json")))
    .exec(pauseAndUpdateTimestamp)

  val searchEvent = feed(requestsFeed).feed(wordsFeed).exec(http("Search").post("/cxs/eventcollector?sessionId=${sessionId}&timestamp=${timestamp}&remoteAddr=${ip}")
    .headers(headers)
    .body(ELFileBody("Search_request.json")))
    .exec(pauseAndUpdateTimestamp)

  val scnRun = feed(usersFeed)
      .exec(flagNewUser)
      .repeat("${numberOfSessions}") {
      feed(sessionsFeed).feed(userAgentFeed).feed(ipListFeed)
        .exec(flagNewSession)
        .exec(loadContext)
        .exec(unflag)
        .randomSwitch(Parameters.loginPercentage -> userLogin)
        .repeat("${sessionSize}") {
        loadContext
          .randomSwitch(
            Parameters.formEventPercentage -> formEvent,
            Parameters.searchEventPercentage -> searchEvent
          )
      }
        .exec(flushSessionCookies)
    }
      .exec(flushCookieJar);

  val scn = scenario("User").during(Parameters.totalTime) {
    scnRun
  }

  val scnSingle = scenario("User").exec(scnRun);
}
