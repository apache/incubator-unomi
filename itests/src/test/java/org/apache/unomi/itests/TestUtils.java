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

package org.apache.unomi.itests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.unomi.api.ContextResponse;
import org.apache.unomi.persistence.spi.CustomObjectMapper;
import org.junit.Assert;

import java.io.IOException;

public class TestUtils {
	private static final String JSON_MYME_TYPE = "application/json";

	public static <T> T retrieveResourceFromResponse(HttpResponse response, Class<T> clazz) throws IOException {
		if (response == null) {
			return null;
		}
		if (response.getEntity() == null) {
			return null;
		}
		String jsonFromResponse = EntityUtils.toString(response.getEntity());
		// ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		ObjectMapper mapper = CustomObjectMapper.getObjectMapper();
		try {
			T value = mapper.readValue(jsonFromResponse, clazz);
			return value;
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return null;
	}

	public RequestResponse executeContextJSONRequest(HttpPost request, String sessionId) throws IOException {
		try (CloseableHttpResponse response = HttpClientBuilder.create().build().execute(request)) {
			// validate mimeType
			String mimeType = ContentType.getOrDefault(response.getEntity()).getMimeType();
			Assert.assertEquals("Response content type should be " + JSON_MYME_TYPE, JSON_MYME_TYPE, mimeType);

			// validate context
			ContextResponse context = TestUtils.retrieveResourceFromResponse(response, ContextResponse.class);
			Assert.assertNotNull("Context should not be null", context);
			Assert.assertNotNull("Context profileId should not be null", context.getProfileId());
			Assert.assertEquals("Context sessionId should be the same as the sessionId used to request the context", sessionId,
				context.getSessionId());

			String cookieHeader = null;
			if (response.containsHeader("Set-Cookie")) {
				cookieHeader = response.getHeaders("Set-Cookie")[0].toString().substring(12);
			}
			return new RequestResponse(context, cookieHeader);
		}
	}

	public static class RequestResponse {
		private ContextResponse contextResponse;
		private String cookieHeaderValue;

		public RequestResponse(ContextResponse contextResponse, String cookieHeaderValue) {
			this.contextResponse = contextResponse;
			this.cookieHeaderValue = cookieHeaderValue;
		}

		public ContextResponse getContextResponse() {
			return contextResponse;
		}

		public String getCookieHeaderValue() {
			return cookieHeaderValue;
		}
	}
}
