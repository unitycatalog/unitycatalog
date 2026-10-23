/*
 * Copyright 2026 The Unity Catalog Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.unitycatalog.server.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.ResponseTimeoutMode;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.client.WebClientBuilder;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.auth.AuthToken;
import com.linecorp.armeria.common.util.Exceptions;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Calls OPA's Data API and permits access only for an HTTP 200 with a Boolean true result. */
public final class OpaAuthorizationProvider implements AuthorizationProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpaAuthorizationProvider.class);
  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private static final long MAX_RESPONSE_LENGTH = 64 * 1024;

  private final WebClient client;
  private final String policyUrl;

  public OpaAuthorizationProvider(ServerProperties properties) {
    policyUrl = properties.get(Property.OPA_POLICY_URL);
    // Like other server HTTP clients, this uses Armeria's shared, managed ClientFactory.
    WebClientBuilder builder =
        WebClient.builder()
            .responseTimeout(properties.getOpaResponseTimeout())
            .responseTimeoutMode(ResponseTimeoutMode.FROM_START)
            .maxResponseLength(MAX_RESPONSE_LENGTH);
    String token = properties.get(Property.OPA_BEARER_TOKEN);
    if (token != null && !token.isBlank()) {
      builder.auth(AuthToken.ofOAuth2(token));
    }
    client = builder.build();
  }

  @Override
  public boolean authorize(AuthorizationRequest request) {
    try {
      AggregatedHttpResponse response =
          client
              .prepare()
              .post(policyUrl)
              .content(MediaType.JSON, MAPPER.writeValueAsString(Map.of("input", request)))
              .execute()
              .aggregate()
              .join();
      if (response.status().equals(HttpStatus.OK)) {
        JsonNode body = MAPPER.readTree(response.contentUtf8());
        JsonNode result = body == null ? null : body.get("result");
        if (result != null && result.isBoolean()) {
          return result.booleanValue();
        }
        LOGGER.warn("OPA did not return a Boolean decision for action {}", request.action());
      } else {
        LOGGER.warn("OPA returned HTTP {} for action {}", response.status(), request.action());
      }
    } catch (JsonProcessingException | RuntimeException e) {
      // Do not log policy input, response bodies, or authentication headers.
      LOGGER.warn(
          "OPA decision failed for action {} ({})",
          request.action(),
          Exceptions.peel(e).getClass().getSimpleName());
    }
    // Do not attach upstream exceptions: error responses may include exception details.
    throw new BaseException(ErrorCode.INTERNAL, "Authorization policy evaluation failed.");
  }
}
