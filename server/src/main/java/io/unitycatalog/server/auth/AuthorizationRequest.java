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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.unitycatalog.server.model.SecurableType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Input to an external permission decision. Resource IDs include resolved parents and any related
 * external location or credential; resource keys retain the names and paths supplied by the caller.
 * Context contains values extracted by {@code @AuthorizeKey}, such as credential scope.
 */
public record AuthorizationRequest(
    Principal principal,
    AuthorizationAction action,
    Map<SecurableType, UUID> resources,
    @JsonProperty("resource_keys") Map<SecurableType, Object> resourceKeys,
    Map<String, Object> context) {

  public AuthorizationRequest {
    resources = Map.copyOf(resources);
    // Optional request fields are represented by null, which Map.copyOf does not accept.
    resourceKeys = Collections.unmodifiableMap(new HashMap<>(resourceKeys));
    context = Collections.unmodifiableMap(new HashMap<>(context));
  }

  /** Identity resolved from the authenticated UC user, rather than request body fields. */
  public record Principal(UUID id, String email) {}
}
