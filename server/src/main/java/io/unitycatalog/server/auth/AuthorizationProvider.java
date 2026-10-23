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

/** Makes an operation-level permission decision after UC has checked storage preconditions. */
@FunctionalInterface
public interface AuthorizationProvider {
  /**
   * @param request authenticated identity, operation, and server-resolved resource context
   * @return true only when the provider explicitly permits this operation
   * @throws io.unitycatalog.server.exception.BaseException if no valid decision can be obtained
   */
  boolean authorize(AuthorizationRequest request);
}
