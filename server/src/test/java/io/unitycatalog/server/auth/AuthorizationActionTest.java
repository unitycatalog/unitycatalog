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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.reflect.ClassPath;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.service.IcebergRestCatalogService;
import io.unitycatalog.server.service.TableService;
import io.unitycatalog.server.service.TemporaryTableCredentialsService;
import io.unitycatalog.server.service.delta.DeltaApiService;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AuthorizationActionTest {
  @Test
  public void permissionEndpointsDeclareActions() throws IOException {
    EnumSet<AuthorizationAction> actions = EnumSet.noneOf(AuthorizationAction.class);
    for (ClassPath.ClassInfo type :
        ClassPath.from(getClass().getClassLoader())
            .getTopLevelClassesRecursive("io.unitycatalog.server.service")) {
      for (Method method : type.load().getDeclaredMethods()) {
        AuthorizeExpression annotation = method.getAnnotation(AuthorizeExpression.class);
        if (annotation == null) {
          continue;
        }
        assertThat(annotation.precondition()).as("%s", method).doesNotContain("#authorize");
        if (annotation.value().trim().equals("#principal != null")) {
          assertThat(annotation.action())
              .as("%s", method)
              .isEqualTo(AuthorizationAction.UNSPECIFIED);
        } else {
          assertThat(annotation.action())
              .as("%s", method)
              .isNotEqualTo(AuthorizationAction.UNSPECIFIED);
        }
        actions.add(annotation.action());
      }
    }
    assertThat(actions).isEqualTo(EnumSet.allOf(AuthorizationAction.class));
  }

  @Test
  public void tableActionsAreSharedAcrossProtocols() {
    for (Class<?> service :
        List.of(TableService.class, DeltaApiService.class, IcebergRestCatalogService.class)) {
      assertAction(service, "createTable", AuthorizationAction.CREATE_TABLE);
    }
    assertAction(
        TemporaryTableCredentialsService.class,
        "generateTemporaryTableCredential",
        AuthorizationAction.GENERATE_TABLE_CREDENTIALS);
    assertAction(
        DeltaApiService.class,
        "getTableCredentials",
        AuthorizationAction.GENERATE_TABLE_CREDENTIALS);
    assertAction(IcebergRestCatalogService.class, "loadCredentials", AuthorizationAction.GET_TABLE);
  }

  @Test
  public void operationsSharingPermissionRulesHaveDistinctActions() {
    assertAction(IcebergRestCatalogService.class, "loadTable", AuthorizationAction.GET_TABLE);
    assertAction(IcebergRestCatalogService.class, "listTables", AuthorizationAction.LIST_TABLES);
    assertAction(
        IcebergRestCatalogService.class, "reportMetrics", AuthorizationAction.REPORT_TABLE_METRICS);
    assertAction(
        DeltaApiService.class, "reportMetrics", AuthorizationAction.REPORT_TABLE_COMMIT_METRICS);
  }

  private static void assertAction(
      Class<?> service, String methodName, AuthorizationAction expected) {
    assertThat(
            Arrays.stream(service.getDeclaredMethods()).filter(m -> m.getName().equals(methodName)))
        .as("%s.%s", service.getSimpleName(), methodName)
        .singleElement()
        .extracting(method -> method.getAnnotation(AuthorizeExpression.class).action())
        .isEqualTo(expected);
  }
}
