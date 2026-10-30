package io.unitycatalog.server.service;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;

import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.GrantsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import org.junit.jupiter.api.Test;

class PermissionServiceAuthorizationDisabledTest extends BaseServerTest {
  @Test
  void validatesGrantAdditionsWithoutTheAuthorizationDecorator() throws Exception {
    var client = TestUtils.createApiClient(serverConfig);
    new CatalogsApi(client).createCatalog(new CreateCatalog().name("catalog"));
    new SchemasApi(client).createSchema(new CreateSchema().catalogName("catalog").name("schema"));
    assertApiException(
        () ->
            new GrantsApi(client)
                .update(
                    SecurableType.SCHEMA,
                    "catalog.schema",
                    new UpdatePermissions()
                        .changes(
                            List.of(
                                new PermissionsChange()
                                    .principal("reader")
                                    .add(List.of(Privilege.USE_CATALOG))))),
        ErrorCode.INVALID_ARGUMENT,
        "Privilege USE CATALOG cannot be granted on schema");
  }
}
