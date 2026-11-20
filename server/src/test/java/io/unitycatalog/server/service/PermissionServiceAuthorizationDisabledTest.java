package io.unitycatalog.server.service;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.GrantsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdateCatalog;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.control.api.UsersApi;
import io.unitycatalog.control.model.Email;
import io.unitycatalog.control.model.UserResource;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.TestUtils;
import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;

class PermissionServiceAuthorizationDisabledTest extends BaseServerTest {
  @Test
  void transfersOwnershipWithoutAuthorization() throws Exception {
    URI uri = URI.create(serverConfig.getServerUrl());
    var controlClient = new io.unitycatalog.control.ApiClient();
    controlClient.setHost(uri.getHost());
    controlClient.setPort(uri.getPort());
    controlClient.setScheme(uri.getScheme());
    String owner = "new-owner@localhost";
    new UsersApi(controlClient)
        .createUser(
            new UserResource()
                .displayName("New owner")
                .emails(List.of(new Email().value(owner).primary(true))));
    CatalogsApi catalogs = new CatalogsApi(TestUtils.createApiClient(serverConfig));
    catalogs.createCatalog(new CreateCatalog().name("catalog"));
    assertThat(catalogs.updateCatalog("catalog", new UpdateCatalog().owner(owner)).getOwner())
        .isEqualTo(owner);
    assertThat(catalogs.getCatalog("catalog", false).getOwner()).isEqualTo(owner);
  }

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
