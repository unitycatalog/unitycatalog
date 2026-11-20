package io.unitycatalog.server.service;

import static io.unitycatalog.server.utils.TestUtils.assertApiExceptionStatusOnly;
import static io.unitycatalog.server.utils.TestUtils.assertIcebergApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.IcebergViewTestUtils;
import io.unitycatalog.server.utils.TestUtils;
import org.apache.iceberg.rest.Endpoint;
import org.junit.jupiter.api.Test;

public class IcebergViewDisabledTest extends BaseServerTest {
  @Test
  public void defaultConfigurationDoesNotEnableViewLifecycle() throws Exception {
    var api = TestUtils.createApiClient(serverConfig);
    new CatalogsApi(api).createCatalog(new CreateCatalog().name("view_catalog"));
    new SchemasApi(api)
        .createSchema(new CreateSchema().catalogName("view_catalog").name("view_schema"));
    var client = new IcebergRestClient(serverConfig);
    assertThat(client.config("view_catalog").endpoints())
        .doesNotContain(
            Endpoint.V1_CREATE_VIEW,
            Endpoint.V1_UPDATE_VIEW,
            Endpoint.V1_DELETE_VIEW,
            Endpoint.V1_LIST_VIEWS,
            Endpoint.V1_VIEW_EXISTS);
    assertIcebergApiException(() -> client.loadView("view_catalog", "view_schema", "v"), 404);
    assertIcebergApiException(
        () ->
            client.createView(
                "view_catalog",
                "view_schema",
                IcebergViewTestUtils.request("v", "spark", "select 1 as id")),
        501);
    assertIcebergApiException(() -> client.dropView("view_catalog", "view_schema", "v"), 501);
    assertApiExceptionStatusOnly(() -> client.viewExists("view_catalog", "view_schema", "v"), 501);
  }
}
