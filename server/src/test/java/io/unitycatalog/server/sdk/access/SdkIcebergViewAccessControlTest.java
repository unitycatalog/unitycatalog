package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertIcebergApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.IcebergViewTestUtils;
import io.unitycatalog.server.utils.ServerProperties.Property;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class SdkIcebergViewAccessControlTest extends SdkAccessControlBaseCRUDTest {
  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.ICEBERG_VIEW_ENABLED.getKey(), "true");
    serverProperties.setProperty(
        Property.ICEBERG_VIEW_METADATA_ROOT.getKey(),
        testDirectoryRoot.resolve("view_metadata").toString());
  }

  @Test
  public void metadataAccessDoesNotGrantCreateDropOrBaseCredentials() throws Exception {
    String catalog = "view_catalog";
    String namespace = "view_schema";
    String fullName = catalog + "." + namespace + ".v";
    createCommonTestUsers();
    setupCommonCatalogAndSchema(catalog, namespace);
    var owner = new IcebergRestClient(createTestUserServerConfig(PRINCIPAL_1));
    var reader = new IcebergRestClient(createTestUserServerConfig(REGULAR_1));
    owner.createTable(
        catalog,
        namespace,
        CreateTableRequest.builder()
            .withName("base_table")
            .withLocation(testDirectoryRoot.resolve("base_data").toUri().toString())
            .withSchema(new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())))
            .build());
    var request =
        IcebergViewTestUtils.request(
            "v", "spark", "select id from " + catalog + "." + namespace + ".base_table");
    assertIcebergApiException(() -> reader.createView(catalog, namespace, request), 403);
    var created = owner.createView(catalog, namespace, request);
    assertIcebergApiException(() -> reader.loadView(catalog, namespace, "v"), 403);
    grantPermissions(REGULAR_1, SecurableType.CATALOG, catalog, Privileges.USE_CATALOG);
    grantPermissions(
        REGULAR_1, SecurableType.SCHEMA, catalog + "." + namespace, Privileges.USE_SCHEMA);
    grantPermissions(REGULAR_1, SecurableType.TABLE, fullName, Privileges.READ_METADATA);
    assertThat(reader.loadView(catalog, namespace, "v").metadata().uuid())
        .isEqualTo(created.metadata().uuid());
    assertThat(reader.viewExists(catalog, namespace, "v")).isTrue();
    assertIcebergApiException(() -> reader.dropView(catalog, namespace, "v"), 403);
    assertIcebergApiException(() -> reader.loadCredentials(catalog, namespace, "v"), 403);
    grantPermissions(REGULAR_1, SecurableType.TABLE, fullName, Privileges.SELECT);
    assertIcebergApiException(() -> reader.loadCredentials(catalog, namespace, "v"), 404);
    assertIcebergApiException(() -> reader.loadCredentials(catalog, namespace, "base_table"), 403);
    owner.dropView(catalog, namespace, "v");
    assertThat(owner.viewExists(catalog, namespace, "v")).isFalse();
    assertIcebergApiException(
        () -> owner.loadView(catalog, namespace, "v"), 404, NoSuchViewException.class, "v");
  }
}
