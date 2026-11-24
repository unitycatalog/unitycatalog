package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertApiExceptionStatusOnly;
import static io.unitycatalog.server.utils.TestUtils.assertIcebergApiException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.IcebergViewTestUtils;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.util.UUID;
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
  public void dropWithADifferentIdKeepsTheViewMetadataAndGrants() throws Exception {
    String catalog = TestUtils.CATALOG_NAME;
    String schema = TestUtils.SCHEMA_NAME;
    String fullName = catalog + "." + schema + ".v";
    var client = new IcebergRestClient(adminConfig);
    var created =
        client.createView(
            catalog, schema, IcebergViewTestUtils.request("v", "spark", "select 1 as id"));
    createTestUser(REGULAR_1);
    grantPermissions(REGULAR_1, SecurableType.TABLE, fullName, Privileges.READ_METADATA);
    var tables =
        new Repositories(
                testDatabase.sessionFactory(),
                new ServerProperties(serverProperties),
                cloudCredentialVendor)
            .getTableRepository();
    var rejected =
        assertThrows(
            BaseException.class,
            () -> tables.deleteViewForIceberg(catalog, schema, "v", UUID.randomUUID()));
    assertThat(rejected.getErrorCode()).isEqualTo(ErrorCode.PERMISSION_DENIED);

    var stored = client.loadView(catalog, schema, "v");
    assertThat(stored.metadata().uuid()).isEqualTo(created.metadata().uuid());
    assertThat(stored.metadataLocation()).isEqualTo(created.metadataLocation());
    assertThat(grantsApi.get(SecurableType.TABLE, fullName, null).getPrivilegeAssignments())
        .filteredOn(assignment -> REGULAR_1.equals(assignment.getPrincipal()))
        .flatExtracting(assignment -> assignment.getPrivileges())
        .containsExactly(Privilege.READ_METADATA);
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
    var created = owner.createView(catalog, namespace, request);
    var readerRequest = IcebergViewTestUtils.request("reader_view", "spark", "select 1 as id");
    grantPermissions(REGULAR_1, SecurableType.CATALOG, catalog, Privileges.USE_CATALOG);
    grantPermissions(
        REGULAR_1, SecurableType.SCHEMA, catalog + "." + namespace, Privileges.USE_SCHEMA);
    assertIcebergApiException(() -> reader.createView(catalog, namespace, readerRequest), 403);
    assertIcebergApiException(() -> reader.loadView(catalog, namespace, "v"), 403);
    assertApiExceptionStatusOnly(() -> reader.viewExists(catalog, namespace, "v"), 403);

    grantPermissions(REGULAR_1, SecurableType.TABLE, fullName, Privileges.READ_METADATA);
    assertThat(reader.loadView(catalog, namespace, "v").metadata().uuid())
        .isEqualTo(created.metadata().uuid());
    assertThat(reader.viewExists(catalog, namespace, "v")).isTrue();
    assertIcebergApiException(() -> reader.createView(catalog, namespace, readerRequest), 403);
    grantPermissions(
        REGULAR_1, SecurableType.SCHEMA, catalog + "." + namespace, Privileges.CREATE_TABLE);
    reader.createView(catalog, namespace, readerRequest);
    assertThat(reader.viewExists(catalog, namespace, "reader_view")).isTrue();
    assertIcebergApiException(() -> reader.dropView(catalog, namespace, "v"), 403);
    assertIcebergApiException(() -> reader.loadCredentials(catalog, namespace, "v"), 403);
    grantPermissions(REGULAR_1, SecurableType.TABLE, fullName, Privileges.SELECT);
    assertIcebergApiException(() -> reader.loadCredentials(catalog, namespace, "v"), 404);
    assertIcebergApiException(() -> reader.loadCredentials(catalog, namespace, "base_table"), 403);
    String viewId = created.metadata().uuid();
    assertThat(countCasbinRulesReferencing(viewId)).isPositive();
    owner.dropView(catalog, namespace, "v");
    assertThat(countCasbinRulesReferencing(viewId)).isZero();
    assertThat(owner.viewExists(catalog, namespace, "v")).isFalse();
    assertIcebergApiException(
        () -> owner.loadView(catalog, namespace, "v"), 404, NoSuchViewException.class, "v");

    var nativeApi =
        new TablesApi(TestUtils.createApiClient(createTestUserServerConfig(PRINCIPAL_1)));
    String nativeName = "native_view";
    String nativeFullName = catalog + "." + namespace + "." + nativeName;
    var nativeView =
        nativeApi.createTable(
            IcebergViewTestUtils.nativeRequest(
                catalog,
                namespace,
                nativeName,
                "select id from " + catalog + "." + namespace + ".base_table"));
    assertIcebergApiException(() -> reader.loadView(catalog, namespace, nativeName), 403);
    grantPermissions(REGULAR_1, SecurableType.TABLE, nativeFullName, Privileges.READ_METADATA);
    var response = reader.loadView(catalog, namespace, nativeName);
    assertThat(response.metadata().uuid()).isEqualTo(nativeView.getTableId());
    assertThat(response.config()).isEmpty();
    assertThat(reader.viewExists(catalog, namespace, nativeName)).isTrue();
    assertIcebergApiException(() -> reader.dropView(catalog, namespace, nativeName), 403);
    assertIcebergApiException(
        () -> owner.dropView(catalog, namespace, nativeName), 400, nativeName);
    assertIcebergApiException(() -> reader.loadCredentials(catalog, namespace, nativeName), 403);
    grantPermissions(REGULAR_1, SecurableType.TABLE, nativeFullName, Privileges.SELECT);
    assertIcebergApiException(() -> reader.loadCredentials(catalog, namespace, nativeName), 404);
    assertIcebergApiException(() -> reader.loadCredentials(catalog, namespace, "base_table"), 403);
    assertThat(nativeApi.getTable(nativeFullName, true, false, false)).isEqualTo(nativeView);
    nativeApi.deleteTable(nativeFullName);
  }
}
