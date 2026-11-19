package io.unitycatalog.server.service;

import static io.unitycatalog.server.utils.IcebergViewTestUtils.nativeRequest;
import static io.unitycatalog.server.utils.IcebergViewTestUtils.request;
import static io.unitycatalog.server.utils.TestUtils.assertIcebergApiException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IcebergViewLifecycleTest extends BaseServerTest {
  private static final String CAT = "view_catalog";
  private static final String NS = "view_schema";
  private IcebergRestClient client;
  private TablesApi nativeApi;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.ICEBERG_VIEW_ENABLED.getKey(), "true");
    serverProperties.setProperty(Property.ICEBERG_TABLE_ENABLED.getKey(), "false");
    serverProperties.setProperty(
        Property.ICEBERG_VIEW_METADATA_ROOT.getKey(),
        testDirectoryRoot.resolve("view_metadata").toString());
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    try {
      var api = TestUtils.createApiClient(serverConfig);
      new CatalogsApi(api).createCatalog(new CreateCatalog().name(CAT));
      new SchemasApi(api).createSchema(new CreateSchema().catalogName(CAT).name(NS));
      nativeApi = new TablesApi(api);
      client = new IcebergRestClient(serverConfig);
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void realClientCreatesLoadsDropsAndRecreatesTheSameName() throws Exception {
    assertThat(client.config(CAT).endpoints())
        .contains(Endpoint.V1_CREATE_VIEW, Endpoint.V1_VIEW_EXISTS, Endpoint.V1_DELETE_VIEW)
        .doesNotContain(Endpoint.V1_UPDATE_VIEW, Endpoint.V1_LIST_VIEWS);
    assertIcebergApiException(
        () -> client.createView(CAT, NS, request("invalid", "trino", "select 1 as id")), 400);
    assertThat(client.viewExists(CAT, NS, "invalid")).isFalse();
    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.initialize(
          "uc",
          Map.of(
              CatalogProperties.URI,
              serverConfig.getServerUrl() + "/api/2.1/unity-catalog/iceberg",
              CatalogProperties.WAREHOUSE_LOCATION,
              CAT,
              OAuth2Properties.TOKEN,
              serverConfig.getAuthToken()));
      TableIdentifier id = TableIdentifier.of(NS, "v");
      var created =
          catalog
              .buildView(id)
              .withSchema(
                  new org.apache.iceberg.Schema(
                      Types.NestedField.optional(1, "id", Types.LongType.get())))
              .withDefaultNamespace(Namespace.of(NS))
              .withDefaultCatalog(CAT)
              .withQuery("spark", "select cast(1 as bigint) as id")
              .create();
      var nativeView = nativeApi.getTable(CAT + "." + NS + ".v", true, false, false);
      assertThat(nativeView.getTableType()).isEqualTo(TableType.VIEW);
      assertThat(nativeView.getTableId()).isEqualTo(created.uuid().toString());
      assertThat(nativeView.getViewDefinition()).isEqualTo("select cast(1 as bigint) as id");
      assertThat(nativeView.getColumns()).extracting(ColumnInfo::getName).containsExactly("id");
      var loaded = client.loadView(CAT, NS, "v");
      String bytes = Files.readString(Path.of(URI.create(loaded.metadataLocation())));
      assertThat(catalog.viewExists(id)).isTrue();
      assertThat(catalog.loadView(id).uuid()).isEqualTo(created.uuid());
      assertThat(client.loadView(CAT, NS, "v").metadataLocation())
          .isEqualTo(loaded.metadataLocation());
      assertThat(Files.readString(Path.of(URI.create(loaded.metadataLocation())))).isEqualTo(bytes);
      assertIcebergApiException(
          () -> client.createView(CAT, NS, request("v", "spark", "select 1 as id")), 409);
      catalog.dropView(id);
      assertThat(catalog.viewExists(id)).isFalse();
      assertIcebergApiException(
          () -> client.loadView(CAT, NS, "v"), 404, NoSuchViewException.class, "v");
      assertThatThrownBy(() -> nativeApi.getTable(CAT + "." + NS + ".v", true, false, false))
          .isInstanceOf(ApiException.class);
      assertThat(
              client.createView(CAT, NS, request("v", "spark", "select 1 as id")).metadata().uuid())
          .isNotEqualTo(created.uuid().toString());
    }
  }

  @Test
  public void failedMetadataWriteRollsBackCanonicalCreate() throws Exception {
    Path root = testDirectoryRoot.resolve("view_metadata");
    Files.writeString(root, "not a directory");
    assertIcebergApiException(
        () -> client.createView(CAT, NS, request("failed", "spark", "select 1 as id")), 500);
    Files.delete(root);
    assertThat(client.viewExists(CAT, NS, "failed")).isFalse();
    assertThatThrownBy(() -> nativeApi.getTable(CAT + "." + NS + ".failed", true, false, false))
        .isInstanceOf(ApiException.class);
  }

  @Test
  public void viewReadsLeaveCanonicalObjectsAndMutationBoundariesUntouched() throws Exception {
    var column =
        new ColumnInfo()
            .name("id")
            .position(0)
            .nullable(true)
            .typeName(ColumnTypeName.LONG)
            .typeText("bigint")
            .typeJson("{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}");
    for (TableType type : List.of(TableType.VIEW, TableType.EXTERNAL)) {
      String name = type == TableType.VIEW ? "native_view" : "data_table";
      var create =
          new CreateTable()
              .catalogName(CAT)
              .schemaName(NS)
              .name(name)
              .tableType(type)
              .columns(List.of(column));
      if (type == TableType.VIEW) {
        create.viewDefinition("select cast(1 as bigint) as id");
      } else {
        create
            .dataSourceFormat(DataSourceFormat.PARQUET)
            .storageLocation(testDirectoryRoot.resolve("data").toUri().toString());
      }
      var before = nativeApi.createTable(create);
      if (type == TableType.VIEW) {
        assertThat(client.viewExists(CAT, NS, name)).isTrue();
        var loaded = client.loadView(CAT, NS, name);
        assertThat(loaded.metadata().uuid()).isEqualTo(before.getTableId());
        assertThat(loaded.metadata().currentVersion().representations()).hasSize(1);
        assertThat(client.loadView(CAT, NS, name).metadataLocation())
            .isEqualTo(loaded.metadataLocation());
        try (var session = hibernateConfigurator.getSessionFactory().openSession()) {
          assertThat(
                  session
                      .get(TableInfoDAO.class, UUID.fromString(before.getTableId()))
                      .getIcebergViewMetadataLocation())
              .isNull();
        }
      } else {
        assertThat(client.viewExists(CAT, NS, name)).isFalse();
        assertIcebergApiException(() -> client.loadView(CAT, NS, name), 404);
      }
      assertIcebergApiException(() -> client.dropView(CAT, NS, name), 404);
      assertThat(nativeApi.getTable(CAT + "." + NS + "." + name, true, false, false))
          .isEqualTo(before);
    }
    assertThat(client.viewExists(CAT, NS, "missing")).isFalse();
    assertIcebergApiException(() -> client.dropView(CAT, NS, "missing"), 404);
  }

  @Test
  public void nativeReadFailuresLeaveTheViewIntactAndRecreationGetsAFreshIdentity()
      throws Exception {
    String name = "native_read";
    String fullName = CAT + "." + NS + "." + name;
    var created =
        nativeApi.createTable(nativeRequest(CAT, NS, name, "select cast(1 as bigint) as id"));
    Path root = testDirectoryRoot.resolve("view_metadata");
    Files.writeString(root, "not a directory");
    assertIcebergApiException(() -> client.loadView(CAT, NS, name), 500);
    assertThat(nativeApi.getTable(fullName, true, false, false)).isEqualTo(created);
    Files.delete(root);
    var loaded = client.loadView(CAT, NS, name);
    assertThat(Files.isRegularFile(Path.of(URI.create(loaded.metadataLocation())))).isTrue();
    nativeApi.deleteTable(fullName);
    assertThat(client.viewExists(CAT, NS, name)).isFalse();
    assertIcebergApiException(() -> client.loadView(CAT, NS, name), 404);
    var replacement =
        nativeApi.createTable(nativeRequest(CAT, NS, name, "select cast(2 as bigint) as id"));
    var recreated = client.loadView(CAT, NS, name);
    assertThat(recreated.metadata().uuid())
        .isEqualTo(replacement.getTableId())
        .isNotEqualTo(created.getTableId());
    assertThat(recreated.metadataLocation()).isNotEqualTo(loaded.metadataLocation());
    assertThat(Files.exists(Path.of(URI.create(loaded.metadataLocation())))).isTrue();
  }

  @Test
  public void nativeViewsWithUnsupportedResolutionStateReturnAnActionableError() throws Exception {
    int index = 0;
    for (var properties :
        List.of(
            Map.of("view.sqlConfig.spark.sql.caseSensitive", "true"),
            Map.of("view.catalogAndNamespace.numParts", "2"))) {
      String name = "unsupported_" + index++;
      var before =
          nativeApi.createTable(
              nativeRequest(CAT, NS, name, "select cast(1 as bigint) as id")
                  .properties(properties));
      assertThat(client.viewExists(CAT, NS, name)).isTrue();
      assertIcebergApiException(() -> client.loadView(CAT, NS, name), 400);
      assertThat(nativeApi.getTable(CAT + "." + NS + "." + name, true, false, false))
          .isEqualTo(before);
    }
    assertThat(Files.exists(testDirectoryRoot.resolve("view_metadata"))).isFalse();
  }
}
