package io.unitycatalog.server.service;

import static io.unitycatalog.server.utils.IcebergViewTestUtils.nativeRequest;
import static io.unitycatalog.server.utils.IcebergViewTestUtils.request;
import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static io.unitycatalog.server.utils.TestUtils.assertIcebergApiException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.server.ServiceRequestContext;
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
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.service.iceberg.IcebergSchemaConverter;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ViewMetadataParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
              .withProperty("comment", "View created through Iceberg REST")
              .withQuery("spark", "select cast(1 as bigint) as id")
              .create();
      var nativeView = nativeApi.getTable(CAT + "." + NS + ".v", true, false, false);
      assertThat(nativeView.getTableType()).isEqualTo(TableType.VIEW);
      assertThat(nativeView.getTableId()).isEqualTo(created.uuid().toString());
      assertThat(nativeView.getViewDefinition()).isEqualTo("select cast(1 as bigint) as id");
      assertThat(nativeView.getComment()).isEqualTo("View created through Iceberg REST");
      assertThat(nativeView.getProperties())
          .containsEntry("view.catalogAndNamespace.numParts", "2");
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
      Path file = Path.of(URI.create(loaded.metadataLocation()));
      try (var paths = Files.walk(testDirectoryRoot.resolve("view_metadata"))) {
        assertThat(paths.skip(1)).containsExactly(file.getParent(), file);
      }
      catalog.dropView(id);
      assertThat(catalog.viewExists(id)).isFalse();
      assertIcebergApiException(
          () -> client.loadView(CAT, NS, "v"), 404, NoSuchViewException.class, "v");
      assertThatThrownBy(() -> nativeApi.getTable(CAT + "." + NS + ".v", true, false, false))
          .isInstanceOf(ApiException.class);
      var recreated = client.createView(CAT, NS, request("v", "spark", "select 1 as id"));
      assertThat(recreated.metadata().uuid()).isNotEqualTo(created.uuid().toString());
      Path recreatedFile = Path.of(URI.create(recreated.metadataLocation()));
      String recreatedJson = Files.readString(recreatedFile);
      nativeApi.deleteTable(CAT + "." + NS + ".v");
      assertThat(catalog.viewExists(id)).isFalse();
      assertThat(Files.readString(recreatedFile)).isEqualTo(recreatedJson);
      nativeApi.createTable(nativeRequest(CAT, NS, "v", "select cast(1 as bigint) as id"));
      assertThatThrownBy(() -> catalog.dropView(id))
          .isInstanceOf(BadRequestException.class)
          .hasMessageContaining("UC Tables API");
      assertThat(catalog.viewExists(id)).isTrue();
    }
  }

  @Test
  public void registrationRejectsADifferentSchemaId() {
    var tables =
        new Repositories(
                testDatabase.sessionFactory(),
                new ServerProperties(serverProperties),
                cloudCredentialVendor)
            .getTableRepository();
    var create =
        new io.unitycatalog.server.model.CreateTable()
            .catalogName(CAT)
            .schemaName(NS)
            .name("v")
            .tableType(io.unitycatalog.server.model.TableType.VIEW)
            .viewDefinition("select 1 as id")
            .columns(
                IcebergSchemaConverter.toColumnInfos(
                    request("v", "spark", "select 1 as id").schema()));
    UUID viewId = UUID.randomUUID();
    var context = ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/"));
    try (var ignored = context.push()) {
      var error =
          assertThrows(
              BaseException.class,
              () ->
                  tables.createViewForIceberg(
                      create, UUID.randomUUID(), viewId, "file:/view.json"));
      assertThat(error.getErrorCode()).isEqualTo(ErrorCode.SCHEMA_NOT_FOUND);
      assertThat(error).hasMessage("Schema not found: " + CAT + "." + NS);
    }
    try (var session = testDatabase.sessionFactory().openSession()) {
      assertThat(session.get(TableInfoDAO.class, viewId)).isNull();
    }
  }

  @Test
  public void loadViewReportsMissingParentsAsNoSuchView() {
    assertIcebergApiException(
        () -> client.loadView("missing_catalog", NS, "v"), 404, NoSuchViewException.class, "v");
    assertIcebergApiException(
        () -> client.loadView(CAT, "missing_schema", "v"), 404, NoSuchViewException.class, "v");
  }

  @Test
  public void viewMetadataIsWrittenBeforeWaitingForTheSchemaLock() throws Exception {
    var api = TestUtils.createApiClient(serverConfig);
    UUID schemaId =
        UUID.fromString(new SchemasApi(api).getSchema(CAT + "." + NS, false).getSchemaId());
    var request = request("v", "spark", "select 42 as id");
    Path root = testDirectoryRoot.resolve("view_metadata");
    var prepared = new ArrayList<Path>();
    var created =
        testDatabase.runBlockedByHeldWrite(
            session -> RepositoryUtils.getForUpdate(session, SchemaInfoDAO.class, schemaId),
            () -> client.createView(CAT, NS, request),
            () -> {
              assertThat(root)
                  .as("metadata root while the view create waits for the schema lock")
                  .isDirectory();
              try (var paths = Files.walk(root)) {
                prepared.addAll(paths.filter(Files::isRegularFile).toList());
              }
              assertThat(prepared).hasSize(1);
              var file = prepared.get(0);
              var metadata =
                  ViewMetadataParser.fromJson(file.toUri().toString(), Files.readString(file));
              assertThat(metadata.currentVersion().representations())
                  .isEqualTo(request.viewVersion().representations());
              return null;
            });
    assertThat(prepared).containsExactly(Path.of(URI.create(created.metadataLocation())));
    assertThat(nativeApi.getTable(CAT + "." + NS + ".v", true, false, false).getTableId())
        .isEqualTo(created.metadata().uuid());
  }

  @ParameterizedTest
  @ValueSource(strings = {"irc-create", "native-create", "rename"})
  public void nameClaimsWaitForConcurrentWrites(String operation) throws Exception {
    var api = TestUtils.createApiClient(serverConfig);
    UUID schemaId =
        UUID.fromString(new SchemasApi(api).getSchema(CAT + "." + NS, false).getSchemaId());
    var request = request("reserved", "spark", "select 1 as id");
    var moving =
        client.createView(CAT, NS, request("moving", "spark", "select 1 as id")).metadata();
    var contender =
        client.createView(CAT, NS, request("contender", "spark", "select 2 as id")).metadata();
    var nativeView = nativeApi.getTable(CAT + "." + NS + ".moving", true, false, false);
    var repositories =
        new Repositories(
            testDatabase.sessionFactory(),
            new ServerProperties(serverProperties),
            cloudCredentialVendor);
    testDatabase.runBlockedByHeldWrite(
        session -> {
          // The row is already renamed, but other connections cannot see the new name until commit.
          RepositoryUtils.getForUpdate(session, SchemaInfoDAO.class, schemaId);
          TestDatabase.setColumn(
              session, TableInfoDAO.class, UUID.fromString(moving.uuid()), "name", "reserved");
        },
        () -> {
          switch (operation) {
            case "irc-create" ->
                assertIcebergApiException(
                    () -> client.createView(CAT, NS, request), 409, "reserved");
            case "native-create" ->
                assertApiException(
                    () ->
                        nativeApi.createTable(
                            new CreateTable()
                                .catalogName(CAT)
                                .schemaName(NS)
                                .tableType(TableType.VIEW)
                                .name("reserved")
                                .columns(nativeView.getColumns())
                                .viewDefinition("select 1 as id")),
                    ErrorCode.TABLE_ALREADY_EXISTS,
                    "reserved");
            case "rename" -> {
              try (var ignored =
                  ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/")).push()) {
                assertThatThrownBy(
                        () ->
                            repositories
                                .getTableRepository()
                                .renameTable(CAT, NS, "contender", "reserved"))
                    .isInstanceOfSatisfying(
                        BaseException.class,
                        e ->
                            assertThat(e.getErrorCode()).isEqualTo(ErrorCode.TABLE_ALREADY_EXISTS));
              }
            }
            default -> throw new IllegalArgumentException(operation);
          }
          return null;
        });
    assertThat(client.loadView(CAT, NS, "reserved").metadata().uuid()).isEqualTo(moving.uuid());
    assertThat(client.loadView(CAT, NS, "contender").metadata().uuid()).isEqualTo(contender.uuid());
  }

  @Test
  public void ambiguousNamesLeaveBothViewsIntactAndAllowUnrelatedCreates() throws Exception {
    var first = client.createView(CAT, NS, request("duplicate", "spark", "select 1 as id"));
    var second = client.createView(CAT, NS, request("other", "spark", "select 2 as id"));
    // Reproduce two existing rows with the same name, both otherwise eligible for IRC drop.
    testDatabase
        .sessionFactory()
        .inTransaction(
            session ->
                TestDatabase.setColumn(
                    session,
                    TableInfoDAO.class,
                    UUID.fromString(second.metadata().uuid()),
                    "name",
                    "duplicate"));

    var created = client.createView(CAT, NS, request("distinct_view", "spark", "select 3 as id"));
    assertThat(client.loadView(CAT, NS, "distinct_view").metadata().uuid())
        .isEqualTo(created.metadata().uuid());
    assertIcebergApiException(() -> client.loadView(CAT, NS, "duplicate"), 500);
    assertIcebergApiException(() -> client.dropView(CAT, NS, "duplicate"), 500);
    try (var session = testDatabase.sessionFactory().openSession()) {
      for (var original : List.of(first, second)) {
        var row = session.get(TableInfoDAO.class, UUID.fromString(original.metadata().uuid()));
        assertThat(row.getName()).isEqualTo("duplicate");
        assertThat(row.getIcebergViewMetadataLocation()).isEqualTo(original.metadataLocation());
      }
    }
  }

  @Test
  public void corruptStoredViewMetadataReturnsAServerError() throws Exception {
    var created = client.createView(CAT, NS, request("v", "spark", "select 1 as id"));
    Path file = Path.of(URI.create(created.metadataLocation()));
    for (String corrupt : List.of("{", "{}")) {
      Files.writeString(file, corrupt);
      assertIcebergApiException(
          () -> client.loadView(CAT, NS, "v"),
          500,
          ServiceFailureException.class,
          "Could not read view metadata");
    }
    Files.delete(file);
    assertIcebergApiException(
        () -> client.loadView(CAT, NS, "v"),
        500,
        ServiceFailureException.class,
        "Invalid persisted view metadata location");
  }

  @Test
  public void failedMetadataWriteDoesNotPublishAView() throws Exception {
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
        assertIcebergApiException(
            () -> client.dropView(CAT, NS, name), 400, BadRequestException.class, "UC Tables API");
      } else {
        assertThat(client.viewExists(CAT, NS, name)).isFalse();
        assertIcebergApiException(() -> client.loadView(CAT, NS, name), 404);
        assertIcebergApiException(() -> client.dropView(CAT, NS, name), 404);
      }
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
