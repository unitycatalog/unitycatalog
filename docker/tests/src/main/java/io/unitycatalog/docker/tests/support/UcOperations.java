package io.unitycatalog.docker.tests.support;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.GrantsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.api.ViewsApi;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.CreateView;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.client.model.ViewInfo;
import java.util.List;

public final class UcOperations {

  private final String serverUrl;
  private final String token;

  public UcOperations(String serverUrl, String token) {
    this.serverUrl = serverUrl;
    this.token = token;
  }

  public static void waitForServer(String serverUrl, String adminToken)
      throws InterruptedException {
    CatalogsApi api = new CatalogsApi(UcClientFactory.catalogClient(serverUrl, adminToken));
    for (int attempt = 1; attempt <= 30; attempt++) {
      try {
        api.listCatalogs(null, 1);
        return;
      } catch (ApiException e) {
        Thread.sleep(1000);
      }
    }
    throw new IllegalStateException("Unity Catalog server not reachable at " + serverUrl);
  }

  public void assertCatalogVisible(String catalog) throws ApiException {
    if (!isCatalogListed(catalog)) {
      throw new IllegalStateException("Catalog not visible: " + catalog);
    }
  }

  public void assertCatalogVisibleOnly(String catalog, String excludedCatalog)
      throws ApiException {
    List<CatalogInfo> catalogs = listCatalogs();
    boolean hasCatalog = catalogs.stream().anyMatch(c -> catalog.equals(c.getName()));
    boolean hasExcluded = catalogs.stream().anyMatch(c -> excludedCatalog.equals(c.getName()));
    if (!hasCatalog || hasExcluded) {
      throw new IllegalStateException(
          "Expected " + catalog + " visible and " + excludedCatalog + " absent");
    }
  }

  public SchemaInfo createSchema(String catalog, String schema, String comment)
      throws ApiException {
    return schemasApi()
        .createSchema(
            new CreateSchema().name(schema).catalogName(catalog).comment(comment));
  }

  public void createSchemaIgnoreConflict(String catalog, String schema, String comment)
      throws ApiException {
    try {
      createSchema(catalog, schema, comment);
    } catch (ApiException e) {
      if (e.getCode() != 409) {
        throw e;
      }
    }
  }

  public void assertCreateSchemaConflict(String catalog, String schema) throws ApiException {
    try {
      schemasApi()
          .createSchema(new CreateSchema().name(schema).catalogName(catalog));
      throw new AssertionError("Expected schema create conflict for " + catalog + "." + schema);
    } catch (ApiException e) {
      if (!isClientError(e)) {
        throw e;
      }
    }
  }

  public void grantUseSchema(String catalog, String schema, String principal)
      throws ApiException {
    PermissionsChange change =
        new PermissionsChange()
            .principal(principal)
            .add(List.of(Privilege.fromValue("USE SCHEMA")))
            .remove(List.of());
    grantsApi()
        .update(
            SecurableType.SCHEMA,
            catalog + "." + schema,
            new UpdatePermissions().changes(List.of(change)));
  }

  public SchemaInfo getSchema(String catalog, String schema) throws ApiException {
    return schemasApi().getSchema(catalog + "." + schema);
  }

public TableInfo createExternalTable(
      String catalog,
      String schema,
      String table,
      String location,
      DataSourceFormat format,
      List<ColumnInfo> columns)
      throws ApiException {
    return tablesApi()
        .createTable(
            new CreateTable()
                .name(table)
                .catalogName(catalog)
                .schemaName(schema)
                .tableType(TableType.EXTERNAL)
                .dataSourceFormat(format)
                .storageLocation(location)
                .columns(columns));
  }

  public TableInfo getTable(String catalog, String schema, String table) throws ApiException {
    return tablesApi().getTable(fullName(catalog, schema, table), true, true);
  }

  public void assertTableFormatDelta(String catalog, String schema, String table)
      throws ApiException {
    TableInfo info = getTable(catalog, schema, table);
    if (info.getDataSourceFormat() != DataSourceFormat.DELTA) {
      throw new AssertionError("Expected DELTA, got " + info.getDataSourceFormat());
    }
  }

  public ViewInfo createView(
      String catalog,
      String schema,
      String view,
      String sourceCatalog,
      String sourceSchema,
      String sourceTable,
      List<ColumnInfo> columns)
      throws ApiException {
    String sql =
        String.format(
            "SELECT * FROM `%s`.`%s`.`%s`", sourceCatalog, sourceSchema, sourceTable);
    return viewsApi()
        .createView(
            new CreateView()
                .name(view)
                .catalogName(catalog)
                .schemaName(schema)
                .viewType(TableType.VIEW)
                .sqlQuery(sql)
                .columns(columns));
  }

  public ViewInfo getView(String catalog, String schema, String view) throws ApiException {
    return viewsApi().getView(fullName(catalog, schema, view));
  }

  public void assertListed(String type, String catalog, String schema, String name)
      throws ApiException {
    if (!isListed(type, catalog, schema, name)) {
      throw new AssertionError(name + " not listed in " + type);
    }
  }

  public void assertNotListed(String type, String catalog, String schema, String name)
      throws ApiException {
    if (isListed(type, catalog, schema, name)) {
      throw new AssertionError(name + " still listed in " + type);
    }
  }

  public void deleteView(String catalog, String schema, String view) throws ApiException {
    viewsApi().deleteView(fullName(catalog, schema, view));
  }

  public void deleteTable(String catalog, String schema, String table) throws ApiException {
    tablesApi().deleteTable(fullName(catalog, schema, table));
  }

  public void deleteSchema(String catalog, String schema, boolean force) throws ApiException {
    schemasApi().deleteSchema(catalog + "." + schema, force ? true : null);
  }

  public void assertDeleteSchemaRejected(String catalog, String schema) throws ApiException {
    try {
      deleteSchema(catalog, schema, false);
      throw new AssertionError("Expected schema delete rejection");
    } catch (ApiException e) {
      if (!isClientError(e)) {
        throw e;
      }
    }
  }

  public void assertExternalLocationsPresent(String... names) throws ApiException {
    List<ExternalLocationInfo> locations =
        new ExternalLocationsApi(UcClientFactory.catalogClient(serverUrl, token))
            .listExternalLocations(200, null)
            .getExternalLocations();
    for (String expected : names) {
      boolean found =
          locations != null
              && locations.stream().anyMatch(el -> expected.equals(el.getName()));
      if (!found) {
        throw new AssertionError("External location not found: " + expected);
      }
    }
  }

  public void assertPathCredentialsDenied(String url, PathOperation operation)
      throws ApiException {
    try {
      pathCredentialsApi()
          .generateTemporaryPathCredentials(
              new GenerateTemporaryPathCredential().url(url).operation(operation));
      throw new AssertionError("Expected path credentials denied for " + url);
    } catch (ApiException e) {
      if (e.getCode() != 401 && e.getCode() != 403) {
        throw e;
      }
    }
  }

  public void assertPathReadAllowed(String url) throws ApiException {
    TemporaryCredentials creds =
        pathCredentialsApi()
            .generateTemporaryPathCredentials(
                new GenerateTemporaryPathCredential()
                    .url(url)
                    .operation(PathOperation.PATH_READ));
    if (creds.getAwsTempCredentials() == null
        || creds.getAwsTempCredentials().getAccessKeyId() == null
        || creds.getAwsTempCredentials().getAccessKeyId().isBlank()) {
      throw new AssertionError("PATH_READ credentials missing access_key_id");
    }
  }

  public static List<ColumnInfo> idIntColumn() {
    return ColumnModels.columns(ColumnModels.column("id", ColumnTypeName.INT, "int", true, 0));
  }

  public static List<ColumnInfo> idNameColumns() {
    return ColumnModels.columns(
        ColumnModels.column("id", ColumnTypeName.INT, "int", true, 0),
        ColumnModels.column("name", ColumnTypeName.STRING, "string", true, 1));
  }

  public static List<ColumnInfo> landingColumns() {
    return ColumnModels.columns(
        ColumnModels.column("id", ColumnTypeName.INT, "int", true, 0),
        ColumnModels.column("label", ColumnTypeName.STRING, "string", true, 1),
        ColumnModels.column("value", ColumnTypeName.DOUBLE, "double", true, 2));
  }

  public static List<ColumnInfo> customerColumns() {
    return ColumnModels.columns(
        ColumnModels.column("customer_id", ColumnTypeName.INT, "int", true, 0),
        ColumnModels.column("name", ColumnTypeName.STRING, "string", true, 1));
  }

  public static List<ColumnInfo> eventColumns() {
    return ColumnModels.columns(
        ColumnModels.column("event_id", ColumnTypeName.INT, "int", true, 0),
        ColumnModels.column("event_type", ColumnTypeName.STRING, "string", true, 1));
  }

  public static List<ColumnInfo> viewColumns() {
    return ColumnModels.columns(
        ColumnModels.column("id", ColumnTypeName.INT, "int", false, 0),
        ColumnModels.column("name", ColumnTypeName.STRING, "string", true, 1));
  }

  private boolean isCatalogListed(String catalog) throws ApiException {
    return listCatalogs().stream().anyMatch(c -> catalog.equals(c.getName()));
  }

  private List<CatalogInfo> listCatalogs() throws ApiException {
    List<CatalogInfo> catalogs = catalogsApi().listCatalogs(null, 100).getCatalogs();
    return catalogs != null ? catalogs : List.of();
  }

  private CatalogsApi catalogsApi() {
    return new CatalogsApi(UcClientFactory.catalogClient(serverUrl, token));
  }

  private boolean isListed(String type, String catalog, String schema, String name)
      throws ApiException {
    return switch (type) {
      case "schemas" -> {
        var schemas = schemasApi().listSchemas(catalog, 100, null).getSchemas();
        yield schemas != null
            && schemas.stream().anyMatch(s -> name.equals(s.getName()));
      }
      case "tables" -> {
        var tables = tablesApi().listTables(catalog, schema, 100, null).getTables();
        yield tables != null
            && tables.stream().anyMatch(t -> name.equals(t.getName()));
      }
      case "views" -> {
        var views = viewsApi().listViews(catalog, schema, 100, null).getViews();
        yield views != null
            && views.stream().anyMatch(v -> name.equals(v.getName()));
      }
      default -> throw new IllegalArgumentException("Unknown list type: " + type);
    };
  }

  private SchemasApi schemasApi() {
    return new SchemasApi(UcClientFactory.catalogClient(serverUrl, token));
  }

  private TablesApi tablesApi() {
    return new TablesApi(UcClientFactory.catalogClient(serverUrl, token));
  }

  private ViewsApi viewsApi() {
    return new ViewsApi(UcClientFactory.catalogClient(serverUrl, token));
  }

  private GrantsApi grantsApi() {
    return new GrantsApi(UcClientFactory.catalogClient(serverUrl, token));
  }

  private TemporaryCredentialsApi pathCredentialsApi() {
    return new TemporaryCredentialsApi(UcClientFactory.catalogClient(serverUrl, token));
  }

  private static String fullName(String catalog, String schema, String object) {
    return catalog + "." + schema + "." + object;
  }

  private static boolean isClientError(ApiException e) {
    int code = e.getCode();
    return code == 400 || code == 403 || code == 404 || code == 409;
  }
}
