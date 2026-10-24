package io.unitycatalog.server.migrate.kit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.FunctionsApi;
import io.unitycatalog.client.api.GrantsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateFunction;
import io.unitycatalog.client.model.CreateFunctionRequest;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.FunctionInfo;
import io.unitycatalog.client.model.FunctionParameterInfo;
import io.unitycatalog.client.model.FunctionParameterInfos;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.PrivilegeAssignment;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.control.api.UsersApi;
import io.unitycatalog.control.model.Email;
import io.unitycatalog.control.model.UserResource;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * The data an upgrade must keep: a catalog, a schema, a managed and an external table with columns,
 * a volume, a function with an {@code ARRAY} parameter (its {@code type_json} is a column an
 * upgrade step widens), a user, and a grant to that user on each. {@link #write} creates it through
 * any server's API; {@link #assertReadBack} checks it through another's.
 */
public final class DataSet {

  private static final String CATALOG = "upgrade_catalog";
  private static final String SCHEMA = CATALOG + ".upgrade_schema";
  private static final String MANAGED_TABLE = SCHEMA + ".managed_table";
  private static final String EXTERNAL_TABLE = SCHEMA + ".external_table";
  private static final String VOLUME = SCHEMA + ".upgrade_volume";
  private static final String FUNCTION = SCHEMA + ".upgrade_function";
  private static final String USER = "upgrade-reader@example.com";
  private static final String STORAGE = "file:///tmp/uc-storage/";
  private static final String COMMENT = "kept across upgrades";
  private static final Map<String, String> PROPERTIES = Map.of("upgrade.key", "upgrade value");
  private static final String ARRAY_TYPE_JSON =
      "{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true}";

  /** The grant {@link #write} gives {@link #USER} on each securable. */
  private static final Map<String, Grant> GRANTS =
      Map.of(
          CATALOG, new Grant(SecurableType.CATALOG, Privilege.USE_CATALOG),
          SCHEMA, new Grant(SecurableType.SCHEMA, Privilege.USE_SCHEMA),
          MANAGED_TABLE, new Grant(SecurableType.TABLE, Privilege.SELECT),
          EXTERNAL_TABLE, new Grant(SecurableType.TABLE, Privilege.MODIFY),
          VOLUME, new Grant(SecurableType.VOLUME, Privilege.READ_VOLUME),
          FUNCTION, new Grant(SecurableType.FUNCTION, Privilege.EXECUTE));

  private record Grant(SecurableType type, Privilege privilege) {}

  private DataSet() {}

  public static void write(ApiClient api) {
    try {
      new CatalogsApi(api)
          .createCatalog(new CreateCatalog().name(CATALOG).comment(COMMENT).properties(PROPERTIES));
      new SchemasApi(api)
          .createSchema(
              new CreateSchema().catalogName(CATALOG).name(name(SCHEMA)).comment(COMMENT));

      TablesApi tables = new TablesApi(api);
      StagingTableInfo staging =
          tables.createStagingTable(
              new CreateStagingTable()
                  .catalogName(CATALOG)
                  .schemaName(name(SCHEMA))
                  .name(name(MANAGED_TABLE)));
      tables.createTable(
          table(MANAGED_TABLE, TableType.MANAGED, staging.getStagingLocation())
              .properties(Map.of("io.unitycatalog.tableId", staging.getId())));
      tables.createTable(
          table(EXTERNAL_TABLE, TableType.EXTERNAL, STORAGE + name(EXTERNAL_TABLE))
              .properties(PROPERTIES));

      new VolumesApi(api)
          .createVolume(
              new CreateVolumeRequestContent()
                  .catalogName(CATALOG)
                  .schemaName(name(SCHEMA))
                  .name(name(VOLUME))
                  .volumeType(VolumeType.EXTERNAL)
                  .storageLocation(STORAGE + name(VOLUME))
                  .comment(COMMENT));
      new FunctionsApi(api).createFunction(new CreateFunctionRequest().functionInfo(function()));

      new UsersApi(controlClient(api))
          .createUser(
              new UserResource()
                  .displayName("Upgrade reader")
                  .emails(List.of(new Email().value(USER).primary(true))));
      GrantsApi grants = new GrantsApi(api);
      for (Map.Entry<String, Grant> grant : GRANTS.entrySet()) {
        grants.update(
            grant.getValue().type(),
            grant.getKey(),
            new UpdatePermissions()
                .changes(
                    List.of(
                        new PermissionsChange()
                            .principal(USER)
                            .add(List.of(grant.getValue().privilege()))
                            .remove(List.of()))));
      }
    } catch (ApiException | io.unitycatalog.control.ApiException e) {
      throw new IllegalStateException("Could not write the data set", e);
    }
  }

  public static void assertReadBack(ApiClient api) {
    try {
      CatalogInfo catalog = new CatalogsApi(api).getCatalog(CATALOG);
      assertThat(catalog.getComment()).isEqualTo(COMMENT);
      assertThat(catalog.getProperties()).isEqualTo(PROPERTIES);
      assertThat(new SchemasApi(api).getSchema(SCHEMA).getComment()).isEqualTo(COMMENT);

      TablesApi tables = new TablesApi(api);
      TableInfo managed = tables.getTable(MANAGED_TABLE, null, null);
      assertThat(managed.getTableType()).isEqualTo(TableType.MANAGED);
      assertColumns(managed);
      TableInfo external = tables.getTable(EXTERNAL_TABLE, null, null);
      assertThat(external.getTableType()).isEqualTo(TableType.EXTERNAL);
      assertThat(external.getStorageLocation()).endsWith("/uc-storage/" + name(EXTERNAL_TABLE));
      assertThat(external.getProperties()).isEqualTo(PROPERTIES);
      assertColumns(external);

      VolumeInfo volume = new VolumesApi(api).getVolume(VOLUME);
      assertThat(volume.getVolumeType()).isEqualTo(VolumeType.EXTERNAL);
      assertThat(volume.getStorageLocation()).endsWith("/uc-storage/" + name(VOLUME));
      assertThat(volume.getComment()).isEqualTo(COMMENT);

      FunctionInfo function = new FunctionsApi(api).getFunction(FUNCTION);
      assertThat(function.getRoutineDefinition()).isEqualTo(function().getRoutineDefinition());
      assertThat(function.getInputParams().getParameters())
          .extracting(
              FunctionParameterInfo::getName,
              FunctionParameterInfo::getTypeText,
              FunctionParameterInfo::getTypeJson)
          .containsExactly(tuple("values", "array<int>", ARRAY_TYPE_JSON));

      GrantsApi grants = new GrantsApi(api);
      for (Map.Entry<String, Grant> grant : GRANTS.entrySet()) {
        assertThat(
                grants.get(grant.getValue().type(), grant.getKey(), USER).getPrivilegeAssignments())
            .as("grants on %s", grant.getKey())
            .extracting(PrivilegeAssignment::getPrincipal, PrivilegeAssignment::getPrivileges)
            .containsExactly(tuple(USER, List.of(grant.getValue().privilege())));
      }
    } catch (ApiException e) {
      throw new IllegalStateException("Could not read the data set back", e);
    }
  }

  private static CreateTable table(String fullName, TableType type, String location) {
    return new CreateTable()
        .catalogName(CATALOG)
        .schemaName(name(SCHEMA))
        .name(name(fullName))
        .tableType(type)
        .dataSourceFormat(DataSourceFormat.DELTA)
        .storageLocation(location)
        .comment(COMMENT)
        .columns(columns());
  }

  private static List<ColumnInfo> columns() {
    return List.of(
        new ColumnInfo()
            .name("id")
            .typeName(ColumnTypeName.LONG)
            .typeText("bigint")
            .typeJson("{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}")
            .position(0)
            .nullable(false),
        new ColumnInfo()
            .name("name")
            .typeName(ColumnTypeName.STRING)
            .typeText("string")
            .typeJson("{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}")
            .position(1)
            .nullable(true)
            .comment(COMMENT));
  }

  private static void assertColumns(TableInfo table) {
    assertThat(table.getColumns())
        .extracting(ColumnInfo::getName, ColumnInfo::getTypeText, ColumnInfo::getPosition)
        .containsExactly(tuple("id", "bigint", 0), tuple("name", "string", 1));
  }

  private static CreateFunction function() {
    return new CreateFunction()
        .catalogName(CATALOG)
        .schemaName(name(SCHEMA))
        .name(name(FUNCTION))
        .comment(COMMENT)
        .inputParams(
            new FunctionParameterInfos()
                .parameters(
                    List.of(
                        new FunctionParameterInfo()
                            .name("values")
                            .typeName(ColumnTypeName.ARRAY)
                            .typeText("array<int>")
                            .typeJson(ARRAY_TYPE_JSON)
                            .position(0))))
        .dataType(ColumnTypeName.INT)
        .fullDataType("int")
        .parameterStyle(CreateFunction.ParameterStyleEnum.S)
        .isDeterministic(true)
        .isNullCall(false)
        .routineBody(CreateFunction.RoutineBodyEnum.EXTERNAL)
        .routineDefinition("return len(values)")
        .externalLanguage("python")
        .securityType(CreateFunction.SecurityTypeEnum.DEFINER)
        .specificName(name(FUNCTION))
        .sqlDataAccess(CreateFunction.SqlDataAccessEnum.NO_SQL);
  }

  /** A control-API client for the same server and credentials as {@code api}. */
  private static io.unitycatalog.control.ApiClient controlClient(ApiClient api) {
    URI uri = URI.create(api.getBaseUri());
    io.unitycatalog.control.ApiClient control = new io.unitycatalog.control.ApiClient();
    control.setScheme(uri.getScheme());
    control.setHost(uri.getHost());
    control.setPort(uri.getPort());
    control.setRequestInterceptor(api.getRequestInterceptor());
    return control;
  }

  private static String name(String fullName) {
    return fullName.substring(fullName.lastIndexOf('.') + 1);
  }
}
