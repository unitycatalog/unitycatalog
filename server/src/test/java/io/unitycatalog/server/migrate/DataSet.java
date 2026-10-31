package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

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
import io.unitycatalog.client.model.FunctionParameterMode;
import io.unitycatalog.client.model.FunctionParameterType;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.PrivilegeAssignment;
import io.unitycatalog.client.model.SchemaInfo;
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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The data an upgrade must keep: a catalog, a schema, a managed and an external table with one
 * column of every type, a volume, a function with one parameter of every type and return
 * parameters, a user, and a grant to that user on each. {@link #write} creates it through any
 * server's API; {@link #assertReadBack} checks it through another's, as whole objects. It writes
 * only fields {@link SchemaMigrator#BASELINE_RELEASE} stores.
 *
 * <p>Each copy is named for the release that writes it ({@code v0.7.0} writes {@code v0_7_0}): its
 * catalog, its user's email and its storage directories, which are siblings of the other copies'.
 */
final class DataSet {

  private static final String STORAGE = "file:///tmp/uc-storage/";
  private static final String COMMENT = "kept across upgrades";
  private static final Map<String, String> PROPERTIES = Map.of("upgrade.key", "upgrade value");
  private static final String TABLE_ID_PROPERTY = "io.unitycatalog.tableId";

  /** The fields a server fills in itself: ids, names derived from names, owner and audit. */
  private static final String GENERATED = "id|.*Id|fullName|owner|(created|updated)(At|By)";

  /** Literal defaults for the parameters whose type has a simple literal. */
  private static final Map<ColumnTypeName, String> DEFAULTS =
      Map.of(
          ColumnTypeName.BOOLEAN, "true",
          ColumnTypeName.INT, "0",
          ColumnTypeName.LONG, "0",
          ColumnTypeName.DOUBLE, "0.0D",
          ColumnTypeName.DATE, "DATE'2026-01-01'",
          ColumnTypeName.STRING, "'none'",
          ColumnTypeName.DECIMAL, "0.00BD",
          ColumnTypeName.NULL, "NULL");

  private record Grant(SecurableType type, Privilege privilege) {}

  private final String copy;
  private final String catalog;
  private final String schema;
  private final String managedTable;
  private final String externalTable;
  private final String volume;
  private final String function;
  private final String user;

  /** The copy {@code release} (a git ref) writes. */
  DataSet(String release) {
    copy = release.replaceAll("\\W", "_");
    catalog = "upgrade_catalog_" + copy;
    schema = catalog + ".upgrade_schema";
    managedTable = schema + ".managed_table";
    externalTable = schema + ".external_table";
    volume = schema + ".upgrade_volume";
    function = schema + ".upgrade_function";
    user = "upgrade-reader-" + copy + "@example.com";
  }

  void write(ApiClient api) {
    try {
      new CatalogsApi(api).createCatalog(as(api, catalog(), CreateCatalog.class));
      new SchemasApi(api).createSchema(as(api, schema(), CreateSchema.class));

      TablesApi tables = new TablesApi(api);
      StagingTableInfo staging =
          tables.createStagingTable(
              new CreateStagingTable()
                  .catalogName(catalog)
                  .schemaName(name(schema))
                  .name(name(managedTable)));
      tables.createTable(
          as(
              api,
              managedTable(staging.getId()).storageLocation(staging.getStagingLocation()),
              CreateTable.class));
      tables.createTable(as(api, externalTable(), CreateTable.class));

      new VolumesApi(api).createVolume(as(api, volume(), CreateVolumeRequestContent.class));
      new FunctionsApi(api)
          .createFunction(
              new CreateFunctionRequest().functionInfo(as(api, function(), CreateFunction.class)));

      new UsersApi(controlClient(api))
          .createUser(
              new UserResource()
                  .displayName("Upgrade reader")
                  .emails(List.of(new Email().value(user).primary(true))));
      GrantsApi grants = new GrantsApi(api);
      for (Map.Entry<String, Grant> grant : grants().entrySet()) {
        grants.update(
            grant.getValue().type(),
            grant.getKey(),
            new UpdatePermissions()
                .changes(
                    List.of(
                        new PermissionsChange()
                            .principal(user)
                            .add(List.of(grant.getValue().privilege()))
                            .remove(List.of()))));
      }
    } catch (ApiException | io.unitycatalog.control.ApiException e) {
      throw new IllegalStateException("Could not write the data set", e);
    }
  }

  /** Checks the data {@link #write} wrote; failures name this copy. */
  void assertReadBack(ApiClient api) {
    try {
      assertKept(new CatalogsApi(api).getCatalog(catalog), catalog(), "catalog");
      assertKept(new SchemasApi(api).getSchema(schema), schema(), "schema");

      TablesApi tables = new TablesApi(api);
      TableInfo managed = tables.getTable(managedTable, null, null);
      assertListKept(managed.getColumns(), managedColumns(), "managed table columns");
      // Its location is the staging location the server chose.
      assertKept(managed, managedTable(managed.getTableId()), "managed table", "storageLocation");
      TableInfo external = tables.getTable(externalTable, null, null);
      assertListKept(external.getColumns(), externalColumns(), "external table columns");
      assertKept(external, externalTable(), "external table");

      assertKept(new VolumesApi(api).getVolume(volume), volume(), "volume");
      FunctionInfo function = new FunctionsApi(api).getFunction(this.function);
      assertListKept(
          function.getInputParams().getParameters(),
          function().getInputParams().getParameters(),
          "function parameters");
      assertListKept(
          function.getReturnParams().getParameters(),
          function().getReturnParams().getParameters(),
          "function return parameters");
      assertKept(function, function(), "function");

      GrantsApi grants = new GrantsApi(api);
      for (Map.Entry<String, Grant> grant : grants().entrySet()) {
        assertKept(
            grants.get(grant.getValue().type(), grant.getKey(), user).getPrivilegeAssignments(),
            List.of(
                new PrivilegeAssignment()
                    .principal(user)
                    .privileges(List.of(grant.getValue().privilege()))),
            "grants on " + grant.getKey());
      }
    } catch (ApiException e) {
      throw new IllegalStateException("Could not read the data set back", e);
    }
  }

  /** Columns or parameters, whole, in order: a short message for the one that changed. */
  private <T> void assertListKept(List<T> actual, List<T> expected, String what) {
    assertThat(actual).as("%s %s", what, copy).containsExactlyElementsOf(expected);
  }

  /**
   * Asserts {@code actual} equals {@code expected} field by field, but for the {@link #GENERATED}
   * ones and {@code alsoIgnored}.
   */
  private void assertKept(Object actual, Object expected, String what, String... alsoIgnored) {
    assertThat(actual)
        .as("%s %s", what, copy)
        .usingRecursiveComparison()
        .ignoringFieldsMatchingRegexes(GENERATED)
        .ignoringFields(alsoIgnored)
        .isEqualTo(expected);
  }

  /** {@code info}'s fields as the create request {@code type}. */
  private static <T> T as(ApiClient api, Object info, Class<T> type) {
    return api.getObjectMapper().convertValue(info, type);
  }

  /** The grant {@link #write} gives {@link #user} on each securable. */
  private Map<String, Grant> grants() {
    return Map.of(
        catalog, new Grant(SecurableType.CATALOG, Privilege.USE_CATALOG),
        schema, new Grant(SecurableType.SCHEMA, Privilege.USE_SCHEMA),
        managedTable, new Grant(SecurableType.TABLE, Privilege.SELECT),
        externalTable, new Grant(SecurableType.TABLE, Privilege.MODIFY),
        volume, new Grant(SecurableType.VOLUME, Privilege.READ_VOLUME),
        function, new Grant(SecurableType.FUNCTION, Privilege.EXECUTE));
  }

  private CatalogInfo catalog() {
    return new CatalogInfo().name(catalog).comment(COMMENT).properties(PROPERTIES);
  }

  private SchemaInfo schema() {
    return new SchemaInfo()
        .catalogName(catalog)
        .name(name(schema))
        .comment(COMMENT)
        .properties(PROPERTIES);
  }

  /** The managed Delta table, whose properties name its id as the staging table's. */
  private TableInfo managedTable(String tableId) {
    Map<String, String> properties = new HashMap<>(PROPERTIES);
    properties.put(TABLE_ID_PROPERTY, tableId);
    return table(managedTable, TableType.MANAGED, DataSourceFormat.DELTA, managedColumns())
        .properties(properties);
  }

  /** The external table: Parquet, so it can have every column type. */
  private TableInfo externalTable() {
    return table(externalTable, TableType.EXTERNAL, DataSourceFormat.PARQUET, externalColumns())
        .storageLocation(STORAGE + copy + "_" + name(externalTable))
        .properties(PROPERTIES);
  }

  private TableInfo table(
      String fullName, TableType type, DataSourceFormat format, List<ColumnInfo> columns) {
    return new TableInfo()
        .catalogName(catalog)
        .schemaName(name(schema))
        .name(name(fullName))
        .tableType(type)
        .dataSourceFormat(format)
        .comment(COMMENT)
        .columns(columns);
  }

  /** A column of every type that Delta has: the server refuses the others in a Delta table. */
  private static List<ColumnInfo> managedColumns() {
    Set<ColumnTypeName> notDelta =
        EnumSet.of(
            ColumnTypeName.INTERVAL,
            ColumnTypeName.NULL,
            ColumnTypeName.USER_DEFINED_TYPE,
            ColumnTypeName.TABLE_TYPE);
    return positioned(
        everyType().stream().filter(column -> !notDelta.contains(column.getTypeName())).toList());
  }

  /** A column of every type, partitioned by the date column. */
  private static List<ColumnInfo> externalColumns() {
    List<ColumnInfo> columns = positioned(everyType());
    columns.stream()
        .filter(column -> column.getTypeName() == ColumnTypeName.DATE)
        .forEach(column -> column.partitionIndex(0));
    return columns;
  }

  private static List<ColumnInfo> positioned(List<ColumnInfo> columns) {
    for (int i = 0; i < columns.size(); i++) {
      columns.get(i).position(i);
    }
    return columns;
  }

  /**
   * One column per {@link ColumnTypeName}, in Spark's catalog text and JSON field format, each with
   * a comment. The function's parameters have the same types, and the baseline release stores a
   * parameter's {@code type_json} in 255 characters, so the nested types stay short.
   */
  private static List<ColumnInfo> everyType() {
    return List.of(
        column(ColumnTypeName.BOOLEAN, "boolean", "\"boolean\""),
        column(ColumnTypeName.BYTE, "tinyint", "\"byte\""),
        column(ColumnTypeName.SHORT, "smallint", "\"short\""),
        column(ColumnTypeName.INT, "int", "\"integer\""),
        column(ColumnTypeName.LONG, "bigint", "\"long\"", false, "{}"),
        column(ColumnTypeName.FLOAT, "float", "\"float\""),
        column(ColumnTypeName.DOUBLE, "double", "\"double\""),
        column(ColumnTypeName.DATE, "date", "\"date\""),
        column(ColumnTypeName.TIMESTAMP, "timestamp", "\"timestamp\""),
        column(ColumnTypeName.TIMESTAMP_NTZ, "timestamp_ntz", "\"timestamp_ntz\""),
        column(ColumnTypeName.STRING, "string", "\"string\""),
        column(ColumnTypeName.BINARY, "binary", "\"binary\""),
        column(ColumnTypeName.DECIMAL, "decimal(10,2)", "\"decimal(10,2)\"")
            .typePrecision(10)
            .typeScale(2),
        column(ColumnTypeName.INTERVAL, "interval day to second", "\"interval day to second\"")
            .typeIntervalType("DAY TO SECOND"),
        column(
            ColumnTypeName.ARRAY,
            "array<struct<x:int>>",
            "{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":["
                + field("x", "\"integer\"")
                + "]},\"containsNull\":true}"),
        column(
            ColumnTypeName.STRUCT,
            "struct<x:bigint,y:array<string>>",
            "{\"type\":\"struct\",\"fields\":["
                + field("x", "\"long\"")
                + ","
                + field(
                    "y", "{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true}")
                + "]}"),
        column(
            ColumnTypeName.MAP,
            "map<string,array<int>>",
            "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":"
                + "{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true},"
                + "\"valueContainsNull\":true}"),
        column(ColumnTypeName.VARIANT, "variant", "\"variant\""),
        // Spark stores char(n) as a string with the declared type in the field's metadata.
        column(
            ColumnTypeName.CHAR,
            "char(10)",
            "\"string\"",
            true,
            "{\"__CHAR_VARCHAR_TYPE_STRING\":\"char(10)\"}"),
        column(ColumnTypeName.NULL, "void", "\"void\""),
        // A user-defined type is stored as its SQL type, as Spark writes it: the server does not
        // parse Spark's {"type":"udt",...} JSON.
        column(
            ColumnTypeName.USER_DEFINED_TYPE,
            "array<double>",
            "{\"type\":\"array\",\"elementType\":\"double\",\"containsNull\":false}"),
        column(
            ColumnTypeName.TABLE_TYPE,
            "table<id:bigint>",
            "{\"type\":\"struct\",\"fields\":[" + field("id", "\"long\"") + "]}"));
  }

  private static ColumnInfo column(ColumnTypeName type, String text, String sparkType) {
    return column(type, text, sparkType, true, "{}");
  }

  private static ColumnInfo column(
      ColumnTypeName type, String text, String sparkType, boolean nullable, String metadata) {
    String name = type.name().toLowerCase(Locale.ROOT) + "_col";
    return new ColumnInfo()
        .name(name)
        .typeName(type)
        .typeText(text)
        .typeJson(
            "{\"name\":\""
                + name
                + "\",\"type\":"
                + sparkType
                + ",\"nullable\":"
                + nullable
                + ",\"metadata\":"
                + metadata
                + "}")
        .nullable(nullable)
        .comment("the " + text + " column");
  }

  private static String field(String name, String sparkType) {
    return "{\"name\":\""
        + name
        + "\",\"type\":"
        + sparkType
        + ",\"nullable\":true,\"metadata\":{}}";
  }

  private VolumeInfo volume() {
    return new VolumeInfo()
        .catalogName(catalog)
        .schemaName(name(schema))
        .name(name(volume))
        .volumeType(VolumeType.EXTERNAL)
        .storageLocation(STORAGE + copy + "_" + name(volume))
        .comment(COMMENT);
  }

  /** A table function: one input parameter per column type, and two return columns. */
  private FunctionInfo function() {
    List<FunctionParameterInfo> inputs =
        positioned(everyType()).stream()
            .map(
                column ->
                    new FunctionParameterInfo()
                        .name(column.getName())
                        .typeName(column.getTypeName())
                        .typeText(column.getTypeText())
                        .typeJson(column.getTypeJson())
                        .typePrecision(column.getTypePrecision())
                        .typeScale(column.getTypeScale())
                        .typeIntervalType(column.getTypeIntervalType())
                        .position(column.getPosition())
                        .parameterMode(FunctionParameterMode.IN)
                        .parameterType(FunctionParameterType.PARAM)
                        .parameterDefault(DEFAULTS.get(column.getTypeName()))
                        .comment(column.getComment()))
            .toList();
    List<FunctionParameterInfo> returns =
        List.of(
            returnColumn("total", ColumnTypeName.LONG, "bigint", "\"long\"", 0),
            returnColumn("label", ColumnTypeName.STRING, "string", "\"string\"", 1));
    return new FunctionInfo()
        .catalogName(catalog)
        .schemaName(name(schema))
        .name(name(function))
        .comment(COMMENT)
        .inputParams(new FunctionParameterInfos().parameters(inputs))
        .returnParams(new FunctionParameterInfos().parameters(returns))
        .dataType(ColumnTypeName.TABLE_TYPE)
        .fullDataType("table<total:bigint,label:string>")
        .parameterStyle(FunctionInfo.ParameterStyleEnum.S)
        .isDeterministic(true)
        .isNullCall(false)
        .routineBody(FunctionInfo.RoutineBodyEnum.EXTERNAL)
        .routineDefinition("return [(len(string_col), string_col)]")
        .externalLanguage("python")
        .securityType(FunctionInfo.SecurityTypeEnum.DEFINER)
        .specificName(name(function))
        .sqlDataAccess(FunctionInfo.SqlDataAccessEnum.NO_SQL);
  }

  private static FunctionParameterInfo returnColumn(
      String name, ColumnTypeName type, String text, String sparkType, int position) {
    return new FunctionParameterInfo()
        .name(name)
        .typeName(type)
        .typeText(text)
        .typeJson(field(name, sparkType))
        .position(position)
        .parameterMode(FunctionParameterMode.IN)
        .parameterType(FunctionParameterType.COLUMN)
        .comment("the " + name + " column");
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
