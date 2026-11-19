package io.unitycatalog.server.utils;

import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.TableType;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;

public final class IcebergViewTestUtils {
  private IcebergViewTestUtils() {}

  private static final String CAT = "view_catalog";
  private static final String NS = "view_schema";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));

  public static CreateTable nativeRequest(
      String catalog, String namespace, String name, String sql) {
    return new CreateTable()
        .catalogName(catalog)
        .schemaName(namespace)
        .name(name)
        .tableType(TableType.VIEW)
        .viewDefinition(sql)
        .columns(
            List.of(
                new ColumnInfo()
                    .name("id")
                    .position(0)
                    .nullable(true)
                    .typeName(ColumnTypeName.LONG)
                    .typeText("bigint")
                    .typeJson(
                        "{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}")));
  }

  public static CreateViewRequest request(String name, String dialect, String sql) {
    return ImmutableCreateViewRequest.builder()
        .name(name)
        .schema(SCHEMA)
        .viewVersion(
            ImmutableViewVersion.builder()
                .versionId(1)
                .schemaId(0)
                .timestampMillis(System.currentTimeMillis())
                .defaultCatalog(CAT)
                .defaultNamespace(Namespace.of(NS))
                .addRepresentations(
                    ImmutableSQLViewRepresentation.builder().dialect(dialect).sql(sql).build())
                .build())
        .build();
  }
}
