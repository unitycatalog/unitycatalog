package io.unitycatalog.server.utils;

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
