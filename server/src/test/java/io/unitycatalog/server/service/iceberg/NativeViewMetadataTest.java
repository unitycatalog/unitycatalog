package io.unitycatalog.server.service.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.model.TableInfo;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class NativeViewMetadataTest {
  private TableInfo view(Map<String, String> properties) {
    return new TableInfo()
        .tableId(UUID.randomUUID().toString())
        .catalogName("catalog")
        .schemaName("schema")
        .createdAt(123L)
        .viewDefinition("select cast(1 as bigint) as id")
        .comment("a native view")
        .columns(
            IcebergSchemaConverter.toColumnInfos(
                new Schema(Types.NestedField.optional(1, "declared_id", Types.LongType.get()))))
        .properties(properties);
  }

  @Test
  void preservesIdentitySchemaAndCapturedContextWithoutChangingNativeProperties() {
    var properties =
        Map.of(
            "view.catalogAndNamespace.numParts",
            "2",
            "view.catalogAndNamespace.part.0",
            "creation_catalog",
            "view.catalogAndNamespace.part.1",
            "creation_schema",
            "view.query.out.numCols",
            "1",
            "view.query.out.col.0",
            "id",
            "team",
            "analytics");
    var info = view(properties);
    var metadata = NativeViewMetadata.from(info, "file:/views/");
    assertThat(metadata.uuid()).isEqualTo(info.getTableId());
    assertThat(metadata.currentVersion().defaultCatalog()).isEqualTo("creation_catalog");
    assertThat(metadata.currentVersion().defaultNamespace())
        .isEqualTo(Namespace.of("creation_schema"));
    assertThat(metadata.currentVersion().timestampMillis()).isEqualTo(123L);
    assertThat(metadata.history().get(0).timestampMillis()).isEqualTo(123L);
    assertThat(metadata.schema().findField("declared_id").type()).isEqualTo(Types.LongType.get());
    assertThat(metadata.currentVersion().representations()).hasSize(1);
    assertThat(ViewMetadataService.sparkSql(metadata.currentVersion()))
        .isEqualTo(info.getViewDefinition());
    assertThat(metadata.properties())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("team", "analytics", "comment", "a native view"));
    assertThat(info.getProperties()).isEqualTo(properties);
  }

  @Test
  void usesCanonicalContextByDefaultAndAllowsCatalogOnlyContext() {
    var info = view(Map.of());
    var metadata = NativeViewMetadata.from(info, "file:/views/");
    assertThat(metadata.currentVersion().defaultCatalog()).isEqualTo("catalog");
    assertThat(metadata.currentVersion().defaultNamespace()).isEqualTo(Namespace.of("schema"));
    info.setProperties(
        Map.of(
            "view.catalogAndNamespace.numParts",
            "1",
            "view.catalogAndNamespace.part.0",
            "creation_catalog"));
    info.setUpdatedAt(456L);
    metadata = NativeViewMetadata.from(info, "file:/views/");
    assertThat(metadata.currentVersion().defaultNamespace()).isEqualTo(Namespace.empty());
    assertThat(metadata.currentVersion().timestampMillis()).isEqualTo(456L);
  }

  @Test
  void rejectsUnrepresentableAndIncompleteNativeResolutionState() {
    for (var properties :
        List.of(
            Map.of("view.sqlConfig.spark.sql.caseSensitive", "true"),
            Map.of("view.schemaMode", "BINDING"),
            Map.of("view.catalogAndNamespace.numParts", "no"),
            Map.of("view.catalogAndNamespace.numParts", "0"),
            Map.of("view.catalogAndNamespace.numParts", "3"),
            Map.of(
                "view.catalogAndNamespace.numParts", "2", "view.catalogAndNamespace.part.0", "c"),
            Map.of(
                "view.catalogAndNamespace.numParts", "1", "view.catalogAndNamespace.part.0", " "),
            Map.of("view.catalogAndNamespace.part.0", "c"),
            Map.of("view.query.out.numCols", "0"),
            Map.of("view.query.out.numCols", "1"),
            Map.of("view.query.out.numCols", "one"),
            Map.of("view.query.out.col.0", "id"))) {
      assertThatThrownBy(() -> NativeViewMetadata.from(view(properties), "file:/views/"))
          .isInstanceOf(BadRequestException.class);
    }
    var info = view(Map.of());
    info.setViewDefinition(" ");
    assertThatThrownBy(() -> NativeViewMetadata.from(info, "file:/views/"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("SQL");
    info.setViewDefinition("select 1");
    info.setColumns(List.of());
    assertThatThrownBy(() -> NativeViewMetadata.from(info, "file:/views/"))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("output column");
  }
}
