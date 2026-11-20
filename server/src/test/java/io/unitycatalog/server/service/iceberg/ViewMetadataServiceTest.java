package io.unitycatalog.server.service.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.utils.IcebergViewTestUtils;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewRepresentation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ViewMetadataServiceTest {
  @TempDir Path root;

  @Test
  void rejectsUnsupportedCreateStateBeforeWritingMetadata() throws Exception {
    var request = IcebergViewTestUtils.request("v", "spark", "select 1 as id");
    var store = new ViewMetadataService(root.toString());
    var info = new TableInfo().tableId(UUID.randomUUID().toString());
    var spark = request.viewVersion().representations().get(0);
    var trino = ImmutableSQLViewRepresentation.builder().dialect("trino").sql("select 1").build();
    for (List<ViewRepresentation> representations :
        List.<List<ViewRepresentation>>of(
            List.of(),
            List.of(trino),
            List.of(spark, spark),
            List.of(spark, trino),
            List.of(ImmutableSQLViewRepresentation.builder().dialect("spark").sql(" ").build()),
            List.of((ViewRepresentation) () -> "custom"))) {
      var invalid =
          ImmutableCreateViewRequest.builder()
              .from(request)
              .viewVersion(
                  ImmutableViewVersion.builder()
                      .from(request.viewVersion())
                      .representations(representations)
                      .build())
              .build();
      assertThatThrownBy(() -> store.create(info, invalid)).isInstanceOf(BadRequestException.class);
    }
    for (var invalid :
        List.of(
            ImmutableCreateViewRequest.builder().from(request).location("file:/untrusted").build(),
            ImmutableCreateViewRequest.builder()
                .from(request)
                .properties(Map.of("view.sqlConfig.x", "true"))
                .build(),
            ImmutableCreateViewRequest.builder().from(request).schema(new Schema()).build(),
            ImmutableCreateViewRequest.builder()
                .from(request)
                .viewVersion(
                    ImmutableViewVersion.builder()
                        .from(request.viewVersion())
                        .defaultNamespace(Namespace.of("a", "b"))
                        .build())
                .build())) {
      assertThatThrownBy(() -> store.create(info, invalid)).isInstanceOf(BadRequestException.class);
    }
    try (var entries = Files.list(root)) {
      assertThat(entries).isEmpty();
    }
    assertThatThrownBy(() -> new ViewMetadataService(null)).isInstanceOf(BaseException.class);
  }

  @Test
  void defaultsToDeclaredColumnNamesAndRejectsAmbiguousQueryNames() {
    var request = IcebergViewTestUtils.request("v", "spark", "select 1 as id");
    assertThat(ViewMetadataService.nativeProperties(request, "cat"))
        .containsEntry("view.query.out.col.0", "id");
    for (String queryNames : List.of("", "first,second")) {
      var invalid =
          ImmutableCreateViewRequest.builder()
              .from(request)
              .properties(Map.of("spark.query-column-names", queryNames))
              .build();
      assertThatThrownBy(() -> ViewMetadataService.nativeProperties(invalid, "cat"))
          .isInstanceOf(BadRequestException.class)
          .hasMessageContaining("spark.query-column-names");
    }
  }

  @Test
  void loadRejectsMetadataOutsideTheViewOrWithADifferentIdentity() throws Exception {
    var store = new ViewMetadataService(root.toString());
    String uuid = UUID.randomUUID().toString();
    var metadata =
        store.create(
            new TableInfo().tableId(uuid),
            IcebergViewTestUtils.request("v", "spark", "select 1 as id"));
    assertThat(store.read(uuid, metadata.metadataFileLocation()).uuid()).isEqualTo(uuid);
    assertThatThrownBy(
            () -> store.read(UUID.randomUUID().toString(), metadata.metadataFileLocation()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("location");
    Path file = Path.of(URI.create(metadata.metadataFileLocation()));
    Files.writeString(file, Files.readString(file).replace(uuid, UUID.randomUUID().toString()));
    assertThatThrownBy(() -> store.read(uuid, metadata.metadataFileLocation()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("identity mismatch");
  }
}
