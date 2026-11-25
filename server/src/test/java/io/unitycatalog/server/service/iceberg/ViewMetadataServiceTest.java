package io.unitycatalog.server.service.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.utils.FileOperationsImpl;
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
  private final FileOperationsImpl files = new FileOperationsImpl(mock(), mock());

  @Test
  void rejectsUnsupportedCreateStateBeforeWritingMetadata() throws Exception {
    var request = IcebergViewTestUtils.request("v", "spark", "select 1 as id");
    var store = new ViewMetadataService(root.toString(), files);
    UUID id = UUID.randomUUID();
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
      assertThatThrownBy(() -> store.create(id, invalid)).isInstanceOf(BadRequestException.class);
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
      assertThatThrownBy(() -> store.create(id, invalid)).isInstanceOf(BadRequestException.class);
    }
    try (var entries = Files.list(root)) {
      assertThat(entries).isEmpty();
    }
    assertThatThrownBy(() -> new ViewMetadataService(null, files))
        .isInstanceOf(BaseException.class);
  }

  @Test
  void reloadPreservesTheSparkVersionAndSchemaFromTheCreateRequest() {
    Path directory = root.resolve("views with spaces");
    var store = new ViewMetadataService(directory.toString(), files);
    UUID id = UUID.randomUUID();
    String sql = "SELECT 1 AS id /* keep the engine SQL */";
    var original = IcebergViewTestUtils.request("v", "spark", sql);
    var version =
        ImmutableViewVersion.builder()
            .from(original.viewVersion())
            .putSummary("engine-version", "4.1")
            .build();
    var request =
        ImmutableCreateViewRequest.builder()
            .from(original)
            .viewVersion(version)
            .properties(Map.of("owner-tag", "science"))
            .build();
    var created = store.create(id, request);
    String file = created.metadataFileLocation();
    assertThat(Path.of(URI.create(file)).getParent()).isEqualTo(directory.resolve(id.toString()));
    var reopened =
        new ViewMetadataService(
            directory.toUri().toString().replace("views%20with", "%76iews%20with"), files);
    for (String pointer : List.of(file, file.replace("file:///", "file:/"))) {
      var loaded = reopened.read(id.toString(), pointer);
      assertThat(loaded.metadataFileLocation()).isEqualTo(pointer);
      assertThat(loaded.uuid()).isEqualTo(id.toString());
      assertThat(loaded.schema().sameSchema(request.schema())).isTrue();
      assertThat(loaded.currentVersion().summary()).isEqualTo(version.summary());
      assertThat(loaded.currentVersion().timestampMillis()).isEqualTo(version.timestampMillis());
      assertThat(loaded.currentVersion().defaultCatalog()).isEqualTo(version.defaultCatalog());
      assertThat(loaded.currentVersion().defaultNamespace()).isEqualTo(version.defaultNamespace());
      assertThat(ViewMetadataService.sparkSql(loaded.currentVersion())).isEqualTo(sql);
      assertThat(loaded.properties()).isEqualTo(request.properties());
      assertThat(loaded.history()).isEqualTo(created.history());
    }
    for (String pointer :
        List.of(
            file + "?ignored",
            file.replace(id + "/", id + "/../" + id + "/"),
            file.replace(".metadata.json", "%2f.metadata.json"),
            "s3://bucket/wrong.metadata.json")) {
      assertThatThrownBy(() -> reopened.read(id.toString(), pointer))
          .isInstanceOf(BaseException.class)
          .hasMessage("Invalid persisted view metadata location");
    }
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
  void rejectsLinkedViewDirectoryForCreateReadAndDiscard() throws Exception {
    var store = new ViewMetadataService(root.toString(), files);
    UUID id = UUID.randomUUID();
    var request = IcebergViewTestUtils.request("v", "spark", "select 1 as id");
    var metadata = store.create(id, request);
    Path file = Path.of(URI.create(metadata.metadataFileLocation()));
    Path original = Files.move(file.getParent(), root.resolve("original"));
    Files.createSymbolicLink(file.getParent(), original);

    assertThatThrownBy(() -> store.create(id, request))
        .isInstanceOf(BaseException.class)
        .hasMessage("Could not persist view metadata");
    assertThatThrownBy(() -> store.read(id.toString(), metadata.metadataFileLocation()))
        .isInstanceOf(BaseException.class)
        .hasMessage("Invalid persisted view metadata location");
    store.discardUnpublished(metadata);
    assertThat(file.getParent()).isSymbolicLink();
    try (var files = Files.list(original)) {
      assertThat(files).containsExactly(original.resolve(file.getFileName()));
    }
  }

  @Test
  void readsRequireARegularMetadataFile() throws Exception {
    var store = new ViewMetadataService(root.toString(), files);
    UUID id = UUID.randomUUID();
    Path directory = Files.createDirectory(root.resolve(id.toString()));
    Path target = Files.createTempFile(directory, "regular", ".json");
    Path link =
        Files.createSymbolicLink(directory.resolve(UUID.randomUUID() + ".metadata.json"), target);
    Path folder = Files.createDirectory(directory.resolve(UUID.randomUUID() + ".metadata.json"));
    for (Path path : List.of(link, folder)) {
      assertThatThrownBy(() -> store.read(id.toString(), path.toUri().toString()))
          .isInstanceOf(BaseException.class)
          .hasMessage("Invalid persisted view metadata location");
    }
  }

  @Test
  void loadRejectsMetadataOutsideTheViewOrWithADifferentIdentity() throws Exception {
    var store = new ViewMetadataService(root.toString(), files);
    UUID id = UUID.randomUUID();
    String uuid = id.toString();
    var metadata = store.create(id, IcebergViewTestUtils.request("v", "spark", "select 1 as id"));
    assertThat(store.read(uuid, metadata.metadataFileLocation()).uuid()).isEqualTo(uuid);
    assertThatThrownBy(
            () -> store.read(UUID.randomUUID().toString(), metadata.metadataFileLocation()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("location");
    Path file = Path.of(URI.create(metadata.metadataFileLocation()));
    String json = Files.readString(file);
    Files.writeString(file, json.replace(metadata.location(), metadata.location() + "/"));
    assertThat(store.read(uuid, metadata.metadataFileLocation()).uuid()).isEqualTo(uuid);
    Files.writeString(file, json.replace(metadata.location(), root.toUri().toString()));
    assertThatThrownBy(() -> store.read(uuid, metadata.metadataFileLocation()))
        .isInstanceOf(BaseException.class)
        .hasMessage("Persisted view metadata location mismatch");
    Files.writeString(file, json.replace(uuid, UUID.randomUUID().toString()));
    assertThatThrownBy(() -> store.read(uuid, metadata.metadataFileLocation()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("identity mismatch");
  }
}
