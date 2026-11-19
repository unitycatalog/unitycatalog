package io.unitycatalog.server.service.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mockStatic;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.utils.IcebergViewTestUtils;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewRepresentation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class ViewMetadataServiceTest {
  @TempDir Path root;

  private TableInfo nativeView() {
    var request = IcebergViewTestUtils.request("v", "spark", "select 1 as id");
    return new TableInfo()
        .tableId(UUID.randomUUID().toString())
        .catalogName("catalog")
        .schemaName("schema")
        .createdAt(123L)
        .viewDefinition("select 1 as id")
        .columns(IcebergSchemaConverter.toColumnInfos(request.schema()))
        .properties(Map.of());
  }

  @Test
  void nativeSnapshotsAreStableRebuildableAndRejectCorruption() throws Exception {
    var store = new ViewMetadataService(root.toString());
    var info = nativeView();
    var first = store.readNative(info);
    Path file = Path.of(URI.create(first.metadataFileLocation()));
    String bytes = Files.readString(file);
    assertThat(store.readNative(info).metadataFileLocation())
        .isEqualTo(first.metadataFileLocation());
    Files.delete(file);
    assertThat(store.readNative(info).metadataFileLocation())
        .isEqualTo(first.metadataFileLocation());
    assertThat(Files.readString(file)).isEqualTo(bytes);
    Files.writeString(file, bytes.replace("select 1 as id", "select 2 as id"));
    assertThatThrownBy(() -> store.readNative(info))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("inconsistent");
    Files.writeString(file, "{");
    assertThatThrownBy(() -> store.readNative(info))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("materialize");
    info.setViewDefinition("select 3 as id");
    assertThat(store.readNative(info).metadataFileLocation())
        .isNotEqualTo(first.metadataFileLocation());
  }

  @Test
  void concurrentNativeReadersPublishOneCompleteSnapshot() throws Exception {
    var store = new ViewMetadataService(root.toString());
    var info = nativeView();
    info.setViewDefinition(info.getViewDefinition() + " /*" + "x".repeat(64_000) + "*/");
    var pool = Executors.newFixedThreadPool(6);
    var start = new CyclicBarrier(6);
    try {
      var futures = new ArrayList<java.util.concurrent.Future<String>>();
      for (int i = 0; i < 6; i++) {
        futures.add(
            pool.submit(
                () -> {
                  start.await(10, TimeUnit.SECONDS);
                  var metadata = store.readNative(info);
                  assertThat(ViewMetadataService.sparkSql(metadata.currentVersion()))
                      .isEqualTo(info.getViewDefinition());
                  return metadata.metadataFileLocation();
                }));
      }
      var locations = new java.util.HashSet<String>();
      for (var future : futures) {
        locations.add(future.get(30, TimeUnit.SECONDS));
      }
      assertThat(locations).hasSize(1);
      try (var files = Files.list(root.resolve(info.getTableId()))) {
        assertThat(files.map(p -> p.getFileName().toString()).toList())
            .hasSize(1)
            .allMatch(name -> name.endsWith(".metadata.json"));
      }
    } finally {
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void nativeReadAcceptsASnapshotPublishedByAnotherReaderBeforeAtomicRename() throws Exception {
    var store = new ViewMetadataService(root.toString());
    var info = nativeView();
    try (MockedStatic<Files> ignored =
        mockStatic(
            Files.class,
            call -> {
              if (call.getMethod().getName().equals("move")) {
                Path temporary = call.getArgument(0);
                Path destination = call.getArgument(1);
                Files.copy(temporary, destination);
                throw new FileAlreadyExistsException(destination.toString());
              }
              return call.callRealMethod();
            })) {
      assertThat(store.readNative(info).uuid()).isEqualTo(info.getTableId());
    }
    try (var files = Files.list(root.resolve(info.getTableId()))) {
      assertThat(files.map(p -> p.getFileName().toString()).toList())
          .hasSize(1)
          .allMatch(name -> name.endsWith(".metadata.json"));
    }
  }

  @Test
  void nativeReadsRejectUnsafeFilesAndReportStorageFailures() throws Exception {
    var info = nativeView();
    var store = new ViewMetadataService(root.toString());
    Path directory = root.resolve(info.getTableId());
    Path elsewhere = Files.createTempDirectory(root, "elsewhere");
    Files.createSymbolicLink(directory, elsewhere);
    assertThatThrownBy(() -> store.readNative(info))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("materialize");
    Files.delete(directory);
    var snapshot = store.readNative(info);
    Path file = Path.of(URI.create(snapshot.metadataFileLocation()));
    Path linked = root.resolve("other.metadata.json");
    Files.move(file, linked);
    Files.createSymbolicLink(file, linked);
    assertThatThrownBy(() -> store.readNative(info))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("location");
    Files.delete(file);
    Files.delete(directory);
    Files.writeString(directory, "not a directory");
    assertThatThrownBy(() -> store.readNative(info))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("materialize");
  }

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
