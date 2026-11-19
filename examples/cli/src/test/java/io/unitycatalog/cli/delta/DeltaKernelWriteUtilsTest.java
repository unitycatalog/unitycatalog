package io.unitycatalog.cli.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests sample data writes on {@code s3a} paths, which {@code table write} uses for S3 tables. */
public class DeltaKernelWriteUtilsTest {
  private static final URI BUCKET = URI.create("s3a://test-bucket");
  private static final StructType SCHEMA = new StructType().add("id", IntegerType.INTEGER);

  /** Serves {@code s3a://test-bucket/<local path>} from the local file system. */
  public static class LocalS3aFileSystem extends RawLocalFileSystem {
    @Override
    public URI getUri() {
      return BUCKET;
    }

    @Override
    public File pathToFile(org.apache.hadoop.fs.Path path) {
      return super.pathToFile(new org.apache.hadoop.fs.Path(path.toUri().getPath()));
    }
  }

  @TempDir Path tempDir;
  private Engine engine;

  @BeforeEach
  public void setUp() {
    Configuration conf = new Configuration();
    conf.set("fs.s3a.impl", LocalS3aFileSystem.class.getName());
    conf.set("fs.s3a.impl.disable.cache", "true");
    engine = DefaultEngine.create(conf);
  }

  @Test
  public void testWriteAppendsToTableInitializedByTableCreate() throws IOException {
    String tablePath = s3aPath("created");
    // Like `bin/uc table create`, which commits an empty version 0 before any `table write`.
    TableManager.buildCreateTableTransaction(tablePath, SCHEMA, "test")
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable());

    DeltaKernelWriteUtils.writeSampleDataToExistingDeltaTable(engine, tablePath, SCHEMA);

    assertSampleRowsAtVersion(tablePath, 1);
  }

  @Test
  public void testWriteCreatesTableInDirectoryWithoutDeltaLog() throws IOException {
    Files.createDirectory(tempDir.resolve("empty"));
    String tablePath = s3aPath("empty");

    DeltaKernelWriteUtils.writeSampleDataToExistingDeltaTable(engine, tablePath, SCHEMA);

    assertSampleRowsAtVersion(tablePath, 0);
  }

  private String s3aPath(String name) {
    return BUCKET + tempDir.resolve(name).toUri().getPath();
  }

  private void assertSampleRowsAtVersion(String tablePath, long version) throws IOException {
    Snapshot snapshot = Table.forPath(engine, tablePath).getLatestSnapshot(engine);
    assertThat(snapshot.getVersion()).isEqualTo(version);
    assertThat(snapshot.getSchema()).isEqualTo(SCHEMA);
    assertThat(
            DeltaKernelReadUtils.readData(
                    engine, SCHEMA, snapshot.getScanBuilder().withReadSchema(SCHEMA).build(), 100)
                .stream()
                .map(row -> row.getInt(0)))
        .containsExactlyInAnyOrderElementsOf(IntStream.rangeClosed(1, 15).boxed().toList());
  }
}
