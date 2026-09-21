package io.unitycatalog.spark;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.SimpleLocalFileIO;
import java.net.URI;
import java.util.stream.Stream;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.params.provider.Arguments;

/**
 * This test suite starts UC server with managed storage root on emulated S3 path and exercise the
 * tests.
 */
public class S3DeltaManagedTableReadWriteTest extends DeltaManagedTableReadWriteTest {
  /**
   * This function provides a set of test parameters that cloud-aware tests should run for this
   * class.
   *
   * @return A stream of Arguments.of(String scheme, boolean renewCredEnabled, boolean
   *     credScopedFsEnabled)
   */
  protected static Stream<Arguments> cloudParameters() {
    return Stream.of(
        Arguments.of("s3", false, false),
        Arguments.of("s3", true, false),
        Arguments.of("s3", false, true));
  }

  @Override
  protected String managedStorageCloudScheme() {
    return "s3";
  }

  @Override
  protected FileOperations decorateFileOperations(FileOperations fileOperations) {
    // The emulated S3 filesystem stores every S3 URI at its URI path on local disk. Give
    // server-side backfill verification the same view so these tests exercise HEADs without
    // contacting AWS.
    SimpleLocalFileIO localFileIO = new SimpleLocalFileIO();
    FileIO s3MappedFileIO = mock(FileIO.class);
    when(s3MappedFileIO.newInputFile(anyString()))
        .thenAnswer(
            invocation ->
                localFileIO.newInputFile(
                    URI.create(invocation.getArgument(0, String.class)).getPath()));
    FileOperations s3MappedFileOperations = mock(FileOperations.class, delegatesTo(fileOperations));
    doReturn(s3MappedFileIO).when(s3MappedFileOperations).getFileIO(any());
    return s3MappedFileOperations;
  }
}
