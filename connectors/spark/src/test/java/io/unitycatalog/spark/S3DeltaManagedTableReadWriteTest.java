package io.unitycatalog.spark;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

/**
 * This test suite starts UC server with managed storage root on emulated S3 path and exercise the
 * tests.
 */
public class S3ManagedTableReadWriteTest extends ManagedTableReadWriteTest {
  /**
   * This function provides a set of test parameters that cloud-aware tests should run for this
   * class.
   *
   * @return A stream of Arguments.of(String scheme, boolean renewCredEnabled)
   */
  protected static Stream<Arguments> cloudParameters() {
    return Stream.of(Arguments.of("s3", false), Arguments.of("s3", true));
  }

  @Override
  protected String managedStorageCloudScheme() {
    return "s3";
  }
}
