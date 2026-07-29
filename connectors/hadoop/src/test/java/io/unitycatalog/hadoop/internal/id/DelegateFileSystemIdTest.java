package io.unitycatalog.hadoop.internal.id;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.net.URI;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DelegateFileSystemIdTest {

  private static final URI TEST_URI = URI.create("s3://bucket/a");

  private static DelegateFileSystemId createId(String location) {
    Configuration conf = new Configuration(false);
    if (location != null) {
      conf.set(UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY, location);
    }
    return DelegateFileSystemId.create(conf, TEST_URI);
  }

  @Test
  void uriFallbackKeysBySchemeAndAuthority() {
    Configuration conf = new Configuration(false);

    assertThat(DelegateFileSystemId.create(conf, URI.create("s3://bucket/a")))
        .isEqualTo(DelegateFileSystemId.create(conf, URI.create("s3://bucket/b")))
        .isNotEqualTo(DelegateFileSystemId.create(conf, URI.create("s3://other/a")));
  }

  @ParameterizedTest
  @MethodSource("equivalentLocations")
  void equivalentLocationsAreEqualAndHaveSameHashCode(String locationA, String locationB) {
    DelegateFileSystemId idA = createId(locationA);
    DelegateFileSystemId idB = createId(locationB);

    assertThat(idA).isEqualTo(idB).hasSameHashCodeAs(idB);
  }

  private static Stream<Arguments> equivalentLocations() {
    return Stream.of(Arguments.of("s3://bucket/a", "s3://bucket/a"));
  }

  @ParameterizedTest
  @MethodSource("differentLocations")
  void differentLocationsAreDifferent(String locationA, String locationB) {
    assertThat(createId(locationA)).isNotEqualTo(createId(locationB));
  }

  private static Stream<Arguments> differentLocations() {
    return Stream.of(
        Arguments.of("s3://bucket/a", "s3://bucket/b"),
        Arguments.of("s3://bucket/a", "s3://bucket/a///"),
        Arguments.of("gs://bucket/a", "gs://bucket/a///"),
        Arguments.of("abfs://container@account/a", "abfs://container@account/a///"),
        Arguments.of("abfss://container@account/a", "abfss://container@account/a///"),
        Arguments.of("s3://bucket/a", "S3A://bucket/a///"),
        Arguments.of("abfs://container@account/a", "abfss://container@account/a"),
        Arguments.of(null, "s3://bucket/a"));
  }
}
