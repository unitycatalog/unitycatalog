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

class FileSystemCredIdTest {

  private static final URI TEST_URI = URI.create("s3://bucket/a");

  private static FileSystemCredId createId(String prefix) {
    Configuration conf = new Configuration(false);
    return FileSystemCredId.create(conf, TEST_URI, prefix);
  }

  @Test
  void uriFallbackKeysBySchemeAndAuthority() {
    Configuration conf = new Configuration(false);

    assertThat(FileSystemCredId.create(conf, URI.create("s3://bucket/a"), null))
        .isEqualTo(FileSystemCredId.create(conf, URI.create("s3://bucket/b"), null))
        .isNotEqualTo(FileSystemCredId.create(conf, URI.create("s3://other/a"), null));
  }

  @Test
  void createUsesProvidedPrefix() {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, "s3://bucket/top-level");

    assertThat(FileSystemCredId.create(conf, TEST_URI, "s3://bucket/selected").prefix())
        .isEqualTo("s3://bucket/selected");
  }

  @Test
  void createWithProvidedPrefixFallsBackToUriIdentity() {
    Configuration conf = new Configuration(false);
    String prefix = "s3://bucket/selected";

    assertThat(FileSystemCredId.create(conf, URI.create("s3://bucket/a"), prefix))
        .isEqualTo(FileSystemCredId.create(conf, URI.create("s3://bucket/b"), prefix))
        .isNotEqualTo(FileSystemCredId.create(conf, URI.create("s3://other/a"), prefix));
  }

  @Test
  void equivalentPrefixesAreEqualAndHaveSameHashCode() {
    FileSystemCredId idA = createId("s3://bucket/a");
    FileSystemCredId idB = createId("s3://bucket/a");

    assertThat(idA).isEqualTo(idB).hasSameHashCodeAs(idB);
  }

  @Test
  void samePrefixWithDifferentUriFallbackCredIdsIsDifferent() {
    Configuration conf = new Configuration(false);
    String prefix = "s3://bucket/shared";

    assertThat(FileSystemCredId.create(conf, URI.create("s3://bucket-a/path"), prefix))
        .isNotEqualTo(FileSystemCredId.create(conf, URI.create("s3://bucket-b/path"), prefix));
  }

  @ParameterizedTest
  @MethodSource("differentPrefixes")
  void differentPrefixesAreDifferent(String prefixA, String prefixB) {
    assertThat(createId(prefixA)).isNotEqualTo(createId(prefixB));
  }

  private static Stream<Arguments> differentPrefixes() {
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
