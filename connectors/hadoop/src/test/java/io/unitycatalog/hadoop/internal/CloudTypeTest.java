package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CloudTypeTest {

  @Test
  void fromSchemeResolvesSupportedSchemes() {
    assertThat(CloudType.fromScheme("s3")).contains(CloudType.S3);
    assertThat(CloudType.fromScheme("s3a")).contains(CloudType.S3);
    assertThat(CloudType.fromScheme("gs")).contains(CloudType.GCS);
    assertThat(CloudType.fromScheme("abfs")).contains(CloudType.ABFS);
    assertThat(CloudType.fromScheme("abfss")).contains(CloudType.ABFS);
  }

  @Test
  void canonicalSchemeIsTheFamilyDefault() {
    assertThat(CloudType.S3.canonicalScheme()).isEqualTo("s3");
    assertThat(CloudType.GCS.canonicalScheme()).isEqualTo("gs");
    assertThat(CloudType.ABFS.canonicalScheme()).isEqualTo("abfs");
  }

  @Test
  void fromSchemeReturnsEmptyForUnsupportedScheme() {
    assertThat(CloudType.fromScheme("s3n")).isEmpty(); // unsupported alias
    assertThat(CloudType.fromScheme("S3")).isEmpty(); // case-sensitive
    assertThat(CloudType.fromScheme("ABFSS")).isEmpty(); // case-sensitive
    assertThat(CloudType.fromScheme("hdfs")).isEmpty();
    assertThat(CloudType.fromScheme("file")).isEmpty();
    assertThat(CloudType.fromScheme("")).isEmpty();
    assertThat(CloudType.fromScheme(null)).isEmpty();
  }
}
