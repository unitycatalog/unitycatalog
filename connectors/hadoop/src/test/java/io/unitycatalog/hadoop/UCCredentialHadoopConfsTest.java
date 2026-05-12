package io.unitycatalog.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.auth.TokenProvider;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link UCCredentialHadoopConfs} builder validation. Credential-to-props mapping is
 * tested in {@link io.unitycatalog.hadoop.internal.CredPropsUtilTest}.
 */
class UCCredentialHadoopConfsTest {

  @Test
  void missingCatalogUriThrows() {
    assertThatThrownBy(() -> UCCredentialHadoopConfs.builder(null, "s3"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("catalogUri");
  }

  @Test
  void missingSchemeThrows() {
    assertThatThrownBy(() -> UCCredentialHadoopConfs.builder("http://uc", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("scheme");
  }

  @Test
  void missingTokenProviderThrows() throws Exception {
    // No tokenProvider → always required to fetch credentials from the UC API.
    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .buildForTable("tid", TableOperation.READ))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("tokenProvider");
  }

  @Test
  void missingTokenProviderThrowsForDeltaPath() throws Exception {
    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .buildForTable(
                        "catalog", "schema", "table", TableOperation.READ_WRITE, "s3://b/t"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("tokenProvider");
  }

  @Test
  void unrecognizedSchemeReturnsEmptyWithoutFetchingCredentials() throws Exception {
    assertThat(
            UCCredentialHadoopConfs.builder("http://uc", "file")
                .buildForTable("tid", TableOperation.READ))
        .isEmpty();
    assertThat(
            UCCredentialHadoopConfs.builder("http://uc", "file")
                .buildForTable(
                    "catalog", "schema", "table", TableOperation.READ_WRITE, "file:///tmp/t"))
        .isEmpty();
    assertThat(
            UCCredentialHadoopConfs.builder("http://uc", "file")
                .buildForPath("file:///tmp/t", PathOperation.PATH_CREATE_TABLE))
        .isEmpty();
  }

  @Test
  void emptyEngineVersionThrows() {
    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .addEngineVersions(Map.of("Spark", "")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Spark");
  }

  @Test
  void deltaTableBuildRejectsMissingFields() {
    TokenProvider tp = TokenProvider.create(Map.of("type", "static", "token", "tok"));

    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .tokenProvider(tp)
                    .buildForTable(null, "schema", "table", TableOperation.READ_WRITE, "s3://b/t"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("catalog is required");

    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .tokenProvider(tp)
                    .buildForTable("catalog", null, "table", TableOperation.READ_WRITE, "s3://b/t"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("schema is required");

    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .tokenProvider(tp)
                    .buildForTable(
                        "catalog", "schema", null, TableOperation.READ_WRITE, "s3://b/t"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("table is required");

    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .tokenProvider(tp)
                    .buildForTable("catalog", "schema", "table", null, "s3://b/t"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("operation is required");

    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .tokenProvider(tp)
                    .buildForTable("catalog", "schema", "table", TableOperation.READ_WRITE, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("location is required");
  }
}
