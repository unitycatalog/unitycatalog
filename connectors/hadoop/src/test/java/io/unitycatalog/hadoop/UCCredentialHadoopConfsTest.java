package io.unitycatalog.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs.PathOperation;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs.TableOperation;
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
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tokenProvider");
  }

  @Test
  void missingTokenProviderThrowsForDeltaPath() throws Exception {
    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .buildForTable(
                        "catalog", "schema", "table", TableOperation.READ_WRITE, "s3://b/t"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tokenProvider");
  }

  @Test
  void unrecognizedSchemeReturnsEmptyWithoutFetchingCredentials() throws Exception {
    // Unroutable URL: any actual HTTP attempt would throw and fail the test.
    TokenProvider tp = TokenProvider.create(Map.of("type", "static", "token", "tok"));
    String unroutableUri = "http://127.0.0.1:1/uc";

    assertThat(
            UCCredentialHadoopConfs.builder(unroutableUri, "file")
                .tokenProvider(tp)
                .buildForTable("tid", TableOperation.READ))
        .isEmpty();
    assertThat(
            UCCredentialHadoopConfs.builder(unroutableUri, "file")
                .tokenProvider(tp)
                .buildForTable(
                    "catalog", "schema", "table", TableOperation.READ_WRITE, "file:///tmp/t"))
        .isEmpty();
    assertThat(
            UCCredentialHadoopConfs.builder(unroutableUri, "file")
                .tokenProvider(tp)
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

  @Test
  void pathOperationValuesRoundTripThroughSdkEnum() {
    // Hadoop enum strings must match SDK enum strings exactly — otherwise the UC
    // server rejects the request at runtime and no other test catches it.
    for (PathOperation hadoopOp : PathOperation.values()) {
      io.unitycatalog.client.model.PathOperation sdkOp =
          io.unitycatalog.client.model.PathOperation.fromValue(hadoopOp.value());
      assertThat(sdkOp.getValue()).isEqualTo(hadoopOp.value());
    }
  }

  @Test
  void tableOperationValuesRoundTripThroughBothSdkEnums() {
    // Hadoop TableOperation is used on both the REST and Delta paths, so it must
    // round-trip through both SDK enums.
    for (TableOperation hadoopOp : TableOperation.values()) {
      io.unitycatalog.client.model.TableOperation restOp =
          io.unitycatalog.client.model.TableOperation.fromValue(hadoopOp.value());
      assertThat(restOp.getValue()).isEqualTo(hadoopOp.value());

      io.unitycatalog.client.delta.model.CredentialOperation deltaOp =
          io.unitycatalog.client.delta.model.CredentialOperation.fromValue(hadoopOp.value());
      assertThat(deltaOp.getValue()).isEqualTo(hadoopOp.value());
    }
  }
}
