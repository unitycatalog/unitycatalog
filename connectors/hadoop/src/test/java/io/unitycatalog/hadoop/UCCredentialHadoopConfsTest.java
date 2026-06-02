package io.unitycatalog.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
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
  void emptyAppVersionThrows() {
    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .addAppVersions(Map.of("Spark", "")))
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
  void missingTokenProviderThrowsForStagingTable() throws Exception {
    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .buildForStagingTable("staging-id", "s3://bucket/staging"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tokenProvider");
  }

  @Test
  void stagingTableBuildRejectsMissingFields() {
    TokenProvider tp = TokenProvider.create(Map.of("type", "static", "token", "tok"));

    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .tokenProvider(tp)
                    .buildForStagingTable(null, "s3://bucket/staging"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("stagingTableId is required");

    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .tokenProvider(tp)
                    .buildForStagingTable("", "s3://bucket/staging"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("stagingTableId is required");

    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .tokenProvider(tp)
                    .buildForStagingTable("staging-id", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("location is required");

    assertThatThrownBy(
            () ->
                UCCredentialHadoopConfs.builder("http://uc", "s3")
                    .tokenProvider(tp)
                    .buildForStagingTable("staging-id", ""))
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

      DeltaCredentialOperation deltaOp = DeltaCredentialOperation.fromValue(hadoopOp.value());
      assertThat(deltaOp.getValue()).isEqualTo(hadoopOp.value());
    }
  }
}
