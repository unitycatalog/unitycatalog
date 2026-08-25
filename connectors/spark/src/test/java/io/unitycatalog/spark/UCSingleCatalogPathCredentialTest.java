package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs.PathOperation;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class UCSingleCatalogPathCredentialTest {

  @ParameterizedTest
  @ValueSource(ints = {400, 403, 404})
  void ambientFallbackIncludesUnmanagedAndPermissionStatuses(int code) {
    assertThat(
            UCSingleCatalog$.MODULE$.isAmbientPathCredentialFailure(new ApiException(code, "miss")))
        .isTrue();
  }

  @ParameterizedTest
  @ValueSource(ints = {401, 429, 500, 503})
  void ambientFallbackExcludesAuthOutageAndServerErrors(int code) {
    assertThat(
            UCSingleCatalog$.MODULE$.isAmbientPathCredentialFailure(new ApiException(code, "boom")))
        .isFalse();
  }

  @Test
  void pathIdentityUsesCanonicalS3SchemeWithoutSecrets() {
    Map<String, String> props =
        UCSingleCatalog$.MODULE$.pathCredentialIdentityProps(
            "s3a://bucket/dir", PathOperation.PATH_READ_WRITE);
    assertThat(props)
        .containsEntry(
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .containsEntry(UCHadoopConfConstants.UC_PATH_KEY, "s3://bucket/dir")
        .containsEntry(UCHadoopConfConstants.UC_PATH_OPERATION_KEY, "PATH_READ_WRITE")
        .doesNotContainKey("fs.s3a.access.key");
  }

  @Test
  void emptyIfAmbientReturnsEmptyForMissAndRethrowsServerError() {
    assertThat(
            UCSingleCatalog$.MODULE$.emptyIfAmbientPathCredentialFailure(
                new ApiException(404, "not found")))
        .isEmpty();
    ApiException boom = new ApiException(500, "internal");
    assertThatThrownBy(() -> UCSingleCatalog$.MODULE$.emptyIfAmbientPathCredentialFailure(boom))
        .isSameAs(boom);
  }
}
