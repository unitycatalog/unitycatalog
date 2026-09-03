package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
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
  void pathSkipMarkersUseCanonicalS3SchemeWithoutPathCredIdOrSecrets() {
    Map<String, String> props =
        UCSingleCatalog$.MODULE$.pathCredentialSkipProps("s3a://bucket/dir");
    assertThat(props)
        .containsEntry(
            UCHadoopConfConstants.UC_PATH_VENDING_ATTEMPTED_KEY,
            UCHadoopConfConstants.UC_PATH_VENDING_ATTEMPTED_VALUE)
        .containsEntry(UCHadoopConfConstants.UC_PATH_VENDING_LOCATION_KEY, "s3://bucket/dir")
        .doesNotContainKey(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY)
        .doesNotContainKey(UCHadoopConfConstants.UC_PATH_KEY)
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
