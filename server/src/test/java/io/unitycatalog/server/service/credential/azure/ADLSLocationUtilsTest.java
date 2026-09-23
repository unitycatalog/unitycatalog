package io.unitycatalog.server.service.credential.azure;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.NormalizedURL;
import org.junit.jupiter.api.Test;

public class ADLSLocationUtilsTest {

  private static final String TEST_CONTAINER = "testcontainer";
  private static final String TEST_STORAGE_ACCOUNT = "teststorage";

  @Test
  public void testContainerInAuthority() {
    NormalizedURL location =
        NormalizedURL.from(
            format(
                "abfs://%s@%s.dfs.core.windows.net/path/to/files",
                TEST_CONTAINER, TEST_STORAGE_ACCOUNT));

    ADLSLocationUtils.ADLSLocationParts parts = ADLSLocationUtils.parseLocation(location);

    assertThat(parts.container()).isEqualTo(TEST_CONTAINER);
    assertThat(parts.accountName()).isEqualTo(TEST_STORAGE_ACCOUNT);
    assertThat(parts.account()).isEqualTo(format("%s.dfs.core.windows.net", TEST_STORAGE_ACCOUNT));
    assertThat(parts.scheme()).isEqualTo("abfs");
  }

  @Test
  public void testContainerNotInAuthority() {
    NormalizedURL location =
        NormalizedURL.from(
            format("abfs://%s.dfs.core.windows.net/path/to/files", TEST_STORAGE_ACCOUNT));

    ADLSLocationUtils.ADLSLocationParts parts = ADLSLocationUtils.parseLocation(location);

    assertThat(parts.container()).isNull();
    assertThat(parts.accountName()).isEqualTo(TEST_STORAGE_ACCOUNT);
    assertThat(parts.account()).isEqualTo(format("%s.dfs.core.windows.net", TEST_STORAGE_ACCOUNT));
    assertThat(parts.scheme()).isEqualTo("abfs");
  }

  @Test
  public void testMissingAuthorityRejected() {
    // An abfs URL with no authority (e.g. "abfs:///path") is accepted by NormalizedURL but has a
    // null URI authority. parseLocation must reject it with a clean INVALID_ARGUMENT rather than
    // throwing a raw NullPointerException.
    NormalizedURL location = NormalizedURL.from("abfs:///path/to/files");

    assertThatThrownBy(() -> ADLSLocationUtils.parseLocation(location))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("missing authority")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INVALID_ARGUMENT);
  }
}
