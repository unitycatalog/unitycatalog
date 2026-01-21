package io.unitycatalog.server.service.credential.azure;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

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
}
