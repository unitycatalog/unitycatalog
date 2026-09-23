package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Covers {@link CredPropsUtil} helper behavior that is neither cloud-specific property emission
 * (see {@link CredPropsBaseTest} and its subclasses) nor credential caching (see {@link
 * CredPropsCacheTest}).
 */
class CredPropsUtilTest {

  @AfterEach
  void resetState() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
    CredPropsUtil.initialCredCache.clear();
  }

  @Test
  void credContextIdVariesByCatalogUriSchemeAndAuth() {
    TokenProvider auth = tokenProvider();
    String base = CredPropsUtil.credContextId("http://uc", "s3", auth);

    assertThat(CredPropsUtil.credContextId("http://uc", "s3", auth)).isEqualTo(base);
    assertThat(CredPropsUtil.credContextId("http://uc-2", "s3", auth)).isNotEqualTo(base);
    assertThat(CredPropsUtil.credContextId("http://uc", "gs", auth)).isNotEqualTo(base);
    assertThat(
            CredPropsUtil.credContextId(
                "http://uc", "s3", TokenProvider.create(Map.of("type", "static", "token", "x"))))
        .isNotEqualTo(base);
  }

  @Test
  void noVendedCredentialsIsRejected() {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> CredPropsBaseTest.mockGenericCredentialFetcher();

    assertThatThrownBy(
            () ->
                CredPropsUtil.createDeltaTableCredProps(
                    false,
                    false,
                    new Configuration(false),
                    "s3",
                    null,
                    "http://uc",
                    tokenProvider(),
                    UCDeltaTableIdentifier.of("catalog", "schema", "table"),
                    "s3://bucket/table/child",
                    UCCredentialHadoopConfs.TableOperation.READ,
                    Map.of()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Initial credentials cannot be null or empty");
  }

  @Test
  void pathCredPropsAlwaysIncludePathIdentityKeys() throws Exception {
    String path = "s3://bucket/data";
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            CredPropsBaseTest.mockGenericCredentialFetcher(
                new io.unitycatalog.hadoop.internal.auth.AwsCredential(
                    "ak", "sk", "st", null, path));

    for (boolean renew : new boolean[] {false, true}) {
      Map<String, String> props =
          CredPropsUtil.createPathCredProps(
              renew,
              false,
              new Configuration(false),
              "s3",
              null,
              "http://uc",
              tokenProvider(),
              path,
              UCCredentialHadoopConfs.PathOperation.PATH_READ,
              Map.of());
      assertThat(props)
          .as("renew=%s", renew)
          .containsEntry(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY, "path")
          .containsEntry(UCHadoopConfConstants.UC_PATH_KEY, path)
          .containsEntry(UCHadoopConfConstants.UC_PATH_OPERATION_KEY, "PATH_READ");
    }
  }

  private static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }
}
