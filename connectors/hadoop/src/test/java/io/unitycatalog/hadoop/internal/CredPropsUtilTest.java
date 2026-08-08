package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
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

    assertThatThrownBy(() -> createTableCredProps(true, true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Initial credentials cannot be null or empty");
  }

  @Test
  void invalidMultipleCredentialConfigurationsAreRejected() {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            CredPropsBaseTest.mockGenericCredentialFetcher(
                s3CredAt("1", "s3://bucket/a"), s3CredAt("2", ""), s3CredAt("3", null));

    assertThatThrownBy(() -> createTableCredProps(true, false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("3 credentials were vended but the credential-scoped filesystem");

    assertThatThrownBy(() -> createTableCredProps(false, true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("credential renewal is disabled");

    assertThatThrownBy(() -> createTableCredProps(true, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Credential prefixes cannot be null or empty when multiple credentials are vended");
  }

  private static Map<String, String> createTableCredProps(
      boolean renewCredEnabled, boolean credScopedFsEnabled) throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);
    return CredPropsUtil.createTableCredProps(
        renewCredEnabled,
        credScopedFsEnabled,
        conf,
        "s3",
        null,
        "http://uc",
        tokenProvider(),
        "tid",
        UCCredentialHadoopConfs.TableOperation.READ_WRITE,
        Map.of());
  }

  private static AwsCredential s3CredAt(String id, String location) {
    return new AwsCredential("ak" + id, "sk" + id, "st" + id, null, location);
  }

  private static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }
}
