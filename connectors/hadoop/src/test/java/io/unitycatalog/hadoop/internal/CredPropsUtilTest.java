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

  // TODO: Remove this test once CredPropsBuilder supports multiple vended credentials.
  @Test
  void multipleDeltaCredentialsAreRejected() {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            CredPropsBaseTest.mockGenericCredentialFetcher(
                new AwsCredential(
                    "parent-ak", "parent-sk", "parent-st", Long.MAX_VALUE, "s3://bucket"),
                new AwsCredential(
                    "child-ak", "child-sk", "child-st", Long.MAX_VALUE, "s3://bucket/table"));

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
        .hasMessage("Only single credential responses are supported, got 2");
  }

  private static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }
}
