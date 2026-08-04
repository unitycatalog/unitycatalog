package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;

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

  // TODO: This test will be deleted and moved to CredPropsBaseTest when multiple vended creds are
  // supported.
  @Test
  void multipleDeltaCredentialsAreSelectedByLocation() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            CredPropsBaseTest.mockGenericCredentialFetcher(
                new AwsCredential(
                    "parent-ak", "parent-sk", "parent-st", Long.MAX_VALUE, "s3://bucket"),
                new AwsCredential(
                    "child-ak", "child-sk", "child-st", Long.MAX_VALUE, "s3://bucket/table"));

    Map<String, String> tableProps =
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
            Map.of());
    Map<String, String> stagingProps =
        CredPropsUtil.createDeltaStagingTableCredProps(
            false,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "staging-table-id",
            "s3://bucket/table/child",
            Map.of());

    assertThat(tableProps).containsEntry("fs.s3a.access.key", "child-ak");
    assertThat(stagingProps).containsEntry("fs.s3a.access.key", "child-ak");
  }

  private static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }
}
