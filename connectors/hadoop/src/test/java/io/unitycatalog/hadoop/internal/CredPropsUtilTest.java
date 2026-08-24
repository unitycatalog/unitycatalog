package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.GcsCredential;
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
        .hasMessageContaining("No vended credential matched storage scheme 's3'");
  }

  @Test
  void icebergPlanPropsCarryRenewalIdentityToExecutors() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            () ->
                java.util.List.of(
                    new AwsCredential("ak", "sk", "st", 4_102_444_800_000L, "s3://bucket/t"));

    Map<String, String> props =
        CredPropsUtil.createIcebergPlanCredProps(
            true,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "http://uc/iceberg/v1/ns/t/credentials",
            "plan-1",
            Map.of("Delta", "4.2.0"));

    assertThat(props)
        .containsEntry(
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_ICEBERG_PLAN_VALUE)
        .containsEntry(
            UCHadoopConfConstants.UC_ICEBERG_CREDENTIALS_ENDPOINT_KEY,
            "http://uc/iceberg/v1/ns/t/credentials")
        .containsEntry(UCHadoopConfConstants.UC_ICEBERG_PLAN_ID_KEY, "plan-1")
        .containsEntry(UCHadoopConfConstants.UC_AUTH_TYPE, "static")
        .containsEntry(UCHadoopConfConstants.UC_AUTH_TOKEN_KEY, "tok")
        .containsEntry(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + "Delta", "4.2.0");

    Configuration serialized = new Configuration(false);
    props.forEach(serialized::set);
    assertThat(io.unitycatalog.hadoop.internal.id.CredId.create(serialized))
        .isInstanceOf(io.unitycatalog.hadoop.internal.id.IcebergPlanCredId.class);
  }

  @Test
  void icebergPlanPropsKeepOnlyCredentialsForRequestedCloud() throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            () ->
                java.util.List.of(
                    new AwsCredential("ak", "sk", "st", 4_102_444_800_000L, "s3://bucket/t"),
                    new GcsCredential("oauth", 4_102_444_800_000L, "gs://bucket/t"));

    Map<String, String> props =
        CredPropsUtil.createIcebergPlanCredProps(
            false,
            false,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            "http://uc/iceberg/v1/ns/t/credentials",
            "plan-1",
            Map.of());

    assertThat(props)
        .containsEntry("fs.s3a.access.key", "ak")
        .doesNotContainKey("fs.gs.auth.access.token.credential");
    assertThat(
            CredentialUtil.decodeCredPrefixes(
                props.get(UCHadoopConfConstants.UC_CREDENTIAL_PREFIXES_KEY).split(",")))
        .containsExactly("s3://bucket/t");
  }

  private static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }
}
