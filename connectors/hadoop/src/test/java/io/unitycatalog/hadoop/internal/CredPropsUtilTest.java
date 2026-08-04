package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.id.DeltaStagingTableCredId;
import io.unitycatalog.hadoop.internal.id.DeltaTableCredId;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/**
 * Covers {@link CredPropsUtil} helper behavior that is neither cloud-specific property emission
 * (see {@link CredPropsBaseTest} and its subclasses) nor credential caching (see {@link
 * CredPropsCacheTest}).
 */
class CredPropsUtilTest {

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
  void multipleVendedCredentialsAreRejected() {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            CredPropsBaseTest.mockGenericCredentialFetcher(
                new AwsCredential("ak1", "sk1", "st1", Long.MAX_VALUE, null),
                new AwsCredential("ak2", "sk2", "st2", Long.MAX_VALUE, null));

    assertThatThrownBy(
            () ->
                CredPropsUtil.createTableCredProps(
                    false,
                    false,
                    new Configuration(false),
                    "s3",
                    null,
                    "http://uc",
                    tokenProvider(),
                    "tid",
                    UCCredentialHadoopConfs.TableOperation.READ_WRITE,
                    Map.of()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Expected exactly one vended credential, got 2");
  }

  @Test
  void multipleDeltaCredentialsAreSelectedByLocation() {
    GenericCredential parent =
        new AwsCredential("parent-ak", "parent-sk", "parent-st", Long.MAX_VALUE, "s3://bucket");
    GenericCredential child =
        new AwsCredential("child-ak", "child-sk", "child-st", Long.MAX_VALUE, "s3://bucket/table");
    List<GenericCredential> credentials = List.of(parent, child);

    DeltaTableCredId tableCredId =
        new DeltaTableCredId(
            "context",
            UCDeltaTableIdentifier.of("catalog", "schema", "table"),
            "READ",
            "s3://bucket/table/child");
    DeltaStagingTableCredId stagingCredId =
        new DeltaStagingTableCredId("context", "staging-table-id", "s3://bucket/table/child");

    assertThat(CredPropsUtil.selectCredential(tableCredId, credentials)).isSameAs(child);
    assertThat(CredPropsUtil.selectCredential(stagingCredId, credentials)).isSameAs(child);
  }

  private static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }
}
