package io.unitycatalog.hadoop.internal.fs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.CredPropsUtil;
import io.unitycatalog.hadoop.internal.CredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import io.unitycatalog.hadoop.internal.id.TableCredId;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/** Round-trip tests for credential properties produced and consumed during filesystem setup. */
class CredPropsRoundTripTest {

  private static final String LOCATION = "s3://bucket/table";

  @BeforeEach
  @AfterEach
  void reset() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
    CredScopedFileSystem.clearCacheForTesting();
  }

  @Test
  void singleCredentialRoundTripsThroughCredScopedFileSystemAndProvider() throws Exception {
    AwsCredential credential =
        new AwsCredential("access-key", "secret-key", "session-token", null, LOCATION);
    CredPropsUtil.genericCredFetcherFactory = (apiClient, credId) -> () -> List.of(credential);

    Configuration initialConf = new Configuration(false);
    initialConf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);
    initialConf.set("fs.s3.impl", RawLocalFileSystem.class.getName());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            /* renewCredEnabled= */ true,
            /* credScopedFsEnabled= */ true,
            initialConf,
            "s3",
            /* apiClient= */ null,
            "http://uc",
            TokenProvider.create(Map.of("type", "static", "token", "token")),
            "table-id",
            UCCredentialHadoopConfs.TableOperation.READ,
            Map.of());

    Configuration confWithCreds = new Configuration(false);
    props.forEach(confWithCreds::set);
    CredScopedFileSystem fs = new CredScopedFileSystem();
    fs.initialize(new URI(LOCATION + "/part-0.parquet"), confWithCreds);

    Configuration delegateConf = fs.getDelegate().getConf();
    AwsVendedTokenProvider provider = new AwsVendedTokenProvider(delegateConf);
    AwsSessionCredentials resolved = (AwsSessionCredentials) provider.resolveCredentials();

    assertThat(provider.accessCredentials().prefix()).isEqualTo(LOCATION);
    assertThat(resolved.accessKeyId()).isEqualTo("access-key");
    assertThat(resolved.secretAccessKey()).isEqualTo("secret-key");
    assertThat(resolved.sessionToken()).isEqualTo("session-token");
  }

  @Test
  void multipleCredentialPrefixesRoundTripThroughCredScopedFileSystem() throws Exception {
    // TODO: Once CredPropsUtil can encode multi-creds, initialCredential should be a list
    // that matches the AwsVendedTokenProvider step at the end of the test.
    AwsCredential initialCredential =
        new AwsCredential("access-key", "secret-key", "session-token", null, LOCATION);
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> () -> List.of(initialCredential);

    Configuration initialConf = new Configuration(false);
    initialConf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);
    initialConf.set("fs.s3.impl", RawLocalFileSystem.class.getName());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            /* renewCredEnabled= */ true,
            /* credScopedFsEnabled= */ true,
            initialConf,
            "s3",
            /* apiClient= */ null,
            "http://uc",
            TokenProvider.create(Map.of("type", "static", "token", "token")),
            "table-id",
            UCCredentialHadoopConfs.TableOperation.READ,
            Map.of());

    Configuration confWithCreds = new Configuration(false);
    props.forEach(confWithCreds::set);

    // TODO: Delete the manual steps below when CredPropsBuilder supports producing
    // properties for multiple vended credentials. This is a temporary test
    // workaround until the encoder side changes land.
    // Unset the initialCredential that was set for multi-cred encodings.
    confWithCreds.unset(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY);
    confWithCreds.unset(UCHadoopConfConstants.S3A_INIT_SECRET_KEY);
    confWithCreds.unset(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN);
    // Encode the list of prefixes.
    confWithCreds.setStrings(
        UCHadoopConfConstants.UC_MULTI_CRED_PREFIXES_KEY,
        CredentialUtil.encodeMultiCredPrefixes(List.of("s3://bucket", LOCATION)));

    CredScopedFileSystem fs = new CredScopedFileSystem();
    fs.initialize(new URI(LOCATION + "/part-0.parquet"), confWithCreds);

    Configuration delegateConf = fs.getDelegate().getConf();
    assertThat(delegateConf.get(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY))
        .isEqualTo(LOCATION);
    assertThat(delegateConf.get(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY)).isNull();
    assertThat(delegateConf.get(UCHadoopConfConstants.S3A_INIT_SECRET_KEY)).isNull();
    assertThat(delegateConf.get(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN)).isNull();

    TableCredId expectedCredId =
        new TableCredId(
            delegateConf.get(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY), "table-id", "READ");
    expectedCredId
        .props()
        .forEach((key, value) -> assertThat(delegateConf.get(key)).isEqualTo(value));

    AwsCredential parent =
        new AwsCredential("parent-ak", "parent-sk", "parent-st", null, "s3://bucket");
    AwsCredential child = new AwsCredential("child-ak", "child-sk", "child-st", null, LOCATION);
    GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
    when(fetcher.createCredentials()).thenReturn(List.of(parent, child));

    try (MockedStatic<GenericCredentialFetcher> mockedFetcher =
        mockStatic(GenericCredentialFetcher.class)) {
      mockedFetcher.when(() -> GenericCredentialFetcher.create(delegateConf)).thenReturn(fetcher);

      AwsVendedTokenProvider provider = new AwsVendedTokenProvider(delegateConf);
      AwsSessionCredentials resolved = (AwsSessionCredentials) provider.resolveCredentials();

      assertThat(provider.accessCredentials().prefix()).isEqualTo(LOCATION);
      assertThat(resolved.accessKeyId()).isEqualTo("child-ak");
      assertThat(resolved.secretAccessKey()).isEqualTo("child-sk");
      assertThat(resolved.sessionToken()).isEqualTo("child-st");
    }
  }
}
