package io.unitycatalog.hadoop.internal.fs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
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

  private static final String CATALOG_URI = "http://uc";
  private static final String SCHEME = "s3";
  private static final String TABLE_ID = "tid-1";
  private static final String LOCATION = "s3://bucket/table";
  private static final String LOCATION_A = "s3://bucket/shared-prefix/location-a";
  private static final String LOCATION_B = "s3://bucket/shared-prefix/location-b";

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
    AwsCredential parent =
        new AwsCredential("parent-ak", "parent-sk", "parent-st", null, "s3://bucket");
    AwsCredential child = new AwsCredential("child-ak", "child-sk", "child-st", null, LOCATION);
    CredPropsUtil.genericCredFetcherFactory = (apiClient, credId) -> () -> List.of(parent, child);

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

  @Test
  void singleVendedCredentialRoundTripsWithoutRequiringUriCoverage() throws Exception {
    mockFetcher(awsCred("a", LOCATION_A));

    Configuration conf = createTableCredProps();
    assertThat(
            CredentialUtil.decodeCredPrefixes(
                conf.getStrings(UCHadoopConfConstants.UC_CREDENTIAL_PREFIXES_KEY)))
        .containsExactly(LOCATION_A);
    assertThat(conf.get(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY)).isEqualTo("ak-a");

    Configuration uncoveredDelegate =
        getDelegateFileSystemConf(new URI("s3://bucket/uncovered/part-0.parquet"), conf);
    assertInitialCredential(uncoveredDelegate, "a", LOCATION_A);
  }

  @Test
  void decodeRejectsUriCoveredByNoVendedCredentials() throws Exception {
    mockFetcher(awsCred("a", LOCATION_A), awsCred("b", LOCATION_B));
    Configuration conf = createTableCredProps();

    assertThatThrownBy(
            () -> getDelegateFileSystemConf(new URI("s3://bucket/uncovered/part-0.parquet"), conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No credential covers storage location");
  }

  /** Runs the real driver encoder for a renewable table request against the current fetcher. */
  private static Configuration createTableCredProps() throws Exception {
    Configuration driverConf = new Configuration(false);
    // Disable the credential cache so each test uses a fresh fetcher.
    driverConf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);
    driverConf.set("fs.s3.impl", RawLocalFileSystem.class.getName());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            /* renewCredEnabled= */ true,
            /* credScopedFsEnabled= */ true,
            driverConf,
            SCHEME,
            /* apiClient= */ null,
            CATALOG_URI,
            tokenProvider(),
            TABLE_ID,
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    Configuration conf = new Configuration(false);
    props.forEach(conf::set);
    return conf;
  }

  /** Runs the real executor decoder and returns the selected delegate's effective configuration. */
  private static Configuration getDelegateFileSystemConf(URI uri, Configuration conf)
      throws Exception {
    CredScopedFileSystem fs = new CredScopedFileSystem();
    fs.initialize(uri, conf);
    return fs.getDelegate().getConf();
  }

  private static void mockFetcher(GenericCredential... credentials) {
    List<GenericCredential> vended = List.of(credentials);
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
          try {
            when(fetcher.createCredentials()).thenReturn(vended);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return fetcher;
        };
  }

  private static AwsCredential awsCred(String id, String location) {
    return new AwsCredential("ak-" + id, "sk-" + id, "st-" + id, null, location);
  }

  private static void assertInitialCredential(
      Configuration conf, String id, String credentialPrefix) {
    assertThat(conf.get(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY)).isEqualTo("ak-" + id);
    assertThat(conf.get(UCHadoopConfConstants.S3A_INIT_SECRET_KEY)).isEqualTo("sk-" + id);
    assertThat(conf.get(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN)).isEqualTo("st-" + id);
    assertThat(conf.get(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER))
        .isEqualTo(AwsVendedTokenProvider.class.getName());
    assertThat(conf.get(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY))
        .isEqualTo(credentialPrefix);
    assertThat(conf.get("fs.s3.impl")).isEqualTo(RawLocalFileSystem.class.getName());
    assertThat(conf.getBoolean("fs.s3.impl.disable.cache", false)).isTrue();
  }

  private static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }
}
