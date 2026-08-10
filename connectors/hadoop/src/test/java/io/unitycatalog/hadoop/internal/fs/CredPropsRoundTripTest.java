package io.unitycatalog.hadoop.internal.fs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.CredPropsUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import io.unitycatalog.hadoop.internal.id.TableCredId;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
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

    // The seeded single credential renews once it enters the renewal lead window.
    assertProviderRenews(delegateConf, LOCATION);
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

    // The selected child credential re-fetches and re-selects when it enters the renewal window.
    assertProviderRenews(delegateConf, LOCATION);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = LOCATION)
  void singleVendedCredentialRoundTripsWithoutRequiringUriCoverage(String credentialPrefix)
      throws Exception {
    AwsCredential credential =
        new AwsCredential("access-key", "secret-key", "session-token", null, credentialPrefix);
    CredPropsUtil.genericCredFetcherFactory = (apiClient, credId) -> () -> List.of(credential);

    Map<String, String> props = createCredProps();

    Configuration confWithCreds = new Configuration(false);
    props.forEach(confWithCreds::set);

    CredScopedFileSystem fs = new CredScopedFileSystem();
    fs.initialize(new URI("s3://totally-different-bucket/uncovered/part-0.parquet"), confWithCreds);
    Configuration delegateConf = fs.getDelegate().getConf();

    assertResolvedCreds(
        delegateConf,
        credentialPrefix == null || credentialPrefix.isEmpty() ? null : credentialPrefix);
  }

  @Test
  void multiCredentialRequiresCoveringPrefix() throws Exception {
    mockFetcher(awsCred("a", LOCATION_A), awsCred("b", LOCATION_B));
    Configuration conf = createTableCredProps();

    assertThatThrownBy(
            () -> getDelegateFileSystemConf(new URI("s3://bucket/uncovered/part-0.parquet"), conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No credential covers storage location");
  }

  /**
   * Drives a full credential renewal through {@code delegateConf} and asserts the provider
   * re-fetches and re-selects the credential covering {@code prefix}. Works for both credential
   * paths: it forces any seeded initial credential to expire and vends a fresh list, so the seeded
   * single-credential path and the fetch-and-select multi-credential path both converge on the
   * re-fetched credential.
   */
  private static void assertProviderRenews(Configuration delegateConf, String prefix)
      throws Exception {
    String clockName = UUID.randomUUID().toString();
    Clock clock = Clock.getManualClock(clockName);
    try {
      Configuration conf = new Configuration(delegateConf);
      // Disable the JVM-wide read-time cache so renewal fetches directly and no static cache
      // state leaks across tests.
      conf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);
      conf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
      conf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1_000L);
      // Expire any seeded initial credential so the first access must renew from the fetcher.
      conf.setLong(
          UCHadoopConfConstants.S3A_INIT_CRED_EXPIRED_TIME, clock.now().toEpochMilli() + 500L);

      // Vend a fresh list on renewal: a non-covering sibling plus the covering credential, so the
      // provider must both re-fetch and re-select the one covering the prefix.
      AwsCredential sibling =
          awsCredExpiring("sibling", "s3://uncovered-sibling", clock.now().toEpochMilli() + 20_000);
      AwsCredential renewed =
          awsCredExpiring("renewed", prefix, clock.now().toEpochMilli() + 20_000L);
      GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
      when(fetcher.createCredentials()).thenReturn(List.of(sibling, renewed));

      try (MockedStatic<GenericCredentialFetcher> mockedFetcher =
          mockStatic(GenericCredentialFetcher.class)) {
        mockedFetcher.when(() -> GenericCredentialFetcher.create(conf)).thenReturn(fetcher);
        AwsVendedTokenProvider provider = new AwsVendedTokenProvider(conf);

        // Advance past the seeded credential's expiry so the next access renews from the fetcher.
        clock.sleep(Duration.ofMillis(1_000L));
        assertThat(accessKeyId(provider)).isEqualTo("ak-renewed");
        assertThat(provider.accessCredentials().prefix()).isEqualTo(prefix);
      }
    } finally {
      Clock.removeManualClock(clockName);
    }
  }

  private static Map<String, String> createCredProps() throws Exception {
    Configuration initialConf = new Configuration(false);
    initialConf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);
    initialConf.set("fs.s3.impl", RawLocalFileSystem.class.getName());
    return CredPropsUtil.createTableCredProps(
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
  }

  private static void assertResolvedCreds(Configuration conf, String credentialPrefix)
      throws Exception {
    AwsVendedTokenProvider provider = new AwsVendedTokenProvider(conf);
    AwsSessionCredentials resolved = (AwsSessionCredentials) provider.resolveCredentials();

    assertThat(provider.accessCredentials().prefix()).isEqualTo(credentialPrefix);
    assertThat(resolved.accessKeyId()).isEqualTo("access-key");
    assertThat(resolved.secretAccessKey()).isEqualTo("secret-key");
    assertThat(resolved.sessionToken()).isEqualTo("session-token");
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

  private static AwsCredential awsCredExpiring(String id, String location, long expiryMillis) {
    return new AwsCredential("ak-" + id, "sk-" + id, "st-" + id, expiryMillis, location);
  }

  private static String accessKeyId(AwsVendedTokenProvider provider) {
    return ((AwsSessionCredentials) provider.resolveCredentials()).accessKeyId();
  }

  private static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }
}
