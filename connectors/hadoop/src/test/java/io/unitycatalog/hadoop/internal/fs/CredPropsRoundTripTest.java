package io.unitycatalog.hadoop.internal.fs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.hadoop.util.AccessTokenProvider.AccessToken;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.CredPropsUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.AbfsVendedTokenProvider;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider;
import io.unitycatalog.hadoop.internal.auth.AzureCredential;
import io.unitycatalog.hadoop.internal.auth.GcsCredential;
import io.unitycatalog.hadoop.internal.auth.GcsVendedTokenProvider;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialProvider;
import io.unitycatalog.hadoop.internal.id.TableCredId;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/** Round-trip tests for credential properties produced and consumed during filesystem setup. */
class CredPropsRoundTripTest {

  // Distinct from the default expiry so dropped expiration properties fail the round trip.
  private static final long EXPIRATION_MILLIS = Long.MAX_VALUE - 1;

  private static final String TABLE_ID = "tid-1";
  private static final UCCredentialHadoopConfs.TableOperation TABLE_OPERATION =
      UCCredentialHadoopConfs.TableOperation.READ_WRITE;

  private enum Cloud {
    AWS(
        "s3",
        "s3://bucket",
        UCHadoopConfConstants.S3A_INIT_CRED_EXPIRED_TIME,
        UCHadoopConfConstants.S3A_INIT_ACCESS_KEY,
        UCHadoopConfConstants.S3A_INIT_SECRET_KEY,
        UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN),
    AZURE(
        "abfss",
        "abfss://container@account.dfs.core.windows.net",
        UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME,
        UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN),
    GCS(
        "gs",
        "gs://bucket",
        UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME,
        UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN);

    final String scheme;
    final String root;
    final String expirationKey;
    final List<String> initialCredentialKeys;

    Cloud(String scheme, String root, String expirationKey, String... initialCredentialKeys) {
      this.scheme = scheme;
      this.root = root;
      this.expirationKey = expirationKey;
      this.initialCredentialKeys = Arrays.asList(initialCredentialKeys);
    }

    String location() {
      return root + "/table";
    }

    GenericCredential credential(String id, String prefix) {
      return credential(id, prefix, EXPIRATION_MILLIS);
    }

    GenericCredential credential(String id, String prefix, Long expirationMillis) {
      switch (this) {
        case AWS:
          return new AwsCredential(id + "-ak", id + "-sk", id + "-st", expirationMillis, prefix);
        case AZURE:
          return new AzureCredential(id + "-sas", expirationMillis, prefix);
        case GCS:
          return new GcsCredential(id + "-oauth", expirationMillis, prefix);
        default:
          throw new IllegalStateException("Unhandled cloud: " + this);
      }
    }

    GenericCredentialProvider provider(Configuration conf) {
      switch (this) {
        case AWS:
          return new AwsVendedTokenProvider(conf);
        case AZURE:
          AbfsVendedTokenProvider azure = new AbfsVendedTokenProvider();
          azure.initialize(conf, "account.dfs.core.windows.net");
          return azure;
        case GCS:
          GcsVendedTokenProvider gcs = new GcsVendedTokenProvider();
          gcs.setConf(conf);
          return gcs;
        default:
          throw new IllegalStateException("Unhandled cloud: " + this);
      }
    }

    void assertResolvedCreds(
        GenericCredentialProvider provider, GenericCredential expected, String expectedPrefix) {
      // Exercise the Hadoop-facing API before inspecting the cached credential.
      switch (this) {
        case AWS:
          AwsCredential aws = (AwsCredential) expected;
          AwsSessionCredentials resolved =
              (AwsSessionCredentials) ((AwsVendedTokenProvider) provider).resolveCredentials();
          assertThat(resolved.accessKeyId()).isEqualTo(aws.accessKeyId());
          assertThat(resolved.secretAccessKey()).isEqualTo(aws.secretAccessKey());
          assertThat(resolved.sessionToken()).isEqualTo(aws.sessionToken());
          break;
        case AZURE:
          assertThat(
                  ((AbfsVendedTokenProvider) provider)
                      .getSASToken("account", "container", "path", "operation"))
              .isEqualTo(((AzureCredential) expected).sasToken());
          break;
        case GCS:
          AccessToken token = ((GcsVendedTokenProvider) provider).getAccessToken();
          assertThat(token.getToken()).isEqualTo(((GcsCredential) expected).oauthToken());
          assertThat(token.getExpirationTime().toEpochMilli())
              .isEqualTo(expected.expirationTimeMillis());
          break;
        default:
          throw new IllegalStateException("Unhandled cloud: " + this);
      }
      GenericCredential credential = provider.accessCredentials();
      assertThat(credential.prefix()).isEqualTo(expectedPrefix);
      assertThat(credential.expirationTimeMillis()).isEqualTo(expected.expirationTimeMillis());
    }
  }

  @BeforeEach
  @AfterEach
  void reset() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
    CredScopedFileSystem.clearCacheForTesting();
  }

  @ParameterizedTest
  @EnumSource(Cloud.class)
  void singleCredentialRoundTripsThroughCredScopedFileSystemAndProvider(Cloud cloud)
      throws Exception {
    GenericCredential credential = cloud.credential("initial", cloud.location());
    mockFetcher(credential);

    Configuration delegateConf =
        getDelegateFileSystemConf(
            new URI(cloud.location() + "/part-0.parquet"), createTableCredProps(cloud));
    cloud.assertResolvedCreds(cloud.provider(delegateConf), credential, cloud.location());

    // The seeded single credential renews once it enters the renewal lead window.
    GenericCredential renewed = cloud.credential("renewed", cloud.location());
    assertProviderRenews(cloud, delegateConf, "initial", /* isSeeded= */ true, renewed, renewed);
  }

  @ParameterizedTest
  @EnumSource(Cloud.class)
  void multipleCredentialPrefixesRoundTripThroughCredScopedFileSystem(Cloud cloud)
      throws Exception {
    GenericCredential parent = cloud.credential("parent", cloud.root);
    GenericCredential child = cloud.credential("child", cloud.location());
    mockFetcher(parent, child);

    Configuration delegateConf =
        getDelegateFileSystemConf(
            new URI(cloud.location() + "/part-0.parquet"), createTableCredProps(cloud));
    assertThat(delegateConf.get(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY))
        .isEqualTo(cloud.location());
    cloud.initialCredentialKeys.forEach(key -> assertThat(delegateConf.get(key)).isNull());

    TableCredId expectedCredId =
        new TableCredId(
            delegateConf.get(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY),
            TABLE_ID,
            TABLE_OPERATION.name());
    expectedCredId
        .props()
        .forEach((key, value) -> assertThat(delegateConf.get(key)).isEqualTo(value));

    GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
    when(fetcher.createCredentials()).thenReturn(List.of(parent, child));
    try (MockedStatic<GenericCredentialFetcher> mockedFetcher =
        mockStatic(GenericCredentialFetcher.class)) {
      mockedFetcher.when(() -> GenericCredentialFetcher.create(delegateConf)).thenReturn(fetcher);
      cloud.assertResolvedCreds(cloud.provider(delegateConf), child, cloud.location());
    }

    // The selected child credential re-fetches and re-selects when it enters the renewal window.
    // Vend a non-covering sibling plus the covering child so the provider re-selects on renewal.
    GenericCredential sibling = cloud.credential("sibling", cloud.root + "/uncovered-sibling");
    GenericCredential renewedChild = cloud.credential("child2", cloud.location());
    assertProviderRenews(
        cloud, delegateConf, "child", /* isSeeded= */ false, renewedChild, sibling, renewedChild);
  }

  @ParameterizedTest
  @MethodSource("cloudsAndPrefixes")
  void singleVendedCredentialRoundTripsWithoutRequiringUriCoverage(
      Cloud cloud, String credentialPrefix) throws Exception {
    GenericCredential credential = cloud.credential("initial", credentialPrefix);
    mockFetcher(credential);

    Configuration delegateConf =
        getDelegateFileSystemConf(
            new URI(cloud.root.replace("://", "://other-") + "/uncovered/part-0.parquet"),
            createTableCredProps(cloud));
    cloud.assertResolvedCreds(
        cloud.provider(delegateConf), credential, normalizePrefix(credentialPrefix));

    // A single vended credential renews without selection, regardless of prefix coverage.
    GenericCredential renewed = cloud.credential("renewed", credentialPrefix);
    assertProviderRenews(cloud, delegateConf, "initial", /* isSeeded= */ true, renewed, renewed);
  }

  @ParameterizedTest
  @EnumSource(Cloud.class)
  void credentialWithoutExpirationRoundTrips(Cloud cloud) throws Exception {
    GenericCredential credential = cloud.credential("initial", cloud.location(), null);
    mockFetcher(credential);
    Configuration delegateConf =
        getDelegateFileSystemConf(
            new URI(cloud.location() + "/part-0.parquet"), createTableCredProps(cloud));
    // The encoder cache setting is not propagated to the decoded provider configuration.
    delegateConf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);
    assertThat(delegateConf.get(cloud.expirationKey)).isNull();
    cloud.initialCredentialKeys.forEach(key -> assertThat(delegateConf.get(key)).isNotNull());

    GenericCredential fetched = cloud.credential("fetched", cloud.location());
    GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
    when(fetcher.createCredentials()).thenReturn(List.of(fetched));
    try (MockedStatic<GenericCredentialFetcher> mockedFetcher =
        mockStatic(GenericCredentialFetcher.class)) {
      mockedFetcher.when(() -> GenericCredentialFetcher.create(delegateConf)).thenReturn(fetcher);
      GenericCredentialProvider provider = cloud.provider(delegateConf);
      verify(fetcher, times(0)).createCredentials();
      // Azure requires a SAS expiry to seed the provider; AWS and GCS default to no expiry.
      GenericCredential expected =
          cloud == Cloud.AZURE
              ? fetched
              : cloud.credential("initial", cloud.location(), Long.MAX_VALUE);
      cloud.assertResolvedCreds(provider, expected, cloud.location());
      verify(fetcher, times(cloud == Cloud.AZURE ? 1 : 0)).createCredentials();
    }
  }

  private static Stream<Arguments> cloudsAndPrefixes() {
    return Arrays.stream(Cloud.values())
        .flatMap(
            cloud ->
                Stream.of(null, "", cloud.location()).map(prefix -> Arguments.of(cloud, prefix)));
  }

  @ParameterizedTest
  @EnumSource(Cloud.class)
  void multiCredentialRequiresCoveringPrefix(Cloud cloud) throws Exception {
    mockFetcher(cloud.credential("a", cloud.root + "/a"), cloud.credential("b", cloud.root + "/b"));
    Configuration conf = createTableCredProps(cloud);

    assertThatThrownBy(
            () ->
                getDelegateFileSystemConf(new URI(cloud.root + "/uncovered/part-0.parquet"), conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No credential covers storage location");
  }

  /** Performs a renewal to ensure the provider supplies the correct credential. */
  private static void assertProviderRenews(
      Cloud cloud,
      Configuration delegateConf,
      String originalId,
      boolean isSeeded,
      GenericCredential expected,
      GenericCredential... vendedCredentials)
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
      long expirationMillis = clock.now().toEpochMilli() + 2_000L;
      conf.setLong(cloud.expirationKey, expirationMillis);
      GenericCredential initial = cloud.credential(originalId, expected.prefix(), expirationMillis);
      cloud.initialCredentialKeys.forEach(
          key ->
              assertThat(conf.get(key) != null).as("seeded property %s", key).isEqualTo(isSeeded));

      GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
      if (isSeeded) {
        when(fetcher.createCredentials()).thenReturn(List.of(vendedCredentials));
      } else {
        when(fetcher.createCredentials()).thenReturn(List.of(initial), List.of(vendedCredentials));
      }

      try (MockedStatic<GenericCredentialFetcher> mockedFetcher =
          mockStatic(GenericCredentialFetcher.class)) {
        mockedFetcher.when(() -> GenericCredentialFetcher.create(conf)).thenReturn(fetcher);
        GenericCredentialProvider provider = cloud.provider(conf);

        cloud.assertResolvedCreds(provider, initial, normalizePrefix(initial.prefix()));
        verify(fetcher, times(isSeeded ? 0 : 1)).createCredentials();

        // Enter the renewal lead window while the original credential is still unexpired.
        clock.sleep(Duration.ofMillis(1_500L));
        cloud.assertResolvedCreds(provider, expected, expected.prefix());
        verify(fetcher, times(isSeeded ? 1 : 2)).createCredentials();
      }
    } finally {
      Clock.removeManualClock(clockName);
    }
  }

  private static String normalizePrefix(String prefix) {
    return prefix == null || prefix.isEmpty() ? null : prefix;
  }

  /** Runs the real encoder for a renewable table request against the current fetcher. */
  private static Configuration createTableCredProps(Cloud cloud) throws Exception {
    Configuration encoderConf = new Configuration(false);
    // Disable the credential cache so each test uses a fresh fetcher.
    encoderConf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);
    encoderConf.set("fs." + cloud.scheme + ".impl", RawLocalFileSystem.class.getName());
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            /* renewCredEnabled= */ true,
            /* credScopedFsEnabled= */ true,
            encoderConf,
            cloud.scheme,
            /* apiClient= */ null,
            "http://uc",
            TokenProvider.create(Map.of("type", "static", "token", "tok")),
            TABLE_ID,
            TABLE_OPERATION,
            Map.of());

    Configuration conf = new Configuration(false);
    props.forEach(conf::set);
    return conf;
  }

  /** Runs the real decoder and returns the selected delegate's effective configuration. */
  private static Configuration getDelegateFileSystemConf(URI uri, Configuration conf)
      throws Exception {
    CredScopedFileSystem fs = new CredScopedFileSystem();
    fs.initialize(uri, conf);
    return fs.getDelegate().getConf();
  }

  private static void mockFetcher(GenericCredential... credentials) {
    List<GenericCredential> vendedCredentials = List.of(credentials);
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
          try {
            when(fetcher.createCredentials()).thenReturn(vendedCredentials);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return fetcher;
        };
  }
}
