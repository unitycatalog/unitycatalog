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
import org.junit.jupiter.api.Test;
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
      switch (this) {
        case AWS:
          return new AwsCredential(id + "-ak", id + "-sk", id + "-st", EXPIRATION_MILLIS, prefix);
        case AZURE:
          return new AzureCredential(id + "-sas", EXPIRATION_MILLIS, prefix);
        case GCS:
          return new GcsCredential(id + "-oauth", EXPIRATION_MILLIS, prefix);
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
    assertResolvedCreds(cloud.provider(delegateConf), credential, cloud.location());

    // The seeded single credential renews once it enters the renewal lead window.
    GenericCredential renewed = cloud.credential("renewed", cloud.location());
    assertProviderRenews(cloud, delegateConf, credential, renewed, renewed);
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
            delegateConf.get(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY), "tid-1", "READ_WRITE");
    expectedCredId
        .props()
        .forEach((key, value) -> assertThat(delegateConf.get(key)).isEqualTo(value));

    GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
    when(fetcher.createCredentials()).thenReturn(List.of(parent, child));
    try (MockedStatic<GenericCredentialFetcher> mockedFetcher =
        mockStatic(GenericCredentialFetcher.class)) {
      mockedFetcher.when(() -> GenericCredentialFetcher.create(delegateConf)).thenReturn(fetcher);
      assertResolvedCreds(cloud.provider(delegateConf), child, cloud.location());
    }

    // The selected child credential re-fetches and re-selects when it enters the renewal window.
    // Vend a non-covering sibling plus the covering child so the provider re-selects on renewal.
    GenericCredential sibling = cloud.credential("sibling", cloud.root + "/uncovered-sibling");
    GenericCredential renewedChild = cloud.credential("child2", cloud.location());
    assertProviderRenews(cloud, delegateConf, child, renewedChild, sibling, renewedChild);
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
    assertResolvedCreds(
        cloud.provider(delegateConf),
        credential,
        credentialPrefix == null || credentialPrefix.isEmpty() ? null : credentialPrefix);

    // A single vended credential renews without selection, regardless of prefix coverage.
    GenericCredential renewed = cloud.credential("renewed", credentialPrefix);
    assertProviderRenews(cloud, delegateConf, credential, renewed, renewed);
  }

  @Test
  void awsCredentialWithoutExpirationRoundTrips() throws Exception {
    AwsCredential credential = new AwsCredential("ak", "sk", "st", null, Cloud.AWS.location());
    mockFetcher(credential);
    Configuration delegateConf =
        getDelegateFileSystemConf(
            new URI(Cloud.AWS.location() + "/part-0.parquet"), createTableCredProps(Cloud.AWS));
    assertThat(delegateConf.get(Cloud.AWS.expirationKey)).isNull();
    assertResolvedCreds(
        Cloud.AWS.provider(delegateConf),
        withExpiration(credential, Long.MAX_VALUE),
        credential.prefix());
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
      GenericCredential original,
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
      GenericCredential initial = withExpiration(original, expirationMillis);
      boolean seeded = cloud.initialCredentialKeys.stream().allMatch(key -> conf.get(key) != null);

      GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
      if (seeded) {
        when(fetcher.createCredentials()).thenReturn(List.of(vendedCredentials));
      } else {
        when(fetcher.createCredentials()).thenReturn(List.of(initial), List.of(vendedCredentials));
      }

      try (MockedStatic<GenericCredentialFetcher> mockedFetcher =
          mockStatic(GenericCredentialFetcher.class)) {
        mockedFetcher.when(() -> GenericCredentialFetcher.create(conf)).thenReturn(fetcher);
        GenericCredentialProvider provider = cloud.provider(conf);

        String initialPrefix = initial.prefix();
        assertResolvedCreds(
            provider,
            initial,
            initialPrefix == null || initialPrefix.isEmpty() ? null : initialPrefix);
        verify(fetcher, times(seeded ? 0 : 1)).createCredentials();

        // Enter the renewal lead window while the original credential is still unexpired.
        clock.sleep(Duration.ofMillis(1_500L));
        assertResolvedCreds(provider, expected, expected.prefix());
        verify(fetcher, times(seeded ? 1 : 2)).createCredentials();
      }
    } finally {
      Clock.removeManualClock(clockName);
    }
  }

  private static GenericCredential withExpiration(
      GenericCredential credential, long expirationMillis) {
    if (credential instanceof AwsCredential) {
      AwsCredential aws = (AwsCredential) credential;
      return new AwsCredential(
          aws.accessKeyId(),
          aws.secretAccessKey(),
          aws.sessionToken(),
          expirationMillis,
          aws.prefix());
    } else if (credential instanceof AzureCredential) {
      return new AzureCredential(
          ((AzureCredential) credential).sasToken(), expirationMillis, credential.prefix());
    } else {
      return new GcsCredential(
          ((GcsCredential) credential).oauthToken(), expirationMillis, credential.prefix());
    }
  }

  private static void assertResolvedCreds(
      GenericCredentialProvider provider, GenericCredential expected, String expectedPrefix) {
    assertThat(provider.accessCredentials().prefix()).isEqualTo(expectedPrefix);
    assertThat(provider.accessCredentials().expirationTimeMillis())
        .isEqualTo(expected.expirationTimeMillis());
    if (expected instanceof AwsCredential) {
      AwsCredential aws = (AwsCredential) expected;
      AwsSessionCredentials resolved =
          (AwsSessionCredentials) ((AwsVendedTokenProvider) provider).resolveCredentials();
      assertThat(resolved.accessKeyId()).isEqualTo(aws.accessKeyId());
      assertThat(resolved.secretAccessKey()).isEqualTo(aws.secretAccessKey());
      assertThat(resolved.sessionToken()).isEqualTo(aws.sessionToken());
    } else if (expected instanceof AzureCredential) {
      assertThat(
              ((AbfsVendedTokenProvider) provider)
                  .getSASToken("account", "container", "path", "operation"))
          .isEqualTo(((AzureCredential) expected).sasToken());
    } else {
      AccessToken resolved = ((GcsVendedTokenProvider) provider).getAccessToken();
      assertThat(resolved.getToken()).isEqualTo(((GcsCredential) expected).oauthToken());
      assertThat(resolved.getExpirationTime().toEpochMilli())
          .isEqualTo(expected.expirationTimeMillis());
    }
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
            "tid-1",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
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
