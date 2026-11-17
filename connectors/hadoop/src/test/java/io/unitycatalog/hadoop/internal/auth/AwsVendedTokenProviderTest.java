package io.unitycatalog.hadoop.internal.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class AwsVendedTokenProviderTest extends BaseTokenProviderTest<AwsVendedTokenProvider> {

  @Override
  protected AwsVendedTokenProvider createTestProvider(
      Configuration conf, TemporaryCredentialsApi mockApi) {
    return new TestAwsVendedTokenProvider(conf, ucFetcher(conf, mockApi));
  }

  @Override
  protected AwsVendedTokenProvider createTestProvider(
      Configuration conf, GenericCredentialFetcher fetcher) {
    return new TestAwsVendedTokenProvider(conf, fetcher);
  }

  static class TestAwsVendedTokenProvider extends AwsVendedTokenProvider {
    private final GenericCredentialFetcher credentialFetcher;

    TestAwsVendedTokenProvider(Configuration conf, GenericCredentialFetcher credentialFetcher) {
      super(conf);
      this.credentialFetcher = credentialFetcher;
    }

    @Override
    GenericCredentialFetcher genericCredentialFetcher() {
      return credentialFetcher;
    }
  }

  @Override
  protected TemporaryCredentials newTempCred(String id, long expirationMillis) {
    AwsCredentials awsCred = new AwsCredentials();
    awsCred.setAccessKeyId("accessKeyId" + id);
    awsCred.setSecretAccessKey("secretAccessKey" + id);
    awsCred.setSessionToken("sessionToken" + id);

    TemporaryCredentials tempCred = new TemporaryCredentials();
    tempCred.setAwsTempCredentials(awsCred);
    tempCred.setExpirationTime(expirationMillis);

    return tempCred;
  }

  @Override
  protected GenericCredential newGenericCred(String id, long expirationMillis, String location) {
    return new AwsCredential(
        "accessKeyId" + id,
        "secretAccessKey" + id,
        "sessionToken" + id,
        expirationMillis,
        location);
  }

  @Override
  protected String location(String path) {
    return "s3://bucket" + path;
  }

  @Override
  protected void setInitialCred(Configuration conf, TemporaryCredentials cred) {
    conf.set(
        UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, cred.getAwsTempCredentials().getAccessKeyId());
    conf.set(
        UCHadoopConfConstants.S3A_INIT_SECRET_KEY,
        cred.getAwsTempCredentials().getSecretAccessKey());
    conf.set(
        UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN,
        cred.getAwsTempCredentials().getSessionToken());
    conf.setLong(UCHadoopConfConstants.S3A_INIT_CRED_EXPIRED_TIME, cred.getExpirationTime());
  }

  @Override
  protected void assertCred(AwsVendedTokenProvider provider, TemporaryCredentials expected) {
    software.amazon.awssdk.auth.credentials.AwsCredentials actual = provider.resolveCredentials();

    assertThat(actual).isInstanceOf(AwsSessionCredentials.class);
    AwsSessionCredentials actualSessionCred = (AwsSessionCredentials) actual;

    assertThat(expected.getAwsTempCredentials()).isNotNull();
    AwsCredentials expectedAwsCred = expected.getAwsTempCredentials();

    assertThat(actualSessionCred.accessKeyId()).isEqualTo(expectedAwsCred.getAccessKeyId());
    assertThat(actualSessionCred.secretAccessKey()).isEqualTo(expectedAwsCred.getSecretAccessKey());
    assertThat(actualSessionCred.sessionToken()).isEqualTo(expectedAwsCred.getSessionToken());
  }

  @Test
  public void testLoadProvider() throws IOException {
    Configuration conf = newTableBasedConf("unity-catalog-table");
    conf.set(Constants.AWS_CREDENTIALS_PROVIDER, AwsVendedTokenProvider.class.getName());

    AWSCredentialProviderList list =
        CredentialProviderListFactory.buildAWSProviderList(
            URI.create("s3://bucket/key"),
            conf,
            Constants.AWS_CREDENTIALS_PROVIDER,
            new ArrayList<>(),
            new HashSet<>());

    List<AwsCredentialsProvider> providers = list.getProviders();
    assertThat(providers).hasSize(1);
    assertThat(providers.get(0)).isInstanceOf(AwsVendedTokenProvider.class);
  }

  @Test
  public void testStaticKeyAmongRenewedCredentialsHasNoToken() throws Exception {
    // One table's credentials can mix session credentials with a static key pair of another
    // location; the key pair is used as is.
    // A bucket is on one storage, so the key pair's location is in a bucket of its own.
    String endpoint = "http://storage.example.com:9000";
    GenericCredential session = newGenericCred("session", Long.MAX_VALUE, location("/session"));
    GenericCredential keyPair =
        new AwsCredential(
            "key-id-1", "key-value-1", null, Long.MAX_VALUE, "s3://other-bucket/static", endpoint);
    Configuration conf = newTableBasedConf("tid");
    conf.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, "s3://other-bucket/static/file");
    conf.set(UCHadoopConfConstants.UC_VENDED_S3_ENDPOINT_PREFIX + "other-bucket", endpoint);
    conf.set(UCHadoopConfConstants.UC_VENDED_S3_REGION_PREFIX + "other-bucket", "");
    GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
    when(fetcher.createCredentials()).thenReturn(List.of(session, keyPair));
    AwsVendedTokenProvider provider = createTestProvider(conf, fetcher);

    assertThat(provider.resolveCredentials())
        .isInstanceOf(AwsBasicCredentials.class)
        .extracting(software.amazon.awssdk.auth.credentials.AwsCredentials::accessKeyId)
        .isEqualTo("key-id-1");
  }

  /** A renewal for the bucket whose storage the table load recorded, null for no record. */
  private static final class Renewal {
    final String recordedEndpoint;
    final String recordedRegion;
    final AwsCredential renewed;
    final boolean isRefused;

    Renewal(
        String recordedEndpoint, String recordedRegion, AwsCredential renewed, boolean isRefused) {
      this.recordedEndpoint = recordedEndpoint;
      this.recordedRegion = recordedRegion;
      this.renewed = renewed;
      this.isRefused = isRefused;
    }

    @Override
    public String toString() {
      return recordedEndpoint + " " + recordedRegion + " -> " + renewed.s3Endpoint();
    }
  }

  @Test
  public void testRenewalForOtherStorageIsRefused() throws Exception {
    String a = "http://storage-a.example.com:9000";
    String b = "http://storage-b.example.com:9000";
    String prefix = location("/t");
    AwsCredential onAws = new AwsCredential("id", "v", "t", Long.MAX_VALUE, prefix);
    AwsCredential onA = new AwsCredential("id", "v", null, Long.MAX_VALUE, prefix, a, "r1");
    for (Renewal renewal :
        List.of(
            new Renewal("", "", onAws, false),
            new Renewal(a, "r1", onA, false),
            // The file system would send the keys where it was set up to send them.
            new Renewal("", "", onA, true),
            new Renewal(a, "r1", onAws, true),
            new Renewal(b, "r1", onA, true),
            // The same storage reporting another region, as after a fallback: the keys still go
            // to the same endpoint.
            new Renewal(a, "r2", onA, false),
            // Set up without a record, which only AWS S3 is.
            new Renewal(null, null, onAws, false),
            new Renewal(null, null, onA, true),
            // A table recreated in another bucket: the open file system still points at this one.
            new Renewal(
                "", "", new AwsCredential("id", "v", "t", Long.MAX_VALUE, "s3://other/t"), true))) {
      Configuration conf = newTableBasedConf("tid");
      conf.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, location("/t/file"));
      // Each case fetches its own renewal rather than one cached for the table by another case.
      conf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);
      if (renewal.recordedEndpoint != null) {
        conf.set(
            UCHadoopConfConstants.UC_VENDED_S3_ENDPOINT_PREFIX + "bucket",
            renewal.recordedEndpoint);
        conf.set(
            UCHadoopConfConstants.UC_VENDED_S3_REGION_PREFIX + "bucket", renewal.recordedRegion);
      }
      GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
      when(fetcher.createCredentials()).thenReturn(List.of(renewal.renewed));
      AwsVendedTokenProvider provider = createTestProvider(conf, fetcher);
      if (renewal.isRefused) {
        assertThatThrownBy(provider::resolveCredentials)
            .as(renewal.toString())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("load the table again");
      } else {
        assertThat(provider.resolveCredentials().accessKeyId())
            .as(renewal.toString())
            .isEqualTo("id");
      }
    }
  }

  @Test
  public void testRenewalWithoutAStoragePathTakesOnlyAwsS3() throws Exception {
    // Neither the file system nor the renewed credential names a bucket, so there is no record to
    // compare with: the file system was set up for AWS S3 and takes only AWS S3 renewals.
    for (Renewal renewal :
        List.of(
            new Renewal(null, null, new AwsCredential("id", "v", "t", Long.MAX_VALUE, null), false),
            new Renewal(
                null,
                null,
                new AwsCredential(
                    "id", "v", null, Long.MAX_VALUE, null, "http://storage.example.com:9000", "r"),
                true))) {
      Configuration conf = newTableBasedConf("tid");
      conf.setBoolean(UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY, false);
      GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
      when(fetcher.createCredentials()).thenReturn(List.of(renewal.renewed));
      AwsVendedTokenProvider provider = createTestProvider(conf, fetcher);
      if (renewal.isRefused) {
        assertThatThrownBy(provider::resolveCredentials)
            .as(renewal.toString())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("with no storage path to check them against");
      } else {
        assertThat(provider.resolveCredentials().accessKeyId()).isEqualTo("id");
      }
    }
  }

  @Test
  public void testTableLoadedForOtherStorageDoesNotShareCachedCredentials() throws Exception {
    // The provider cache is shared by the table's providers. After a credential's storage moved
    // from A to B, a table loaded for B gets B's credential, not the one cached for A.
    String a = "http://storage-a.example.com:9000";
    String b = "http://storage-b.example.com:9000";
    String table = "tid-" + UUID.randomUUID();
    for (String endpoint : List.of(a, b)) {
      Configuration conf = newTableBasedConf(table);
      conf.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, location("/t/file"));
      conf.set(UCHadoopConfConstants.UC_VENDED_S3_ENDPOINT_PREFIX + "bucket", endpoint);
      conf.set(UCHadoopConfConstants.UC_VENDED_S3_REGION_PREFIX + "bucket", "");
      GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
      when(fetcher.createCredentials())
          .thenReturn(
              List.of(
                  new AwsCredential(
                      "id-" + endpoint, "v", null, Long.MAX_VALUE, location("/t"), endpoint)));

      assertThat(createTestProvider(conf, fetcher).resolveCredentials().accessKeyId())
          .isEqualTo("id-" + endpoint);
    }
  }

  @Test
  public void testStorageIsCheckedOncePerCredential() throws Exception {
    // The SDK resolves credentials for every request; the check runs when the credential changes.
    String endpoint = "http://storage.example.com:9000";
    Configuration conf = newTableBasedConf("tid-" + UUID.randomUUID());
    conf.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, location("/t/file"));
    conf.set(UCHadoopConfConstants.UC_VENDED_S3_ENDPOINT_PREFIX + "bucket", endpoint);
    conf.set(UCHadoopConfConstants.UC_VENDED_S3_REGION_PREFIX + "bucket", "");
    GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
    when(fetcher.createCredentials())
        .thenReturn(
            List.of(new AwsCredential("id", "v", null, Long.MAX_VALUE, location("/t"), endpoint)));
    AwsVendedTokenProvider provider = createTestProvider(conf, fetcher);
    assertThat(provider.resolveCredentials().accessKeyId()).isEqualTo("id");

    // A record that no longer matches is not read again for the same credential.
    conf.set(UCHadoopConfConstants.UC_VENDED_S3_ENDPOINT_PREFIX + "bucket", "http://other:9000");
    assertThat(provider.resolveCredentials().accessKeyId()).isEqualTo("id");
  }

  @Test
  public void testInitialCredentialMatchesTheRecordedStorage() throws Exception {
    // The table load's own credential, read back from the init keys, carries its region too.
    String endpoint = "http://storage.example.com:9000";
    Configuration conf = newTableBasedConf("tid");
    conf.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, location("/t"));
    conf.set(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "id");
    conf.set(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, "v");
    conf.set(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, "");
    conf.set(UCHadoopConfConstants.S3A_INIT_S3_ENDPOINT, endpoint);
    conf.set(UCHadoopConfConstants.S3A_INIT_S3_REGION, "r1");
    conf.set(UCHadoopConfConstants.UC_VENDED_S3_ENDPOINT_PREFIX + "bucket", endpoint);
    conf.set(UCHadoopConfConstants.UC_VENDED_S3_REGION_PREFIX + "bucket", "r1");
    GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);

    assertThat(createTestProvider(conf, fetcher).resolveCredentials().accessKeyId())
        .isEqualTo("id");
    verify(fetcher, never()).createCredentials();
  }

  @Test
  public void testEndpointOnlyCredentialCannotBeResolved() {
    GenericCredential endpointOnly =
        new AwsCredential(
            null, null, null, Long.MAX_VALUE, location("/t"), "http://storage.example.com:9000");
    AwsVendedTokenProvider provider = provider(location("/t/file"), endpointOnly);

    assertThatThrownBy(provider::resolveCredentials)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("now vends only the storage endpoint");
  }

  @Test
  public void testInitialKeyPairNeedsATokenOrAnS3Endpoint() throws Exception {
    Configuration conf = newTableBasedConf();
    AwsVendedTokenProvider provider = createTestProvider(conf, mock(TemporaryCredentialsApi.class));
    Configuration init = new Configuration(false);
    init.set(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "key-id-1");
    init.set(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, "key-value-1");
    // AWS S3 always vends a token, so without one the credential is fetched from Unity Catalog.
    assertThat(provider.initGenericCredential(init)).isNull();
    // An empty token is none, so a token configured elsewhere is not paired with the key pair.
    init.set(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, "");
    assertThat(provider.initGenericCredential(init)).isNull();
    init.set(UCHadoopConfConstants.S3A_INIT_S3_ENDPOINT, "http://storage.example.com:9000");
    assertThat(((AwsCredential) provider.initGenericCredential(init)).sessionToken()).isNull();
  }

  @Test
  public void testKeyPairWithLifetimeRenewsToTheUpdatedKeyPair() throws Exception {
    // A vended key pair carries a lifetime and no session token. Renewing it picks up the key
    // pair the credential was updated to.
    Configuration conf = newTableBasedConf();
    conf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
    conf.setLong(UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY, 1000L);
    conf.set(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "key-id-1");
    conf.set(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, "key-value-1");
    conf.set(UCHadoopConfConstants.S3A_INIT_S3_ENDPOINT, "http://storage.example.com:9000");
    // The table load's record of the bucket's storage; no prefix, as without credential-scoped
    // file systems, so the check takes the only recorded bucket.
    conf.set(
        UCHadoopConfConstants.UC_VENDED_S3_ENDPOINT_PREFIX + "bucket",
        "http://storage.example.com:9000");
    conf.set(UCHadoopConfConstants.UC_VENDED_S3_REGION_PREFIX + "bucket", "");
    conf.setLong(
        UCHadoopConfConstants.S3A_INIT_CRED_EXPIRED_TIME, clock.now().toEpochMilli() + 2000L);
    TemporaryCredentials updated =
        new TemporaryCredentials()
            .awsTempCredentials(
                new AwsCredentials()
                    .accessKeyId("key-id-2")
                    .secretAccessKey("key-value-2")
                    .s3Endpoint("http://storage.example.com:9000"))
            .url(location("/t"))
            .expirationTime(clock.now().toEpochMilli() + 3_600_000L);
    TemporaryCredentialsApi tempCredApi = mock(TemporaryCredentialsApi.class);
    when(tempCredApi.generateTemporaryTableCredentials(any())).thenReturn(updated);
    AwsVendedTokenProvider provider = createTestProvider(conf, tempCredApi);

    assertThat(provider.resolveCredentials())
        .isInstanceOf(AwsBasicCredentials.class)
        .extracting(software.amazon.awssdk.auth.credentials.AwsCredentials::accessKeyId)
        .isEqualTo("key-id-1");

    clock.sleep(Duration.ofMillis(1500));
    assertThat(provider.resolveCredentials())
        .isInstanceOf(AwsBasicCredentials.class)
        .extracting(software.amazon.awssdk.auth.credentials.AwsCredentials::accessKeyId)
        .isEqualTo("key-id-2");
  }
}
