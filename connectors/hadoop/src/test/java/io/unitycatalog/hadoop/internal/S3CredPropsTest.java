package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.props.CredPropsBuilder;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class S3CredPropsTest extends CredPropsBaseTest {

  private static final String CUSTOM_IMPL = "com.example.CustomS3FileSystem";
  private static final String S3A_FS = "org.apache.hadoop.fs.s3a.S3AFileSystem";
  private static final String S3A_AFS = "org.apache.hadoop.fs.s3a.S3A";
  private static final String VENDED_PROVIDER =
      "io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider";
  private static final String COMPAT_ENDPOINT = "http://storage.example.com:9000";

  @Override
  String scheme() {
    return "s3";
  }

  @Override
  String location() {
    return "s3://bucket/data";
  }

  @Override
  GenericCredential vendedCred(Long expirationMillis, String prefix) {
    return new AwsCredential("ak", "sk", "st", expirationMillis, prefix);
  }

  @Override
  Map<String, String> defaultKeys() {
    return props(
        "fs.s3a.path.style.access", "true",
        "fs.s3.impl.disable.cache", "true",
        "fs.s3a.impl.disable.cache", "true");
  }

  @Override
  Map<String, String> vendedStorageKeys() {
    // AWS S3, which renewal checks against.
    return props(
        UCHadoopConfConstants.UC_VENDED_S3_ENDPOINT_PREFIX + "bucket",
        "",
        UCHadoopConfConstants.UC_VENDED_S3_REGION_PREFIX + "bucket",
        "");
  }

  @Override
  Map<String, String> staticCredKeys(Long expiration) {
    // S3's static path carries no expiration key.
    return props(
        "fs.s3a.access.key", "ak",
        "fs.s3a.secret.key", "sk",
        "fs.s3a.session.token", "st");
  }

  @Override
  Map<String, String> initialCredKeys(Long expiration) {
    Map<String, String> keys =
        props(
            UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak",
            UCHadoopConfConstants.S3A_INIT_SECRET_KEY, "sk",
            UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, "st");
    if (expiration != null) {
      keys.put(UCHadoopConfConstants.S3A_INIT_CRED_EXPIRED_TIME, String.valueOf(expiration));
    }
    return keys;
  }

  @Override
  Map<String, String> renewableProviderKeys() {
    return props(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER, VENDED_PROVIDER);
  }

  @Override
  Map<String, String> fileSystemImplKeys() {
    return props(
        "fs.s3.impl", S3A_FS,
        "fs.s3a.impl", S3A_FS);
  }

  @Override
  Map<String, String> abstractFileSystemImplKeys() {
    return props(
        "fs.AbstractFileSystem.s3.impl", S3A_AFS,
        "fs.AbstractFileSystem.s3a.impl", S3A_AFS);
  }

  @Override
  Map<String, String> customImplSeed() {
    return props("fs.s3.impl", CUSTOM_IMPL, "fs.s3a.impl", CUSTOM_IMPL);
  }

  @Test
  void endpointAndRegionAreSetPerBucketOfEachCredential() {
    Map<String, String> conf =
        CredPropsBuilder.forCloud(CloudType.S3, new Configuration())
            .initialCredentials(
                // Endpoint-only: several keyed ones would need renewal.
                List.of(
                    new AwsCredential(
                        null, null, null, null, "s3://bucket-a/t", "http://a:9000", "eu-test-1"),
                    // No region known: S3A applies its own default.
                    new AwsCredential(null, null, null, null, "s3://bucket-b/t", "http://b:9000"),
                    // A bucket name that is not a valid URI host.
                    new AwsCredential(null, null, null, null, "s3://bucket_d/t", "http://d:9000"),
                    // AWS S3: no endpoint, so the SDK resolves it.
                    new AwsCredential("ak", "sk", "st", null, "s3://bucket-c/t")))
            .build();

    assertThat(conf)
        .containsEntry("fs.s3a.bucket.bucket-a.endpoint", "http://a:9000")
        .containsEntry("fs.s3a.bucket.bucket-a.endpoint.region", "eu-test-1")
        .containsEntry("fs.s3a.bucket.bucket-b.endpoint", "http://b:9000")
        .containsEntry("fs.s3a.bucket.bucket_d.endpoint", "http://d:9000")
        .doesNotContainKeys(
            "fs.s3a.bucket.bucket-b.endpoint.region",
            "fs.s3a.bucket.bucket-c.endpoint",
            "fs.s3a.endpoint",
            "fs.s3a.endpoint.region");
  }

  @Test
  void keyPairWithoutSessionTokenAndEndpointOnlyAreNotRenewed() throws Exception {
    String endpoint = "http://storage.example.com:9000";
    AwsCredential keyPair = new AwsCredential("ak", "sk", null, null, location(), endpoint);
    AwsCredential endpointOnly = new AwsCredential(null, null, null, null, location(), endpoint);
    for (AwsCredential vended : List.of(keyPair, endpointOnly)) {
      CredPropsUtil.initialCredCache.clear();
      CredPropsUtil.genericCredFetcherFactory =
          (apiClient, credId) -> mockGenericCredentialFetcher(vended);

      Map<String, String> conf =
          CredPropsUtil.createTableCredProps(
              /* renewCredEnabled= */ true,
              /* credScopedFsEnabled= */ false,
              new Configuration(false),
              scheme(),
              null,
              CATALOG_URI,
              tokenProvider(),
              TABLE_ID,
              UCCredentialHadoopConfs.TableOperation.READ_WRITE,
              Map.of());

      // Renewal is requested, but there is nothing to renew: no UC provider replaces S3A's own.
      assertThat(conf)
          .as("vended %s", vended.hasKeys() ? "key pair" : "endpoint only")
          .containsEntry("fs.s3a.bucket.bucket.endpoint", endpoint)
          .doesNotContainKeys(
              UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER,
              UCHadoopConfConstants.S3A_INIT_ACCESS_KEY);
      if (vended.hasKeys()) {
        // An empty session token, so that one already configured is not paired with the keys.
        assertThat(conf)
            .containsEntry("fs.s3a.access.key", "ak")
            .containsEntry("fs.s3a.secret.key", "sk")
            .containsEntry("fs.s3a.session.token", "");
      } else {
        assertThat(conf)
            .doesNotContainKeys("fs.s3a.access.key", "fs.s3a.secret.key", "fs.s3a.session.token");
      }
    }
  }

  private Map<String, String> tableCredProps(boolean renew, AwsCredential... vended)
      throws Exception {
    CredPropsUtil.initialCredCache.clear();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(vended);
    return CredPropsUtil.createTableCredProps(
        renew,
        /* credScopedFsEnabled= */ false,
        new Configuration(false),
        scheme(),
        null,
        CATALOG_URI,
        tokenProvider(),
        TABLE_ID,
        UCCredentialHadoopConfs.TableOperation.READ_WRITE,
        Map.of());
  }

  @Test
  void credentialSetsTheConnectorCannotServeAreRejected() throws Exception {
    String endpoint = "http://storage.example.com:9000";
    // An endpoint without keys relies on the engine's own keys, which S3A cannot use for one bucket
    // while the vended provider serves the others.
    assertThatThrownBy(
            () ->
                tableCredProps(
                    true,
                    new AwsCredential("ak1", "sk1", "st1", 1L, location()),
                    new AwsCredential(null, null, null, null, "s3://other-bucket/t", endpoint)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("an S3 endpoint without keys together with credentials with keys");
    // S3A takes one endpoint per bucket.
    assertThatThrownBy(
            () ->
                tableCredProps(
                    true,
                    new AwsCredential("ak1", "sk1", "st1", 1L, "s3://bucket/a", endpoint),
                    new AwsCredential(
                        "ak2", "sk2", "st2", 1L, "s3://bucket/b", "http://other.example.com")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("of bucket bucket are on different storage");
    // AWS S3 counts as storage too: the bucket's endpoint would take the AWS path elsewhere.
    AwsCredential onAws = new AwsCredential("ak1", "sk1", "st1", 1L, "s3://bucket/a");
    AwsCredential onCompat = new AwsCredential("ak2", "sk2", "st2", 1L, "s3://bucket/b", endpoint);
    for (AwsCredential[] set :
        List.of(new AwsCredential[] {onAws, onCompat}, new AwsCredential[] {onCompat, onAws})) {
      assertThatThrownBy(() -> tableCredProps(true, set))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("of bucket bucket are on different storage")
          .hasMessageContaining("AWS S3");
    }
    // An S3 endpoint needs a bucket to apply to.
    assertThatThrownBy(
            () ->
                tableCredProps(true, new AwsCredential("ak", "sk", "st", 1L, "s3:///t", endpoint)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No bucket in S3 path");
  }

  @Test
  void severalCredentialsWithoutKeysLeaveTheEngineKeysInUse() throws Exception {
    // Neither vends keys, so each bucket gets its endpoint and the engine's own keys serve both.
    String other = "http://other.example.com:9000";
    Map<String, String> props =
        tableCredProps(
            true,
            new AwsCredential(null, null, null, null, "s3://bucket/a", COMPAT_ENDPOINT),
            new AwsCredential(null, null, null, null, "s3://other-bucket/b", other));
    assertThat(props)
        .containsEntry("fs.s3a.bucket.bucket.endpoint", COMPAT_ENDPOINT)
        .containsEntry("fs.s3a.bucket.other-bucket.endpoint", other)
        .doesNotContainKey("fs.s3a.aws.credentials.provider")
        .doesNotContainKey("fs.s3a.access.key");
  }

  @Test
  void keyPairWithoutTokenClearsTokensAndSetsPathStyleAtBucketScope() throws Exception {
    // Bucket options in the engine's configuration override global ones, so these are set at the
    // bucket's scope too; the empty init token keeps a configured one from being paired.
    Map<String, String> renewed =
        tableCredProps(
            true, new AwsCredential("ak", "sk", null, 1L, "s3://bucket/t", COMPAT_ENDPOINT));
    assertThat(renewed)
        .containsEntry("fs.s3a.init.session.token", "")
        .containsEntry("fs.s3a.bucket.bucket.path.style.access", "true")
        .containsEntry("fs.s3a.bucket.bucket.aws.credentials.provider", VENDED_PROVIDER);
    // Fixed keys go to the bucket's scope as well, with an empty token.
    assertThat(
            tableCredProps(
                false, new AwsCredential("ak", "sk", null, null, "s3://bucket/t", COMPAT_ENDPOINT)))
        .containsEntry("fs.s3a.bucket.bucket.access.key", "ak")
        .containsEntry("fs.s3a.bucket.bucket.secret.key", "sk")
        .containsEntry("fs.s3a.bucket.bucket.session.token", "")
        .containsEntry(
            "fs.s3a.bucket.bucket.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    // With a session token, the provider that reads it too.
    assertThat(
            tableCredProps(
                false, new AwsCredential("ak", "sk", "st", null, "s3://bucket/t", COMPAT_ENDPOINT)))
        .containsEntry(
            "fs.s3a.bucket.bucket.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
    // Without keys there is no token to clear.
    assertThat(
            tableCredProps(
                true, new AwsCredential(null, null, null, null, "s3://bucket/t", COMPAT_ENDPOINT)))
        .containsEntry("fs.s3a.bucket.bucket.path.style.access", "true")
        .doesNotContainKey("fs.s3a.bucket.bucket.session.token")
        .doesNotContainKey("fs.s3a.bucket.bucket.access.key")
        .doesNotContainKey("fs.s3a.bucket.bucket.aws.credentials.provider");
  }

  private Map<String, String> endpointOnlyProps(Configuration hadoopConf) throws Exception {
    CredPropsUtil.initialCredCache.clear();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            mockGenericCredentialFetcher(
                new AwsCredential(null, null, null, null, "s3://bucket/t", COMPAT_ENDPOINT));
    return CredPropsUtil.createTableCredProps(
        true,
        /* credScopedFsEnabled= */ true,
        hadoopConf,
        scheme(),
        null,
        CATALOG_URI,
        tokenProvider(),
        TABLE_ID,
        UCCredentialHadoopConfs.TableOperation.READ_WRITE,
        Map.of());
  }

  @Test
  void credentialsWithoutKeysUseTheCredentialScopedFileSystem() throws Exception {
    // Its cache key covers the endpoint and the engine's own keys (FileSystemCredId).
    assertThat(endpointOnlyProps(new Configuration(false)))
        .containsEntry("fs.s3.impl", CRED_SCOPED_FS)
        .containsEntry("fs.s3.impl.disable.cache", "true");
  }

  @Test
  void severalKeyPairsForS3CompatibleStorageNeedRenewal() {
    // Without renewal no keys would be set for them, and their storage would get the engine's.
    assertThatThrownBy(
            () ->
                tableCredProps(
                    false,
                    new AwsCredential("ak1", "sk1", null, null, "s3://bucket/a", COMPAT_ENDPOINT),
                    new AwsCredential(
                        "ak2", "sk2", null, null, "s3://other-bucket/b", COMPAT_ENDPOINT)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("need credential renewal");
  }

  @Test
  void severalKeyPairsTakeTheVendedProvider() throws Exception {
    // Neither expires, but only the vended provider picks the key pair covering each path.
    Map<String, String> conf =
        tableCredProps(
            true,
            new AwsCredential("ak1", "sk1", null, null, location(), COMPAT_ENDPOINT),
            new AwsCredential("ak2", "sk2", null, null, "s3://other-bucket/t", COMPAT_ENDPOINT));
    for (String bucket : List.of("bucket", "other-bucket")) {
      assertThat(conf)
          .containsEntry("fs.s3a.bucket." + bucket + ".aws.credentials.provider", VENDED_PROVIDER)
          .containsEntry("fs.s3a.bucket." + bucket + ".endpoint", COMPAT_ENDPOINT)
          .doesNotContainKey("fs.s3a.bucket." + bucket + ".access.key");
    }
  }

  @Test
  void bucketOfAPathThatIsNoValidUriGetsItsEndpoint() throws Exception {
    Map<String, String> conf =
        tableCredProps(
            false,
            new AwsCredential(
                "ak", "sk", "st", 1L, "s3://bucket_x/my table", "http://storage.example.com:9000"));
    assertThat(conf)
        .containsEntry("fs.s3a.bucket.bucket_x.endpoint", "http://storage.example.com:9000");
  }

  @Test
  void sessionCredentialMixedWithKeyPairIsRenewed() throws Exception {
    // One table's credentials, for example a clone and its source, can mix session credentials
    // with a static key pair; the session credentials must still be renewed.
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            mockGenericCredentialFetcher(
                new AwsCredential("ak1", "sk1", "st1", 1L, location()),
                new AwsCredential(
                    "ak2", "sk2", null, null, "s3://other-bucket/t", COMPAT_ENDPOINT));

    Map<String, String> conf =
        CredPropsUtil.createTableCredProps(
            /* renewCredEnabled= */ true,
            /* credScopedFsEnabled= */ false,
            new Configuration(false),
            scheme(),
            null,
            CATALOG_URI,
            tokenProvider(),
            TABLE_ID,
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(conf)
        .containsEntry(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER, VENDED_PROVIDER)
        .containsEntry("fs.s3a.bucket.other-bucket.aws.credentials.provider", VENDED_PROVIDER)
        .containsEntry("fs.s3a.bucket.other-bucket.endpoint", COMPAT_ENDPOINT)
        // The AWS S3 bucket keeps the default endpoint.
        .doesNotContainKey("fs.s3a.bucket.bucket.endpoint");
  }

  @Test
  void keyPairWithLifetimeIsRenewed() throws Exception {
    // A key pair vended with a lifetime is renewed like a session, so an update of the key pair
    // reaches the engine; its session token is empty, so a configured one is not paired with it.
    long expiration = System.currentTimeMillis() + 3_600_000L;
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            mockGenericCredentialFetcher(
                new AwsCredential(
                    "ak", "sk", null, expiration, location(), "http://storage.example.com:9000"));

    Map<String, String> conf =
        CredPropsUtil.createTableCredProps(
            /* renewCredEnabled= */ true,
            /* credScopedFsEnabled= */ false,
            new Configuration(false),
            scheme(),
            null,
            CATALOG_URI,
            tokenProvider(),
            TABLE_ID,
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(conf)
        .containsKey(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER)
        .containsEntry(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "ak")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, "sk")
        .containsEntry(UCHadoopConfConstants.S3A_INIT_CRED_EXPIRED_TIME, String.valueOf(expiration))
        .containsEntry(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, "");
  }
}
