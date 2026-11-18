package io.unitycatalog.hadoop.internal.id;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FileSystemCredIdTest {

  private static final URI TEST_URI = URI.create("s3://bucket/a");

  private static FileSystemCredId createId(String prefix) {
    Configuration conf = new Configuration(false);
    return FileSystemCredId.create(conf, TEST_URI, prefix);
  }

  @Test
  void uriFallbackKeysBySchemeAndAuthority() {
    Configuration conf = new Configuration(false);

    assertThat(FileSystemCredId.create(conf, URI.create("s3://bucket/a"), null))
        .isEqualTo(FileSystemCredId.create(conf, URI.create("s3://bucket/b"), null))
        .isNotEqualTo(FileSystemCredId.create(conf, URI.create("s3://other/a"), null));
  }

  @Test
  void createUsesProvidedPrefix() {
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY, "s3://bucket/top-level");

    assertThat(FileSystemCredId.create(conf, TEST_URI, "s3://bucket/selected").prefix())
        .isEqualTo("s3://bucket/selected");
  }

  @Test
  void createWithProvidedPrefixFallsBackToUriIdentity() {
    Configuration conf = new Configuration(false);
    String prefix = "s3://bucket/selected";

    assertThat(FileSystemCredId.create(conf, URI.create("s3://bucket/a"), prefix))
        .isEqualTo(FileSystemCredId.create(conf, URI.create("s3://bucket/b"), prefix))
        .isNotEqualTo(FileSystemCredId.create(conf, URI.create("s3://other/a"), prefix));
  }

  @Test
  void equivalentPrefixesAreEqualAndHaveSameHashCode() {
    FileSystemCredId idA = createId("s3://bucket/a");
    FileSystemCredId idB = createId("s3://bucket/a");

    assertThat(idA).isEqualTo(idB).hasSameHashCodeAs(idB);
  }

  @Test
  void samePrefixWithDifferentUriFallbackCredIdsIsDifferent() {
    Configuration conf = new Configuration(false);
    String prefix = "s3://bucket/shared";

    assertThat(FileSystemCredId.create(conf, URI.create("s3://bucket-a/path"), prefix))
        .isNotEqualTo(FileSystemCredId.create(conf, URI.create("s3://bucket-b/path"), prefix));
  }

  private static FileSystemCredId s3CompatibleId(String... settings) {
    Configuration conf = new Configuration(false);
    for (int i = 0; i < settings.length; i += 2) {
      conf.set(settings[i], settings[i + 1]);
    }
    return FileSystemCredId.create(conf, TEST_URI, "s3://bucket/a");
  }

  @Test
  void s3CompatibleStorageAndEngineKeysArePartOfTheId() {
    String endpoint = "fs.s3a.bucket.bucket.endpoint";
    FileSystemCredId onA = s3CompatibleId(endpoint, "http://a:9000");
    // The same bucket name on another storage, or in another region, gets its own filesystem.
    assertThat(onA)
        .isEqualTo(s3CompatibleId(endpoint, "http://a:9000"))
        .isNotEqualTo(s3CompatibleId(endpoint, "http://b:9000"))
        .isNotEqualTo(
            s3CompatibleId(endpoint, "http://a:9000", "fs.s3a.bucket.bucket.endpoint.region", "r"));
    // So do the engine's own keys, at bucket scope or global, which serve a vend without keys.
    assertThat(s3CompatibleId(endpoint, "http://a:9000", "fs.s3a.access.key", "engine-1"))
        .isNotEqualTo(s3CompatibleId(endpoint, "http://a:9000", "fs.s3a.access.key", "engine-2"))
        .isNotEqualTo(onA);
    assertThat(
            s3CompatibleId(endpoint, "http://a:9000", "fs.s3a.bucket.bucket.secret.key", "value"))
        .isNotEqualTo(onA);
    // So do the credential stores S3A reads secrets from, and the role it assumes.
    assertThat(
            s3CompatibleId(
                endpoint, "http://a:9000", "hadoop.security.credential.provider.path", "jceks://a"))
        .isNotEqualTo(
            s3CompatibleId(
                endpoint,
                "http://a:9000",
                "hadoop.security.credential.provider.path",
                "jceks://b"));
    assertThat(
            s3CompatibleId(
                endpoint, "http://a:9000", "fs.s3a.security.credential.provider.path", "jceks://a"))
        .isNotEqualTo(onA);
    assertThat(s3CompatibleId(endpoint, "http://a:9000", "fs.s3a.assumed.role.arn", "role"))
        .isNotEqualTo(onA);
    assertThat(s3CompatibleId(endpoint, "http://a:9000", "fs.s3a.assumed.role.policy", "{}"))
        .isNotEqualTo(onA);
    assertThat(
            s3CompatibleId(
                endpoint, "http://a:9000", "fs.s3a.assumed.role.sts.endpoint", "http://s"))
        .isNotEqualTo(onA);
    // So does every other option S3A reads for the bucket, such as transport and signing.
    for (String option :
        List.of(
            "fs.s3a.connection.ssl.enabled",
            "fs.s3a.proxy.host",
            "fs.s3a.signing-algorithm",
            "fs.s3a.bucket.bucket.encryption.algorithm")) {
      assertThat(s3CompatibleId(endpoint, "http://a:9000", option, "value"))
          .as(option)
          .isNotEqualTo(onA);
    }
    // Not other buckets' options, nor the initial credentials of each table load.
    assertThat(s3CompatibleId(endpoint, "http://a:9000", "fs.s3a.bucket.other.proxy.host", "p"))
        .isEqualTo(onA);
    assertThat(s3CompatibleId(endpoint, "http://a:9000", "fs.s3a.init.access.key", "load-1"))
        .isEqualTo(onA);
    // Values are encoded unambiguously: one value that reads like two settings is not two settings.
    assertThat(
            s3CompatibleId(
                endpoint, "http://a:9000", "fs.s3a.secret.key", "left, fs.s3a.session.token=right"))
        .isNotEqualTo(
            s3CompatibleId(
                endpoint,
                "http://a:9000",
                "fs.s3a.secret.key",
                "left",
                "fs.s3a.session.token",
                "right"));
    // The id holds a hash of the settings, not the settings.
    Configuration conf = new Configuration(false);
    conf.set(endpoint, "http://a:9000");
    conf.set("fs.s3a.secret.key", "engine-value");
    assertThat(FileSystemCredId.s3Storage(conf, TEST_URI))
        .matches("[0-9a-f]+")
        .doesNotContain("engine-value");
  }

  private static FileSystemCredId idAs(UserGroupInformation user, Configuration conf)
      throws Exception {
    return user.doAs(
        (PrivilegedExceptionAction<FileSystemCredId>)
            () -> FileSystemCredId.create(conf, TEST_URI, "s3://bucket/a"));
  }

  @Test
  void keylessVendIsPerHadoopUser() throws Exception {
    // Without vended keys the engine signs with its own, which may differ per user however the
    // engine resolves them, so each Hadoop user gets its own filesystem. Users are told apart as
    // Hadoop's own cache does: two of one name may hold different credentials.
    Configuration conf = new Configuration(false);
    conf.set("fs.s3a.bucket.bucket.endpoint", "http://a:9000");
    conf.set(UCHadoopConfConstants.UC_VENDED_S3_KEYS_PREFIX + "bucket", "false");
    UserGroupInformation alice = UserGroupInformation.createRemoteUser("alice");
    assertThat(idAs(alice, conf))
        .isEqualTo(idAs(alice, conf))
        .isNotEqualTo(idAs(UserGroupInformation.createRemoteUser("alice"), conf))
        .isNotEqualTo(idAs(UserGroupInformation.createRemoteUser("bob"), conf));
    // Vended keys sign every user's requests alike.
    conf.set(UCHadoopConfConstants.UC_VENDED_S3_KEYS_PREFIX + "bucket", "true");
    assertThat(idAs(alice, conf))
        .isEqualTo(idAs(UserGroupInformation.createRemoteUser("bob"), conf));
  }

  @Test
  void uppercaseSchemeGetsTheStorageHash() {
    Configuration conf = new Configuration(false);
    conf.set("fs.s3a.bucket.bucket.endpoint", "http://a:9000");
    assertThat(FileSystemCredId.s3Storage(conf, URI.create("S3A://bucket/a")))
        .isEqualTo(FileSystemCredId.s3Storage(conf, URI.create("s3a://bucket/a")))
        .isNotEmpty();
  }

  @Test
  void awsS3KeysAreNotPartOfTheId() throws Exception {
    // With keys vended for a bucket on AWS S3 the id is as before.
    String keys = UCHadoopConfConstants.UC_VENDED_S3_KEYS_PREFIX + "bucket";
    assertThat(s3CompatibleId(keys, "true", "fs.s3a.access.key", "engine-1"))
        .isEqualTo(s3CompatibleId(keys, "true", "fs.s3a.access.key", "engine-2"))
        .isEqualTo(s3CompatibleId(keys, "true"));
    // Unless the engine routes S3 elsewhere itself, globally: then its settings count.
    for (String global :
        List.of(
            "fs.s3a.endpoint", "fs.s3a.endpoint.region", "fs.s3a.bucket.bucket.endpoint.region")) {
      assertThat(s3CompatibleId(keys, "true", global, "a"))
          .as(global)
          .isNotEqualTo(s3CompatibleId(keys, "true", global, "b"))
          .isNotEqualTo(s3CompatibleId(keys, "true"));
    }
    // Two buckets the table load did not record, with no options of their own, stay apart.
    Configuration noOptions = new Configuration(false);
    assertThat(FileSystemCredId.create(noOptions, URI.create("s3://one/a"), "s3://one/a"))
        .isNotEqualTo(FileSystemCredId.create(noOptions, URI.create("s3://two/a"), "s3://one/a"));
    // A bucket the table load did not record is signed by the engine: its keys and the user count.
    assertThat(s3CompatibleId("fs.s3a.access.key", "engine-1"))
        .isNotEqualTo(s3CompatibleId("fs.s3a.access.key", "engine-2"));
    Configuration conf = new Configuration(false);
    UserGroupInformation alice = UserGroupInformation.createRemoteUser("alice");
    assertThat(idAs(alice, conf))
        .isEqualTo(idAs(alice, conf))
        .isNotEqualTo(idAs(UserGroupInformation.createRemoteUser("bob"), conf));
  }

  @ParameterizedTest
  @MethodSource("differentPrefixes")
  void differentPrefixesAreDifferent(String prefixA, String prefixB) {
    assertThat(createId(prefixA)).isNotEqualTo(createId(prefixB));
  }

  private static Stream<Arguments> differentPrefixes() {
    return Stream.of(
        Arguments.of("s3://bucket/a", "s3://bucket/b"),
        Arguments.of("s3://bucket/a", "s3://bucket/a///"),
        Arguments.of("gs://bucket/a", "gs://bucket/a///"),
        Arguments.of("abfs://container@account/a", "abfs://container@account/a///"),
        Arguments.of("abfss://container@account/a", "abfss://container@account/a///"),
        Arguments.of("s3://bucket/a", "S3A://bucket/a///"),
        Arguments.of("abfs://container@account/a", "abfss://container@account/a"),
        Arguments.of(null, "s3://bucket/a"));
  }
}
