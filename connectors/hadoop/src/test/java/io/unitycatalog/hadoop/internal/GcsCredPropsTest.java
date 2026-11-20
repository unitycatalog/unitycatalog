package io.unitycatalog.hadoop.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AccessTokenProviderCredentials;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.GcsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class GcsCredPropsTest extends CredPropsBaseTest {

  private static final String CONFLICT_CHECK_KEY = "fs.gs.create.items.conflict.check.enable";
  private static final String HNS_FOLDERS_KEY = "fs.gs.hierarchical.namespace.folders.enable";
  private static final String IMPLICIT_DIR_REPAIR_KEY = "fs.gs.implicit.dir.repair.enable";
  private static final String STATIC_EXPIRATION_KEY = "fs.gs.auth.access.token.expiration";
  private static final String CUSTOM_IMPL = "com.example.CustomGcsFileSystem";
  private static final String GHFS = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem";
  private static final String GHFS_AFS = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS";
  private static final String VENDED_PROVIDER =
      "io.unitycatalog.hadoop.internal.auth.GcsVendedTokenProvider";
  private static final String FIXED_PROVIDER =
      "io.unitycatalog.hadoop.internal.auth.GcsFixedTokenProvider";
  private static final String VENDED_TOKEN = "vended-gcs-token";
  // 2100-01-01T00:00:00Z.
  private static final long VENDED_EXPIRATION_MILLIS = 4_102_444_800_000L;

  @Override
  String scheme() {
    return "gs";
  }

  @Override
  String location() {
    return "gs://bucket/data";
  }

  @Override
  GenericCredential vendedCred(Long expirationMillis, String prefix) {
    return new GcsCredential("token", expirationMillis, prefix);
  }

  @Override
  Map<String, String> defaultKeys() {
    return props(
        CONFLICT_CHECK_KEY,
        "false",
        HNS_FOLDERS_KEY,
        "false",
        IMPLICIT_DIR_REPAIR_KEY,
        "false",
        "fs.gs.impl.disable.cache",
        "true");
  }

  /**
   * GCS is the sole cloud whose static path emits an expiration: {@link #STATIC_EXPIRATION_KEY} is
   * always set, defaulting to {@code Long.MAX_VALUE} when the credential has none.
   */
  @Override
  Map<String, String> staticCredKeys(Long expiration) {
    Map<String, String> keys = new HashMap<>();
    keys.put("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER");
    keys.put("fs.gs.auth.access.token.provider", FIXED_PROVIDER);
    keys.put("fs.gs.auth.access.token.credential", "token");
    keys.put(
        STATIC_EXPIRATION_KEY, String.valueOf(expiration == null ? Long.MAX_VALUE : expiration));
    return keys;
  }

  @Override
  Map<String, String> initialCredKeys(Long expiration) {
    Map<String, String> keys = props(UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN, "token");
    if (expiration != null) {
      keys.put(
          UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME, String.valueOf(expiration));
    }
    return keys;
  }

  @Override
  Map<String, String> renewableProviderKeys() {
    return props(
        "fs.gs.auth.type",
        "ACCESS_TOKEN_PROVIDER",
        "fs.gs.auth.access.token.provider",
        VENDED_PROVIDER);
  }

  @Override
  Map<String, String> fileSystemImplKeys() {
    return props("fs.gs.impl", GHFS);
  }

  @Override
  Map<String, String> abstractFileSystemImplKeys() {
    return props("fs.AbstractFileSystem.gs.impl", GHFS_AFS);
  }

  @Override
  Map<String, String> customImplSeed() {
    return props("fs.gs.impl", CUSTOM_IMPL);
  }

  // ---- GCS-only tests ------

  /**
   * The GCS connector resolves its credentials from the built props to the vended token and its
   * expiration, whether or not renewal is enabled, rather than to the engine's own GCS identity.
   */
  @ParameterizedTest(name = "renew={0} credScoped={1} expiring={2} sessionAuthType={3}")
  @CsvSource({
    "false, false, false,",
    "false, false, true,",
    "true, false, false,",
    "true, false, true,",
    "false, true, true,",
    "false, false, true, APPLICATION_DEFAULT",
    "true, false, true, APPLICATION_DEFAULT"
  })
  void gcsConnectorAuthenticatesWithTheVendedToken(
      boolean renew, boolean credScoped, boolean expiring, String sessionAuthType)
      throws Exception {
    Long expiration = expiring ? VENDED_EXPIRATION_MILLIS : null;
    Configuration conf = new Configuration(false);
    if (sessionAuthType != null) {
      conf.set("fs.gs.auth.type", sessionAuthType);
    }
    GoogleCredentials credentials = gcsConnectorCredentials(renew, credScoped, expiration, conf);

    assertThat(credentials).isInstanceOf(AccessTokenProviderCredentials.class);
    assertThat(credentials.getRequestMetadata())
        .containsExactlyEntriesOf(Map.of("Authorization", List.of("Bearer " + VENDED_TOKEN)));
    assertThat(credentials.getAccessToken().getExpirationTime())
        .isEqualTo(new Date(expiring ? VENDED_EXPIRATION_MILLIS : Long.MAX_VALUE));
  }

  /**
   * The connector refreshes a token a few minutes before it expires. Without renewal, the token is
   * still used until its expiration, and from then on the refresh fails.
   */
  @Test
  void fixedTokenIsUsedUntilItExpires() throws Exception {
    String clockName = UUID.randomUUID().toString();
    Clock clock = Clock.getManualClock(clockName);
    try {
      Configuration conf = new Configuration(false);
      conf.set(UCHadoopConfConstants.UC_TEST_CLOCK_NAME, clockName);
      // Inside google-auth's expiration margin, so every request calls refresh() on the provider.
      Instant expiration = clock.now().plusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
      GoogleCredentials credentials =
          gcsConnectorCredentials(false, false, expiration.toEpochMilli(), conf);

      clock.sleep(Duration.between(clock.now(), expiration).minusMillis(1));
      assertThat(credentials.getRequestMetadata())
          .containsExactlyEntriesOf(Map.of("Authorization", List.of("Bearer " + VENDED_TOKEN)));
      assertThat(credentials.getAccessToken().getExpirationTime()).isEqualTo(Date.from(expiration));

      clock.sleep(Duration.ofMillis(1));
      assertThatThrownBy(credentials::getRequestMetadata)
          .isInstanceOf(IOException.class)
          .hasMessage(
              "The GCS token vended by Unity Catalog expired at "
                  + expiration
                  + " and credential renewal is disabled, so it cannot be renewed. Load the table"
                  + " or path again to get a new token, or enable credential renewal.");
    } finally {
      Clock.removeManualClock(clockName);
    }
  }

  /** Returns the credentials the GCS connector builds from {@code conf} plus the built props. */
  private GoogleCredentials gcsConnectorCredentials(
      boolean renew, boolean credScoped, Long expiration, Configuration conf) throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            mockGenericCredentialFetcher(new GcsCredential(VENDED_TOKEN, expiration, location()));
    createCredPropsFor(CredKind.TABLE, renew, credScoped).forEach(conf::set);
    return HadoopCredentialsConfiguration.getCredentials(conf, "fs.gs");
  }

  @Test
  void conflictCheckDefaultsFalse() throws Exception {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            false,
            new Configuration(false),
            scheme(),
            null,
            CATALOG_URI,
            tokenProvider(),
            TABLE_ID,
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props).containsEntry(CONFLICT_CHECK_KEY, "false");
  }

  @Test
  void conflictCheckRespectsUserOverrideToTrue() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(CONFLICT_CHECK_KEY, "true");

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            false,
            conf,
            scheme(),
            null,
            CATALOG_URI,
            tokenProvider(),
            TABLE_ID,
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    assertThat(props).containsEntry(CONFLICT_CHECK_KEY, "true");
  }

  /**
   * Hadoop 3.5 ships {@code fs.gs.create.items.conflict.check.enable=true} in its built-in {@code
   * core-default.xml}. That is not the user's choice, so it must not keep the check on. (Hadoop has
   * no default for the other GCS keys; the connector's own {@code true} default for the
   * hierarchical namespace key is not in the Hadoop configuration at all.)
   */
  @Test
  void hadoopBuiltInDefaultDoesNotKeepTheGcsChecksOn() throws Exception {
    Configuration conf = new Configuration(false);
    String defaults =
        "<configuration><property><name>"
            + CONFLICT_CHECK_KEY
            + "</name><value>true</value></property></configuration>";
    conf.addResource(new ByteArrayInputStream(defaults.getBytes(UTF_8)), "core-default.xml");
    assertThat(conf.get(CONFLICT_CHECK_KEY)).isEqualTo("true");
    assertThat(conf.get(HNS_FOLDERS_KEY)).isNull();

    assertThat(tableProps(conf))
        .containsEntry(CONFLICT_CHECK_KEY, "false")
        .containsEntry(HNS_FOLDERS_KEY, "false")
        .containsEntry(IMPLICIT_DIR_REPAIR_KEY, "false");
  }

  /** Values from {@code core-site.xml}, which loads after the defaults, are the user's own. */
  @Test
  void coreSiteValuesOnTopOfHadoopDefaultsAreRespected() throws Exception {
    Configuration conf = new Configuration(false);
    String defaults =
        "<configuration><property><name>"
            + CONFLICT_CHECK_KEY
            + "</name><value>true</value></property></configuration>";
    String site =
        "<configuration>"
            + "<property><name>"
            + CONFLICT_CHECK_KEY
            + "</name><value>true</value></property>"
            + "<property><name>"
            + HNS_FOLDERS_KEY
            + "</name><value>true</value></property>"
            + "<property><name>"
            + IMPLICIT_DIR_REPAIR_KEY
            + "</name><value>true</value></property>"
            + "</configuration>";
    conf.addResource(new ByteArrayInputStream(defaults.getBytes(UTF_8)), "core-default.xml");
    conf.addResource(new ByteArrayInputStream(site.getBytes(UTF_8)), "core-site.xml");

    assertThat(tableProps(conf))
        .containsEntry(CONFLICT_CHECK_KEY, "true")
        .containsEntry(HNS_FOLDERS_KEY, "true")
        .containsEntry(IMPLICIT_DIR_REPAIR_KEY, "true");
  }

  /** Each key is decided on its own: setting one does not change the other two. */
  @Test
  void eachKeyIsDecidedIndependently() throws Exception {
    Map<String, String> expected =
        Map.of(
            CONFLICT_CHECK_KEY,
            "false",
            HNS_FOLDERS_KEY,
            "false",
            IMPLICIT_DIR_REPAIR_KEY,
            "false");
    for (String key : expected.keySet()) {
      Configuration conf = new Configuration(false);
      conf.set(key, "true");

      Map<String, String> props = tableProps(conf);

      expected.forEach((k, v) -> assertThat(props).containsEntry(k, k.equals(key) ? "true" : v));
    }
  }

  @Test
  void userValuesWinOverHadoopDefaults() throws Exception {
    Configuration conf = new Configuration(false);
    String defaults =
        "<configuration><property><name>"
            + CONFLICT_CHECK_KEY
            + "</name><value>true</value></property></configuration>";
    conf.addResource(new ByteArrayInputStream(defaults.getBytes(UTF_8)), "core-default.xml");
    // A user setting, even one that equals Hadoop's default, is respected.
    conf.set(CONFLICT_CHECK_KEY, "true");
    conf.set(HNS_FOLDERS_KEY, "true");
    conf.set(IMPLICIT_DIR_REPAIR_KEY, "true");

    assertThat(tableProps(conf))
        .containsEntry(CONFLICT_CHECK_KEY, "true")
        .containsEntry(HNS_FOLDERS_KEY, "true")
        .containsEntry(IMPLICIT_DIR_REPAIR_KEY, "true");
  }

  private Map<String, String> tableProps(Configuration conf) throws Exception {
    return CredPropsUtil.createTableCredProps(
        false,
        false,
        conf,
        scheme(),
        null,
        CATALOG_URI,
        tokenProvider(),
        TABLE_ID,
        UCCredentialHadoopConfs.TableOperation.READ_WRITE,
        Map.of());
  }

  @Test
  void gcsChecksDefaultFalseAcrossPathAndDeltaCredProps() throws Exception {
    Map<String, String> pathProps =
        CredPropsUtil.createPathCredProps(
            false,
            false,
            new Configuration(false),
            scheme(),
            null,
            CATALOG_URI,
            tokenProvider(),
            location(),
            UCCredentialHadoopConfs.PathOperation.PATH_READ,
            Map.of());
    assertThat(pathProps)
        .containsEntry(CONFLICT_CHECK_KEY, "false")
        .containsEntry(HNS_FOLDERS_KEY, "false")
        .containsEntry(IMPLICIT_DIR_REPAIR_KEY, "false");

    Map<String, String> deltaProps =
        CredPropsUtil.createDeltaTableCredProps(
            false,
            false,
            new Configuration(false),
            scheme(),
            null,
            CATALOG_URI,
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            location(),
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());
    assertThat(deltaProps)
        .containsEntry(CONFLICT_CHECK_KEY, "false")
        .containsEntry(HNS_FOLDERS_KEY, "false")
        .containsEntry(IMPLICIT_DIR_REPAIR_KEY, "false");
  }
}
