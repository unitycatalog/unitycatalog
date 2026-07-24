package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import io.unitycatalog.hadoop.internal.id.CredId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Shared coverage for {@link CredPropsUtil} that must hold identically for every supported cloud
 * store. Asserts that the props emitted are exactly as expected.
 */
abstract class CredPropsBaseTest {

  static final String CATALOG_URI = "http://uc";
  static final String TABLE_ID = "tid";
  static final String STAGING_TABLE_ID = "staging-uuid";
  static final String CRED_SCOPED_FS = "io.unitycatalog.hadoop.internal.fs.CredScopedFileSystem";
  static final String CRED_SCOPED_AFS = "io.unitycatalog.hadoop.internal.fs.CredScopedFs";

  static final Map<String, String> APP_VERSIONS = Map.of("Spark", "4.0.0", "Delta", "3.3.0");

  static final long EXPIRATION_MILLIS = 999L;

  private static final boolean[] BOOLS = {false, true};

  /** The four credential entry points on {@link CredPropsUtil}. */
  enum CredKind {
    TABLE,
    DELTA_TABLE,
    DELTA_STAGING,
    PATH
  }

  // ---- cloud seams filled in by the subclass -------------------------------------------------

  /** Storage scheme handed to {@code create*CredProps} (e.g. {@code "s3"}). */
  abstract String scheme();

  /** A storage location under {@link #scheme()} used for delta/staging/path requests. */
  abstract String location();

  /**
   * A vended credential of the cloud's type. A null {@code expirationMillis} models a credential
   * with no expiration set.
   */
  abstract GenericCredential vendedCred(Long expirationMillis);

  /** The renewable-path key that carries the credential expiration for this cloud. */
  abstract String initExpirationKey();

  /** Keys the props builder always emits for this cloud. */
  abstract Map<String, String> constructorKeys();

  /** Keys added only when {@code credScoped} is enabled (impl overrides + saved originals). */
  abstract Map<String, String> implOverrideKeys();

  /** Cloud value keys for the static ({@code renew=false}) path, excluding expiration. */
  abstract Map<String, String> staticCredKeys();

  /** Cloud value keys for the renewable ({@code renew=true}) path, excluding expiration. */
  abstract Map<String, String> renewableCredKeys();

  /** {@code fs.<scheme>.impl} entries to pre-seed the conf with, to test original preservation. */
  abstract Map<String, String> customImplSeed();

  /** The {@code .original} entries expected after {@link #customImplSeed()} is preserved. */
  abstract Map<String, String> customImplOriginals();

  /**
   * The expiration key(s) emitted for the given path and credential expiration. The default matches
   * S3/ABFS: an expiration key appears only on the renewable path, only when the credential carries
   * one. GCS overrides this because its static path always emits an expiration.
   */
  Map<String, String> expirationKeys(boolean renew, Long expirationMillis) {
    if (renew && expirationMillis != null) {
      return Map.of(initExpirationKey(), String.valueOf(expirationMillis));
    }
    return Map.of();
  }

  // ---- fixtures ------------------------------------------------------------------------------

  @BeforeEach
  void installFetcher() {
    CredPropsUtil.initialCredCache.clear();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(vendedCred(null));
  }

  @AfterEach
  void resetFetcher() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
    CredPropsUtil.initialCredCache.clear();
  }

  // ---- the cred props matrix: kind x renew x credScoped x customImpl x expiring x appVersions ---

  @ParameterizedTest(
      name = "{0} renew={1} credScoped={2} customImpl={3} expiring={4} appVersions={5}")
  @MethodSource("testCaseMatrix")
  void credPropsMatchExactlyAcrossMatrix(
      CredKind kind,
      boolean renew,
      boolean credScoped,
      boolean customImpl,
      boolean expiring,
      boolean appVersions)
      throws Exception {
    Long expiration = expiring ? EXPIRATION_MILLIS : null;
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(vendedCred(expiration));

    Configuration conf = createConf(customImpl);
    Map<String, String> props =
        createCredPropsFor(kind, renew, credScoped, conf, appVersions ? APP_VERSIONS : Map.of());

    assertThat(props)
        .containsExactlyInAnyOrderEntriesOf(
            expected(kind, renew, credScoped, customImpl, expiring, appVersions));
  }

  static Stream<Arguments> testCaseMatrix() {
    Stream.Builder<Arguments> cases = Stream.builder();
    for (CredKind kind : CredKind.values()) {
      for (boolean renew : BOOLS) {
        for (boolean credScoped : BOOLS) {
          for (boolean customImpl : BOOLS) {
            for (boolean expiring : BOOLS) {
              for (boolean appVersions : BOOLS) {
                cases.add(Arguments.of(kind, renew, credScoped, customImpl, expiring, appVersions));
              }
            }
          }
        }
      }
    }
    return cases.build();
  }

  // ---- other property behavior, also run once per cloud --------------------------------------

  @ParameterizedTest(name = "{0}")
  @EnumSource(CredKind.class)
  void unknownSchemeReturnsEmptyMap(CredKind kind) throws Exception {
    Map<String, String> props = createCredPropsForScheme(kind, "hdfs", false, false);
    assertThat(props).isEmpty();
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(CredKind.class)
  void entryPointAssemblesExpectedCredIdRequest(CredKind kind) throws Exception {
    AtomicReference<CredId> captured = new AtomicReference<>();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> {
          captured.set(credId);
          return mockGenericCredentialFetcher(vendedCred(null));
        };

    createCredPropsFor(kind, true, false);

    assertThat(captured.get().props()).containsExactlyInAnyOrderEntriesOf(credIdKeys(kind));
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(CredKind.class)
  void returnedCredMapIsUnmodifiable(CredKind kind) throws Exception {
    Map<String, String> props = createCredPropsFor(kind, false, false);
    assertThatThrownBy(() -> props.put("k", "v")).isInstanceOf(UnsupportedOperationException.class);
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(UCCredentialHadoopConfs.TableOperation.class)
  void tableOperationPropagatesToProps(UCCredentialHadoopConfs.TableOperation op) throws Exception {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            true,
            false,
            new Configuration(false),
            scheme(),
            null,
            CATALOG_URI,
            tokenProvider(),
            TABLE_ID,
            op,
            Map.of());

    Map<String, String> expected = expected(CredKind.TABLE, true, false, false, false, false);
    expected.put(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, op.value());
    assertThat(props).containsExactlyInAnyOrderEntriesOf(expected);
  }

  /**
   * The Delta-table entry point assembles a {@code DeltaTableCredId} whose {@code props()} is
   * distinct from the ordinary table path, so its operation propagation is covered separately.
   */
  @ParameterizedTest(name = "{0}")
  @EnumSource(UCCredentialHadoopConfs.TableOperation.class)
  void deltaTableOperationPropagatesToProps(UCCredentialHadoopConfs.TableOperation op)
      throws Exception {
    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
            true,
            false,
            new Configuration(false),
            scheme(),
            null,
            CATALOG_URI,
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            location(),
            op,
            Map.of());

    Map<String, String> expected = expected(CredKind.DELTA_TABLE, true, false, false, false, false);
    expected.put(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, op.value());
    assertThat(props).containsExactlyInAnyOrderEntriesOf(expected);
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(UCCredentialHadoopConfs.PathOperation.class)
  void pathOperationPropagatesToProps(UCCredentialHadoopConfs.PathOperation op) throws Exception {
    Map<String, String> props =
        CredPropsUtil.createPathCredProps(
            true,
            false,
            new Configuration(false),
            scheme(),
            null,
            CATALOG_URI,
            tokenProvider(),
            location(),
            op,
            Map.of());

    Map<String, String> expected = expected(CredKind.PATH, true, false, false, false, false);
    expected.put(UCHadoopConfConstants.UC_PATH_OPERATION_KEY, op.value());
    assertThat(props).containsExactlyInAnyOrderEntriesOf(expected);
  }

  // ---- expected-map composition --------------------------------------------------------------

  private Map<String, String> expected(
      CredKind kind,
      boolean renew,
      boolean credScoped,
      boolean customImpl,
      boolean expiring,
      boolean appVersions) {
    Map<String, String> expected = new HashMap<>(constructorKeys());
    if (credScoped) {
      expected.putAll(implOverrideKeys());
      if (customImpl) {
        // A pre-seeded fs.<scheme>.impl is recorded as the .original in place of the default.
        expected.putAll(customImplOriginals());
      }
    }
    if (renew) {
      expected.putAll(renewableCredKeys());
      expected.putAll(renewableContextKeys(kind));
      if (appVersions) {
        APP_VERSIONS.forEach(
            (engine, version) ->
                expected.put(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + engine, version));
      }
    } else {
      expected.putAll(staticCredKeys());
    }
    expected.putAll(expirationKeys(renew, expiring ? EXPIRATION_MILLIS : null));
    return expected;
  }

  /** The UC request context lifted onto renewable props: credential identity + uri + auth. */
  private Map<String, String> renewableContextKeys(CredKind kind) {
    Map<String, String> keys = new HashMap<>(credIdKeys(kind));
    keys.put(UCHadoopConfConstants.UC_URI_KEY, CATALOG_URI);
    tokenProvider()
        .configs()
        .forEach((k, v) -> keys.put(UCHadoopConfConstants.UC_AUTH_PREFIX + k, v));
    return keys;
  }

  /** The credential-identity props for {@code kind}; mirrors the corresponding {@code CredId}. */
  private Map<String, String> credIdKeys(CredKind kind) {
    String contextId = CredPropsUtil.credContextId(CATALOG_URI, scheme(), tokenProvider());
    Map<String, String> keys = new HashMap<>();
    keys.put(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY, contextId);
    switch (kind) {
      case TABLE:
        keys.put(
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
        keys.put(UCHadoopConfConstants.UC_TABLE_ID_KEY, TABLE_ID);
        keys.put(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE");
        break;
      case DELTA_TABLE:
        keys.put(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true");
        keys.put(
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
        keys.put(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "cat");
        keys.put(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "sch");
        keys.put(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "tbl");
        keys.put(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, location());
        keys.put(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE");
        break;
      case DELTA_STAGING:
        keys.put(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true");
        keys.put(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY, STAGING_TABLE_ID);
        keys.put(UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY, location());
        break;
      case PATH:
        keys.put(
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
            UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE);
        keys.put(UCHadoopConfConstants.UC_PATH_KEY, location());
        keys.put(UCHadoopConfConstants.UC_PATH_OPERATION_KEY, "PATH_READ_WRITE");
        break;
      default:
        throw new IllegalArgumentException("Unhandled kind: " + kind);
    }
    return keys;
  }

  // ---- entry-point dispatch ------------------------------------------------------------------

  /** A fresh conf, pre-seeded with this cloud's custom impl keys iff {@code customImpl}. */
  private Configuration createConf(boolean customImpl) {
    Configuration conf = new Configuration(false);
    if (customImpl) {
      customImplSeed().forEach(conf::set);
    }
    return conf;
  }

  private Map<String, String> createCredPropsFor(CredKind kind, boolean renew, boolean credScoped)
      throws Exception {
    return createCredPropsFor(kind, renew, credScoped, new Configuration(false), Map.of());
  }

  private Map<String, String> createCredPropsFor(
      CredKind kind,
      boolean renew,
      boolean credScoped,
      Configuration conf,
      Map<String, String> appVersions)
      throws Exception {
    return createCredProps(kind, scheme(), renew, credScoped, conf, appVersions);
  }

  private Map<String, String> createCredPropsForScheme(
      CredKind kind, String scheme, boolean renew, boolean credScoped) throws Exception {
    return createCredProps(kind, scheme, renew, credScoped, new Configuration(false), Map.of());
  }

  private Map<String, String> createCredProps(
      CredKind kind,
      String scheme,
      boolean renew,
      boolean credScoped,
      Configuration conf,
      Map<String, String> appVersions)
      throws Exception {
    switch (kind) {
      case TABLE:
        return CredPropsUtil.createTableCredProps(
            renew,
            credScoped,
            conf,
            scheme,
            null,
            CATALOG_URI,
            tokenProvider(),
            TABLE_ID,
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            appVersions);
      case DELTA_TABLE:
        return CredPropsUtil.createDeltaTableCredProps(
            renew,
            credScoped,
            conf,
            scheme,
            null,
            CATALOG_URI,
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            location(),
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            appVersions);
      case DELTA_STAGING:
        return CredPropsUtil.createDeltaStagingTableCredProps(
            renew,
            credScoped,
            conf,
            scheme,
            null,
            CATALOG_URI,
            tokenProvider(),
            STAGING_TABLE_ID,
            location(),
            appVersions);
      case PATH:
        return CredPropsUtil.createPathCredProps(
            renew,
            credScoped,
            conf,
            scheme,
            null,
            CATALOG_URI,
            tokenProvider(),
            location(),
            UCCredentialHadoopConfs.PathOperation.PATH_READ_WRITE,
            appVersions);
      default:
        throw new IllegalArgumentException("Unhandled kind: " + kind);
    }
  }

  // ---- shared helpers ------------------------------------------------------------------------

  /** Builds a props map from alternating key/value pairs. */
  static Map<String, String> props(String... kv) {
    Map<String, String> m = new HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1]);
    }
    return m;
  }

  static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }

  static GenericCredentialFetcher mockGenericCredentialFetcher(GenericCredential creds) {
    GenericCredentialFetcher api = mock(GenericCredentialFetcher.class);
    try {
      when(api.createCredential()).thenReturn(creds);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return api;
  }
}
