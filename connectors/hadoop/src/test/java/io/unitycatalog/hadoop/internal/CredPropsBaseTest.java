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
import io.unitycatalog.hadoop.internal.id.DeltaStagingTableCredId;
import io.unitycatalog.hadoop.internal.id.DeltaTableCredId;
import io.unitycatalog.hadoop.internal.id.PathCredId;
import io.unitycatalog.hadoop.internal.id.TableCredId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;

/**
 * Shared coverage for {@link CredPropsUtil} that must hold identically for every supported cloud
 * store. Asserts that the props emitted are exactly as expected.
 */
abstract class CredPropsBaseTest {

  static final String CATALOG_URI = "http://uc";
  static final String TABLE_ID = "tid";
  static final String STAGING_TABLE_ID = "staging-uuid";
  static final UCDeltaTableIdentifier DELTA_TABLE = UCDeltaTableIdentifier.of("cat", "sch", "tbl");
  static final String CRED_SCOPED_FS = "io.unitycatalog.hadoop.internal.fs.CredScopedFileSystem";
  static final String CRED_SCOPED_AFS = "io.unitycatalog.hadoop.internal.fs.CredScopedFs";

  static final Map<String, String> APP_VERSIONS = Map.of("Spark", "4.0.0", "Delta", "3.3.0");

  private static final List<Map<String, String>> APP_VERSION_CASES =
      List.of(Map.of(), APP_VERSIONS);

  static final long EXPIRATION_MILLIS = 999L;

  private static final boolean[] BOOLS = {false, true};

  /** The four credential entry points on {@link CredPropsUtil}. */
  enum CredKind {
    TABLE,
    DELTA_TABLE,
    DELTA_STAGING,
    PATH
  }

  // ---- per cloud details ---------------------------------------------

  /** Storage scheme handed to {@code create*CredProps} (e.g. {@code "s3"}). */
  abstract String scheme();

  /** A storage location under {@link #scheme()} used for delta/staging/path requests. */
  abstract String location();

  /**
   * A vended credential of the cloud's type. A null {@code expirationMillis} models a credential
   * with no expiration set.
   */
  abstract GenericCredential vendedCred(Long expirationMillis, String prefix);

  private GenericCredential vendedCred(Long expirationMillis) {
    return vendedCred(expirationMillis, location());
  }

  // ---- per cloud configuration keys ---------------------------------------------

  /** Keys emitted on every path. */
  abstract Map<String, String> defaultKeys();

  /** Non-renewable credentials keys. */
  abstract Map<String, String> staticCredKeys(Long expiration);

  /** The initial keys for renewable credentials. */
  abstract Map<String, String> initialCredKeys(Long expiration);

  /** The renewable vended-provider registration. */
  abstract Map<String, String> renewableProviderKeys();

  /** Concrete FileSystem override implementation keys. */
  abstract Map<String, String> fileSystemImplKeys();

  /** Abstract FileSystem override implementation keys. */
  abstract Map<String, String> abstractFileSystemImplKeys();

  /** {@code fs.<scheme>.impl} entries to pre-seed the conf with, to test original preservation. */
  abstract Map<String, String> customImplSeed();

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

  // ---- assert exact credential configurations across the full test matrix ----------------

  @ParameterizedTest(
      name = "{0} renew={1} credScoped={2} customImpl={3} expiring={4} appVersions={5}")
  @MethodSource("testCaseMatrix")
  void credPropsMatchExactlyAcrossMatrix(
      CredKind kind,
      boolean renew,
      boolean credScoped,
      boolean customImpl,
      boolean expiring,
      Map<String, String> appVersions)
      throws Exception {
    Long expiration = expiring ? EXPIRATION_MILLIS : null;
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(vendedCred(expiration));

    Configuration conf = createConf(customImpl);
    Map<String, String> props = createCredPropsFor(kind, renew, credScoped, conf, appVersions);

    assertThat(props)
        .containsExactlyInAnyOrderEntriesOf(
            expected(kind, renew, credScoped, conf, expiration, appVersions));
  }

  @ParameterizedTest(
      name = "{0} renew={1} credScoped={2} customImpl={3} expiring={4} appVersions={5}")
  @MethodSource("testCaseMatrix")
  void multipleCredPropsMatchExactlyAcrossMatrix(
      CredKind kind,
      boolean renew,
      boolean credScoped,
      boolean customImpl,
      boolean expiring,
      Map<String, String> appVersions)
      throws Exception {
    for (int prefixCount : List.of(2, 3, 5)) {
      List<String> prefixes = createPrefixes(prefixCount);
      setupMockFetcher(expiring, prefixes);
      Configuration conf = createConf(customImpl);
      Map<String, String> created = createCredPropsFor(kind, renew, credScoped, conf, appVersions);
      Map<String, String> expected =
          expectedForMultipleCredentials(kind, renew, credScoped, conf, prefixes, appVersions);
      assertThat(created).containsExactlyInAnyOrderEntriesOf(expected);
    }
  }

  @ParameterizedTest(
      name = "{0} renew={1} credScoped={2} customImpl={3} expiring={4} appVersions={5}")
  @MethodSource("testCaseMatrix")
  void malformedCredentialPrefixesSkippedDoesNotThrow(
      CredKind kind,
      boolean renew,
      boolean credScoped,
      boolean customImpl,
      boolean expiring,
      Map<String, String> appVersions)
      throws Exception {
    for (List<String> prefixes : malformedPrefixLists()) {
      setupMockFetcher(expiring, prefixes);
      Configuration conf = createConf(customImpl);
      Map<String, String> created = createCredPropsFor(kind, renew, credScoped, conf, appVersions);
      List<String> expectedPrefixes = nonEmptyPrefixes(prefixes);
      Map<String, String> expected =
          expectedForMultipleCredentials(
              kind,
              renew,
              credScoped,
              conf,
              expectedPrefixes.isEmpty() ? null : expectedPrefixes,
              appVersions);
      assertThat(created).containsExactlyInAnyOrderEntriesOf(expected);
    }
  }

  static Stream<Arguments> testCaseMatrix() {
    Stream.Builder<Arguments> cases = Stream.builder();
    for (CredKind kind : CredKind.values()) {
      for (boolean renew : BOOLS) {
        for (boolean credScoped : BOOLS) {
          for (boolean customImpl : BOOLS) {
            for (boolean expiring : BOOLS) {
              for (Map<String, String> appVersions : APP_VERSION_CASES) {
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
    Map<String, String> props =
        createCredProps(kind, "hdfs", false, false, new Configuration(false), Map.of());
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

  @ParameterizedTest
  @NullAndEmptySource
  void credentialPrefixMayBeEmpty(String prefix) throws Exception {
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) -> mockGenericCredentialFetcher(vendedCred(null, prefix));

    Map<String, String> props = createCredPropsFor(CredKind.TABLE, true, false);
    if (prefix == null || prefix.isEmpty()) {
      assertThat(props)
          .doesNotContainKeys(
              UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY,
              UCHadoopConfConstants.UC_CREDENTIAL_PREFIXES_KEY);
    } else {
      assertThat(props)
          .doesNotContainKey(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY)
          .containsEntry(
              UCHadoopConfConstants.UC_CREDENTIAL_PREFIXES_KEY,
              CredentialUtil.encodeCredPrefixes(List.of(prefix))[0]);
    }
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

    Map<String, String> expected =
        expected(CredKind.TABLE, true, false, new Configuration(false), null, Map.of());
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
            DELTA_TABLE,
            location(),
            op,
            Map.of());

    Map<String, String> expected =
        expected(CredKind.DELTA_TABLE, true, false, new Configuration(false), null, Map.of());
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

    Map<String, String> expected =
        expected(CredKind.PATH, true, false, new Configuration(false), null, Map.of());
    expected.put(UCHadoopConfConstants.UC_PATH_OPERATION_KEY, op.value());
    assertThat(props).containsExactlyInAnyOrderEntriesOf(expected);
  }

  // ---- expected-map composition --------------------------------------------------------------

  private Map<String, String> expectedForMultipleCredentials(
      CredKind kind,
      boolean renew,
      boolean credScoped,
      Configuration conf,
      List<String> prefixes,
      Map<String, String> appVersions) {
    Map<String, String> expected = new HashMap<>(defaultKeys());
    expected.putAll(credIdKeys(kind));
    if (renew) {
      expected.putAll(renewableProviderKeys());
      expected.putAll(requestContext(appVersions));
    }
    if (prefixes != null) {
      expected.put(
          UCHadoopConfConstants.UC_CREDENTIAL_PREFIXES_KEY,
          String.join(",", CredentialUtil.encodeCredPrefixes(prefixes)));
    }
    if (credScoped) {
      expected.putAll(customImplKeys(conf));
    }
    return expected;
  }

  private Map<String, String> expected(
      CredKind kind,
      boolean renew,
      boolean credScoped,
      Configuration conf,
      Long expiration,
      Map<String, String> appVersions) {
    Map<String, String> expected = new HashMap<>(defaultKeys());
    expected.putAll(credIdKeys(kind));
    if (renew) {
      expected.putAll(initialCredKeys(expiration));
      expected.putAll(renewableProviderKeys());
      expected.putAll(requestContext(appVersions));
    } else {
      expected.putAll(staticCredKeys(expiration));
    }
    expected.put(
        UCHadoopConfConstants.UC_CREDENTIAL_PREFIXES_KEY,
        CredentialUtil.encodeCredPrefixes(List.of(location()))[0]);
    if (credScoped) {
      expected.putAll(customImplKeys(conf));
    }
    return expected;
  }

  /** Verify the credScopedFS impl overrides are applied, and the original values are preserved. */
  private Map<String, String> customImplKeys(Configuration conf) {
    Map<String, String> keys = new HashMap<>();
    addImplOverrides(keys, fileSystemImplKeys(), CRED_SCOPED_FS, conf);
    addImplOverrides(keys, abstractFileSystemImplKeys(), CRED_SCOPED_AFS, conf);
    return keys;
  }

  private static void addImplOverrides(
      Map<String, String> keys,
      Map<String, String> defaults,
      String credScopedWrapper,
      Configuration conf) {
    defaults.forEach(
        (implKey, defaultImpl) -> {
          keys.put(implKey, credScopedWrapper);
          keys.put(implKey + ".original", conf.get(implKey, defaultImpl));
        });
  }

  /** Cloud-agnostic renewal context (URI, auth, engine versions). Cred identity is separate. */
  private Map<String, String> requestContext(Map<String, String> appVersions) {
    Map<String, String> keys = new HashMap<>();
    keys.put(UCHadoopConfConstants.UC_URI_KEY, CATALOG_URI);
    tokenProvider()
        .configs()
        .forEach((k, v) -> keys.put(UCHadoopConfConstants.UC_AUTH_PREFIX + k, v));
    appVersions.forEach(
        (engine, version) ->
            keys.put(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + engine, version));
    return keys;
  }

  /**
   * The credential-identity props for {@code kind}. Built from the corresponding {@code CredId} so
   * the concrete prop keys stay encapsulated in the {@code CredId} subclasses rather than being
   * duplicated here.
   */
  private Map<String, String> credIdKeys(CredKind kind) {
    String contextId = CredPropsUtil.credContextId(CATALOG_URI, scheme(), tokenProvider());
    switch (kind) {
      case TABLE:
        return new TableCredId(contextId, TABLE_ID, "READ_WRITE").props();
      case DELTA_TABLE:
        return new DeltaTableCredId(contextId, DELTA_TABLE, "READ_WRITE", location()).props();
      case DELTA_STAGING:
        return new DeltaStagingTableCredId(contextId, STAGING_TABLE_ID, location()).props();
      case PATH:
        return new PathCredId(contextId, location(), "PATH_READ_WRITE").props();
      default:
        throw new IllegalArgumentException("Unhandled kind: " + kind);
    }
  }

  // ---- entry-point dispatch ------------------------------------------------------------------

  private List<String> createPrefixes(int count) {
    return IntStream.range(0, count)
        .mapToObj(index -> location() + "/" + index)
        .collect(Collectors.toList());
  }

  private List<List<String>> malformedPrefixLists() {
    String validPrefix = location() + "/valid";
    return List.of(
        List.of(validPrefix, ""),
        List.of("", validPrefix),
        Arrays.asList(validPrefix, null),
        Arrays.asList(null, validPrefix),
        List.of("", ""),
        Arrays.asList(null, null));
  }

  private static List<String> nonEmptyPrefixes(List<String> prefixes) {
    return prefixes.stream()
        .filter(prefix -> prefix != null && !prefix.isEmpty())
        .collect(Collectors.toList());
  }

  private void setupMockFetcher(boolean expiring, List<String> prefixes) {
    Long expiration = expiring ? EXPIRATION_MILLIS : null;
    CredPropsUtil.initialCredCache.clear();
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            mockGenericCredentialFetcher(
                prefixes.stream()
                    .map(prefix -> vendedCred(expiration, prefix))
                    .toArray(GenericCredential[]::new));
  }

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
            DELTA_TABLE,
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

  static GenericCredentialFetcher mockGenericCredentialFetcher(GenericCredential... creds) {
    GenericCredentialFetcher api = mock(GenericCredentialFetcher.class);
    try {
      when(api.createCredentials()).thenReturn(List.of(creds));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return api;
  }
}
