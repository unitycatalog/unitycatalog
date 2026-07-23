package io.unitycatalog.hadoop.internal.fs;

import static io.unitycatalog.hadoop.internal.id.CredIdTest.EMPTY_CRED_CONTEXT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies the namespaced-credential mechanism of {@link CredScopedFileSystem}: reading the count,
 * selecting the credential whose location covers the accessed URI, and overlaying that credential's
 * keys (and only that credential's keys) onto the delegate filesystem's configuration.
 *
 * <p>Uses {@code file://} URIs with the local filesystem so no cloud SDK is required. The
 * delegate's effective configuration is inspected via {@link CredScopedFileSystem#getDelegate()}.
 */
class CredScopedFileSystemNamespaceTest {

  private static final String TEST_CREDENTIAL_KEY = "fs.unitycatalog.test.credential";
  private static final String ACCESS_KEY = "fs.cloud.access.key";
  private static final String LOCATION_KEY = UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY;

  @AfterEach
  void clearCache() {
    CredScopedFileSystem.clearCacheForTesting();
  }

  private static FileSystem initDelegate(URI uri, Configuration conf) throws Exception {
    CredScopedFileSystem fs = new CredScopedFileSystem();
    fs.initialize(uri, conf);
    return fs.getDelegate();
  }

  private static Configuration tableConf() {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY, EMPTY_CRED_CONTEXT_ID);
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_TABLE_ID_KEY, "tid-1");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ");
    conf.set("fs.file.impl.disable.cache", "true");
    return conf;
  }

  private static void setScopedKey(Configuration conf, int index, String key, String value) {
    conf.set(UCHadoopConfConstants.UC_SCOPED_CRED_PREFIX + index + "." + key, value);
  }

  /** A snapshot of every entry in {@code conf}, for asserting the source config is not mutated. */
  private static Map<String, String> snapshot(Configuration conf) {
    Map<String, String> entries = new HashMap<>();
    for (Map.Entry<String, String> entry : conf) {
      entries.put(entry.getKey(), entry.getValue());
    }
    return entries;
  }

  // --- count == 0 (legacy top-level layout, no prefix matching)
  // -----------------------------------------------------

  @Test
  void legacyLayoutNeitherLiftsNorOverridesWithNamespacedKeys() throws Exception {
    Configuration conf = tableConf();
    // Backwards compatibility test, scoped cred count not set.
    conf.set(TEST_CREDENTIAL_KEY, "top-level-credential");
    setScopedKey(conf, 0, LOCATION_KEY, "file:///tmp/table");
    setScopedKey(conf, 0, TEST_CREDENTIAL_KEY, "namespaced-0");
    setScopedKey(conf, 1, LOCATION_KEY, "file:///tmp/b");
    setScopedKey(conf, 1, TEST_CREDENTIAL_KEY, "namespaced-1");
    setScopedKey(conf, 2, LOCATION_KEY, "file:///tmp/c");
    setScopedKey(conf, 2, TEST_CREDENTIAL_KEY, "namespaced-2");
    Map<String, String> before = snapshot(conf);

    FileSystem delegate = initDelegate(new URI("file:///tmp/table/data"), conf);

    assertThat(delegate.getConf().get(TEST_CREDENTIAL_KEY)).isEqualTo("top-level-credential");
    // The source configuration is not mutated by initialization.
    assertThat(snapshot(conf)).isEqualTo(before);
  }

  // --- count > 1 requires selection and overlay
  // ----------------------------------------------------------

  /**
   * Across credential sets of varying size, the one whose location is the longest prefix covering
   * the URI is selected, and exactly that credential's keys are lifted to the top level (overriding
   * any stale top-level placeholder), while the source configuration is left unchanged.
   */
  @ParameterizedTest(name = "creds={0} uri={1} selects index {2}")
  @MethodSource("multiCredentialSelectionCases")
  void multipleScopedCredentialsSelectAndOverlayCoveringCredential(
      List<Map<String, String>> credentials, String uri, int expectedIndex) throws Exception {
    Configuration conf = tableConf();
    conf.setInt(UCHadoopConfConstants.UC_SCOPED_CRED_COUNT_KEY, credentials.size());
    for (int i = 0; i < credentials.size(); i++) {
      for (Map.Entry<String, String> key : credentials.get(i).entrySet()) {
        setScopedKey(conf, i, key.getKey(), key.getValue());
      }
    }
    Map<String, String> before = snapshot(conf);

    FileSystem delegate = initDelegate(new URI(uri), conf);

    // The selected credential's keys are lifted to the top level.
    Configuration delegateConf = delegate.getConf();
    credentials
        .get(expectedIndex)
        .forEach((key, value) -> assertThat(delegateConf.get(key)).isEqualTo(value));
    // The source configuration is not mutated by the overlay.
    assertThat(snapshot(conf)).isEqualTo(before);
  }

  private static Map<String, String> credential(int i, String location) {
    Map<String, String> keys = new HashMap<>();
    if (location != null) {
      keys.put(LOCATION_KEY, location);
    }
    keys.put(TEST_CREDENTIAL_KEY, "cred-" + i);
    keys.put(ACCESS_KEY, "ak-" + i);
    return keys;
  }

  private static Stream<Arguments> multiCredentialSelectionCases() {
    return Stream.of(
        Arguments.of(
            List.of(credential(0, "file:///tmp/table"), credential(1, "file:///tmp/table/clone")),
            "file:///tmp/table/clone/data",
            1),
        Arguments.of(
            List.of(credential(0, "file:///tmp/table"), credential(1, "file:///tmp/table/clone")),
            "file:///tmp/table/other",
            0),
        Arguments.of(
            List.of(
                credential(0, "file:///tmp/a"),
                credential(1, "file:///tmp/a/b"),
                credential(2, "file:///tmp/c")),
            "file:///tmp/a/b/data",
            1));
  }

  // --- malformed configuration ------------------------------------------------------------------

  /**
   * Malformed namespaced configurations are rejected with a descriptive {@link
   * IllegalArgumentException}.
   */
  @ParameterizedTest(name = "{3}")
  @MethodSource("malformedConfigurationCases")
  void malformedConfigurationIsRejected(
      int count, List<Map<String, String>> credentials, String uri, String expectedMessage) {
    Configuration conf = tableConf();
    conf.setInt(UCHadoopConfConstants.UC_SCOPED_CRED_COUNT_KEY, count);
    for (int i = 0; i < credentials.size(); i++) {
      for (Map.Entry<String, String> key : credentials.get(i).entrySet()) {
        setScopedKey(conf, i, key.getKey(), key.getValue());
      }
    }

    assertThatThrownBy(() -> initDelegate(new URI(uri), conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedMessage);
  }

  private static Stream<Arguments> malformedConfigurationCases() {
    List<Map<String, String>> twoCreds =
        List.of(credential(0, "file:///tmp/a"), credential(1, "file:///tmp/b"));
    return Stream.of(
        // No encoded credential covers the accessed location.
        Arguments.of(2, twoCreds, "file:///tmp/other/data", "file:///tmp/other/data"),
        // A credential in the set is missing its location key.
        Arguments.of(
            2,
            List.of(credential(0, "file:///tmp/a"), credential(1, null)),
            "file:///tmp/a/data",
            "missing its location"),
        // Count claims more credentials than are encoded; index 2 is a phantom with no keys.
        Arguments.of(
            3, twoCreds, "file:///tmp/a/data", "Scoped credential 2 is missing its location"),
        // Count of one: a single credential must use the legacy top-level layout, not a namespace.
        Arguments.of(
            1,
            List.of(credential(0, "file:///tmp/table")),
            "file:///tmp/table/data",
            "must be greater than 1"),
        // Negative count.
        Arguments.of(
            -1, List.<Map<String, String>>of(), "file:///tmp/table/data", "must be greater than 1"),
        // Count above the hard ceiling.
        Arguments.of(
            Integer.MAX_VALUE,
            List.<Map<String, String>>of(),
            "file:///tmp/table/data",
            "must be greater than 1"));
  }
}
