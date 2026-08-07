package io.unitycatalog.hadoop.internal.fs;

import static io.unitycatalog.hadoop.internal.id.CredIdTest.EMPTY_CRED_CONTEXT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.hadoop.internal.CredentialUtil;
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

/** Verifies prefix selection for multi-credential configurations. */
class CredScopedFileSystemNamespaceTest {

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
    Configuration conf = new Configuration(false);
    conf.set(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY, EMPTY_CRED_CONTEXT_ID);
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_TABLE_ID_KEY, "tid-1");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ");
    conf.set("fs.file.impl.disable.cache", "true");
    return conf;
  }

  private static Map<String, String> snapshot(Configuration conf) {
    Map<String, String> entries = new HashMap<>();
    for (Map.Entry<String, String> entry : conf) {
      entries.put(entry.getKey(), entry.getValue());
    }
    return entries;
  }

  private static void setPrefixes(Configuration conf, List<String> prefixes) {
    conf.setStrings(
        UCHadoopConfConstants.UC_CREDENTIAL_PREFIXES_KEY,
        CredentialUtil.encodeCredPrefixes(prefixes));
  }

  @Test
  void missingOrEmptyCredPrefixesDoesNotSetPrefix() throws Exception {
    Configuration missing = tableConf();
    Configuration empty = tableConf();
    empty.set(UCHadoopConfConstants.UC_CREDENTIAL_PREFIXES_KEY, "");

    for (Configuration conf : List.of(missing, empty)) {
      Map<String, String> before = snapshot(conf);
      FileSystem delegate = initDelegate(new URI("file:///tmp/table/data"), conf);

      assertThat(delegate.getConf().get(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY)).isNull();
      assertThat(snapshot(conf)).isEqualTo(before);
    }
  }

  @Test
  void singleCredPrefixDoesNotDoSelection() throws Exception {
    Configuration conf = tableConf();
    String prefix = "file:///tmp/other";
    setPrefixes(conf, List.of(prefix));
    Map<String, String> before = snapshot(conf);

    // The prefix does not cover the URI, so multi-credential selection would throw.
    FileSystem delegate = initDelegate(new URI("file:///tmp/table/data"), conf);

    assertThat(delegate.getConf().get(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY))
        .isEqualTo(prefix);
    assertThat(snapshot(conf)).isEqualTo(before);
  }

  @ParameterizedTest(name = "prefixes={0} uri={1} selects {2}")
  @MethodSource("multiCredentialSelectionCases")
  void multiCredentialPrefixListSelectsLongestCoveringPrefix(
      List<String> prefixes, String uri, String expectedPrefix) throws Exception {
    Configuration conf = tableConf();
    setPrefixes(conf, prefixes);
    Map<String, String> before = snapshot(conf);

    FileSystem delegate = initDelegate(new URI(uri), conf);

    assertThat(delegate.getConf().get(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY))
        .isEqualTo(expectedPrefix);
    assertThat(snapshot(conf)).isEqualTo(before);
  }

  private static Stream<Arguments> multiCredentialSelectionCases() {
    return Stream.of(
        Arguments.of(
            List.of("file:///tmp/table", "file:///tmp/table/child"),
            "file:///tmp/table/child/data",
            "file:///tmp/table/child"),
        Arguments.of(
            List.of("file:///tmp/table", "file:///tmp/table/child"),
            "file:///tmp/table/other",
            "file:///tmp/table"),
        Arguments.of(
            List.of("file:///tmp/a", "file:///tmp/a/b", "file:///tmp/c"),
            "file:///tmp/a/b",
            "file:///tmp/a/b"),
        Arguments.of(
            List.of("file:///tmp/a", "file:///tmp/a/b", "file:///tmp/c"),
            "file:///tmp/c/data",
            "file:///tmp/c"),
        Arguments.of(
            List.of("file:///tmp/table", "file:///tmp/table,archive"),
            "file:///tmp/table,archive/data",
            "file:///tmp/table,archive"));
  }

  @ParameterizedTest
  @MethodSource("uncoveredLocationCases")
  void multiCredentialPrefixesRejectUncoveredLocation(List<String> prefixes, String uri) {
    Configuration conf = tableConf();
    setPrefixes(conf, prefixes);

    assertThatThrownBy(() -> initDelegate(new URI(uri), conf))
        .hasMessageContaining("No credential covers storage location");
  }

  private static Stream<Arguments> uncoveredLocationCases() {
    return Stream.of(
        Arguments.of(List.of("file:///tmp/a", "file:///tmp/b"), "file:///tmp/other/data"),
        Arguments.of(
            List.of("file:///tmp/table", "file:///tmp/table/child"),
            "file:///tmp/table-other/data"),
        Arguments.of(List.of("s3://bucket/a", "s3://bucket/b"), "s3a://bucket/a/data"),
        Arguments.of(List.of("s3://bucket/a", "s3://bucket/b"), "gs://bucket/a/data"));
  }

  @Test
  void malformedPrefixListIsRejected() {
    Configuration conf = tableConf();
    String validPrefix = CredentialUtil.encodeCredPrefixes(List.of("file:///tmp/valid"))[0];
    conf.setStrings(UCHadoopConfConstants.UC_CREDENTIAL_PREFIXES_KEY, validPrefix, "not-base64!");

    assertThatThrownBy(() -> initDelegate(new URI("file:///tmp/credential/data"), conf))
        .hasMessageContaining("Illegal base64");
  }
}
