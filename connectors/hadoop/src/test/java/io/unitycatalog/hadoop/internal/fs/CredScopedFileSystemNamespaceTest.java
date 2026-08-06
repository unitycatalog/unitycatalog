package io.unitycatalog.hadoop.internal.fs;

import static io.unitycatalog.hadoop.internal.id.CredIdTest.EMPTY_CRED_CONTEXT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.hadoop.internal.CredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

  private static final int MAX_COUNT = 10;

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
        UCHadoopConfConstants.UC_MULTI_CRED_PREFIXES_KEY,
        CredentialUtil.encodeMultiCredPrefixes(prefixes));
  }

  @Test
  void missingOrEmptyMultiCredentialPrefixesUseSingleCredentialPath() throws Exception {
    Configuration missing = tableConf();
    Configuration empty = tableConf();
    empty.set(UCHadoopConfConstants.UC_MULTI_CRED_PREFIXES_KEY, "");

    for (Configuration conf : List.of(missing, empty)) {
      Map<String, String> before = snapshot(conf);
      FileSystem delegate = initDelegate(new URI("file:///tmp/table/data"), conf);

      assertThat(delegate.getConf().get(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY)).isNull();
      assertThat(snapshot(conf)).isEqualTo(before);
    }
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
            "file:///tmp/a/b/data",
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

  @Test
  void defaultMaximumCredentialCountIsAccepted() throws Exception {
    List<String> prefixes =
        IntStream.range(0, MAX_COUNT)
            .mapToObj(i -> "file:///tmp/credential-" + i)
            .collect(Collectors.toList());
    Configuration conf = tableConf();
    setPrefixes(conf, prefixes);

    FileSystem delegate =
        initDelegate(new URI("file:///tmp/credential-" + (MAX_COUNT - 1) + "/data"), conf);

    assertThat(delegate.getConf().get(UCHadoopConfConstants.UC_CREDENTIAL_PREFIX_KEY))
        .isEqualTo("file:///tmp/credential-" + (MAX_COUNT - 1));
  }

  @Test
  void maximumCredentialCountIsCheckedBeforeDecoding() {
    Configuration conf = tableConf();
    String[] invalidEncodedPrefixes =
        Collections.nCopies(MAX_COUNT + 1, "not-base64!").toArray(String[]::new);
    conf.setStrings(UCHadoopConfConstants.UC_MULTI_CRED_PREFIXES_KEY, invalidEncodedPrefixes);

    assertThatThrownBy(() -> initDelegate(new URI("file:///tmp/credential/data"), conf))
        .hasMessageContaining("between 2 and " + MAX_COUNT);
  }

  @ParameterizedTest(name = "{2}")
  @MethodSource("malformedPrefixLists")
  void malformedPrefixListIsRejected(List<String> prefixes, String uri, String expectedMessage) {
    Configuration conf = tableConf();
    setPrefixes(conf, prefixes);

    assertThatThrownBy(() -> initDelegate(new URI(uri), conf))
        .hasMessageContaining(expectedMessage);
  }

  private static Stream<Arguments> malformedPrefixLists() {
    return Stream.of(
        Arguments.of(
            List.of("file:///tmp/a", "file:///tmp/b"),
            "file:///tmp/other/data",
            "No credential covers storage location"),
        Arguments.of(
            List.of("file:///tmp/table"),
            "file:///tmp/table/data",
            "Number of credentials must be between 2 and " + MAX_COUNT + ": got 1"));
  }
}
