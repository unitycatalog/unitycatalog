package io.unitycatalog.server.testdatabase;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** Keeps the sbt aliases in build.sbt in sync with {@link SupportedDatabase}. */
class TestDatabaseAliasesTest {

  private static final Pattern ALIAS =
      Pattern.compile("addCommandAlias\\(\"(\\w+)\", testOnDatabase\\(\"(\\w+)\"\\)\\)");

  static String aliasName(SupportedDatabase database) {
    return "test"
        + Arrays.stream(database.name().split("_"))
            .map(part -> part.charAt(0) + part.substring(1).toLowerCase())
            .collect(Collectors.joining());
  }

  @Test
  void everyDatabaseExceptH2HasAnAliasAndEveryAliasNamesADatabase() throws Exception {
    // Server tests run with user.dir set to the repository root, where build.sbt lives.
    String buildSbt = Files.readString(Path.of("build.sbt"));
    Map<String, String> aliases = new TreeMap<>();
    Matcher matcher = ALIAS.matcher(buildSbt);
    while (matcher.find()) {
      aliases.put(matcher.group(1), matcher.group(2));
    }
    Map<String, String> expected =
        Arrays.stream(SupportedDatabase.values())
            .filter(database -> database != SupportedDatabase.H2)
            .collect(
                Collectors.toMap(
                    TestDatabaseAliasesTest::aliasName, Enum::name, (a, b) -> a, TreeMap::new));
    assertThat(aliases).isEqualTo(expected);
  }
}
