package io.unitycatalog.server.migrate.kit;

import io.unitycatalog.server.migrate.SchemaMigrator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.ParameterDeclarations;

/**
 * The ways a database reaches this code: the old servers that write it, run in order on one
 * database. This enum is the only list. {@value #ENV} (a comma list of names; unset means all) runs
 * a subset.
 */
public enum UpgradePath {
  /** The server jar built from the code this change starts from ({@value #BASE_REF_ENV}). */
  BASE(List.of(baseRef())),
  /**
   * One server jar per release, oldest first: the latest patch of each minor release from {@link
   * SchemaMigrator#BASELINE_RELEASE} up to, not including, this build's version. The release tags
   * in {@value #RELEASE_CHAIN_ENV} (space-separated), or else the repository's {@code vX.Y.Z} tags.
   */
  RELEASE_CHAIN(releaseChain());

  public static final String ENV = "UC_TEST_UPGRADE_PATHS";
  public static final String BASE_REF_ENV = "UC_TEST_BASE_REF";
  public static final String RELEASE_CHAIN_ENV = "UC_TEST_RELEASE_CHAIN";

  public final List<OldImage> images;

  UpgradePath(List<String> refs) {
    this.images = refs.stream().map(OldImage::jarBuiltFrom).toList();
  }

  /** The git ref {@link #BASE} is built from: {@value #BASE_REF_ENV}, unset or empty means main. */
  private static String baseRef() {
    String ref = System.getenv(BASE_REF_ENV);
    return ref == null || ref.isBlank() ? "origin/main" : ref;
  }

  private static List<String> releaseChain() {
    String chain = System.getenv(RELEASE_CHAIN_ENV);
    if (chain != null && !chain.isBlank()) {
      return List.of(chain.trim().split("\\s+"));
    }
    // ThisBuild / version := "0.8.0-SNAPSHOT"
    String sbt = OldImage.git("show", "HEAD:version.sbt");
    String version = sbt.substring(sbt.indexOf('"') + 1, sbt.lastIndexOf('"'));
    return releaseChain(OldImage.git("tag", "--list").lines(), version);
  }

  /** The tags {@link #RELEASE_CHAIN} runs, given all tags and this build's version. */
  static List<String> releaseChain(Stream<String> tags, String version) {
    return tags
        .filter(tag -> tag.matches("v\\d+\\.\\d+\\.\\d+"))
        .filter(
            tag ->
                number(tag) >= number(SchemaMigrator.BASELINE_RELEASE)
                    && number(tag) < number(version))
        .sorted(Comparator.comparingLong(UpgradePath::number))
        .collect(
            Collectors.toMap(
                tag -> number(tag) / 1_000_000, tag -> tag, (older, newer) -> newer, TreeMap::new))
        .values()
        .stream()
        .toList();
  }

  /** {@code v1.2.3} or {@code 1.2.3-SNAPSHOT} as a number that sorts like the version. */
  private static long number(String version) {
    String[] parts = version.replaceFirst("^v", "").replaceFirst("-.*", "").split("\\.");
    return (Long.parseLong(parts[0]) * 1_000_000 + Long.parseLong(parts[1])) * 1_000_000
        + Long.parseLong(parts[2]);
  }

  /** The paths {@value #ENV} selects. */
  public static List<UpgradePath> selected() {
    String value = System.getenv(ENV);
    if (value == null || value.isBlank()) {
      return List.of(values());
    }
    Set<String> names =
        Arrays.stream(value.split(",")).map(String::trim).collect(Collectors.toSet());
    List<UpgradePath> paths =
        Arrays.stream(values()).filter(path -> names.remove(path.name())).toList();
    if (!names.isEmpty()) {
      throw new IllegalArgumentException(
          ENV + "=" + value + " names " + names + ", not in " + Arrays.toString(values()));
    }
    return paths;
  }

  /** Supplies {@link #selected()} to {@link AcrossUpgradePaths}. */
  public static final class Provider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(
        ParameterDeclarations parameters, ExtensionContext context) {
      return selected().stream().map(Arguments::of);
    }
  }
}
