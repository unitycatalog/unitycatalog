package io.unitycatalog.server.migrate.kit;

import io.unitycatalog.server.migrate.SchemaMigrator;
import io.unitycatalog.server.utils.VersionUtils;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * The old servers a database passes through before this code, run in order on one database: one
 * server jar per release, oldest first, every release from {@link SchemaMigrator#BASELINE_RELEASE}
 * up to, not including, this build's version, from the repository's {@code vX.Y.Z} tags.
 *
 * <p>It is the only upgrade path the tests take:
 *
 * <ul>
 *   <li>Supported upgrades start from a database of the baseline release or later and go one
 *       release at a time, so the path walks every release from the baseline, then the current
 *       code.
 *   <li>Each release writes its own copy of the data: an upgrade can break on rows only one release
 *       wrote, such as a column it left NULL that a later step makes NOT NULL, a value a middle
 *       release stored differently and a later one fixed without a backfill, or rows an x.y.0
 *       release wrote that its later patches don't.
 *   <li>No path starts from {@code main}: data unreleased code writes never reaches a released
 *       user's database, since its steps ship with the change, so it would only find the same
 *       problems earlier.
 *   <li>No path ends at the latest release alone: the chain's last release writes its own copy.
 * </ul>
 */
public final class UpgradePath {

  private UpgradePath() {}

  /** The release chain's server jars, oldest first. */
  public static List<OldImage> releaseChain() {
    return releaseTags().stream().map(OldImage::jarBuiltFrom).toList();
  }

  private static List<String> releaseTags() {
    return releaseTags(OldImage.git("tag", "--list").lines(), VersionUtils.VERSION);
  }

  /** The release tags of the chain, given all tags and this build's version. */
  static List<String> releaseTags(Stream<String> tags, String version) {
    return tags.filter(tag -> tag.matches("v\\d+\\.\\d+\\.\\d+"))
        .filter(
            tag ->
                number(tag) >= number(SchemaMigrator.BASELINE_RELEASE)
                    && number(tag) < number(version))
        .sorted(Comparator.comparingLong(UpgradePath::number))
        .toList();
  }

  /** {@code v1.2.3} or {@code 1.2.3-SNAPSHOT} as a number that sorts like the version. */
  private static long number(String version) {
    String[] parts = version.replaceFirst("^v", "").replaceFirst("-.*", "").split("\\.");
    return (Long.parseLong(parts[0]) * 1_000_000 + Long.parseLong(parts[1])) * 1_000_000
        + Long.parseLong(parts[2]);
  }
}
