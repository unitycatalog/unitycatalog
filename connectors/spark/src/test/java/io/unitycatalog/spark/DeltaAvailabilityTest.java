package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Pure-helper tests for {@link DeltaAvailability}: the semver / reflective-version helpers and the
 * {@link DeltaAvailability#isDeltaRestApiReady} predicate that decides whether a managed-Delta
 * CREATE goes through the new UC Delta API path.
 */
public class DeltaAvailabilityTest {

  // ---------- compareVersions ----------

  @Test
  public void compareVersions_equalVersions_returnsZero() {
    assertThat(DeltaAvailability.compareVersions("1.2.3", "1.2.3")).isZero();
  }

  @Test
  public void compareVersions_majorMinorPatchOrdering() {
    assertThat(DeltaAvailability.compareVersions("2.0.0", "1.9.9")).isPositive();
    assertThat(DeltaAvailability.compareVersions("1.2.3", "1.1.99")).isPositive();
    assertThat(DeltaAvailability.compareVersions("1.2.3", "1.2.2")).isPositive();
    assertThat(DeltaAvailability.compareVersions("1.2.2", "1.2.3")).isNegative();
  }

  @Test
  public void compareVersions_missingPartsTreatedAsZero() {
    // "5" should compare equal to "5.0.0" and "5.0".
    assertThat(DeltaAvailability.compareVersions("5", "5.0.0")).isZero();
    assertThat(DeltaAvailability.compareVersions("5.0", "5.0.0")).isZero();
    assertThat(DeltaAvailability.compareVersions("5", "5.0.1")).isNegative();
  }

  @Test
  public void compareVersions_preReleaseSuffixIgnored() {
    // The "-SNAPSHOT" suffix on either side must be stripped before numeric comparison.
    assertThat(DeltaAvailability.compareVersions("4.3.0-SNAPSHOT", "4.3.0")).isZero();
    assertThat(DeltaAvailability.compareVersions("4.3.0", "4.3.0-RC1")).isZero();
    assertThat(DeltaAvailability.compareVersions("4.3.0-RC1", "4.3.0-FINAL")).isZero();
    assertThat(DeltaAvailability.compareVersions("4.3.1-SNAPSHOT", "4.3.0")).isPositive();
  }

  @Test
  public void compareVersions_nonNumericPiecesTreatedAsZero() {
    // "x" parses as 0 via the NumberFormatException fallback; "1.x.3" == "1.0.3".
    assertThat(DeltaAvailability.compareVersions("1.x.3", "1.0.3")).isZero();
    assertThat(DeltaAvailability.compareVersions("1.x.3", "1.1.3")).isNegative();
  }

  @Test
  public void compareVersions_asymmetricLengths() {
    // Extra trailing parts beat shorter versions only if non-zero.
    assertThat(DeltaAvailability.compareVersions("1.2.3.4", "1.2.3")).isPositive();
    assertThat(DeltaAvailability.compareVersions("1.2.3.0", "1.2.3")).isZero();
    assertThat(DeltaAvailability.compareVersions("1.2.3", "1.2.3.4")).isNegative();
  }

  // ---------- deltaVersionAtLeast ----------

  @Test
  public void deltaVersionAtLeast_classpathBundledDeltaBeatsZero() {
    // Delta is on the connectors/spark test classpath, so the reflective lookup succeeds and
    // returns a real version string; anything compared to "0.0.0" must be >=.
    assertThat(DeltaAvailability.deltaVersionAtLeast("0.0.0")).isTrue();
  }

  @Test
  public void deltaVersionAtLeast_unrealisticallyHighTargetReturnsFalse() {
    assertThat(DeltaAvailability.deltaVersionAtLeast("999.0.0")).isFalse();
  }

  // ---------- isDeltaRestApiReady ----------

  @Test
  public void isDeltaRestApiReady_deltaCatalogNotLoaded_returnsFalse() {
    assertThat(DeltaAvailability.isDeltaRestApiReady(false, true)).isFalse();
  }

  @Test
  public void isDeltaRestApiReady_deltaRestApiNotEnabled_returnsFalse() {
    assertThat(DeltaAvailability.isDeltaRestApiReady(true, false)).isFalse();
  }

  @Test
  public void isDeltaRestApiReady_bothFalse_returnsFalse() {
    assertThat(DeltaAvailability.isDeltaRestApiReady(false, false)).isFalse();
  }

  @Test
  public void isDeltaRestApiReady_bothTrueButVersionTooOld_returnsFalse() {
    // Skip when the bundled Delta on the test classpath is already at/past
    // MIN_DELTA_VERSION_FOR_REST_API (e.g. CI's Delta master-SNAPSHOT). The version conjunct's
    // false branch is what this test is for; sibling tests cover the other two conjuncts.
    Assumptions.assumeFalse(
        DeltaAvailability.deltaVersionAtLeast(DeltaAvailability.MIN_DELTA_VERSION_FOR_REST_API()),
        "bundled Delta is >= MIN_DELTA_VERSION_FOR_REST_API; version-too-old case is moot");
    assertThat(DeltaAvailability.isDeltaRestApiReady(true, true)).isFalse();
  }
}
