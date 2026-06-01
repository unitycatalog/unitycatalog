package io.unitycatalog.spark;

import static io.unitycatalog.spark.DeltaVersionUtils.MIN_DELTA_VERSION_FOR_REST_API;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DeltaVersionUtils}: the reflective-version helper and the {@link
 * DeltaVersionUtils#isDeltaRestApiReady} predicate that decides whether a managed-Delta CREATE goes
 * through the new UC Delta API path.
 */
public class DeltaVersionUtilsTest {

  // ---------- isDeltaAtLeast ----------

  @Test
  public void isDeltaAtLeast_classpathBundledDeltaBeatsZero() {
    // Delta is on the connectors/spark test classpath, so the reflective lookup succeeds and
    // returns a real version string; anything compared to "0.0.0" must be >=.
    assertThat(DeltaVersionUtils.isDeltaAtLeast("0.0.0")).isTrue();
  }

  @Test
  public void isDeltaAtLeast_unrealisticallyHighTargetReturnsFalse() {
    assertThat(DeltaVersionUtils.isDeltaAtLeast("999.0.0")).isFalse();
  }

  // ---------- isDeltaRestApiReady ----------

  @Test
  public void isDeltaRestApiReady_deltaCatalogNotLoaded_returnsFalse() {
    assertThat(DeltaVersionUtils.isDeltaRestApiReady(false, true)).isFalse();
  }

  @Test
  public void isDeltaRestApiReady_deltaRestApiNotEnabled_returnsFalse() {
    assertThat(DeltaVersionUtils.isDeltaRestApiReady(true, false)).isFalse();
  }

  @Test
  public void isDeltaRestApiReady_bothFalse_returnsFalse() {
    assertThat(DeltaVersionUtils.isDeltaRestApiReady(false, false)).isFalse();
  }

  @Test
  public void isDeltaRestApiReady_bothTrueButVersionTooOld_returnsFalse() {
    // Skip when the bundled Delta on the test classpath is already at/past
    // MIN_DELTA_VERSION_FOR_REST_API (e.g. CI's Delta master-SNAPSHOT). The version conjunct's
    // false branch is what this test is for; sibling tests cover the other two conjuncts.
    Assumptions.assumeFalse(
        DeltaVersionUtils.isDeltaAtLeast(MIN_DELTA_VERSION_FOR_REST_API),
        "bundled Delta is >= MIN_DELTA_VERSION_FOR_REST_API; version-too-old case is moot");
    assertThat(DeltaVersionUtils.isDeltaRestApiReady(true, true)).isFalse();
  }
}
