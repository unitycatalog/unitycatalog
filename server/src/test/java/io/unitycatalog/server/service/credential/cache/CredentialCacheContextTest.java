package io.unitycatalog.server.service.credential.cache;

import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class CredentialCacheContextTest {

  private static final NormalizedURL LOC = NormalizedURL.from("s3://bucket/tableA");
  private static final NormalizedURL OTHER_LOC = NormalizedURL.from("s3://bucket/tableB");

  private static CredentialCacheKey key(
      NormalizedURL loc, UriScheme scheme, Set<CredentialContext.Privilege> privs, String roleArn) {
    return new CredentialCacheKey(loc, privs, scheme, roleArn);
  }

  /** Two-arg key helper using LOC and S3 scheme. */
  private static CredentialCacheKey key(String roleArn, Set<CredentialContext.Privilege> privs) {
    return new CredentialCacheKey(LOC, privs, UriScheme.S3, roleArn);
  }

  private static CredentialCacheContext ctx(String roleArn, Long t1, long t2) {
    return new CredentialCacheContext(
        CredentialCacheContext.CURRENT_SCHEMA_VERSION,
        LOC,
        UriScheme.S3,
        Set.of(SELECT),
        roleArn,
        t1,
        t2);
  }

  // ─── effectiveExpiryEpochMs() ─────────────────────────────────────────────

  @Test
  void effectiveExpiry_t1LessThanT2_returnsT1() {
    assertEquals(100L, ctx("r", 100L, 200L).effectiveExpiryEpochMs());
  }

  @Test
  void effectiveExpiry_t1GreaterThanT2_returnsT2() {
    assertEquals(150L, ctx("r", 300L, 150L).effectiveExpiryEpochMs());
  }

  @Test
  void effectiveExpiry_t1EqualT2_returnsThatValue() {
    assertEquals(500L, ctx("r", 500L, 500L).effectiveExpiryEpochMs());
  }

  @Test
  void effectiveExpiry_staticCredential_returnsT2() {
    assertEquals(500L, ctx("r", null, 500L).effectiveExpiryEpochMs());
  }

  // ─── fresh(now, lead) ─────────────────────────────────────────────────────

  @Test
  void fresh_bothT1FarAndT2Far_returnsTrue() {
    long now = 1_000_000L;
    long lead = 60_000L;
    assertTrue(ctx("r", now + 600_000L, now + 600_000L).fresh(now, lead));
  }

  @Test
  void fresh_withinLeadOfT1_returnsFalse() {
    long now = 1_000_000L;
    long lead = 60_000L;
    // T1 only 30s away — inside the 60s lead window
    assertFalse(ctx("r", now + 30_000L, now + 600_000L).fresh(now, lead));
  }

  @Test
  void fresh_oneSecondInsideLead_returnsFalse() {
    // lead=60s, T1 = now+1s → 1s inside lead window → must re-vend
    long now = 1_000_000L;
    long lead = 60_000L;
    assertFalse(ctx("r", now + 1_000L, now + 600_000L).fresh(now, lead));
  }

  @Test
  void fresh_leadZero_justBeforeT1_returnsTrue() {
    long now = 1_000_000L;
    // lead=0: now < T1 → fresh (no subtraction)
    assertTrue(ctx("r", now + 1L, now + 600_000L).fresh(now, 0L));
  }

  @Test
  void fresh_leadZero_atT1_returnsFalse() {
    long now = 1_000_000L;
    // lead=0: now == T1 → not fresh (condition is strictly-less-than)
    assertFalse(ctx("r", now, now + 600_000L).fresh(now, 0L));
  }

  @Test
  void fresh_t2ExpiredWhileT1StillValid_returnsFalse() {
    long now = 1_000_000L;
    long lead = 60_000L;
    // T1 is far in the future but cache cap T2 has already passed
    assertFalse(ctx("r", now + 600_000L, now - 1).fresh(now, lead));
  }

  @Test
  void fresh_staticCredential_beforeT2_returnsTrue() {
    long now = 1_000_000L;
    long lead = 60_000L;
    // No T1 (static credential), T2 still valid
    assertTrue(ctx("r", null, now + 10_000L).fresh(now, lead));
  }

  @Test
  void fresh_staticCredential_atOrAfterT2_returnsFalse() {
    long now = 1_000_000L;
    long lead = 60_000L;
    // No T1, T2 has expired (at boundary and past it)
    assertFalse(ctx("r", null, now).fresh(now, lead));
    assertFalse(ctx("r", null, now - 1).fresh(now, lead));
  }

  @Test
  void fresh_exactLeadBoundary_returnsFalse() { // Gap 2
    long now = 1_000_000L, lead = 60_000L;
    // T1-lead == now -> strict < -> false
    assertFalse(ctx("r", now + lead, now + 600_000L).fresh(now, lead));
  }

  @Test
  void fresh_oneMsBeyondLeadBoundary_returnsTrue() { // Gap 2
    long now = 1_000_000L, lead = 60_000L;
    assertTrue(ctx("r", now + lead + 1, now + 600_000L).fresh(now, lead));
  }

  @Test
  void fresh_nonStaticAtT2Boundary_returnsFalse() { // Gap 4
    long now = 1_000_000L;
    // now < T2(==now) -> false; T2=now is positive (1_000_000L > 0)
    assertFalse(ctx("r", now + 600_000L, now).fresh(now, 60_000L));
  }

  @Test
  void fresh_negativeLead_throws() { // Gap 3
    assertThrows(
        IllegalArgumentException.class, () -> ctx("r", 1_000_000L, 2_000_000L).fresh(1_000L, -1L));
  }

  // ─── matches(key) ─────────────────────────────────────────────────────────

  @Test
  void matches_exactMatch_returnsTrue() {
    CredentialCacheContext c = ctx("arn:role/A", 0L, 1_000L);
    assertTrue(c.matches(key(LOC, UriScheme.S3, Set.of(SELECT), "arn:role/A")));
  }

  @Test
  void matches_differentLocation_returnsFalse() {
    CredentialCacheContext c = ctx("arn:role/A", 0L, 1_000L);
    assertFalse(c.matches(key(OTHER_LOC, UriScheme.S3, Set.of(SELECT), "arn:role/A")));
  }

  @Test
  void matches_differentScheme_returnsFalse() {
    CredentialCacheContext c = ctx("arn:role/A", 0L, 1_000L);
    NormalizedURL gcsLoc = NormalizedURL.from("gs://bucket/tableA");
    CredentialCacheKey gcsKey =
        new CredentialCacheKey(gcsLoc, Set.of(SELECT), UriScheme.GS, "arn:role/A");
    assertFalse(c.matches(gcsKey));
  }

  @Test
  void matches_differentPrivileges_returnsFalse() {
    CredentialCacheContext c = ctx("arn:role/A", 0L, 1_000L);
    // SELECT+UPDATE instead of just SELECT
    assertFalse(c.matches(key(LOC, UriScheme.S3, Set.of(SELECT, UPDATE), "arn:role/A")));
  }

  @Test
  void matches_differentRoleArn_returnsFalse() {
    CredentialCacheContext c = ctx("arn:role/A", 0L, 1_000L);
    assertFalse(c.matches(key(LOC, UriScheme.S3, Set.of(SELECT), "arn:role/B")));
  }

  @Test
  void matches_staleSchemaVersion_returnsFalse() {
    CredentialCacheContext c =
        new CredentialCacheContext(
            CredentialCacheContext.CURRENT_SCHEMA_VERSION + 1,
            LOC,
            UriScheme.S3,
            Set.of(SELECT),
            "arn:role/A",
            0L,
            1_000L);
    assertFalse(c.matches(key(LOC, UriScheme.S3, Set.of(SELECT), "arn:role/A")));
  }

  @Test
  void matches_nullRoleArnBothSides_returnsTrue() {
    // Per-bucket vend: no role ARN on either side
    CredentialCacheContext c = ctx(null, 0L, 1_000L);
    assertTrue(c.matches(key(LOC, UriScheme.S3, Set.of(SELECT), null)));
  }

  @Test
  void matches_nullRoleArnVsNonNull_returnsFalse() {
    CredentialCacheContext c = ctx(null, 0L, 1_000L);
    assertFalse(c.matches(key(LOC, UriScheme.S3, Set.of(SELECT), "arn:role/A")));
  }

  @Test
  void matches_schemaVersionBelowCurrent_returnsFalse() { // Gap 5
    CredentialCacheContext c =
        new CredentialCacheContext(
            CredentialCacheContext.CURRENT_SCHEMA_VERSION - 1,
            LOC,
            UriScheme.S3,
            Set.of(SELECT),
            "arn:role/A",
            0L,
            1_000L);
    assertFalse(c.matches(key("arn:role/A", Set.of(SELECT))));
  }

  @Test
  void matches_nonNullRoleArnVsNullKey_returnsFalse() { // Gap 6
    assertFalse(ctx("arn:role/A", 0L, 1_000L).matches(key(null, Set.of(SELECT))));
  }

  // ─── constructor invariants ────────────────────────────────────────────────

  @Test
  void cacheExpiresNonPositive_throws() { // new T2 > 0 invariant
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new CredentialCacheContext(
                CredentialCacheContext.CURRENT_SCHEMA_VERSION,
                LOC,
                UriScheme.S3,
                Set.of(SELECT),
                "r",
                100L,
                0L));
  }

  @Test
  void key_nullLocation_throws() {
    assertThrows(
        NullPointerException.class,
        () -> new CredentialCacheKey(null, Set.of(SELECT), UriScheme.S3, "r"));
  }

  @Test
  void key_inconsistentScheme_throws() {
    NormalizedURL gcsLoc = NormalizedURL.from("gs://bucket/path");
    assertThrows(
        IllegalArgumentException.class,
        () -> new CredentialCacheKey(gcsLoc, Set.of(SELECT), UriScheme.S3, "r"));
  }
}
