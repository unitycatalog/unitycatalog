package io.unitycatalog.server.service.credential.cache;

import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

  // ─── matches(key) ─────────────────────────────────────────────────────────

  @Test
  void matches_exactMatch_returnsTrue() {
    CredentialCacheContext c = ctx("arn:role/A", 0L, 0L);
    assertTrue(c.matches(key(LOC, UriScheme.S3, Set.of(SELECT), "arn:role/A")));
  }

  @Test
  void matches_differentLocation_returnsFalse() {
    CredentialCacheContext c = ctx("arn:role/A", 0L, 0L);
    assertFalse(c.matches(key(OTHER_LOC, UriScheme.S3, Set.of(SELECT), "arn:role/A")));
  }

  @Test
  void matches_differentScheme_returnsFalse() {
    CredentialCacheContext c = ctx("arn:role/A", 0L, 0L);
    CredentialCacheKey gcsKey =
        new CredentialCacheKey(LOC, Set.of(SELECT), UriScheme.GS, "arn:role/A");
    assertFalse(c.matches(gcsKey));
  }

  @Test
  void matches_differentPrivileges_returnsFalse() {
    CredentialCacheContext c = ctx("arn:role/A", 0L, 0L);
    // SELECT+UPDATE instead of just SELECT
    assertFalse(c.matches(key(LOC, UriScheme.S3, Set.of(SELECT, UPDATE), "arn:role/A")));
  }

  @Test
  void matches_differentRoleArn_returnsFalse() {
    CredentialCacheContext c = ctx("arn:role/A", 0L, 0L);
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
            0L);
    assertFalse(c.matches(key(LOC, UriScheme.S3, Set.of(SELECT), "arn:role/A")));
  }

  @Test
  void matches_nullRoleArnBothSides_returnsTrue() {
    // Per-bucket vend: no role ARN on either side
    CredentialCacheContext c = ctx(null, 0L, 0L);
    assertTrue(c.matches(key(LOC, UriScheme.S3, Set.of(SELECT), null)));
  }

  @Test
  void matches_nullRoleArnVsNonNull_returnsFalse() {
    CredentialCacheContext c = ctx(null, 0L, 0L);
    assertFalse(c.matches(key(LOC, UriScheme.S3, Set.of(SELECT), "arn:role/A")));
  }
}
