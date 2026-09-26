package io.unitycatalog.server.service.credential.cache;

import static io.unitycatalog.server.service.credential.CredentialContext.READ_ONLY;
import static io.unitycatalog.server.service.credential.CredentialContext.READ_WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class StorageCredentialCacheTest {

  private static final NormalizedURL LOC = NormalizedURL.from("s3://bucket/tableA");
  private static final long T0 = 1_000_000_000_000L; // fixed "now" for the injected clock

  private static ServerProperties props(String... kv) {
    Properties p = new Properties();
    for (int i = 0; i < kv.length; i += 2) {
      p.setProperty(kv[i], kv[i + 1]);
    }
    return new ServerProperties(p);
  }

  /** A clock pinned to {@code epochMs}; advance a test by re-stubbing {@link #tick}. */
  private static Clock clockAt(long epochMs) {
    Clock clock = mock(Clock.class);
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(epochMs));
    return clock;
  }

  private static void tick(Clock clock, long epochMs) {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(epochMs));
  }

  /** A cloud vend result expiring at {@code expiryEpochMs} (null = static credential, no T1). */
  private static TemporaryCredentials creds(Long expiryEpochMs) {
    TemporaryCredentials c =
        new TemporaryCredentials()
            .awsTempCredentials(
                new AwsCredentials().accessKeyId("AK").secretAccessKey("SK").sessionToken("TK"));
    if (expiryEpochMs != null) {
      c.expirationTime(expiryEpochMs);
    }
    return c;
  }

  /** A READ_ONLY context for LOC with an AWS credential DAO carrying the given role ARN. */
  private static CredentialContext ctx(String roleArn) {
    return ctx(roleArn, READ_ONLY);
  }

  /** A context for LOC with the given role ARN and privilege set. */
  private static CredentialContext ctx(
      String roleArn, Set<CredentialContext.Privilege> privileges) {
    CredentialDAO dao = mock(CredentialDAO.class);
    when(dao.getAwsIamRoleResponse()).thenReturn(new AwsIamRoleResponse().roleArn(roleArn));
    return CredentialContext.create(LOC, privileges, Optional.of(dao));
  }

  @Test
  void secondCallForSameBindingSkipsTheVend() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L)); // expires 1h out → fresh
    StorageCredentialCache cache = new StorageCredentialCache(vendor, props(), clockAt(T0));

    cache.get(ctx("arn:role/A"));
    cache.get(ctx("arn:role/A"));

    verify(vendor, times(1)).vendCredential(any()); // vended once, served from cache once
  }

  @Test
  void differentRoleForSameLocationReVends() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    StorageCredentialCache cache = new StorageCredentialCache(vendor, props(), clockAt(T0));

    cache.get(ctx("arn:role/A"));
    cache.get(ctx("arn:role/B")); // rebind to a different role → different key → miss

    verify(vendor, times(2)).vendCredential(any());
  }

  @Test
  void differentPrivilegesForSameBindingReVends() {
    // Privileges are part of the key and must match exactly: a READ_ONLY credential must never be
    // served for a READ_WRITE request (insufficient access), nor vice versa (privilege escalation).
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    StorageCredentialCache cache = new StorageCredentialCache(vendor, props(), clockAt(T0));

    cache.get(ctx("arn:role/A", READ_ONLY));
    cache.get(ctx("arn:role/A", READ_WRITE)); // exact privilege set differs → miss

    verify(vendor, times(2)).vendCredential(any());
  }

  @Test
  void disabledBypassesCacheAndVendsEveryCall() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    StorageCredentialCache cache =
        new StorageCredentialCache(
            vendor, props("server.storage-credential-cache.enabled", "false"));

    cache.get(ctx("arn:role/A"));
    cache.get(ctx("arn:role/A"));

    verify(vendor, times(2)).vendCredential(any());
  }

  @Test
  void credentialWithinRenewalLeadIsRefetched() {
    // Credential expires at T0+120s; lead=60s → renewal window opens at T0+60s.
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 120_000L));
    Clock clock = clockAt(T0);
    StorageCredentialCache cache =
        new StorageCredentialCache(
            vendor,
            props(
                "server.storage-credential-cache.renewal-lead-time", "PT1M",
                "server.storage-credential-cache.max-age", "PT5M"),
            clock);

    cache.get(ctx("arn:role/A")); // vend 1
    tick(clock, T0 + 59_000L); // 1s before the lead window → still fresh
    cache.get(ctx("arn:role/A"));
    verify(vendor, times(1)).vendCredential(any()); // served from cache

    tick(clock, T0 + 61_000L); // 1s inside the lead window → refetch
    cache.get(ctx("arn:role/A"));
    verify(vendor, times(2)).vendCredential(any());
  }

  @Test
  void returnedCredentialHasUrlSet() {
    CloudCredentialVendor vendor = mock(CloudCredentialVendor.class);
    when(vendor.vendCredential(any())).thenReturn(creds(T0 + 3_600_000L));
    StorageCredentialCache cache = new StorageCredentialCache(vendor, props(), clockAt(T0));

    TemporaryCredentials out = cache.get(ctx("arn:role/A"));
    assertEquals(LOC.toString(), out.getUrl());
  }
}
