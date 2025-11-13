package io.unitycatalog.server.service.credential.gcp;

import static java.lang.String.format;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.base.CharMatcher;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.sql.Date;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.Files;

/**
 * Vends GCS OAuth tokens for Unity Catalog by downscoping credentials against the configured bucket
 * allow list.
 *
 * <p>Credential generation mirrors the AWS path: administrators can either rely on the default
 * service-account credentials or plug in a custom {@link GcpCredentialsGenerator} using the {@code
 * gcs.credentialsGenerator.*} property. Local integration tests also support two synthetic
 * credential sources:
 *
 * <ul>
 *   <li>{@code testing://<bucket>} returns a non-expiring token to mirror the fixed-credential
 *       experience.
 *   <li>{@code testing-renew://<clock-name>/<bucket>} issues short-lived, time-bucketed tokens
 *       whose expiration aligns with the shared manual clock. This mirrors the AWS renewal tests,
 *       but is wired through the same generator abstraction instead of a bespoke code path.
 * </ul>
 */
public class GcpCredentialVendor {

  public static final List<String> INITIAL_SCOPES =
      List.of("https://www.googleapis.com/auth/cloud-platform");
  /** Prefix for static test tokens issued by the embedded server. */
  private static final String TEST_STATIC_PREFIX = "testing://";
  /** Prefix for renewable test tokens keyed by clock name plus bucket. */
  private static final String TEST_RENEW_PREFIX = "testing-renew://";
  /** Renewal cadence for synthetic tokens, matches the integration tests. */
  private static final long TEST_RENEW_INTERVAL_MILLIS = 30_000L;

  private final Map<String, GcsStorageConfig> gcsConfigurations;
  private final Map<String, GcpCredentialsGenerator> credentialGenerators =
      new ConcurrentHashMap<>();

  /**
   * Pluggable contract for generating GCS OAuth tokens. This is used to allow for custom credential
   * generation for local integration tests.
   */
  public interface GcpCredentialsGenerator {
    AccessToken generate(CredentialContext context);
  }

  public GcpCredentialVendor(ServerProperties serverProperties) {
    this.gcsConfigurations = serverProperties.getGcsConfigurations();
  }

  public AccessToken vendGcpToken(CredentialContext credentialContext) {
    String storageBase = credentialContext.getStorageBase();
    GcsStorageConfig storageConfig = gcsConfigurations.get(storageBase);

    // Cache a generator per bucket so repeated requests reuse the same credential strategy.
    GcpCredentialsGenerator generator =
        credentialGenerators.computeIfAbsent(
            storageBase, key -> createGenerator(key, storageConfig));
    return generator.generate(credentialContext);
  }

  // Resolve user-provided generators first, otherwise fall back to built-in options.
  private GcpCredentialsGenerator createGenerator(
      String storageBase, GcsStorageConfig storageConfig) {
    if (storageConfig != null) {
      String generatorClass = storageConfig.getCredentialsGenerator();
      if (generatorClass != null && !generatorClass.isEmpty()) {
        try {
          return (GcpCredentialsGenerator)
              Class.forName(generatorClass).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
          throw new RuntimeException(
              "Unable to instantiate GCS credentials generator " + generatorClass, e);
        }
      }
    }

    String jsonKeyFilePath = storageConfig != null ? storageConfig.getJsonKeyFilePath() : null;
    if (jsonKeyFilePath != null && jsonKeyFilePath.isEmpty()) {
      jsonKeyFilePath = null;
    }

    if (jsonKeyFilePath != null && jsonKeyFilePath.startsWith(TEST_RENEW_PREFIX)) {
      // testing-renew loads a clock-aware generator that emits short-lived tokens.
      return new TestingRenewalCredentialsGenerator(jsonKeyFilePath, storageBase);
    }

    if (jsonKeyFilePath != null && jsonKeyFilePath.startsWith(TEST_STATIC_PREFIX)) {
      // testing:// maps to a single token that never expires, used for simple flows.
      return new StaticTestingCredentialsGenerator(jsonKeyFilePath);
    }

    // Default behavior: produce service-account derived tokens for real deployments.
    return new ServiceAccountCredentialsGenerator(jsonKeyFilePath);
  }

  private static long currentTestMillis(String clockName) {
    if (clockName != null && !clockName.isEmpty()) {
      try {
        Class<?> clockClass = Class.forName("io.unitycatalog.spark.utils.Clock");
        Method getManualClock = clockClass.getMethod("getManualClock", String.class);
        Object manualClock = getManualClock.invoke(null, clockName);
        if (manualClock != null) {
          Method nowMethod = manualClock.getClass().getMethod("now");
          Object instant = nowMethod.invoke(manualClock);
          if (instant instanceof Instant) {
            // Use the manual clock if Spark injected one so tests can drive expiry.
            return ((Instant) instant).toEpochMilli();
          }
        }
      } catch (ReflectiveOperationException ignored) {
        // Fall through to the system clock when the Spark Clock is not available.
      }
    }
    return System.currentTimeMillis();
  }

  private static long alignToInterval(long millis) {
    // Snap the timestamp to the start of the renewal window for deterministic tokens.
    return Math.floorDiv(millis, TEST_RENEW_INTERVAL_MILLIS) * TEST_RENEW_INTERVAL_MILLIS;
  }

  OAuth2Credentials downscopeGcpCreds(GoogleCredentials credentials, CredentialContext context) {
    CredentialAccessBoundary.Builder boundaryBuilder = CredentialAccessBoundary.newBuilder();
    List<String> roles = resolvePrivilegesToRoles(context.getPrivileges());

    context
        .getLocations()
        .forEach(
            location -> {
              URI locationUri = URI.create(location);
              String path = CharMatcher.is('/').trimLeadingFrom(locationUri.getPath());

              String resource =
                  format("//storage.googleapis.com/projects/_/buckets/%s", locationUri.getHost());

              // for reading/writing objects
              String resourceNameStartsWithExpr =
                  format(
                      "resource.name.startsWith('projects/_/buckets/%s/objects/%s')",
                      locationUri.getHost(), path);

              // for listing objects
              String objectListPrefixStartsWithExpr =
                  format(
                      "api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('%s')",
                      path);

              String combinedExpr =
                  resourceNameStartsWithExpr + " || " + objectListPrefixStartsWithExpr;

              boundaryBuilder.addRule(
                  CredentialAccessBoundary.AccessBoundaryRule.newBuilder()
                      .setAvailablePermissions(roles)
                      .setAvailabilityCondition(
                          CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition
                              .newBuilder()
                              .setExpression(combinedExpr)
                              .build())
                      .setAvailableResource(resource)
                      .build());
            });

    return DownscopedCredentials.newBuilder()
        .setSourceCredential(credentials)
        .setCredentialAccessBoundary(boundaryBuilder.build())
        .build();
  }

  List<String> resolvePrivilegesToRoles(Set<CredentialContext.Privilege> privileges) {
    if (privileges.contains(CredentialContext.Privilege.UPDATE)) {
      return List.of("inRole:roles/storage.objectAdmin");
    } else if (privileges.contains(CredentialContext.Privilege.SELECT)) {
      return List.of("inRole:roles/storage.objectViewer");
    }
    return List.of();
  }

  /**
   * Generates access tokens by loading service-account JSON when specified, or by falling back to
   * Google application-default credentials. Tokens are then downscoped to the caller’s bucket
   * privileges, matching the production flow used outside of tests.
   */
  private final class ServiceAccountCredentialsGenerator implements GcpCredentialsGenerator {
    private final String jsonKeyFilePath;

    private ServiceAccountCredentialsGenerator(String jsonKeyFilePath) {
      // Retain the optional service-account path so we can load explicit keys when present.
      this.jsonKeyFilePath = jsonKeyFilePath;
    }

    @Override
    public AccessToken generate(CredentialContext context) {
      try {
        GoogleCredentials creds;
        if (jsonKeyFilePath != null && !jsonKeyFilePath.isEmpty()) {
          // Local files leverage the Iceberg helper to load service-account JSON.
          creds =
              ServiceAccountCredentials.fromStream(Files.localInput(jsonKeyFilePath).newStream());
        } else {
          // Otherwise fall back to GCP default credentials configured on the host.
          creds = GoogleCredentials.getApplicationDefault();
        }
        return downscopeGcpCreds(creds.createScoped(INITIAL_SCOPES), context).refreshAccessToken();
      } catch (IOException e) {
        throw new BaseException(ErrorCode.FAILED_PRECONDITION, "GCS credentials not found.", e);
      }
    }
  }

  /**
   * Returns a fixed, non-expiring access token for {@code testing://} scenarios. This allows local
   * integration tests to validate plumbing and downscoping without exercising refresh logic or
   * service-account IO.
   */
  private static final class StaticTestingCredentialsGenerator implements GcpCredentialsGenerator {
    private final AccessToken staticToken;

    private StaticTestingCredentialsGenerator(String value) {
      // Bake the provided value into an effectively non-expiring AccessToken instance.
      this.staticToken =
          AccessToken.newBuilder()
              .setTokenValue(value)
              .setExpirationTime(Date.from(Instant.ofEpochMilli(253370790000000L)))
              .build();
    }

    @Override
    public AccessToken generate(CredentialContext context) {
      return staticToken;
    }
  }

  /**
   * Issues short-lived tokens for {@code testing-renew://} buckets. Each token encodes the active
   * renewal window and bucket hint so tests can assert rotation frequency while staying within the
   * same generator abstraction used in production.
   */
  private static final class TestingRenewalCredentialsGenerator implements GcpCredentialsGenerator {
    private final String clockName;
    private final String bucketHint;

    private TestingRenewalCredentialsGenerator(String spec, String defaultBucket) {
      // Parse "clock/bucket" components so tests can override renewal timing and bucket labeling.
      String payload = spec.substring(TEST_RENEW_PREFIX.length());
      String localClock = null;
      String localBucket = payload;
      int slashIdx = payload.indexOf('/');
      if (slashIdx >= 0) {
        localClock = payload.substring(0, slashIdx);
        localBucket = payload.substring(slashIdx + 1);
      }
      if (localBucket == null || localBucket.isEmpty()) {
        localBucket = defaultBucket;
      }
      this.clockName = localClock;
      this.bucketHint = localBucket;
    }

    @Override
    public AccessToken generate(CredentialContext context) {
      long currentMillis = currentTestMillis(clockName);
      long windowStart = alignToInterval(currentMillis);
      Instant expiration = Instant.ofEpochMilli(windowStart + TEST_RENEW_INTERVAL_MILLIS);
      String bucketComponent =
          bucketHint == null || bucketHint.isEmpty() ? context.getStorageBase() : bucketHint;
      // Token embeds bucket plus renewal window so the filesystem can verify rotation.
      String tokenValue = format("%s%s#%d", TEST_RENEW_PREFIX, bucketComponent, windowStart);
      return AccessToken.newBuilder()
          .setTokenValue(tokenValue)
          .setExpirationTime(Date.from(expiration))
          .build();
    }
  }
}
