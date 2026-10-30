package io.unitycatalog.server.audit;

import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.service.AuthDecorator;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.Purpose;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds audit records and hands them to the configured {@link AuditSink}. A sink failure at
 * runtime is logged (at most once per minute) and never fails the caller.
 */
public final class Auditor implements AutoCloseable {
  static final String CLEANUP_USER = "system:storage-cleanup";

  /** Work the server does outside any request, such as the admin grant at first start. */
  static final String SERVER_USER = "system:server";

  private static final Logger LOGGER = LoggerFactory.getLogger(Auditor.class);
  private static final long REPORT_INTERVAL_MS = 60_000;
  // An Azure container name: 3 to 63 lowercase letters, digits and hyphens.
  private static final Pattern ABFS_CONTAINER = Pattern.compile("[a-z0-9-]{3,63}");

  private final AuditSink sink;
  private final Clock clock;
  private final AtomicLong lastReportMs;
  private final AtomicLong unreported = new AtomicLong();
  private final AtomicBoolean closed = new AtomicBoolean();

  Auditor(AuditSink sink, Clock clock) {
    this.sink = sink;
    this.clock = clock;
    this.lastReportMs = new AtomicLong(clock.millis() - REPORT_INTERVAL_MS);
  }

  /**
   * Returns the auditor for {@code server.audit.sink}, or empty when it is unset. Throws when the
   * sink can't be loaded, so a misconfigured server doesn't start.
   */
  public static Optional<Auditor> fromProperties(ServerProperties properties) {
    String sink = properties.getAuditSink();
    if (sink == null || sink.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(new Auditor(loadSink(sink.trim()), Clock.systemUTC()));
  }

  private static AuditSink loadSink(String className) {
    try {
      return Class.forName(className)
          .asSubclass(AuditSink.class)
          .getDeclaredConstructor()
          .newInstance();
    } catch (InvocationTargetException e) {
      // The sink's own message says what to fix, e.g. FileAuditSink names the Log4j settings.
      throw new IllegalStateException(
          "Audit sink '"
              + className
              + "' (server.audit.sink) failed to start: "
              + e.getCause().getMessage(),
          e.getCause());
    } catch (ReflectiveOperationException | LinkageError | ClassCastException e) {
      throw new IllegalStateException(
          "Cannot load audit sink '"
              + className
              + "' (server.audit.sink): it must be on the server classpath, implement "
              + AuditSink.class.getName()
              + " and have a public no-arg constructor",
          e);
    }
  }

  /** Wraps a vendor so every credential it vends is recorded. */
  public StorageCredentialVendor wrap(StorageCredentialVendor delegate) {
    return new AuditingStorageCredentialVendor(delegate, this);
  }

  /**
   * Wraps an authorizer so each grant that adds a grant, and every revoke and clear, is recorded.
   */
  public UnityCatalogAuthorizer wrap(UnityCatalogAuthorizer delegate) {
    return new AuditingAuthorizer(delegate, this);
  }

  void recordCredential(
      NormalizedURL path,
      Set<CredentialContext.Privilege> privileges,
      Purpose purpose,
      Optional<UUID> resourceId,
      TemporaryCredentials credential) {
    emit(
        () ->
            base(AuditRecord.Action.ISSUE_CREDENTIAL, purpose)
                .purpose(purpose)
                .resourceId(resourceId.orElse(null))
                .location(sanitizeLocation(path.toString()))
                .access(
                    privileges.contains(CredentialContext.Privilege.UPDATE)
                        ? AuditRecord.Access.READ_WRITE
                        : AuditRecord.Access.READ)
                .expiresAt(
                    credential.getExpirationTime() == null
                        ? null
                        : Instant.ofEpochMilli(credential.getExpirationTime()))
                .build());
  }

  void recordPermission(
      AuditRecord.Action action, UUID principalId, UUID resourceId, Privileges privilege) {
    emit(
        () ->
            base(action, null)
                .principalId(principalId)
                .resourceId(resourceId)
                .privilege(privilege == null ? null : privilege.getValue())
                .build());
  }

  /**
   * Keeps scheme, authority and path, so query strings, fragments and user info (which can carry
   * secrets) never reach a sink. ABFS keeps its {@code container@account} authority, with anything
   * before the {@code @} that is not a container name replaced by {@code [REDACTED]}. A location
   * that can't be parsed is logged and left out of the record.
   */
  String sanitizeLocation(String url) {
    if (url == null) {
      return null;
    }
    try {
      URI uri = URI.create(url);
      String authority = uri.getRawAuthority();
      if (authority != null && authority.contains("@")) {
        int at = authority.lastIndexOf('@');
        String host = authority.substring(at + 1);
        boolean abfs = uri.getScheme().startsWith("abfs");
        if (!abfs) {
          authority = host;
        } else if (!ABFS_CONTAINER.matcher(authority.substring(0, at)).matches()) {
          authority = "[REDACTED]@" + host;
        }
      }
      String path = uri.getRawPath() == null ? "" : uri.getRawPath();
      return uri.getScheme() + "://" + (authority == null ? "" : authority) + path;
    } catch (RuntimeException e) {
      // Neither the URL nor the exception is logged: a string that can't be parsed may still hold
      // a secret, and the parse error's message repeats the whole input.
      LOGGER.warn("Audit record written without its location: cannot parse it");
      return null;
    }
  }

  /** Closes the sink once; later calls do nothing. */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    reportLostRecords();
    try {
      sink.close();
    } catch (Throwable t) {
      LOGGER.error("Audit sink {} failed to close", sink.getClass().getName(), t);
    }
  }

  private AuditRecord.AuditRecordBuilder base(AuditRecord.Action action, Purpose purpose) {
    ServiceRequestContext ctx = ServiceRequestContext.currentOrNull();
    return AuditRecord.builder()
        .time(clock.instant())
        .action(action)
        .user(user(purpose, ctx))
        .requestId(ctx == null ? null : ctx.id().text());
  }

  private static String user(Purpose purpose, ServiceRequestContext ctx) {
    if (purpose == Purpose.CLEANUP) {
      return CLEANUP_USER;
    }
    if (ctx == null) {
      return SERVER_USER;
    }
    DecodedJWT jwt = ctx.attr(AuthDecorator.DECODED_JWT_ATTR);
    return jwt == null ? null : jwt.getClaim(JwtClaim.SUBJECT.key()).asString();
  }

  private void emit(Supplier<AuditRecord> build) {
    if (closed.get()) {
      // A request that outlived the shutdown drain; the lost records were already reported.
      LOGGER.warn("Audit record dropped: sink {} is closed", sink.getClass().getName());
      return;
    }
    try {
      sink.record(build.get());
    } catch (Throwable t) {
      reportFailure(t);
      return;
    }
    if (unreported.get() > 0 && claimReportSlot()) {
      reportLostRecords();
    }
  }

  /** Logs failures counted since the last report, so the number of lost records is known. */
  private void reportLostRecords() {
    long lost = unreported.getAndSet(0);
    if (lost > 0) {
      LOGGER.error(
          "Writing an audit record failed {} more time(s) since the last report (sink {})",
          lost,
          sink.getClass().getName());
    }
  }

  /** True for at most one caller per minute; that caller writes the report. */
  private boolean claimReportSlot() {
    long now = clock.millis();
    long last = lastReportMs.get();
    return now - last >= REPORT_INTERVAL_MS && lastReportMs.compareAndSet(last, now);
  }

  private void reportFailure(Throwable t) {
    if (claimReportSlot()) {
      LOGGER.error(
          "Writing an audit record failed (sink {}); {} more failure(s) since the last report",
          sink.getClass().getName(),
          unreported.getAndSet(0),
          t);
    } else {
      unreported.incrementAndGet();
    }
  }
}
