package io.unitycatalog.server.audit;

import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.audit.AuditRecord.Purpose;
import io.unitycatalog.server.model.TableOperation;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.security.JwtClaim;
import io.unitycatalog.server.service.AuthDecorator;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.time.Clock;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds audit records and hands them to the configured {@link AuditSink}. A sink failure at
 * runtime is logged (at most once per minute) and never fails the caller.
 */
public final class Auditor implements AutoCloseable {
  static final String CLEANUP_USER = "system:storage-cleanup";

  private static final Logger LOGGER = LoggerFactory.getLogger(Auditor.class);
  private static final long REPORT_INTERVAL_MS = 60_000;

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

  void recordCredential(
      NormalizedURL path,
      Set<CredentialContext.Privilege> privileges,
      Purpose purpose,
      TemporaryCredentials credential) {
    emit(
        () ->
            base(AuditRecord.Action.ISSUE_CREDENTIAL, purpose)
                .purpose(purpose)
                .resourceId(purpose == Purpose.CLEANUP ? cleanupResourceId(path) : null)
                .location(sanitizeLocation(path.toString()))
                .access(
                    privileges.contains(CredentialContext.Privilege.UPDATE)
                        ? TableOperation.READ_WRITE
                        : TableOperation.READ)
                .expiresAt(credential.getExpirationTime())
                .build());
  }

  /** Returns the resource ID from a cleanup location, which ends in {@code /<segment>/<id>}. */
  private UUID cleanupResourceId(NormalizedURL location) {
    try {
      String path = location.toUri().getPath();
      return UUID.fromString(path.substring(path.lastIndexOf('/') + 1));
    } catch (RuntimeException e) {
      reportFailure(e);
      return null;
    }
  }

  /**
   * Keeps scheme, authority and path, so query strings, fragments and user info (which can carry
   * secrets) never reach a sink. ABFS keeps its {@code container@account} authority.
   */
  static String sanitizeLocation(String url) {
    if (url == null) {
      return null;
    }
    try {
      URI uri = URI.create(url);
      String authority = uri.getRawAuthority();
      if (authority != null && !uri.getScheme().startsWith("abfs")) {
        authority = authority.substring(authority.lastIndexOf('@') + 1);
      }
      String path = uri.getRawPath() == null ? "" : uri.getRawPath();
      return uri.getScheme() + "://" + (authority == null ? "" : authority) + path;
    } catch (RuntimeException e) {
      // Vended paths are already parsed by NormalizedURL, so this only drops the field.
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
    DecodedJWT jwt = ctx == null ? null : ctx.attr(AuthDecorator.DECODED_JWT_ATTR);
    return jwt == null ? null : jwt.getClaim(JwtClaim.SUBJECT.key()).asString();
  }

  private void emit(Supplier<AuditRecord> build) {
    try {
      sink.record(build.get());
    } catch (Throwable t) {
      reportFailure(t);
      return;
    }
    if (unreported.get() > 0) {
      reportLostRecords();
    }
  }

  /** Logs failures counted since the last report, so the number of lost records is known. */
  private void reportLostRecords() {
    long lost = unreported.getAndSet(0);
    if (lost > 0) {
      LOGGER.error(
          "Audit sink {} failed {} more time(s) since the last report",
          sink.getClass().getName(),
          lost);
    }
  }

  private void reportFailure(Throwable t) {
    long now = clock.millis();
    long last = lastReportMs.get();
    if (now - last >= REPORT_INTERVAL_MS && lastReportMs.compareAndSet(last, now)) {
      LOGGER.error(
          "Audit sink {} failed; {} more failure(s) since the last report",
          sink.getClass().getName(),
          unreported.getAndSet(0),
          t);
    } else {
      unreported.incrementAndGet();
    }
  }
}
