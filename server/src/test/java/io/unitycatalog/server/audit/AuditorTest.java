package io.unitycatalog.server.audit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.util.SafeCloseable;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.CleanupTaskRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.service.AuthDecorator;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.CredentialUse;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.testdatabase.TestDatabases;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AuditorTest {
  private static final UUID ID = UUID.fromString("7f3a0000-0000-0000-0000-000000000001");
  private final Clock clock = Clock.fixed(Instant.parse("2026-10-20T10:00:00Z"), ZoneOffset.UTC);
  private Auditor auditor;

  @BeforeEach
  void setUp() {
    CapturingAuditSink.reset();
    auditor = new Auditor(new CapturingAuditSink(), clock);
  }

  @Test
  void cleanupCredentialNamesTheCleanupUserAndResource() {
    NormalizedURL path = NormalizedURL.from("s3://key:secret@bkt/root/tables/" + ID + "?sig=x");
    TemporaryCredentials credential = new TemporaryCredentials().expirationTime(1792500120000L);
    StorageCredentialVendor delegate = mock(StorageCredentialVendor.class);
    when(delegate.vendCredential(path, CredentialContext.READ_WRITE, CredentialUse.cleanup(ID)))
        .thenReturn(credential);
    StorageCredentialVendor audited = auditor.wrap(delegate);
    assertThat(
            audited.vendCredential(path, CredentialContext.READ_WRITE, CredentialUse.cleanup(ID)))
        .isSameAs(credential);
    AuditRecord r = CapturingAuditSink.RECORDS.get(0);
    assertThat(r.getUser()).isEqualTo(Auditor.CLEANUP_USER);
    assertThat(r.getResourceId()).isEqualTo(ID);
    assertThat(r.getAccess()).isEqualTo(AuditRecord.Access.READ_WRITE);
    assertThat(r.getLocation()).isEqualTo("s3://bkt/root/tables/" + ID); // no secrets
    // A client vend with read-only access is recorded as READ, without a resource ID.
    NormalizedURL clientPath = NormalizedURL.from("s3://bkt/t");
    when(delegate.vendCredential(clientPath, CredentialContext.READ_ONLY, CredentialUse.CLIENT))
        .thenReturn(credential);
    audited.vendCredential(clientPath, CredentialContext.READ_ONLY, CredentialUse.CLIENT);
    AuditRecord client = CapturingAuditSink.RECORDS.get(1);
    assertThat(client.getPurpose()).isEqualTo(AuditRecord.Purpose.CLIENT);
    assertThat(client.getAccess()).isEqualTo(AuditRecord.Access.READ);
    assertThat(client.getResourceId()).isNull();
    // With a decoded token on the request, the caller is the token's subject.
    DecodedJWT jwt =
        JWT.decode(JWT.create().withSubject("bob@example.com").sign(Algorithm.HMAC256("secret")));
    ServiceRequestContext request = ServiceRequestContext.of(HttpRequest.of(HttpMethod.GET, "/"));
    request.setAttr(AuthDecorator.DECODED_JWT_ATTR, jwt);
    try (SafeCloseable ignored = request.push()) {
      audited.vendCredential(clientPath, CredentialContext.READ_ONLY, CredentialUse.CLIENT);
    }
    assertThat(CapturingAuditSink.RECORDS.get(2).getUser()).isEqualTo("bob@example.com");
    // ABFS keeps its container@account authority.
    assertThat(auditor.sanitizeLocation("abfss://my-ctr@acct.dfs.core.windows.net/p?sv=x"))
        .isEqualTo("abfss://my-ctr@acct.dfs.core.windows.net/p");
    // Anything else before the @ may be a secret.
    assertThat(auditor.sanitizeLocation("abfss://user:pw@acct.dfs.core.windows.net/p"))
        .isEqualTo("abfss://[REDACTED]@acct.dfs.core.windows.net/p");
    // Vending decodes the authority, so an encoded @ ends user info too.
    assertThat(auditor.sanitizeLocation("abfss://user%3Apw%40acct.dfs.core.windows.net/p"))
        .isEqualTo("abfss://[REDACTED]@acct.dfs.core.windows.net/p");
    assertThat(FileAuditSink.toJson(r))
        .isEqualTo(
            "{\"time\":\"2026-10-20T10:00:00.000Z\",\"action\":\"ISSUE_CREDENTIAL\","
                + "\"user\":\"system:storage-cleanup\",\"purpose\":\"CLEANUP\","
                + "\"location\":\"s3://bkt/root/tables/"
                + ID
                + "\",\"access\":\"READ_WRITE\",\"expires_at\":\"2026-10-20T12:42:00.000Z\","
                + "\"resource_id\":\""
                + ID
                + "\"}");
  }

  @Test
  void sinkFailuresNeverPropagate() {
    // Plugin jars fail with Errors (e.g. a missing dependency); those must not escape either.
    Error missingDependency = new NoClassDefFoundError("plugin dependency");
    Auditor erroring =
        new Auditor(
            new AuditSink() {
              @Override
              public void record(AuditRecord r) {
                throw missingDependency;
              }

              @Override
              public void close() {
                throw new IllegalStateException("close");
              }
            },
            clock);
    assertThatCode(
            () ->
                erroring.recordCredential(
                    NormalizedURL.from("s3://bkt/t"),
                    CredentialContext.READ_ONLY,
                    CredentialUse.CLIENT,
                    new TemporaryCredentials()))
        .doesNotThrowAnyException();
    assertThatCode(erroring::close).doesNotThrowAnyException();
  }

  @Test
  void failuresWithinAMinuteAreCountedThenReportedOnce() {
    AtomicLong now = new AtomicLong(clock.millis());
    Clock ticking =
        new Clock() {
          @Override
          public ZoneId getZone() {
            return ZoneOffset.UTC;
          }

          @Override
          public Clock withZone(ZoneId zone) {
            return this;
          }

          @Override
          public Instant instant() {
            return Instant.ofEpochMilli(now.get());
          }
        };
    AtomicBoolean failing = new AtomicBoolean(true);
    Auditor flaky =
        new Auditor(
            r -> {
              if (failing.get()) {
                throw new IllegalStateException("sink down");
              }
            },
            ticking);
    Runnable vend =
        () ->
            flaky.recordCredential(
                NormalizedURL.from("s3://bkt/t"),
                CredentialContext.READ_ONLY,
                CredentialUse.CLIENT,
                new TemporaryCredentials());

    // The first failure is reported at once; the next ones in the same minute are only counted.
    vend.run();
    vend.run();
    vend.run();
    assertThat(flaky.unreported).hasValue(2);
    // A minute later, the next failure reports the count, and counting starts again.
    now.addAndGet(Duration.ofMinutes(1).toMillis());
    vend.run();
    assertThat(flaky.unreported).hasValue(0);
    vend.run();
    assertThat(flaky.unreported).hasValue(1);
    // A minute later, a record that is written reports the count.
    now.addAndGet(Duration.ofMinutes(1).toMillis());
    failing.set(false);
    vend.run();
    assertThat(flaky.unreported).hasValue(0);
    // Close reports what is left.
    failing.set(true);
    vend.run();
    assertThat(flaky.unreported).hasValue(1);
    flaky.close();
    assertThat(flaky.unreported).hasValue(0);
  }

  private static ServerProperties sinkProperty(String sink) {
    Properties properties = new Properties();
    if (sink != null) {
      properties.setProperty("server.audit.sink", sink);
    }
    return new ServerProperties(properties);
  }

  @Test
  void unsetSinkIsOffAndMisconfiguredSinkFailsStartup() throws Exception {
    assertThat(Auditor.fromProperties(sinkProperty(null))).isEmpty();
    assertThatThrownBy(() -> Auditor.fromProperties(sinkProperty("com.example.Missing")))
        .hasMessageContaining("server.audit.sink");
    assertThatThrownBy(() -> Auditor.fromProperties(sinkProperty(String.class.getName())))
        .hasMessageContaining("AuditSink");

    // FileAuditSink accepts the shipped Log4j config and rejects one without the audit logger.
    assertThatCode(() -> validate("etc/conf/server.log4j2.properties")).doesNotThrowAnyException();
    Path noAuditLogger = Files.createTempFile("log4j2", ".properties");
    Files.writeString(noAuditLogger, "rootLogger.level = info\n");
    assertThatThrownBy(() -> validate(noAuditLogger.toString()))
        .hasMessageContaining("logger.audit");
    Path ignoringFailures = Files.createTempFile("log4j2", ".properties");
    Files.writeString(
        ignoringFailures,
        String.join(
            "\n",
            "appender.a.type = Console",
            "appender.a.name = A",
            "appender.a.ignoreExceptions = true",
            "logger.audit.name = audit",
            "logger.audit.level = info",
            "logger.audit.additivity = false",
            "logger.audit.appenderRef.a.ref = A"));
    assertThatThrownBy(() -> validate(ignoringFailures.toString()))
        .hasMessageContaining("ignoreExceptions");
    // A level on the link to the appender can drop every record although the logger passes INFO.
    Path droppingRecords = Files.createTempFile("log4j2", ".properties");
    Files.writeString(
        droppingRecords,
        String.join(
            "\n",
            "appender.a.type = Console",
            "appender.a.name = A",
            "appender.a.ignoreExceptions = false",
            "logger.audit.name = audit",
            "logger.audit.level = info",
            "logger.audit.additivity = false",
            "logger.audit.appenderRef.a.ref = A",
            "logger.audit.appenderRef.a.level = warn"));
    assertThatThrownBy(() -> validate(droppingRecords.toString()))
        .hasMessageContaining("appender reference");
    // Appender rules apply to file and console appenders only. Others, such as a socket appender
    // that ships records off the host, are the operator's choice even if they ignore exceptions.
    try (ServerSocket collector = new ServerSocket(0)) {
      Path otherAppender = Files.createTempFile("log4j2", ".properties");
      Files.writeString(
          otherAppender,
          String.join(
              "\n",
              "appender.s.type = Socket",
              "appender.s.name = S",
              "appender.s.host = localhost",
              "appender.s.port = " + collector.getLocalPort(),
              "appender.s.layout.type = PatternLayout",
              "logger.audit.name = audit",
              "logger.audit.level = info",
              "logger.audit.additivity = false",
              "logger.audit.appenderRef.s.ref = S"));
      assertThatCode(() -> validate(otherAppender.toString())).doesNotThrowAnyException();
    }
    // A filter on the configuration, the audit logger or a console appender can drop records too.
    for (String filter :
        List.of(
            "filter.t.type = ThresholdFilter\nfilter.t.level = warn",
            "logger.audit.filter.t.type = ThresholdFilter\nlogger.audit.filter.t.level = warn",
            "appender.a.filter.t.type = ThresholdFilter\nappender.a.filter.t.level = warn")) {
      Path filtered = Files.createTempFile("log4j2", ".properties");
      Files.writeString(
          filtered,
          String.join(
              "\n",
              "appender.a.type = Console",
              "appender.a.name = A",
              "appender.a.ignoreExceptions = false",
              "logger.audit.name = audit",
              "logger.audit.level = info",
              "logger.audit.additivity = false",
              "logger.audit.appenderRef.a.ref = A",
              filter));
      assertThatThrownBy(() -> validate(filtered.toString()))
          .hasMessageContaining("must not have a filter");
    }
  }

  @Test
  void fileSinkWritesEachRecordAsItsJsonAtInfo() {
    Logger logger = mock(Logger.class);
    AuditRecord record =
        AuditRecord.builder()
            .time(clock.instant())
            .action(AuditRecord.Action.ISSUE_CREDENTIAL)
            .purpose(AuditRecord.Purpose.CLIENT)
            .location("s3://bkt/t")
            .build();
    new FileAuditSink(logger).record(record);
    verify(logger).info("{}", FileAuditSink.toJson(record));
  }

  @Test
  void shippedConfigWritesAuditRecordsAsJsonLinesToTheirOwnFile() throws Exception {
    Configuration config = load("etc/conf/server.log4j2.properties");
    try {
      Map<String, Appender> appenders =
          config.getLoggers().get(FileAuditSink.LOGGER_NAME).getAppenders();
      // Only its own appender, so no record reaches server.log.
      assertThat(appenders).containsOnlyKeys("AuditFile");
      RollingFileAppender file = (RollingFileAppender) appenders.get("AuditFile");
      assertThat(file.getFileName()).isEqualTo("etc/logs/audit.log");
      // One JSON line per record, with nothing added.
      assertThat(((PatternLayout) file.getLayout()).getConversionPattern()).isEqualTo("%m%n");
    } finally {
      config.stop();
    }
  }

  /** Validates the Log4j properties file, then stops its appenders, which can hold sockets. */
  private static void validate(String file) throws Exception {
    Configuration config = load(file);
    try {
      FileAuditSink.validate(config);
    } finally {
      config.stop();
    }
  }

  /** Loads and initializes a Log4j properties file; the caller stops it. */
  private static Configuration load(String file) {
    Configuration config =
        new PropertiesConfigurationFactory()
            .getConfiguration(
                new LoggerContext("audit-test"),
                ConfigurationSource.fromUri(Path.of(file).toUri()));
    config.initialize();
    return config;
  }

  @Test
  void afterCommitRunsOnlyOnCommitAndNeverFailsIt() {
    AtomicInteger runs = new AtomicInteger();
    try (TestDatabase database = TestDatabases.newDatabase();
        Session session = database.sessionFactory().openSession()) {
      Transaction rolledBack = session.beginTransaction();
      auditor.afterCommit(session, runs::incrementAndGet);
      rolledBack.rollback();
      assertThat(runs).hasValue(0);

      Transaction committed = session.beginTransaction();
      auditor.afterCommit(session, runs::incrementAndGet);
      // Hibernate rethrows from commit() what an after-completion callback throws; the guard
      // keeps a failing action from failing a commit that already happened.
      auditor.afterCommit(
          session,
          () -> {
            throw new IllegalStateException("audit action failed");
          });
      assertThatCode(committed::commit).doesNotThrowAnyException();
      assertThat(runs).hasValue(1);
    }
  }

  @Test
  void grantRecordsThePrincipalAndThePublicPrivilegeName() {
    UnityCatalogAuthorizer delegate = mock(UnityCatalogAuthorizer.class);
    UUID principal = UUID.randomUUID();
    when(delegate.grantAuthorization(principal, ID, Privileges.USE_CATALOG)).thenReturn(true);
    auditor.wrap(delegate).grantAuthorization(principal, ID, Privileges.USE_CATALOG);
    assertThat(FileAuditSink.toJson(CapturingAuditSink.RECORDS.get(0)))
        .contains(
            "\"action\":\"GRANT\"",
            "\"resource_id\":\"" + ID + "\"",
            "\"principal_id\":\"" + principal + "\"",
            "\"privilege\":\"USE CATALOG\"");
  }

  @Test
  void permissionChangeThatThrowsIsNotRecorded() {
    UnityCatalogAuthorizer delegate = mock(UnityCatalogAuthorizer.class);
    UUID principal = UUID.randomUUID();
    UUID resource = UUID.randomUUID();
    // jdbc-adapter throws an Error when the database delete keeps failing.
    Error failed = new Error("delete failed");
    when(delegate.revokeAuthorization(principal, resource, Privileges.OWNER)).thenThrow(failed);
    when(delegate.clearAuthorizationsForPrincipal(principal)).thenThrow(failed);
    when(delegate.clearAuthorizationsForResource(resource)).thenThrow(failed);
    UnityCatalogAuthorizer audited = auditor.wrap(delegate);

    assertThatThrownBy(() -> audited.revokeAuthorization(principal, resource, Privileges.OWNER))
        .isSameAs(failed);
    assertThatThrownBy(() -> audited.clearAuthorizationsForPrincipal(principal)).isSameAs(failed);
    assertThatThrownBy(() -> audited.clearAuthorizationsForResource(resource)).isSameAs(failed);
    assertThat(CapturingAuditSink.RECORDS).isEmpty();
  }

  /**
   * A default method added to a wrapped interface is inherited silently, so it would bypass the
   * decorator. Each decorator must declare every default method.
   */
  @Test
  void decoratorsForwardEveryDefaultMethod() {
    assertDeclaresDefaults(UnityCatalogAuthorizer.class, AuditingAuthorizer.class);
    assertDeclaresDefaults(StorageCredentialVendor.class, AuditingStorageCredentialVendor.class);
    assertDeclaresDefaults(CleanupTaskRepository.class, AuditingCleanupTaskRepository.class);
  }

  private static void assertDeclaresDefaults(Class<?> api, Class<?> decorator) {
    assertThat(Arrays.stream(api.getMethods()).filter(Method::isDefault))
        .allSatisfy(
            m ->
                assertThatCode(
                        () -> decorator.getDeclaredMethod(m.getName(), m.getParameterTypes()))
                    .as("%s must forward %s", decorator.getSimpleName(), m)
                    .doesNotThrowAnyException());
  }
}
