package io.unitycatalog.server.audit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.audit.AuditRecord.Purpose;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.model.TableOperation;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.CleanupTaskRepository;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationFactory;
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
    auditor.recordCredential(
        path,
        CredentialContext.READ_WRITE,
        Purpose.CLEANUP,
        new TemporaryCredentials().expirationTime(1792500120000L));
    AuditRecord r = CapturingAuditSink.RECORDS.get(0);
    assertThat(r.getUser()).isEqualTo(Auditor.CLEANUP_USER);
    assertThat(r.getResourceId()).isEqualTo(ID);
    assertThat(r.getAccess()).isEqualTo(TableOperation.READ_WRITE);
    assertThat(r.getLocation()).isEqualTo("s3://bkt/root/tables/" + ID); // no secrets
    // ABFS keeps its container@account authority.
    assertThat(auditor.sanitizeLocation("abfss://ctr@acct.dfs.core.windows.net/p?sv=x"))
        .isEqualTo("abfss://ctr@acct.dfs.core.windows.net/p");
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
                    Purpose.CLIENT,
                    new TemporaryCredentials()))
        .doesNotThrowAnyException();
    assertThatCode(erroring::close).doesNotThrowAnyException();
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
    assertThatCode(() -> FileAuditSink.validate(load("etc/conf/server.log4j2.properties")))
        .doesNotThrowAnyException();
    Path noAuditLogger = Files.createTempFile("log4j2", ".properties");
    Files.writeString(noAuditLogger, "rootLogger.level = info\n");
    assertThatThrownBy(() -> FileAuditSink.validate(load(noAuditLogger.toString())))
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
    assertThatThrownBy(() -> FileAuditSink.validate(load(ignoringFailures.toString())))
        .hasMessageContaining("ignoreExceptions");
  }

  private static Configuration load(String file) throws Exception {
    Configuration config =
        new PropertiesConfigurationFactory()
            .getConfiguration(
                new LoggerContext("audit-test"),
                ConfigurationSource.fromUri(Path.of(file).toUri()));
    config.initialize();
    return config;
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
