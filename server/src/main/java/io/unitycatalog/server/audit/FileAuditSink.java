package io.unitycatalog.server.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.MemoryMappedFileAppender;
import org.apache.logging.log4j.core.appender.RandomAccessFileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.RollingRandomAccessFileAppender;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.filter.Filterable;

/**
 * Writes each record as one JSON line to the {@code audit} logger. The logger needs {@code
 * additivity = false} and a level that enables INFO. It writes to {@code etc/logs/audit.log} as
 * shipped (see etc/conf/server.log4j2.properties), or to any Log4j appender, such as one that ships
 * records to a cloud service. A file or console appender needs {@code ignoreExceptions = false}.
 */
public class FileAuditSink implements AuditSink {
  static final String LOGGER_NAME = "audit";
  private static final String FIX =
      " Copy the audit appender and logger.audit block from etc/conf/server.log4j2.properties.";

  // The appender kinds whose settings are checked: those that write to a local file or the console.
  private static final List<Class<? extends Appender>> CHECKED_APPENDERS =
      List.of(
          FileAppender.class,
          RollingFileAppender.class,
          RandomAccessFileAppender.class,
          RollingRandomAccessFileAppender.class,
          MemoryMappedFileAppender.class,
          ConsoleAppender.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter TIME =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

  private final Logger logger;

  public FileAuditSink() {
    validate(((LoggerContext) LogManager.getContext(false)).getConfiguration());
    this.logger = LogManager.getLogger(LOGGER_NAME);
  }

  /** Writes to {@code logger} without checking the Log4j configuration. */
  FileAuditSink(Logger logger) {
    this.logger = logger;
  }

  /** Fails when the Log4j configuration can drop audit records or hide a failed write. */
  static void validate(Configuration configuration) {
    LoggerConfig config = configuration.getLoggers().get(LOGGER_NAME);
    if (config == null) {
      throw misconfigured("FileAuditSink needs a Log4j logger named 'audit' (logger.audit).");
    }
    if (config.getAppenders().isEmpty()) {
      throw misconfigured("The Log4j 'audit' logger has no appender.");
    }
    if (config.isAdditive()) {
      throw misconfigured(
          "The Log4j 'audit' logger must set additivity = false, or audit records also go to the"
              + " server log.");
    }
    if (!config.getLevel().isLessSpecificThan(Level.INFO)) {
      throw misconfigured(
          "The Log4j 'audit' logger must enable INFO; level "
              + config.getLevel()
              + " drops every audit record.");
    }
    if (configuration.hasFilter()) {
      throw misconfigured(
          "The Log4j configuration must not have a filter, which can drop audit records.");
    }
    if (config.hasFilter()) {
      throw misconfigured(
          "The Log4j 'audit' logger must not have a filter, which can drop audit records.");
    }
    for (AppenderRef ref : config.getAppenderRefs()) {
      String name = "The Log4j 'audit' logger's appender reference " + ref.getRef();
      if (ref.getLevel() != null && !ref.getLevel().isLessSpecificThan(Level.INFO)) {
        throw misconfigured(
            name + " has level " + ref.getLevel() + ", which drops every audit record.");
      }
      if (ref.getFilter() != null) {
        throw misconfigured(name + " must not have a filter, which can drop audit records.");
      }
    }
    for (Appender appender : config.getAppenders().values()) {
      if (CHECKED_APPENDERS.stream().noneMatch(kind -> kind.isInstance(appender))) {
        continue;
      }
      String name = "The Log4j 'audit' logger's appender " + appender.getName();
      if (appender.ignoreExceptions()) {
        throw misconfigured(
            name + " must set ignoreExceptions = false, or failed writes lose records silently.");
      }
      if (appender instanceof Filterable filterable && filterable.hasFilter()) {
        throw misconfigured(name + " must not have a filter, which can drop audit records.");
      }
    }
  }

  private static IllegalStateException misconfigured(String problem) {
    return new IllegalStateException(problem + FIX);
  }

  @Override
  public void record(AuditRecord record) {
    logger.info("{}", toJson(record));
  }

  /** One JSON line with a fixed field order; null fields are omitted. */
  static String toJson(AuditRecord r) {
    Map<String, Object> m = new LinkedHashMap<>();
    put(m, "time", TIME.format(r.getTime()));
    put(m, "action", r.getAction());
    put(m, "user", r.getUser());
    put(m, "request_id", r.getRequestId());
    put(m, "purpose", r.getPurpose());
    put(m, "location", r.getLocation());
    put(m, "access", r.getAccess());
    put(m, "expires_at", r.getExpiresAt() == null ? null : TIME.format(r.getExpiresAt()));
    put(m, "resource_id", r.getResourceId());
    put(m, "principal_id", r.getPrincipalId());
    put(m, "privilege", r.getPrivilege());
    try {
      return MAPPER.writeValueAsString(m);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot serialize audit record", e);
    }
  }

  private static void put(Map<String, Object> m, String key, Object value) {
    if (value != null) {
      m.put(key, value);
    }
  }
}
