package io.unitycatalog.server.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

/**
 * Writes each record as one JSON line to the {@code audit} logger. The Log4j configuration must
 * give that logger its own appender with {@code ignoreExceptions = false}, {@code additivity =
 * false} and level INFO; see etc/conf/server.log4j2.properties.
 */
public class FileAuditSink implements AuditSink {
  static final String LOGGER_NAME = "audit";
  private static final String FIX =
      " Copy the audit appender and logger.audit block from etc/conf/server.log4j2.properties.";

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter TIME =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

  private final Logger logger;

  public FileAuditSink() {
    validate(((LoggerContext) LogManager.getContext(false)).getConfiguration());
    this.logger = LogManager.getLogger(LOGGER_NAME);
  }

  /** Fails unless audit records reach their own appender, and only that appender. */
  static void validate(Configuration configuration) {
    LoggerConfig config = configuration.getLoggers().get(LOGGER_NAME);
    if (config == null) {
      throw new IllegalStateException(
          "FileAuditSink needs a Log4j logger named 'audit' (logger.audit)." + FIX);
    }
    if (config.getAppenders().isEmpty()) {
      throw new IllegalStateException("The Log4j 'audit' logger has no appender." + FIX);
    }
    if (config.isAdditive()) {
      throw new IllegalStateException(
          "The Log4j 'audit' logger must set additivity = false, or audit records also go to the"
              + " server log."
              + FIX);
    }
    if (!config.getLevel().isLessSpecificThan(Level.INFO)) {
      throw new IllegalStateException(
          "The Log4j 'audit' logger must enable INFO; level "
              + config.getLevel()
              + " drops every audit record."
              + FIX);
    }
    for (Appender appender : config.getAppenders().values()) {
      if (appender.ignoreExceptions()) {
        throw new IllegalStateException(
            "The Log4j 'audit' logger's appender "
                + appender.getName()
                + " must set ignoreExceptions = false, or failed writes lose records silently."
                + FIX);
      }
    }
  }

  @Override
  public void record(AuditRecord record) {
    logger.info("{}", toJson(record));
  }

  /** One JSON line with a fixed field order; null fields are omitted. */
  static String toJson(AuditRecord r) {
    Map<String, Object> m = new LinkedHashMap<>();
    put(m, "time", r.getTime() == null ? null : TIME.format(r.getTime()));
    put(m, "action", r.getAction());
    put(m, "user", r.getUser());
    put(m, "request_id", r.getRequestId());
    put(m, "purpose", r.getPurpose());
    put(m, "location", r.getLocation());
    put(m, "access", r.getAccess());
    put(m, "expires_at", r.getExpiresAt());
    put(m, "resource_id", r.getResourceId());
    try {
      return MAPPER.writeValueAsString(m);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot serialize audit record", e);
    }
  }

  private static void put(Map<String, Object> m, String key, Object value) {
    if (value != null) {
      // Enums and UUIDs as strings; numbers stay JSON numbers.
      m.put(key, value instanceof Enum<?> || value instanceof UUID ? value.toString() : value);
    }
  }
}
