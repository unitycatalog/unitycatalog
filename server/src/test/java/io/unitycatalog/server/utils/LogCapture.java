package io.unitycatalog.server.utils;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

/**
 * Records the events one class's logger writes, at debug and above, until closed. Closing restores
 * the logger's level and detaches the recorder.
 */
public final class LogCapture implements AutoCloseable {

  private final Logger logger;
  private final Level previousLevel;
  private final AbstractAppender appender;
  private final List<LogEvent> events = new CopyOnWriteArrayList<>();

  private LogCapture(Class<?> loggerClass) {
    logger = (Logger) LogManager.getLogger(loggerClass);
    previousLevel = logger.getLevel();
    appender =
        new AbstractAppender(
            "capture-" + UUID.randomUUID(), null, null, true, Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.DEBUG);
  }

  public static LogCapture of(Class<?> loggerClass) {
    return new LogCapture(loggerClass);
  }

  public List<LogEvent> events() {
    return List.copyOf(events);
  }

  /** The events at {@code level} or more severe. */
  public List<LogEvent> eventsAtLeast(Level level) {
    return events.stream().filter(e -> e.getLevel().isMoreSpecificThan(level)).toList();
  }

  @Override
  public void close() {
    logger.removeAppender(appender);
    logger.setLevel(previousLevel);
    appender.stop();
  }
}
