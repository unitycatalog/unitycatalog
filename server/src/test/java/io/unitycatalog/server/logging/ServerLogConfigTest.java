package io.unitycatalog.server.logging;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.util.NetUtils;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.util.SortedArrayStringMap;
import org.junit.jupiter.api.Test;

/** The shipped server log pattern names the serving instance and the request on every line. */
public class ServerLogConfigTest {

  private static String format(LogEvent event) throws Exception {
    Properties properties = new Properties();
    try (InputStream in = Files.newInputStream(Path.of("etc/conf/server.log4j2.properties"))) {
      properties.load(in);
    }
    Configuration configuration = LoggerContext.getContext(false).getConfiguration();
    // Resolve ${...} lookups the way log4j does when it reads the file.
    String pattern =
        configuration
            .getStrSubstitutor()
            .replace(properties.getProperty("appender.rollingFile.layout.pattern"));
    return PatternLayout.newBuilder()
        .withPattern(pattern)
        .withConfiguration(configuration)
        .build()
        .toSerializable(event);
  }

  private static Log4jLogEvent.Builder event() {
    return Log4jLogEvent.newBuilder()
        .setLoggerName("io.unitycatalog.server.Example")
        .setLevel(Level.INFO)
        .setMessage(new SimpleMessage("hello"));
  }

  @Test
  public void namesTheInstanceAndTheRequest() throws Exception {
    SortedArrayStringMap contextData = new SortedArrayStringMap();
    contextData.putValue(RequestIdContextDataProvider.LOG_KEY, "req-9");

    String line = format(event().setContextData(contextData).build());

    assertThat(line).contains(NetUtils.getLocalHostname()).contains("req-9").contains("hello");
  }

  @Test
  public void omitsTheRequestOutsideOne() throws Exception {
    String line = format(event().build());

    assertThat(line)
        .contains(NetUtils.getLocalHostname())
        .doesNotContain(RequestIdContextDataProvider.LOG_KEY);
  }
}
