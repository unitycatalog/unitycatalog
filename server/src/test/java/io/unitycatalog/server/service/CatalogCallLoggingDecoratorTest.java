package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Route;
import com.linecorp.armeria.server.ServiceRequestContext;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link CatalogCallLoggingDecorator}: verifies exactly one INFO log line is emitted
 * per catalog API call, containing the operation name only (no row payload).
 */
class CatalogCallLoggingDecoratorTest {

  private static final String CATALOGS_PATH = "/api/2.1/unity-catalog/catalogs";

  private final List<LogEvent> events = new CopyOnWriteArrayList<>();
  private AbstractAppender appender;
  private org.apache.logging.log4j.core.Logger log4jLogger;

  @BeforeEach
  void setUp() {
    LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
    log4jLogger = loggerContext.getLogger(CatalogCallLoggingDecorator.class.getName());
    appender =
        new AbstractAppender(
            "CatalogCallLoggingTestAppender", null, null, true, Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    appender.start();
    log4jLogger.addAppender(appender);
    log4jLogger.setLevel(Level.INFO);
  }

  @AfterEach
  void tearDown() {
    log4jLogger.removeAppender(appender);
    appender.stop();
  }

  @Test
  void logsExactlyOneInfoLineWithOperationNameOnly() throws Exception {
    CatalogCallLoggingDecorator decorator = new CatalogCallLoggingDecorator();
    HttpService delegate = (ctx, req) -> HttpResponse.of(HttpStatus.OK);
    Route route = Route.builder().pathPrefix(CATALOGS_PATH).build();
    ServiceRequestContext ctx =
        ServiceRequestContext.builder(HttpRequest.of(HttpMethod.GET, CATALOGS_PATH))
            .route(route)
            .build();

    HttpResponse response = decorator.serve(delegate, ctx, ctx.request());
    response.aggregate().join();

    assertThat(events).hasSize(1);
    LogEvent event = events.get(0);
    assertThat(event.getLevel()).isEqualTo(Level.INFO);
    String message = event.getMessage().getFormattedMessage();
    assertThat(message).isEqualTo("Catalog API call: GET " + CATALOGS_PATH + "/*");
    // Operation name only: no JSON body, no row payload, no credential material
    assertThat(message).doesNotContain("{");
    assertThat(message).doesNotContain("credential");
    assertThat(message).doesNotContain("token");
  }
}
