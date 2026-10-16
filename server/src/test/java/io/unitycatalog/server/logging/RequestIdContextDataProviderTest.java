package io.unitycatalog.server.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.RequestContext;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.utils.LogCapture;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestIdContextDataProviderTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(RequestIdContextDataProviderTest.class);

  @Test
  public void logLinesWrittenWhileServingARequestCarryItsId() {
    ServiceRequestContext ctx = ServiceRequestContext.of(HttpRequest.of(HttpMethod.GET, "/"));
    try (LogCapture logs = LogCapture.of(RequestIdContextDataProviderTest.class)) {
      try (var ignored = ctx.push()) {
        LOGGER.info("inside the request");
      }
      LOGGER.info("outside any request");

      LogEvent inside = logs.events().get(0);
      LogEvent outside = logs.events().get(1);
      assertThat(inside.getContextData().<String>getValue(RequestIdContextDataProvider.LOG_KEY))
          .isEqualTo(ctx.id().text());
      assertThat(outside.getContextData().<String>getValue(RequestIdContextDataProvider.LOG_KEY))
          .isNull();
    }
  }

  @Test
  public void logLinesWrittenWhileArmeriaIsStartingHaveNoId() {
    // While Armeria's Flags class initializes, it logs before its request context storage exists,
    // and looking up the current context throws.
    try (MockedStatic<RequestContext> requestContext = mockStatic(RequestContext.class)) {
      requestContext.when(RequestContext::currentOrNull).thenThrow(new NullPointerException());
      assertThat(new RequestIdContextDataProvider().supplyContextData()).isEmpty();
    }
  }
}
