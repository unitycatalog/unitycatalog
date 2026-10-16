package io.unitycatalog.server.logging;

import com.linecorp.armeria.common.RequestContext;
import com.linecorp.armeria.server.ServiceRequestContext;
import java.util.Map;
import org.apache.logging.log4j.core.util.ContextDataProvider;

/**
 * Adds the ID of the request being served to each log event, under {@value #LOG_KEY}, so the lines
 * one request writes can be found together. The ID is the one Armeria generates for each request.
 * Log4j loads this class through {@code META-INF/services}. Armeria keeps the request context
 * current on whichever thread serves the request, including the blocking task executor, which a
 * thread-local MDC set once per request would not follow.
 */
public final class RequestIdContextDataProvider implements ContextDataProvider {

  /** The key the ID is logged under. */
  public static final String LOG_KEY = "request_id";

  @Override
  public Map<String, String> supplyContextData() {
    RequestContext ctx;
    try {
      ctx = RequestContext.currentOrNull();
    } catch (RuntimeException e) {
      // Armeria's Flags class logs while it initializes, before the context storage exists, when
      // an operator sets a com.linecorp.armeria.* system property. Those lines have no request.
      return Map.of();
    }
    ServiceRequestContext served = ctx == null ? null : ctx.root();
    return served == null ? Map.of() : Map.of(LOG_KEY, served.id().text());
  }
}
