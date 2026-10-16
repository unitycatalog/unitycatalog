package io.unitycatalog.server.logging;

import com.linecorp.armeria.common.RequestContext;
import com.linecorp.armeria.common.logging.BuiltInProperty;
import com.linecorp.armeria.common.logging.RequestContextExporter;
import com.linecorp.armeria.server.ServiceRequestContext;
import java.util.Map;
import org.apache.logging.log4j.core.util.ContextDataProvider;

/**
 * Adds fields of the request being served to each log event, such as its ID, so the lines one
 * request writes can be found together. Log4j loads this class through {@code META-INF/services}.
 * Armeria keeps the request context current on whichever thread serves the request, including the
 * blocking task executor, which a thread-local MDC set once per request would not follow.
 */
public final class RequestContextDataProvider implements ContextDataProvider {

  /** The key the request's ID is logged under. */
  public static final String REQUEST_ID_KEY = "request_id";

  /**
   * The request fields added to each log line, one {@code builtIn} per field, each under the key it
   * is logged under. Every field is attached to every line a request writes, so add only fields
   * that are safe to log.
   *
   * <p>Built on first use, while a request is being served. Building it earlier can initialize
   * Armeria's Flags class while that class is still logging its own startup.
   */
  private static final class Fields {
    static final RequestContextExporter EXPORTER =
        RequestContextExporter.builder().builtIn(BuiltInProperty.REQ_ID, REQUEST_ID_KEY).build();
  }

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
    // root(): the served request, even while a client call made to serve it is the current one.
    ServiceRequestContext served = ctx == null ? null : ctx.root();
    return served == null ? Map.of() : Fields.EXPORTER.export(served);
  }
}
