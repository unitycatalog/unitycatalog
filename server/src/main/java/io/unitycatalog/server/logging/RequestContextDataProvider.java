package io.unitycatalog.server.logging;

import com.linecorp.armeria.common.RequestContext;
import com.linecorp.armeria.common.logging.BuiltInProperty;
import com.linecorp.armeria.common.logging.RequestContextExporter;
import com.linecorp.armeria.server.ServiceRequestContext;
import java.util.Map;
import org.apache.logging.log4j.core.util.ContextDataProvider;

/** Adds fields of the request being served, such as its ID, to each log line. */
public final class RequestContextDataProvider implements ContextDataProvider {

  /** The key the request's ID is logged under. */
  private static final String REQUEST_ID_KEY = "request_id";

  /** One {@code builtIn} per field. Built lazily, so it can't initialize Armeria's Flags early. */
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
      // Armeria's Flags logs before its context storage exists; those lines have no request.
      return Map.of();
    }
    // root(): the served request, even during a client call made while serving it.
    ServiceRequestContext served = ctx == null ? null : ctx.root();
    return served == null ? Map.of() : Fields.EXPORTER.export(served);
  }
}
