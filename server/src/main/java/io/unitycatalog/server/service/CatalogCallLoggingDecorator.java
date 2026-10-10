package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs exactly one INFO line per catalog API call. The log line contains the operation name only
 * (HTTP method + route pattern). No row data, file bytes, or credentials are ever logged here.
 */
public class CatalogCallLoggingDecorator implements DecoratingHttpServiceFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogCallLoggingDecorator.class);

  @Override
  public HttpResponse serve(HttpService delegate, ServiceRequestContext ctx, HttpRequest req)
      throws Exception {
    LOGGER.info(
        "Catalog API call: {} {}", ctx.method().name(), ctx.config().route().patternString());
    return delegate.serve(ctx, req);
  }
}
