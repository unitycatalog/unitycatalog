package io.unitycatalog.server.utils;

import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.service.AuthDecorator;

public class IdentityUtils {
  public static String findPrincipalEmailAddress() {
    ServiceRequestContext ctx = ServiceRequestContext.current();
    return ctx.attr(AuthDecorator.PRINCIPAL_EMAIL_ATTR);
  }
}
