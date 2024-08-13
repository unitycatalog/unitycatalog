package io.unitycatalog.server.auth.decorator;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.model.ResourceType;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * Spring Expression Language access control evaluator.
 *
 * <p>A simple class to allow evaluating an SpEL authorization expression. This class exposes all
 * the principal and resource ids provided as SpEL context variables. It also exposes functions
 * authorization functions to call the configured authorization backend implementation which can be
 * used with SpEL logical operators to define compound authorization rules.
 *
 * <p>Example:
 *
 * <p>#authorize(#principal, #schema, 'USE SCHEMA') || #authorize(#principal, #table, 'OWNER')
 */
public class UnityAccessEvaluator {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnityAccessEvaluator.class);

  private final UnityCatalogAuthorizer authenticator;
  private final ExpressionParser parser;
  private final MethodHandle authorizeHandle;
  private final MethodHandle authorizeAnyHandle;
  private final MethodHandle authorizeAllHandle;

  public UnityAccessEvaluator(UnityCatalogAuthorizer authenticator)
      throws NoSuchMethodException, IllegalAccessException {
    this.authenticator = authenticator;
    this.parser = new SpelExpressionParser();

    MethodHandles.Lookup lookup = MethodHandles.lookup();
    MethodType mt = MethodType.methodType(boolean.class, UUID.class, UUID.class, Privilege.class);
    MethodHandle mh = lookup.findVirtual(authenticator.getClass(), "authorize", mt);
    authorizeHandle = mh.bindTo(this.authenticator);

    mt = MethodType.methodType(boolean.class, UUID.class, UUID.class, Privilege[].class);
    mh = lookup.findVirtual(authenticator.getClass(), "authorizeAny", mt);
    authorizeAnyHandle = mh.bindTo(this.authenticator);

    mt = MethodType.methodType(boolean.class, UUID.class, UUID.class, Privilege[].class);
    mh = lookup.findVirtual(authenticator.getClass(), "authorizeAll", mt);
    authorizeAllHandle = mh.bindTo(this.authenticator);
  }

  public boolean evaluate(
      UUID principal, String expression, Map<ResourceType, Object> resourceIds) {

    StandardEvaluationContext context = new StandardEvaluationContext();

    context.setVariable("deny", Boolean.FALSE);
    context.setVariable("authorize", authorizeHandle);
    context.setVariable("authorizeAny", authorizeAnyHandle);
    context.setVariable("authorizeAll", authorizeAllHandle);
    context.setVariable("principal", principal);

    resourceIds.forEach((k, v) -> context.setVariable(k.name().toLowerCase(), v));

    Boolean result = parser.parseExpression(expression).getValue(context, Boolean.class);

    LOGGER.debug("evaluating {} = {}", expression, result);

    return result != null ? result : false;
  }
}
