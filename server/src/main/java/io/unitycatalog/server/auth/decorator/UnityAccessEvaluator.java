package io.unitycatalog.server.auth.decorator;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.model.Privileges;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
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

  private final UnityCatalogAuthorizer authorizer;
  private final ExpressionParser parser;
  private final MethodHandle authorizeHandle;
  private final MethodHandle authorizeAnyHandle;
  private final MethodHandle authorizeAllHandle;

  public UnityAccessEvaluator(UnityCatalogAuthorizer authorizer)
      throws NoSuchMethodException, IllegalAccessException {
    this.authorizer = authorizer;
    this.parser = new SpelExpressionParser();

    MethodHandles.Lookup lookup = MethodHandles.lookup();
    MethodType mt = MethodType.methodType(boolean.class, UUID.class, UUID.class, Privileges.class);
    MethodHandle mh = lookup.findVirtual(authorizer.getClass(), "authorize", mt);
    authorizeHandle = mh.bindTo(this.authorizer);

    mt = MethodType.methodType(boolean.class, Object[].class);
    mh = lookup.findVirtual(this.getClass(), "authorizeAny", mt);
    authorizeAnyHandle = mh.bindTo(this);

    mt = MethodType.methodType(boolean.class, Object[].class);
    mh = lookup.findVirtual(this.getClass(), "authorizeAll", mt);
    authorizeAllHandle = mh.bindTo(this);
  }

  protected boolean authorizeAny(Object... parameters) {
    // TODO: Find a better way to deal with the varargs authorizeAny() method.
    UUID principalId = (UUID) parameters[0];
    UUID resource = (UUID) parameters[1];
    Privileges[] privileges = new Privileges[parameters.length - 2];
    System.arraycopy(parameters, 2, privileges, 0, privileges.length);
    return authorizer.authorizeAny(principalId, resource, privileges);
  }

  protected boolean authorizeAll(Object... parameters) {
    // TODO: Find a better way to deal with the varargs authorizeAll() method.
    UUID principalId = (UUID) parameters[0];
    UUID resource = (UUID) parameters[1];
    Privileges[] privileges = new Privileges[parameters.length - 2];
    System.arraycopy(parameters, 2, privileges, 0, privileges.length);
    return authorizer.authorizeAll(principalId, resource, privileges);
  }

  public boolean evaluate(
      UUID principal, String expression, Map<SecurableType, Object> resourceIds) {

    StandardEvaluationContext context = new StandardEvaluationContext(Privileges.class);

    context.registerFunction("authorize", authorizeHandle);
    context.registerFunction("authorizeAny", authorizeAnyHandle);
    context.registerFunction("authorizeAll", authorizeAllHandle);

    context.setVariable("deny", Boolean.FALSE);
    context.setVariable("permit", Boolean.TRUE);
    context.setVariable("defer", Boolean.TRUE);
    context.setVariable("principal", principal);

    resourceIds.forEach((k, v) -> context.setVariable(k.name().toLowerCase(), v));

    Boolean result = parser.parseExpression(expression).getValue(context, Boolean.class);

    LOGGER.debug("evaluating {} = {}", expression, result);

    return result != null ? result : false;
  }

  public <T> void filter(
      UUID principalId,
      String expression,
      List<T> entries,
      Function<T, Map<SecurableType, Object>> resolver) {
    entries.removeIf(c -> !evaluate(principalId, expression, resolver.apply(c)));
  }
}
