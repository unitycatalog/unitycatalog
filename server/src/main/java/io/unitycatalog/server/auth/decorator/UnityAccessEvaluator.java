package io.unitycatalog.server.auth.decorator;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.model.Privileges;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelEvaluationException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * Spring Expression Language access control evaluator.
 *
 * <p>Exposes the principal UUID and resource values (type and UUID) as SpEL context variables.
 * Authorization functions call the configured backend and can be combined with SpEL logical
 * operators to define compound authorization rules.
 *
 * <p>{@code #strict_privilege_checks} is a boolean from server configuration. Request values cannot
 * override it.
 *
 * <p>Example:
 *
 * <p>#authorize(#principal, #schema, 'USE SCHEMA') || #authorize(#principal, #table, 'OWNER')
 *
 * <p>To check one expression against many resources (list filtering), use {@link #batch}: a {@link
 * Batch} remembers each authorizer decision, so checks on the shared parents (metastore, catalog,
 * schema) run once per list instead of once per item, since authorizer checks can be expensive.
 */
public class UnityAccessEvaluator {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnityAccessEvaluator.class);

  private final UnityCatalogAuthorizer authorizer;
  private final boolean strictPrivilegeChecks;
  private final ExpressionParser parser;
  private final MethodHandle authorizeHandle;
  private final MethodHandle authorizeAnyHandle;
  private final MethodHandle authorizeAllHandle;

  public UnityAccessEvaluator(UnityCatalogAuthorizer authorizer, boolean strictPrivilegeChecks)
      throws NoSuchMethodException, IllegalAccessException {
    this.authorizer = authorizer;
    this.strictPrivilegeChecks = strictPrivilegeChecks;
    this.parser = new SpelExpressionParser();

    // Unbound; each Batch binds them to itself so the expression's checks go through its cache.
    MethodHandles.Lookup lookup = MethodHandles.lookup();
    MethodType mt =
        MethodType.methodType(boolean.class, UUID.class, Resource.class, Privileges.class);
    authorizeHandle = lookup.findVirtual(Batch.class, "authorize", mt);

    mt = MethodType.methodType(boolean.class, Object[].class);
    authorizeAnyHandle = lookup.findVirtual(Batch.class, "authorizeAny", mt);
    authorizeAllHandle = lookup.findVirtual(Batch.class, "authorizeAll", mt);
  }

  /**
   * Evaluate authorization expression with resource IDs and parameter values.
   *
   * @param principal The principal UUID
   * @param expression The SpEL authorization expression
   * @param resourceIds Map of resource types to their UUIDs
   * @param nonResourceValues Map of parameter names to their values (from @AuthorizeKey)
   */
  public boolean evaluate(
      UUID principal,
      String expression,
      Map<SecurableType, UUID> resourceIds,
      Map<String, Object> nonResourceValues) {
    return evaluate(principal, expression, resourceIds, nonResourceValues, System.nanoTime());
  }

  /**
   * Evaluate authorization expression with resource IDs and parameter values.
   *
   * <p>On deny, refreshes policy if needed and re-evaluates. If refresh is disabled or throws, the
   * original deny is kept.
   *
   * @param principal The principal UUID
   * @param expression The SpEL authorization expression
   * @param resourceIds Map of resource types to their UUIDs
   * @param nonResourceValues Map of parameter names to their values (from @AuthorizeKey)
   * @param operationStartNanos {@link System#nanoTime()} from the start of this operation
   */
  public boolean evaluate(
      UUID principal,
      String expression,
      Map<SecurableType, UUID> resourceIds,
      Map<String, Object> nonResourceValues,
      long operationStartNanos) {
    return batch(principal, expression, nonResourceValues, operationStartNanos)
        .evaluate(resourceIds);
  }

  /**
   * Starts evaluating one expression for one principal against many resources, as list filtering
   * does. The returned batch is not thread-safe and should not outlive the operation: it remembers
   * each authorizer decision and clears them only once, when its single refresh runs. A grant or
   * revoke made while it is in use is seen only by checks it has not made yet.
   *
   * @param principal The principal UUID
   * @param expression The SpEL authorization expression
   * @param nonResourceValues Map of parameter names to their values (from @AuthorizeKey)
   * @param operationStartNanos {@link System#nanoTime()} from the start of this operation
   */
  Batch batch(
      UUID principal,
      String expression,
      Map<String, Object> nonResourceValues,
      long operationStartNanos) {
    return new Batch(
        principal, parser.parseExpression(expression), nonResourceValues, operationStartNanos);
  }

  private record Resource(SecurableType type, UUID id) {}

  /** Cache key for one authorizer decision. {@code function} is the SpEL function name. */
  private record Decision(
      String function, UUID principal, Resource resource, List<Privileges> actions) {}

  /** One expression evaluated for one principal against many resources; see {@link #batch}. */
  final class Batch {
    private final UUID principal;
    private final Expression expression;
    private final Map<String, Object> nonResourceValues;
    private final long operationStartNanos;
    private final MethodHandle boundAuthorize;
    private final MethodHandle boundAuthorizeAny;
    private final MethodHandle boundAuthorizeAll;
    private final Map<Decision, Boolean> decisions = new HashMap<>();
    private boolean refreshAttempted;

    private Batch(
        UUID principal,
        Expression expression,
        Map<String, Object> nonResourceValues,
        long operationStartNanos) {
      this.principal = principal;
      this.expression = expression;
      this.nonResourceValues = nonResourceValues;
      this.operationStartNanos = operationStartNanos;
      this.boundAuthorize = authorizeHandle.bindTo(this);
      this.boundAuthorizeAny = authorizeAnyHandle.bindTo(this);
      this.boundAuthorizeAll = authorizeAllHandle.bindTo(this);
    }

    /**
     * Evaluates the expression for one resource.
     *
     * <p>On deny, refreshes policy if needed, forgets the remembered decisions, and re-evaluates.
     * That happens at most once per batch: one refresh already brings the policy up to the start of
     * the operation, so later denies in the same batch are final. The first deny uses up that one
     * attempt whatever the outcome: if refresh returns false (disabled, failed probe, interrupt) or
     * throws, that deny and every later one in the batch are kept without retrying.
     *
     * @param resourceIds Map of resource types to their UUIDs
     */
    boolean evaluate(Map<SecurableType, UUID> resourceIds) {
      StandardEvaluationContext context = newContext(resourceIds);
      if (eval(context)) {
        return true;
      }
      if (refreshAttempted) {
        return false;
      }
      refreshAttempted = true;

      try {
        if (!authorizer.refreshAuthorizations(operationStartNanos)) {
          return false;
        }
      } catch (RuntimeException e) {
        LOGGER.warn(
            "Authorization refresh failed for principal {}; keeping original deny", principal, e);
        return false;
      }

      decisions.clear();
      boolean allowedAfterRefresh = eval(context);
      if (allowedAfterRefresh) {
        LOGGER.debug("Access allowed after refreshing authorizations for principal {}", principal);
      }
      return allowedAfterRefresh;
    }

    private StandardEvaluationContext newContext(Map<SecurableType, UUID> resourceIds) {
      StandardEvaluationContext context = new StandardEvaluationContext(Privileges.class);

      context.registerFunction("authorize", boundAuthorize);
      context.registerFunction("authorizeAny", boundAuthorizeAny);
      context.registerFunction("authorizeAll", boundAuthorizeAll);

      context.setVariable("deny", Boolean.FALSE);
      context.setVariable("permit", Boolean.TRUE);
      context.setVariable("principal", principal);

      resourceIds.forEach(
          (type, id) ->
              context.setVariable(
                  type.name().toLowerCase(), id == null ? null : new Resource(type, id)));
      nonResourceValues.forEach(context::setVariable);
      context.setVariable("strict_privilege_checks", strictPrivilegeChecks);
      return context;
    }

    private boolean eval(StandardEvaluationContext context) {
      try {
        Boolean result = expression.getValue(context, Boolean.class);
        return result != null && result;
      } catch (SpelEvaluationException e) {
        if (e.getCause() instanceof BaseException failure
            && failure.getErrorCode().getHttpStatus().isServerError()) {
          throw failure;
        }
        throw e;
      }
    }

    boolean authorize(UUID principalId, Resource resource, Privileges privilege) {
      return decisions.computeIfAbsent(
          new Decision("authorize", principalId, resource, List.of(privilege)),
          d -> authorizer.authorize(principalId, resource.type(), resource.id(), privilege));
    }

    boolean authorizeAny(Object... parameters) {
      // TODO: Find a better way to deal with the varargs authorizeAny() method.
      UUID principalId = (UUID) parameters[0];
      Resource resource = (Resource) parameters[1];
      Privileges[] privileges = privileges(parameters);
      return decisions.computeIfAbsent(
          new Decision("authorizeAny", principalId, resource, List.of(privileges)),
          d -> authorizer.authorizeAny(principalId, resource.type(), resource.id(), privileges));
    }

    boolean authorizeAll(Object... parameters) {
      // TODO: Find a better way to deal with the varargs authorizeAll() method.
      UUID principalId = (UUID) parameters[0];
      Resource resource = (Resource) parameters[1];
      Privileges[] privileges = privileges(parameters);
      return decisions.computeIfAbsent(
          new Decision("authorizeAll", principalId, resource, List.of(privileges)),
          d -> authorizer.authorizeAll(principalId, resource.type(), resource.id(), privileges));
    }

    private static Privileges[] privileges(Object[] parameters) {
      Privileges[] privileges = new Privileges[parameters.length - 2];
      System.arraycopy(parameters, 2, privileges, 0, privileges.length);
      return privileges;
    }
  }
}
