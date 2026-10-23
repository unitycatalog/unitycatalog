package io.unitycatalog.server.auth.decorator;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;

import io.unitycatalog.server.auth.AuthorizationRequest;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.ResponseAuthorizeFilter;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CredentialInfo;
import io.unitycatalog.server.model.ExternalLocationInfo;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.model.RegisteredModelInfo;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.service.AuthorizedService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filters list responses based on user authorization permissions.
 *
 * <p>This class is used in the authorization decorator layer to filter LIST operation results. When
 * a service method is annotated with {@code @ResponseAuthorizeFilter}, the {@link
 * UnityAccessDecorator} creates a ResultFilter instance and stores it in the request context. The
 * service method then retrieves this filter and calls it to remove items that the user is not
 * authorized to access.
 *
 * <p><b>How It Works:</b>
 *
 * <ol>
 *   <li>UnityAccessDecorator intercepts HTTP requests and extracts authorization parameters
 *       (principal ID, expression, resource IDs) from method annotations
 *   <li>It creates a ResultFilter with these parameters and stores it in the request context
 *   <li>The service method retrieves the filter via {@code applyResponseFilter()} and applies it to
 *       the response list
 *   <li>The filter evaluates authorization for each item and removes unauthorized items
 * </ol>
 *
 * <p><b>Resource ID Resolution:</b><br>
 * For each item in the list, the filter resolves resource IDs of items but expect the catalog and
 * schema IDs to be provided in the request already. Special handling exists for REGISTERED_MODEL
 * because models can be listed without specifying catalog/schema filters. In this case, the filter
 * extracts the catalog and schema names from each model and resolves them to IDs.
 *
 * <p><b>Security:</b><br>
 * The {@code wasCalled()} method allows UnityAccessDecorator to verify that the filter was actually
 * used. If a method is annotated with {@code @ResponseAuthorizeFilter} but doesn't call the filter,
 * a security exception is thrown to prevent data leakage.
 *
 * @see ResponseAuthorizeFilter
 * @see UnityAccessDecorator
 * @see AuthorizedService#applyResponseFilter
 */
public class ResultFilter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultFilter.class);

  private final AuthorizeExpression rule;
  private final AuthorizationRequest request;
  private final UnityAccessEvaluator evaluator;
  private final KeyMapper keyMapper;
  private final AtomicBoolean called;

  public ResultFilter(
      UnityAccessEvaluator evaluator,
      AuthorizeExpression rule,
      AuthorizationRequest request,
      KeyMapper keyMapper) {
    this.rule = rule;
    this.request = request;
    this.evaluator = evaluator;
    this.keyMapper = keyMapper;
    this.called = new AtomicBoolean(false);
  }

  /**
   * Filters a list of resources based on the user's authorization permissions.
   *
   * <p>This method evaluates the authorization expression for each item in the list and removes
   * items that the user is not authorized to access. For each item, it:
   *
   * <ol>
   *   <li>Resolves the item's resource ID (e.g., table ID, volume ID)
   *   <li>Resolves parent (catalog and schema) resource IDs if it's registered model
   *   <li>Evaluates the authorization expression with all resolved and provided IDs
   *   <li>Removes the item if the expression evaluates to false
   * </ol>
   *
   * <p>A native authorization error filters out the item. An external policy failure fails the
   * request, so an unavailable policy engine cannot look like a successful empty list.
   *
   * @param securableType The type of resources being filtered (TABLE, VOLUME, FUNCTION, etc.)
   * @param items The list of items to filter in-place. This list is modified by removing
   *     unauthorized items.
   * @param <T> The type of items in the list (e.g., TableInfo, VolumeInfo)
   * @throws RuntimeException if the securableType is already resolved in the pre-resolved IDs,
   *     which indicates a programming error
   */
  public <T> void filter(SecurableType securableType, List<T> items) {
    called.set(true);

    if (items == null || items.isEmpty()) {
      return;
    }

    LOGGER.debug("Filtering {} items with authorization expression", items.size());
    if (request.resources().containsKey(securableType)) {
      // This simply indicates a bug in code.
      throw new RuntimeException("Securable type " + securableType + " is already resolved");
    }

    long operationStartNanos = System.nanoTime();

    items.removeIf(
        item -> {
          try {
            boolean authorized =
                evaluator.evaluate(
                    rule, resolveRequestForItem(securableType, item), operationStartNanos);
            if (!authorized) {
              LOGGER.debug("Item filtered out: {}", item.getClass().getSimpleName());
            }
            return !authorized;
          } catch (RuntimeException e) {
            // A failed policy call must fail the list request, rather than look like an empty list.
            if (evaluator.isExternalAuthorization(rule)) {
              throw e;
            }
            LOGGER.warn(
                "Error evaluating authorization for item, filtering out: {}", e.getMessage());
            return true;
          }
        });

    LOGGER.debug("After filtering: {} items remain", items.size());
  }

  private AuthorizationRequest resolveRequestForItem(SecurableType securableType, Object item) {
    Resource resource = resolveResource(securableType, item);
    Map<SecurableType, UUID> resourceIds = new HashMap<>(request.resources());
    resourceIds.put(securableType, UUID.fromString(resource.id()));
    Map<SecurableType, Object> resourceKeys = new HashMap<>(request.resourceKeys());
    resourceKeys.put(securableType, resource.name());
    if (securableType == REGISTERED_MODEL) {
      RegisteredModelInfo model = (RegisteredModelInfo) item;
      resourceKeys.put(CATALOG, model.getCatalogName());
      resourceKeys.put(SCHEMA, model.getSchemaName());
      // Models can be listed without catalog/schema filters, so resolve their parents if needed.
      if (!resourceIds.containsKey(CATALOG) && !resourceIds.containsKey(SCHEMA)) {
        resourceIds.putAll(
            keyMapper.mapResourceKeys(
                Map.of(CATALOG, model.getCatalogName(), SCHEMA, model.getSchemaName())));
      }
    }
    return new AuthorizationRequest(
        request.principal(), request.action(), resourceIds, resourceKeys, request.context());
  }

  private Resource resolveResource(SecurableType securableType, Object item) {
    return switch (securableType) {
      // Iceberg table listings pass DAOs rather than materializing API models.
      case TABLE ->
          item instanceof TableInfoDAO dao
              ? new Resource(dao.getId().toString(), dao.getName())
              : new Resource(((TableInfo) item).getTableId(), ((TableInfo) item).getName());
      case VOLUME -> new Resource(((VolumeInfo) item).getVolumeId(), ((VolumeInfo) item).getName());
      case FUNCTION ->
          new Resource(((FunctionInfo) item).getFunctionId(), ((FunctionInfo) item).getName());
      case REGISTERED_MODEL ->
          new Resource(
              ((RegisteredModelInfo) item).getId(), ((RegisteredModelInfo) item).getName());
      case CATALOG -> new Resource(((CatalogInfo) item).getId(), ((CatalogInfo) item).getName());
      case SCHEMA -> new Resource(((SchemaInfo) item).getSchemaId(), ((SchemaInfo) item).getName());
      case CREDENTIAL ->
          new Resource(((CredentialInfo) item).getId(), ((CredentialInfo) item).getName());
      case EXTERNAL_LOCATION ->
          new Resource(
              ((ExternalLocationInfo) item).getId(), ((ExternalLocationInfo) item).getName());
      default -> throw new RuntimeException("Unsupported securable type: " + securableType);
    };
  }

  private record Resource(String id, String name) {}

  /**
   * Returns whether the filter has been called.
   *
   * <p>This method is used by {@link UnityAccessDecorator} to verify that methods annotated with
   * {@code @ResponseAuthorizeFilter} actually call the filter on successful responses. This is a
   * security enforcement mechanism to prevent data leakage when developers forget to filter
   * results.
   *
   * @return true if {@link #filter} has been called at least once, false otherwise
   */
  public boolean wasCalled() {
    return called.get();
  }
}
