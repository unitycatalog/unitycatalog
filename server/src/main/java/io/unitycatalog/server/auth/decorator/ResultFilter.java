package io.unitycatalog.server.auth.decorator;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.unitycatalog.server.service.AuthorizedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;

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

  private final UUID principalId;
  private final String expression;
  private final UnityAccessEvaluator evaluator;
  private final Map<SecurableType, UUID> resourceIds;
  private final Map<String, Object> nonResourceValues;
  private final KeyMapper keyMapper;
  private boolean called = false;

  public ResultFilter(
      UnityAccessEvaluator evaluator,
      UUID principalId,
      String expression,
      Map<SecurableType, UUID> resourceIds,
      Map<String, Object> nonResourceValues,
      KeyMapper keyMapper) {
    this.principalId = principalId;
    this.expression = expression;
    this.evaluator = evaluator;
    this.resourceIds = resourceIds;
    this.nonResourceValues = nonResourceValues;
    this.keyMapper = keyMapper;
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
   * <p>If an error occurs during authorization evaluation for an item, that item is filtered out
   * (removed from the list) as a security precaution.
   *
   * @param securableType The type of resources being filtered (TABLE, VOLUME, FUNCTION, etc.)
   * @param items The list of items to filter in-place. This list is modified by removing
   *     unauthorized items.
   * @param <T> The type of items in the list (e.g., TableInfo, VolumeInfo)
   * @throws RuntimeException if the securableType is already resolved in the pre-resolved IDs,
   *     which indicates a programming error
   */
  public <T> void filter(SecurableType securableType, List<T> items) {
    called = true;

    if (items == null || items.isEmpty()) {
      return;
    }

    LOGGER.debug("Filtering {} items with authorization expression", items.size());
    if (resourceIds.containsKey(securableType)) {
      // This simply indicates a bug in code.
      throw new RuntimeException("Securable type " + securableType + " is already resolved");
    }

    items.removeIf(
        item -> {
          try {
            // Resolve the item's resource ID and any parent resource IDs to make a complete
            // collection of all needed resource IDs for this item.
            Map<SecurableType, UUID> resourceIdsForItem =
                resolveResourceIdsForItem(securableType, item, resourceIds);
            boolean authorized =
                evaluator.evaluate(principalId, expression, resourceIdsForItem, nonResourceValues);
            if (!authorized) {
              LOGGER.debug("Item filtered out: {}", item.getClass().getSimpleName());
            }
            return !authorized;
          } catch (Exception e) {
            LOGGER.warn(
                "Error evaluating authorization for item, filtering out: {}", e.getMessage());
            return true;
          }
        });

    LOGGER.debug("After filtering: {} items remain", items.size());
  }

  private Map<SecurableType, UUID> resolveResourceIdsForItem(
      SecurableType securableType, Object item, Map<SecurableType, UUID> preResolvedIds) {
    // First, resolve the item's own resource ID
    UUID itemId = resolveResourceId(securableType, item);
    Map<SecurableType, UUID> combined = new HashMap<>(preResolvedIds);
    combined.put(securableType, itemId);
    // For REGISTERED_MODEL, resolve catalog and schema if both are not present.
    if (securableType == REGISTERED_MODEL
        && !preResolvedIds.containsKey(CATALOG)
        && !preResolvedIds.containsKey(SCHEMA)) {
      RegisteredModelInfo model = (RegisteredModelInfo) item;
      combined.putAll(
          keyMapper.mapResourceKeys(
              Map.of(CATALOG, model.getCatalogName(), SCHEMA, model.getSchemaName())));
    }
    // For everything else, the catalog and schema IDs are already included in preResolvedIds
    // if needed.
    return combined;
  }

  private UUID resolveResourceId(SecurableType securableType, Object item) {
    String id = switch (securableType) {
      case TABLE -> ((TableInfo)item).getTableId();
      case VOLUME -> ((VolumeInfo)item).getVolumeId();
      case FUNCTION -> ((FunctionInfo)item).getFunctionId();
      case REGISTERED_MODEL -> ((RegisteredModelInfo)item).getId();
      case CATALOG -> ((CatalogInfo)item).getId();
      case SCHEMA -> ((SchemaInfo)item).getSchemaId();
      case CREDENTIAL -> ((CredentialInfo)item).getId();
      case EXTERNAL_LOCATION -> ((ExternalLocationInfo)item).getId();
      default -> throw new RuntimeException("Unsupported securable type: " + securableType);
    };
    return UUID.fromString(id);
  }

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
    return called;
  }
}
