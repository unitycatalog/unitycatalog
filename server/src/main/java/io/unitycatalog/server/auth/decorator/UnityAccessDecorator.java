package io.unitycatalog.server.auth.decorator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.internal.server.annotation.AnnotatedService;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.SimpleDecoratingHttpService;
import com.linecorp.armeria.server.annotation.Param;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeKeys;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.model.RegisteredModelInfo;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.FunctionRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.ModelRepository;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.VolumeRepository;
import io.unitycatalog.server.utils.IdentityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.unitycatalog.server.auth.decorator.KeyLocator.Source.PARAM;
import static io.unitycatalog.server.auth.decorator.KeyLocator.Source.PAYLOAD;
import static io.unitycatalog.server.auth.decorator.KeyLocator.Source.SYSTEM;
import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.FUNCTION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.model.SecurableType.VOLUME;

/**
 * Armeria access control Decorator.
 * <p>
 * This decorator provides the ability to protect Armeria service methods with per method access
 * control rules. This decorator is used in conjunction with two annotations, @AuthorizeExpression
 * and @AuthorizeKey to define authorization rules and identify requests parameters for objects
 * to authorize with.
 *
 * @AuthorizeExpression - This defines a Spring Expression Language expression to evaluate to make
 * an authorization decision.
 * @AuthorizeKey - This annotation is used to define request and payload parameters for the authorization
 * context. These are typically things like catalog, schema and table names. This annotation may be used
 * at both the method and method parameter context. It may be specified more than once per method to
 * map parameters to object keys.
 */
public class UnityAccessDecorator implements DecoratingHttpServiceFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnityAccessDecorator.class);
  public static final ObjectMapper MAPPER = new ObjectMapper();

  private final UnityAccessEvaluator evaluator;

  public UnityAccessDecorator(UnityCatalogAuthorizer authorizer) throws BaseException {
    try {
      evaluator = new UnityAccessEvaluator(authorizer);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new BaseException(ErrorCode.INTERNAL, "Error initializing access evaluator.", e);
    }
  }

  @Override
  public HttpResponse serve(HttpService delegate, ServiceRequestContext ctx, HttpRequest req)
          throws Exception {
    LOGGER.debug("AccessDecorator checking {}", req.path());

    Method method = findServiceMethod(ctx.config().service());

    if (method != null) {

      // Find the authorization parameters to use for this service method.
      String expression = findAuthorizeExpression(method);
      List<KeyLocator> locator = findAuthorizeKeys(method);

      if (expression != null) {
        if (!locator.isEmpty()) {
          UUID principal = IdentityUtils.findPrincipalId();
          return authorizeByRequest(delegate, ctx, req, principal, locator, expression);
        } else {
          LOGGER.warn("No authorization resource(s) found.");
          // going to assume the expression is just #deny, #permit or #defer
        }
      } else {
        LOGGER.debug("No authorization expression found.");
      }
    } else {
      LOGGER.warn("Couldn't unwrap service.");
    }

    return delegate.serve(ctx, req);
  }

  private HttpResponse authorizeByRequest(HttpService delegate, ServiceRequestContext ctx,
                                          HttpRequest req, UUID principal, List<KeyLocator> locators,
                                          String expression) throws Exception {
    //
    // Based on the query and payload parameters defined on the service method (that
    // have been gathered as Locators), we'll attempt to find the entity/resource that
    // we want to authorize against.

    Map<SecurableType, Object> resourceKeys = new HashMap<>();

    // Split up the locators by type, because we have to extract the value from the request
    // different ways for different types

    List<KeyLocator> systemLocators = locators.stream().filter(l -> l.getSource().equals(SYSTEM)).toList();
    List<KeyLocator> paramLocators = locators.stream().filter(l -> l.getSource().equals(PARAM)).toList();
    List<KeyLocator> payloadLocators = locators.stream().filter(l -> l.getSource().equals(PAYLOAD)).toList();

    // Add system-type keys, just metastore for now.
    systemLocators.forEach(l -> resourceKeys.put(l.getType(), "metastore"));

    // Extract the query/path parameter values just by grabbing them from the request
    paramLocators.forEach(l -> {
      String value = ctx.pathParam(l.getKey()) != null ? ctx.pathParam(l.getKey()) : ctx.queryParam(l.getKey());
      resourceKeys.put(l.getType(), value);
    });

    if (payloadLocators.isEmpty()) {
      // If we don't have any PAYLOAD locators, we're ready to evaluate the authorization and allow or deny
      // the request.
      LOGGER.debug("Checking authorization before method.");
      checkAuthorization(principal, expression, resourceKeys);

      return delegate.serve(ctx, req);
    } else {
      // Since we have PAYLOAD locators, we can only interrogate the payload while the request
      // is being evaluated, via peekData()
      LOGGER.debug("Checking authorization before in peekData.");

      PeekDataHandler peekDataHandler = new PeekDataHandler(req.contentType(), payloadLocators, resourceKeys);

      // Note that peekData only gets called for requests that actually have data (like PUT and POST)

      var peekReq = req.peekData(data -> {
        LOGGER.debug("Authorization peekData invoked.");

        if (peekDataHandler.processPeekData(data)) {
          checkAuthorization(principal, expression, resourceKeys);
        }
      });

      return delegate.serve(ctx, peekReq);
    }
  }

  private static Object findPayloadValue(String key, Map<String, Object> payload) {
    // TODO: investigate better object traversal functionality
    String[] args = key.split("[.]", 2);
    if (args.length == 1) {
      return payload.get(args[0]);
    } else {
      if (payload.get(args[0]) instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> value = (Map<String, Object>) payload.get(args[0]);
        return findPayloadValue(args[1], value);
      } else {
        return null;
      }
    }
  }

  private void checkAuthorization(UUID principal, String expression, Map<SecurableType, Object> resourceKeys) {
    LOGGER.debug("resourceKeys = {}", resourceKeys);

    Map<SecurableType, Object> resourceIds = mapResourceKeys(resourceKeys);

    if (!resourceIds.keySet().containsAll(resourceKeys.keySet())) {
      LOGGER.warn("Some resource keys have unresolved ids.");
    }

    LOGGER.debug("resourceIds = {}", resourceIds);

    if (!evaluator.evaluate(principal, expression, resourceIds)) {
      throw new BaseException(ErrorCode.PERMISSION_DENIED, "Access denied.");
    }
  }

  private Map<SecurableType, Object> mapResourceKeys(Map<SecurableType, Object> resourceKeys) {
    Map<SecurableType, Object> resourceIds = new HashMap<>();

    if (resourceKeys.containsKey(CATALOG) && resourceKeys.containsKey(SCHEMA) && resourceKeys.containsKey(TABLE)) {
      String fullName = resourceKeys.get(CATALOG) + "." + resourceKeys.get(SCHEMA) + "." + resourceKeys.get(TABLE);
      TableInfo table = TableRepository.getInstance().getTable(fullName);
      resourceIds.put(TABLE, UUID.fromString(table.getTableId()));
    }

    // If only TABLE is specified, assuming its value is a full table name (including catalog and schema)
    if (!resourceKeys.containsKey(CATALOG) && !resourceKeys.containsKey(SCHEMA) && resourceKeys.containsKey(TABLE)) {
      String fullName = (String) resourceKeys.get(TABLE);
      // If the full name contains a dot, we assume it's a full name, otherwise we assume it's an id
      TableInfo table = fullName.contains(".") ?
        TableRepository.getInstance().getTable(fullName):TableRepository.getInstance().getTableById(fullName);
      String fullSchemaName = table.getCatalogName() + "." + table.getSchemaName();
      SchemaInfo schema = SchemaRepository.getInstance().getSchema(fullSchemaName);
      CatalogInfo catalog = CatalogRepository.getInstance().getCatalog(table.getCatalogName());
      resourceIds.put(TABLE, UUID.fromString(table.getTableId()));
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(CATALOG) && resourceKeys.containsKey(SCHEMA) && resourceKeys.containsKey(VOLUME)) {
      String fullName = resourceKeys.get(CATALOG) + "." + resourceKeys.get(SCHEMA) + "." + resourceKeys.get(VOLUME);
      VolumeInfo volume = VolumeRepository.getInstance().getVolume(fullName);
      resourceIds.put(VOLUME, UUID.fromString(volume.getVolumeId()));
    }

    // If only VOLUME is specified, assuming its value is a full volume name (including catalog and schema)
    if (!resourceKeys.containsKey(CATALOG) && !resourceKeys.containsKey(SCHEMA) && resourceKeys.containsKey(VOLUME)) {
      String fullName = (String) resourceKeys.get(VOLUME);
      // If the full name contains a dot, we assume it's a full name, otherwise we assume it's an id
      VolumeInfo volume = (fullName.contains(".")) ?
              VolumeRepository.getInstance().getVolume(fullName): VolumeRepository.getInstance().getVolumeById(fullName);
      String fullSchemaName = volume.getCatalogName() + "." + volume.getSchemaName();
      SchemaInfo schema = SchemaRepository.getInstance().getSchema(fullSchemaName);
      CatalogInfo catalog = CatalogRepository.getInstance().getCatalog(volume.getCatalogName());
      resourceIds.put(VOLUME, UUID.fromString(volume.getVolumeId()));
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(CATALOG) && resourceKeys.containsKey(SCHEMA) && resourceKeys.containsKey(FUNCTION)) {
      String fullName = resourceKeys.get(CATALOG) + "." + resourceKeys.get(SCHEMA) + "." + resourceKeys.get(FUNCTION);
      FunctionInfo function = FunctionRepository.getInstance().getFunction(fullName);
      resourceIds.put(FUNCTION, UUID.fromString(function.getFunctionId()));
    }

    // If only FUNCTION is specified, assuming its value is a full volume name (including catalog and schema)
    if (!resourceKeys.containsKey(CATALOG) && !resourceKeys.containsKey(SCHEMA) && resourceKeys.containsKey(FUNCTION)) {
      String fullName = (String) resourceKeys.get(FUNCTION);
      FunctionInfo function = FunctionRepository.getInstance().getFunction(fullName);
      String fullSchemaName = function.getCatalogName() + "." + function.getSchemaName();
      SchemaInfo schema = SchemaRepository.getInstance().getSchema(fullSchemaName);
      CatalogInfo catalog = CatalogRepository.getInstance().getCatalog(function.getCatalogName());
      resourceIds.put(FUNCTION, UUID.fromString(function.getFunctionId()));
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(CATALOG) && resourceKeys.containsKey(SCHEMA) && resourceKeys.containsKey(REGISTERED_MODEL)) {
      String fullName = resourceKeys.get(CATALOG) + "." + resourceKeys.get(SCHEMA) + "." + resourceKeys.get(REGISTERED_MODEL);
      RegisteredModelInfo model = ModelRepository.getInstance().getRegisteredModel(fullName);
      resourceIds.put(REGISTERED_MODEL, UUID.fromString(model.getId()));
    }

    // If only REGISTERED_MODEL is specified, assuming its value is a full volume name (including catalog and schema)
    if (!resourceKeys.containsKey(CATALOG) && !resourceKeys.containsKey(SCHEMA) && resourceKeys.containsKey(REGISTERED_MODEL)) {
      String fullName = (String) resourceKeys.get(REGISTERED_MODEL);
      RegisteredModelInfo model = ModelRepository.getInstance().getRegisteredModel(fullName);
      String fullSchemaName = model.getCatalogName() + "." + model.getSchemaName();
      SchemaInfo schema = SchemaRepository.getInstance().getSchema(fullSchemaName);
      CatalogInfo catalog = CatalogRepository.getInstance().getCatalog(model.getCatalogName());
      resourceIds.put(REGISTERED_MODEL, UUID.fromString(model.getId()));
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }


    if (resourceKeys.containsKey(CATALOG) && resourceKeys.containsKey(SCHEMA)) {
      String fullName = resourceKeys.get(CATALOG) + "." + resourceKeys.get(SCHEMA);
      SchemaInfo schema = SchemaRepository.getInstance().getSchema(fullName);
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
    }

    // if only SCHEMA is specified, assuming its value is a full schema name (including catalog)
    if (!resourceKeys.containsKey(CATALOG) && resourceKeys.containsKey(SCHEMA)) {
      String fullName = (String) resourceKeys.get(SCHEMA);
      SchemaInfo schema = SchemaRepository.getInstance().getSchema(fullName);
      CatalogInfo catalog = CatalogRepository.getInstance().getCatalog(schema.getCatalogName());
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(CATALOG)) {
      String fullName = (String) resourceKeys.get(CATALOG);
      CatalogInfo catalog = CatalogRepository.getInstance().getCatalog(fullName);
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(METASTORE)) {
      resourceIds.put(METASTORE, MetastoreRepository.getInstance().getMetastoreId());
    }

    return resourceIds;
  }

  private static String findAuthorizeExpression(Method method) {
    // TODO: Cache this by method

    AuthorizeExpression annotation = method.getAnnotation(AuthorizeExpression.class);

    if (annotation != null) {
      LOGGER.debug("authorize expression = {}", annotation.value());
      return annotation.value();
    } else {
      LOGGER.debug("authorize = (none found)");
      return null;
    }
  }

  private static List<KeyLocator> findAuthorizeKeys(Method method) {
    // TODO: Cache this by method

    List<KeyLocator> locators = new ArrayList<>();

    AuthorizeKey methodKey = method.getAnnotation(AuthorizeKey.class);

    // If resource is on the method, its source is from a global/system variable
    if (methodKey != null) {
      locators.add(KeyLocator.builder().source(SYSTEM).type(methodKey.value()).build());
    }

    for (Parameter parameter : method.getParameters()) {
      AuthorizeKey paramKey = parameter.getAnnotation(AuthorizeKey.class);
      AuthorizeKeys paramKeys = parameter.getAnnotation(AuthorizeKeys.class);

      if (paramKey != null && paramKeys != null) {
        LOGGER.warn("Both AuthorizeKey and AuthorizeKeys present");
      }

      List<AuthorizeKey> allKeys = new ArrayList<>();
      if (paramKey != null) {
        allKeys.add(paramKey);
      }
      if (paramKeys != null) {
        allKeys.addAll(Arrays.asList(paramKeys.value()));
      }

      for (AuthorizeKey key : allKeys) {
        if (!key.key().isEmpty()) {
          // Explicitly declaring a key, so it's the source is from the payload data
          locators.add(KeyLocator.builder().source(PAYLOAD).type(key.value()).key(key.key()).build());
        } else {
          // No key defined so implicitly referencing an (annotated) (query) parameter
          Param param = parameter.getAnnotation(Param.class);
          if (param != null) {
            locators.add(KeyLocator.builder().source(PARAM).type(key.value()).key(param.value()).build());
          } else {
            LOGGER.warn("Couldn't find param key for authorization key");
          }
        }
      }
    }
    return locators;
  }

  private static Method findServiceMethod(HttpService httpService) throws ClassNotFoundException {
    if (httpService.unwrap() instanceof SimpleDecoratingHttpService decoratingService &&
            decoratingService.unwrap() instanceof AnnotatedService service) {

      LOGGER.debug("serviceName = {}, methodName = {}", service.serviceName(), service.methodName());

      Class<?> clazz = Class.forName(service.serviceName());
      List<Method> methods = findMethodsByName(clazz, service.methodName());
      return (methods.size() == 1) ? methods.get(0) : null;
    } else {
      return null;
    }
  }

  private static List<Method> findMethodsByName(Class<?> clazz, String methodName) {
    List<Method> matchingMethods = new ArrayList<>();
    Method[] methods = clazz.getDeclaredMethods();

    for (Method method : methods) {
      if (method.getName().equals(methodName)) {
        matchingMethods.add(method);
      }
    }

    return matchingMethods;
  }

  private static class PeekDataHandler {
    // This is a little ugly - peekData provides only a block of data at a time, so lets
    // buffer it up until we think its complete. A better long term solution would be to
    // abandon this method and either integrate with Spring Boot to get full AOP support
    // with aspect weaving, or build implement custom aspect weaving so we can intercept
    // the method call directly and extract the payload data from the method arguments.

    private final MediaType contentType;
    private final List<KeyLocator> payloadLocators;
    private final Map<SecurableType, Object> resourceKeys;
    private final ByteArrayOutputStream dataStream = new ByteArrayOutputStream();

    private PeekDataHandler(MediaType contentType, List<KeyLocator> payloadLocators, Map<SecurableType, Object> resourceKeys) {
      this.contentType = contentType;
      this.payloadLocators = payloadLocators;
      this.resourceKeys = resourceKeys;
    }

    private boolean processPeekData(HttpData data) {
      // TODO: For now, we're going to assume JSON data, but might need to support other
      // content types.
      if (contentType.equals(MediaType.JSON)) {
        try {
          dataStream.write(data.array());
          LOGGER.debug("Payload: {}", dataStream.toString());
        } catch (IOException e) {
          // IGNORE
        }

        // Unfortunately we don't appear to get a signal that we've got all the data, so we have to
        // resort to attempting to parse the data whenever it _looks_ complete.
        // TODO: try to optimize this using Jackson streaming or something else.
        if (data.array()[data.array().length - 1] == '}') {
          try {
            Map<String, Object> payload = MAPPER.readValue(dataStream.toByteArray(), new TypeReference<>() {
            });

            payloadLocators.forEach(l -> resourceKeys.put(l.getType(), findPayloadValue(l.getKey(), payload)));
            return true;
          } catch (IOException e) {
            // This is probably because we read partial data.
            LOGGER.warn("Error parsing payload: {}", e.getMessage());
          }
        }

      } else {
        LOGGER.warn("Skipping content-type: {}", contentType);
      }

      return false;
    }

  }
}