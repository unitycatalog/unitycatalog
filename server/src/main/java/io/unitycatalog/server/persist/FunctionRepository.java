package io.unitycatalog.server.persist;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateFunction;
import io.unitycatalog.server.model.CreateFunctionRequest;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.model.ListFunctionsResponse;
import io.unitycatalog.server.persist.dao.FunctionInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.JsonUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRepository.class);
  private final Repositories repositories;
  private final SessionFactory sessionFactory;
  private static final PagedListingHelper<FunctionInfoDAO> LISTING_HELPER =
      new PagedListingHelper<>(FunctionInfoDAO.class);
  private static final ObjectReader STRICT_JSON_READER =
      JsonUtils.getInstance()
          .reader()
          .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .with(StreamReadFeature.STRICT_DUPLICATE_DETECTION);

  public FunctionRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
  }

  public FunctionInfo createFunction(CreateFunctionRequest createFunctionRequest) {
    ValidationUtils.validateSqlObjectName(createFunctionRequest.getFunctionInfo().getName());
    CreateFunction createFunction = createFunctionRequest.getFunctionInfo();
    Map<String, String> properties = parseProperties(createFunction.getProperties());
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    Long createTime = System.currentTimeMillis();
    FunctionInfo functionInfo =
        new FunctionInfo()
            .functionId(UUID.randomUUID().toString())
            .name(createFunction.getName())
            .catalogName(createFunction.getCatalogName())
            .schemaName(createFunction.getSchemaName())
            .comment(createFunction.getComment())
            .properties(toPropertiesJson(properties))
            .owner(callerId)
            .createdAt(createTime)
            .createdBy(callerId)
            .updatedAt(createTime)
            .updatedBy(callerId)
            .dataType(createFunction.getDataType())
            .fullDataType(createFunction.getFullDataType())
            .inputParams(createFunction.getInputParams())
            .returnParams(createFunction.getReturnParams())
            .fullName(
                createFunction.getCatalogName()
                    + "."
                    + createFunction.getSchemaName()
                    + "."
                    + createFunction.getName())
            .externalLanguage(createFunction.getExternalLanguage())
            .isDeterministic(createFunction.getIsDeterministic())
            .isNullCall(createFunction.getIsNullCall())
            .parameterStyle(
                FunctionInfo.ParameterStyleEnum.valueOf(createFunction.getParameterStyle().name()))
            .routineBody(
                FunctionInfo.RoutineBodyEnum.valueOf(createFunction.getRoutineBody().name()))
            .routineDefinition(createFunction.getRoutineDefinition())
            .securityType(
                FunctionInfo.SecurityTypeEnum.valueOf(createFunction.getSecurityType().name()))
            .specificName(createFunction.getSpecificName());
    if (createFunction.getSqlDataAccess() != null) {
      functionInfo.setSqlDataAccess(
          FunctionInfo.SqlDataAccessEnum.valueOf(createFunction.getSqlDataAccess().toString()));
    }

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String catalogName = createFunction.getCatalogName();
          String schemaName = createFunction.getSchemaName();
          SchemaInfoDAO schemaInfo =
              repositories
                  .getSchemaRepository()
                  .getSchemaDaoOrThrow(session, catalogName, schemaName);
          if (getFunctionDAO(session, catalogName, schemaName, createFunction.getName()) != null) {
            throw new BaseException(
                ErrorCode.RESOURCE_ALREADY_EXISTS,
                "Function already exists: " + createFunction.getName());
          }
          FunctionInfoDAO dao = FunctionInfoDAO.from(functionInfo);
          dao.setSchemaId(schemaInfo.getId());
          dao.getInputParams()
              .forEach(
                  p -> {
                    p.setId(UUID.randomUUID());
                    p.setFunction(dao);
                  });
          dao.getReturnParams()
              .forEach(
                  p -> {
                    p.setId(UUID.randomUUID());
                    p.setFunction(dao);
                  });
          session.persist(dao);
          PropertyDAO.from(properties, dao.getId(), Constants.FUNCTION).forEach(session::persist);
          return functionInfo;
        },
        "Failed to create function",
        /* readOnly= */ false);
  }

  private void addNamespaceData(FunctionInfo functionInfo, String catalogName, String schemaName) {
    functionInfo.setCatalogName(catalogName);
    functionInfo.setSchemaName(schemaName);
    functionInfo.setFullName(catalogName + "." + schemaName + "." + functionInfo.getName());
  }

  /**
   * Return the list of functions in ascending order of function name.
   *
   * @param catalogName
   * @param schemaName
   * @param maxResults
   * @param pageToken
   * @return
   */
  public ListFunctionsResponse listFunctions(
      String catalogName,
      String schemaName,
      Optional<Integer> maxResults,
      Optional<String> pageToken) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UUID schemaId =
              repositories
                  .getSchemaRepository()
                  .getSchemaIdOrThrow(session, catalogName, schemaName);
          return listFunctions(session, schemaId, catalogName, schemaName, maxResults, pageToken);
        },
        "Failed to list functions",
        /* readOnly= */ true);
  }

  public ListFunctionsResponse listFunctions(
      Session session,
      UUID schemaId,
      String catalogName,
      String schemaName,
      Optional<Integer> maxResults,
      Optional<String> pageToken) {
    List<FunctionInfoDAO> functionInfoDAOList =
        LISTING_HELPER.listEntity(session, maxResults, pageToken, schemaId);
    String nextPageToken = LISTING_HELPER.getNextPageToken(functionInfoDAOList, maxResults);
    Map<UUID, List<PropertyDAO>> propertiesByFunctionId =
        PropertyRepository.findPropertiesByEntityIds(
            session,
            functionInfoDAOList.stream().map(FunctionInfoDAO::getId).toList(),
            Constants.FUNCTION);
    List<FunctionInfo> result = new ArrayList<>();
    for (FunctionInfoDAO functionInfoDAO : functionInfoDAOList) {
      FunctionInfo functionInfo = functionInfoDAO.toFunctionInfo();
      functionInfo.setProperties(
          toPropertiesJson(
              PropertyDAO.toMap(
                  propertiesByFunctionId.getOrDefault(functionInfoDAO.getId(), List.of()))));
      addNamespaceData(functionInfo, catalogName, schemaName);
      result.add(functionInfo);
    }
    return new ListFunctionsResponse().functions(result).nextPageToken(nextPageToken);
  }

  public FunctionInfo getFunction(String name) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String[] parts = ValidationUtils.splitFullName(name, 3, "Invalid function name: " + name);
          String catalogName = parts[0], schemaName = parts[1], functionName = parts[2];
          FunctionInfoDAO functionInfoDAO =
              getFunctionDAO(session, catalogName, schemaName, functionName);
          if (functionInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Function not found: " + name);
          }
          FunctionInfo functionInfo = functionInfoDAO.toFunctionInfo();
          addNamespaceInfo(functionInfo, catalogName, schemaName);
          functionInfo.setProperties(
              toPropertiesJson(
                  PropertyDAO.toMap(
                      PropertyRepository.findProperties(
                          session, functionInfoDAO.getId(), Constants.FUNCTION))));
          return functionInfo;
        },
        "Failed to get function",
        /* readOnly= */ true);
  }

  public void addNamespaceInfo(FunctionInfo functionInfo, String catalogName, String schemaName) {
    addNamespaceData(functionInfo, catalogName, schemaName);
  }

  public FunctionInfoDAO getFunctionDAO(
      Session session, String catalogName, String schemaName, String functionName) {
    UUID schemaId =
        repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalogName, schemaName);
    return getFunctionDAO(session, schemaId, functionName);
  }

  public FunctionInfoDAO getFunctionDAO(Session session, UUID schemaId, String functionName) {
    Query<FunctionInfoDAO> query =
        session.createQuery(
            "FROM FunctionInfoDAO WHERE name = :name and schemaId = :schemaId",
            FunctionInfoDAO.class);
    query.setParameter("name", functionName);
    query.setParameter("schemaId", schemaId);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public void deleteFunction(String name, Boolean force) {
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String[] parts = ValidationUtils.splitFullName(name, 3, "Invalid function name: " + name);
          String catalogName = parts[0], schemaName = parts[1], functionName = parts[2];
          UUID schemaId =
              repositories
                  .getSchemaRepository()
                  .getSchemaIdOrThrow(session, catalogName, schemaName);
          deleteFunction(session, schemaId, functionName);
          LOGGER.info("Deleted function: {}", functionName);
          return null;
        },
        "Failed to delete function",
        /* readOnly= */ false);
  }

  public void deleteFunction(Session session, UUID schemaId, String functionName) {
    FunctionInfoDAO functionInfoDAO = getFunctionDAO(session, schemaId, functionName);
    if (functionInfoDAO == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Function not found: " + functionName);
    }
    PropertyRepository.findProperties(session, functionInfoDAO.getId(), Constants.FUNCTION)
        .forEach(session::remove);
    session.remove(functionInfoDAO);
  }

  /**
   * Parses the {@code properties} of a function create request: a JSON object whose values are all
   * strings, with no duplicate keys and nothing after it. An absent field or a JSON {@code null}
   * means no properties.
   *
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} for any other value
   */
  private static Map<String, String> parseProperties(String properties) {
    if (properties == null) {
      return Map.of();
    }
    JsonNode node;
    try {
      node = STRICT_JSON_READER.readTree(properties);
    } catch (JsonProcessingException e) {
      throw invalidProperties();
    }
    if (node.isNull()) {
      return Map.of();
    }
    if (!node.isObject()) {
      throw invalidProperties();
    }
    Map<String, String> parsed = new HashMap<>();
    for (Map.Entry<String, JsonNode> field : node.properties()) {
      if (!field.getValue().isTextual()) {
        throw invalidProperties();
      }
      parsed.put(field.getKey(), field.getValue().textValue());
    }
    return parsed;
  }

  private static BaseException invalidProperties() {
    return new BaseException(
        ErrorCode.INVALID_ARGUMENT,
        "Function properties must be a JSON object whose values are all strings.");
  }

  /** Serializes properties as a JSON object with keys in sorted order, or null when empty. */
  private static String toPropertiesJson(Map<String, String> properties) {
    if (properties.isEmpty()) {
      return null;
    }
    try {
      return JsonUtils.getInstance().writeValueAsString(new TreeMap<>(properties));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }
}
