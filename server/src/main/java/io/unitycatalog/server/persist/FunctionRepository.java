package io.unitycatalog.server.persist;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateFunction;
import io.unitycatalog.server.model.CreateFunctionRequest;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.model.ListFunctionsResponse;
import io.unitycatalog.server.model.UpdateFunction;
import io.unitycatalog.server.persist.dao.FunctionInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.JsonUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.exception.DataException;
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
          .with(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
          .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .without(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES);

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
          repositories
              .getOwnershipRepository()
              .initializeOwner(session, dao.getId(), callerId, schemaInfo.getId());
          // Write the function and its parameters first, so only property rows reach
          // flushProperties and its mapping of database errors to property errors.
          session.flush();
          PropertyDAO.from(properties, dao.getId(), Constants.FUNCTION).forEach(session::persist);
          flushProperties(session);
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
      setProperties(
          functionInfo, propertiesByFunctionId.getOrDefault(functionInfoDAO.getId(), List.of()));
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
          setProperties(
              functionInfo,
              PropertyRepository.findProperties(
                  session, functionInfoDAO.getId(), Constants.FUNCTION));
          return functionInfo;
        },
        "Failed to get function",
        /* readOnly= */ true);
  }

  public void addNamespaceInfo(FunctionInfo functionInfo, String catalogName, String schemaName) {
    addNamespaceData(functionInfo, catalogName, schemaName);
  }

  /** Transfers ownership without changing the function definition or its dependencies. */
  public FunctionInfo updateFunction(String fullName, UpdateFunction updateFunction) {
    return updateFunction(fullName, updateFunction, () -> {});
  }

  public FunctionInfo updateFunction(
      String fullName, UpdateFunction updateFunction, Runnable beforeCommit) {
    String[] parts =
        ValidationUtils.splitFullName(fullName, 3, "Invalid function name: " + fullName);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          FunctionInfoDAO dao = getFunctionDAO(session, parts[0], parts[1], parts[2]);
          if (dao == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Function not found: " + fullName);
          }
          dao.setOwner(
              repositories
                  .getOwnershipRepository()
                  .updateOwner(session, dao, updateFunction.getOwner(), beforeCommit));
          dao.setUpdatedAt(System.currentTimeMillis());
          dao.setUpdatedBy(IdentityUtils.findPrincipalEmailAddress());
          FunctionInfo result = dao.toFunctionInfo();
          addNamespaceInfo(result, parts[0], parts[1]);
          return RepositoryUtils.attachProperties(
              result, result.getFunctionId(), Constants.FUNCTION, session);
        },
        "Failed to update function",
        /* readOnly= */ false,
        Optional.of(Connection.TRANSACTION_READ_COMMITTED));
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
   * Parses the {@code properties} of a function create request: a single JSON object with unique
   * keys and nothing after it. A value that is not a string is stored as its JSON text, with each
   * number at its exact decimal value. An absent field, a blank string or a JSON {@code null} means
   * no properties.
   *
   * @throws BaseException with {@link ErrorCode#INVALID_ARGUMENT} for any other value, for a key
   *     longer than {@link PropertyDAO#MAX_KEY_LENGTH} characters, for a NUL character in a key or
   *     value, which PostgreSQL text cannot store, for a number whose exponent is out of range, or
   *     for JSON over the parser's {@link StreamReadConstraints}
   */
  private static Map<String, String> parseProperties(String properties) {
    if (properties == null || properties.isBlank()) {
      return Map.of();
    }
    JsonNode node;
    try {
      node = STRICT_JSON_READER.readTree(properties);
    } catch (StreamConstraintsException e) {
      StreamReadConstraints limits = STRICT_JSON_READER.getFactory().streamReadConstraints();
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          String.format(
              "Function properties are too large: numbers can have at most %d digits, strings at"
                  + " most %d characters, and nesting at most %d levels.",
              limits.getMaxNumberLength(),
              limits.getMaxStringLength(),
              limits.getMaxNestingDepth()));
    } catch (JsonProcessingException e) {
      throw invalidProperties();
    } catch (NumberFormatException e) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Function properties contain a number whose exponent is out of range.");
    }
    if (node.isNull()) {
      return Map.of();
    }
    if (!node.isObject()) {
      throw invalidProperties();
    }
    Map<String, String> parsed = new HashMap<>();
    for (Map.Entry<String, JsonNode> field : node.properties()) {
      String key = field.getKey();
      if (key.codePointCount(0, key.length()) > PropertyDAO.MAX_KEY_LENGTH) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Function property keys cannot be longer than "
                + PropertyDAO.MAX_KEY_LENGTH
                + " characters.");
      }
      JsonNode value = field.getValue();
      String text = value.isTextual() ? value.textValue() : value.toString();
      if (key.indexOf('\0') >= 0 || text.indexOf('\0') >= 0) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Function property keys and values cannot contain the NUL character (\\u0000).");
      }
      parsed.put(key, text);
    }
    return parsed;
  }

  /**
   * Writes the pending property rows, and turns a row the database rejects into {@link
   * ErrorCode#INVALID_ARGUMENT}: keys its collation treats as equal (such as MySQL's default
   * case-insensitive one), or a key over the column size where the database counts UTF-16 units
   * (H2).
   *
   * <p>This mapping stays here rather than in {@code DatabaseErrors}, which maps a SQLSTATE for
   * every repository and keeps the database's own message. A unique or data violation is only known
   * to be the caller's fault here, where the flush writes nothing but property rows.
   */
  private static void flushProperties(Session session) {
    try {
      session.flush();
    } catch (RuntimeException e) {
      for (Throwable cause = e; cause != null; cause = cause.getCause()) {
        if (cause instanceof ConstraintViolationException) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT,
              "Function property keys must be unique, and the database treats two of these keys"
                  + " as the same key.",
              e);
        }
        if (cause instanceof DataException) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT,
              "The database cannot store a key or value of these function properties.",
              e);
        }
      }
      throw e;
    }
  }

  private static BaseException invalidProperties() {
    return new BaseException(
        ErrorCode.INVALID_ARGUMENT,
        "Function properties must be a single JSON object with unique keys.");
  }

  private static void setProperties(FunctionInfo functionInfo, List<PropertyDAO> properties) {
    functionInfo.setProperties(toPropertiesJson(PropertyDAO.toMap(properties)));
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
