package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.FunctionInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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

  public FunctionRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
  }

  public FunctionInfo createFunction(CreateFunctionRequest createFunctionRequest) {
    ValidationUtils.validateSqlObjectName(createFunctionRequest.getFunctionInfo().getName());
    CreateFunction createFunction = createFunctionRequest.getFunctionInfo();
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    Long createTime = System.currentTimeMillis();
    FunctionInfo functionInfo =
        new FunctionInfo()
            .functionId(UUID.randomUUID().toString())
            .name(createFunction.getName())
            .catalogName(createFunction.getCatalogName())
            .schemaName(createFunction.getSchemaName())
            .comment(createFunction.getComment())
            .properties(createFunction.getProperties())
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
              repositories.getSchemaRepository().getSchemaDAO(session, catalogName, schemaName);
          if (schemaInfo == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + schemaName);
          }
          if (getFunctionDAO(session, catalogName, schemaName, createFunction.getName()) != null) {
            throw new BaseException(
                ErrorCode.ALREADY_EXISTS, "Function already exists: " + createFunction.getName());
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
          return functionInfo;
        },
        "Failed to create function",
        /* readOnly = */ false);
  }

  private void addNamespaceData(FunctionInfo functionInfo, String catalogName, String schemaName) {
    functionInfo.setCatalogName(catalogName);
    functionInfo.setSchemaName(schemaName);
    functionInfo.setFullName(catalogName + "." + schemaName + "." + functionInfo.getName());
  }

  public UUID getSchemaId(Session session, String catalogName, String schemaName) {
    SchemaInfoDAO schemaInfo =
        repositories.getSchemaRepository().getSchemaDAO(session, catalogName, schemaName);
    if (schemaInfo == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + schemaName);
    }
    return schemaInfo.getId();
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
          UUID schemaId = getSchemaId(session, catalogName, schemaName);
          return listFunctions(session, schemaId, catalogName, schemaName, maxResults, pageToken);
        },
        "Failed to list functions",
        /* readOnly = */ true);
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
    List<FunctionInfo> result = new ArrayList<>();
    for (FunctionInfoDAO functionInfoDAO : functionInfoDAOList) {
      FunctionInfo functionInfo = functionInfoDAO.toFunctionInfo();
      RepositoryUtils.attachProperties(
          functionInfo, functionInfo.getFunctionId(), Constants.FUNCTION, session);
      addNamespaceData(functionInfo, catalogName, schemaName);
      result.add(functionInfo);
    }
    return new ListFunctionsResponse().functions(result).nextPageToken(nextPageToken);
  }

  public FunctionInfo getFunction(String name) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String[] parts = name.split("\\.");
          if (parts.length != 3) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid function name: " + name);
          }
          String catalogName = parts[0], schemaName = parts[1], functionName = parts[2];
          FunctionInfoDAO functionInfoDAO =
              getFunctionDAO(session, catalogName, schemaName, functionName);
          if (functionInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Function not found: " + name);
          }
          FunctionInfo functionInfo = functionInfoDAO.toFunctionInfo();
          addNamespaceInfo(functionInfo, catalogName, schemaName);
          RepositoryUtils.attachProperties(
              functionInfo, functionInfo.getFunctionId(), Constants.FUNCTION, session);
          return functionInfo;
        },
        "Failed to get function",
        /* readOnly = */ true);
  }

  public void addNamespaceInfo(FunctionInfo functionInfo, String catalogName, String schemaName) {
    addNamespaceData(functionInfo, catalogName, schemaName);
  }

  public FunctionInfoDAO getFunctionDAO(
      Session session, String catalogName, String schemaName, String functionName) {
    UUID schemaId = getSchemaId(session, catalogName, schemaName);
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
          String[] parts = name.split("\\.");
          if (parts.length != 3) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid function name: " + name);
          }
          String catalogName = parts[0], schemaName = parts[1], functionName = parts[2];
          SchemaInfoDAO schemaInfo =
              repositories.getSchemaRepository().getSchemaDAO(session, catalogName, schemaName);
          if (schemaInfo == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + schemaName);
          }
          deleteFunction(session, schemaInfo.getId(), functionName);
          LOGGER.info("Deleted function: {}", functionName);
          return null;
        },
        "Failed to delete function",
        /* readOnly = */ false);
  }

  public void deleteFunction(Session session, UUID schemaId, String functionName) {
    FunctionInfoDAO functionInfoDAO = getFunctionDAO(session, schemaId, functionName);
    if (functionInfoDAO == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Function not found: " + functionName);
    }
    session.remove(functionInfoDAO);
  }
}
