package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.FunctionInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.utils.ValidationUtils;
import lombok.Getter;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class FunctionRepository {
    @Getter
    private static final FunctionRepository instance = new FunctionRepository();
    private static final SchemaRepository schemaRepository = SchemaRepository.getInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRepository.class);
    private static final SessionFactory SESSION_FACTORY = HibernateUtil.getSessionFactory();

    private FunctionRepository() {}

    public FunctionInfo createFunction(CreateFunctionRequest createFunctionRequest) {
        ValidationUtils.validateSqlObjectName(createFunctionRequest.getFunctionInfo().getName());
        CreateFunction createFunction = createFunctionRequest.getFunctionInfo();
        FunctionInfo functionInfo = new FunctionInfo()
                .functionId(UUID.randomUUID().toString())
                .name(createFunction.getName())
                .catalogName(createFunction.getCatalogName())
                .schemaName(createFunction.getSchemaName())
                .comment(createFunction.getComment())
                .properties(createFunction.getProperties())
                .createdAt(System.currentTimeMillis())
                .dataType(createFunction.getDataType())
                .fullDataType(createFunction.getFullDataType())
                .inputParams(createFunction.getInputParams())
                .returnParams(createFunction.getReturnParams())
                .fullName(createFunction.getCatalogName() + "." + createFunction.getSchemaName() + "." + createFunction.getName())
                .externalLanguage(createFunction.getExternalLanguage())
                .isDeterministic(createFunction.getIsDeterministic())
                .isNullCall(createFunction.getIsNullCall())
                .parameterStyle(FunctionInfo.ParameterStyleEnum.valueOf(createFunction.getParameterStyle().name()))
                .routineBody(FunctionInfo.RoutineBodyEnum.valueOf(createFunction.getRoutineBody().name()))
                .routineDefinition(createFunction.getRoutineDefinition())
                .securityType(FunctionInfo.SecurityTypeEnum.valueOf(createFunction.getSecurityType().name()))
                .specificName(createFunction.getSpecificName());
        if (createFunction.getSqlDataAccess() != null)
            functionInfo.setSqlDataAccess(FunctionInfo.SqlDataAccessEnum.valueOf(createFunction.getSqlDataAccess().toString()));

        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                String catalogName = createFunction.getCatalogName();
                String schemaName = createFunction.getSchemaName();
                SchemaInfoDAO schemaInfo = schemaRepository.getSchemaDAO(session,
                        catalogName, schemaName);
                if (schemaInfo == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + schemaName);
                }
                if (getFunctionDAO(session, catalogName, schemaName, createFunction.getName()) != null) {
                    throw new BaseException(ErrorCode.ALREADY_EXISTS, "Function already exists: " + createFunction.getName());
                }
                FunctionInfoDAO dao = FunctionInfoDAO.from(functionInfo);
                dao.setSchemaId(schemaInfo.getId());
                dao.getInputParams().forEach(p -> {
                    p.setId(UUID.randomUUID().toString());
                    p.setFunction(dao);
                });
                dao.getReturnParams().forEach(p -> {
                    p.setId(UUID.randomUUID().toString());
                    p.setFunction(dao);
                });
                session.persist(dao);
                tx.commit();
                return functionInfo;
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public ListFunctionsResponse listFunctions(String catalogName, String schemaName,
                                               Optional<Integer> maxResults, Optional<String> nextPageToken) {

        try (Session session = SESSION_FACTORY.openSession()) {
            session.setDefaultReadOnly(true);
            Transaction tx = session.beginTransaction();
            try {
                SchemaInfoDAO schemaInfo = schemaRepository.getSchemaDAO(session, catalogName + "." + schemaName);
                if (schemaInfo == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + schemaName);
                }
                ListFunctionsResponse response = listFunctions(session, schemaInfo.getId(), catalogName,
                        schemaName, maxResults, nextPageToken);
                tx.commit();
                return response;
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public ListFunctionsResponse listFunctions(Session session, UUID schemaId, String catalogName, String schemaName,
                                               Optional<Integer> maxResults, Optional<String> nextPageToken) {
        ListFunctionsResponse response = new ListFunctionsResponse();
        Query<FunctionInfoDAO> query = session.createQuery(
                "FROM FunctionInfoDAO WHERE schemaId = :schemaId", FunctionInfoDAO.class);
        query.setParameter("schemaId", schemaId);
        maxResults.ifPresent(query::setMaxResults);
        if (nextPageToken.isPresent()) {
            // Perform pagination logic here if needed
            // Example: query.setFirstResult(startIndex);
        }
        response.setFunctions(
                query.list().stream().map(FunctionInfoDAO::toFunctionInfo)
                        .peek(f -> addNamespaceInfo(f, catalogName, schemaName))
                        .collect(Collectors.toList()));
        return response;
    }


    public FunctionInfo getFunction(String name) {
        FunctionInfo functionInfo = null;
        try (Session session = SESSION_FACTORY.openSession()) {
            session.setDefaultReadOnly(true);
            Transaction tx = session.beginTransaction();
            try {
                String[] parts = name.split("\\.");
                if (parts.length != 3) {
                    throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid function name: " + name);
                }
                String catalogName = parts[0], schemaName = parts[1], functionName = parts[2];
                FunctionInfoDAO functionInfoDAO = getFunctionDAO(session, catalogName, schemaName, functionName);
                if (functionInfoDAO == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Function not found: " + name);
                }
                functionInfo = functionInfoDAO.toFunctionInfo();
                addNamespaceInfo(functionInfo, catalogName, schemaName);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        } catch (Exception e) {
            LOGGER.error("Error getting function", e);
            return null;
        }
        return functionInfo;
    }

    public void addNamespaceInfo(FunctionInfo functionInfo, String catalogName, String schemaName) {
        functionInfo.setCatalogName(catalogName);
        functionInfo.setSchemaName(schemaName);
        functionInfo.setFullName(catalogName + "." + schemaName + "." + functionInfo.getName());
    }

    public FunctionInfoDAO getFunctionDAO(Session session, String catalogName, String schemaName, String functionName) {
        SchemaInfoDAO schemaInfo = schemaRepository.getSchemaDAO(session, catalogName, schemaName);
        if (schemaInfo == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + schemaName);
        }
        return getFunctionDAO(session, schemaInfo.getId(), functionName);
    }

    public FunctionInfoDAO getFunctionDAO(Session session, UUID schemaId, String functionName) {
        Query<FunctionInfoDAO> query = session.createQuery(
                "FROM FunctionInfoDAO WHERE name = :name and schemaId = :schemaId", FunctionInfoDAO.class);
        query.setParameter("name", functionName);
        query.setParameter("schemaId", schemaId);
        query.setMaxResults(1);
        return query.uniqueResult();
    }

    public void deleteFunction(String name, Boolean force) {
        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                String[] parts = name.split("\\.");
                if (parts.length != 3) {
                    throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid function name: " + name);
                }
                String catalogName = parts[0], schemaName = parts[1], functionName = parts[2];
                SchemaInfoDAO schemaInfo = schemaRepository.getSchemaDAO(session,
                        catalogName, schemaName);
                if (schemaInfo == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + schemaName);
                }
                deleteFunction(session, schemaInfo.getId(), functionName);
                tx.commit();
                LOGGER.info("Deleted function: {}", functionName);
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public void deleteFunction(Session session, UUID schemaId, String functionName) {
        FunctionInfoDAO functionInfoDAO = getFunctionDAO(session, schemaId, functionName);
        if (functionInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Function not found: " + functionName);
        }
        session.remove(functionInfoDAO);
    }
}