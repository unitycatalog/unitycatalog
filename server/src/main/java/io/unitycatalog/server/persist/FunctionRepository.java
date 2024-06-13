package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateFunction;
import io.unitycatalog.server.model.CreateFunctionRequest;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.model.ListFunctionsResponse;
import io.unitycatalog.server.persist.dao.FunctionInfoDAO;
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
            FunctionInfoDAO dao = FunctionInfoDAO.from(functionInfo);
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
        } catch(Exception e) {
            LOGGER.error("Error adding function", e);
            return null;
        }
    }

    public ListFunctionsResponse listFunctions(String catalogName, String schemaName, Optional<Integer> maxResults, Optional<String> nextPageToken) {
        ListFunctionsResponse response = new ListFunctionsResponse();
        try (Session session = SESSION_FACTORY.openSession()) {
            String queryString = "from FunctionInfoDAO f where f.catalogName = :catalogName and f.schemaName = :schemaName";
            Query<FunctionInfoDAO> query = session.createQuery(queryString, FunctionInfoDAO.class);
            query.setParameter("catalogName", catalogName);
            query.setParameter("schemaName", schemaName);

            maxResults.ifPresent(query::setMaxResults);

            if (nextPageToken.isPresent()) {
                // Perform pagination logic here if needed
                // Example: query.setFirstResult(startIndex);
            }
            List<FunctionInfoDAO> functions = query.list();
            response.setFunctions(functions.stream().map(FunctionInfoDAO::toFunctionInfo).collect(Collectors.toList()));
            return response;
        } catch(Exception e) {
            LOGGER.error("Error listing functions", e);
            return null;
        }
    }

    public FunctionInfo getFunction(String name) {
        try (Session session = SESSION_FACTORY.openSession()) {
            session.beginTransaction();
            Query<FunctionInfoDAO> query = session.createQuery("FROM FunctionInfoDAO WHERE fullName = :value", FunctionInfoDAO.class);
            query.setParameter("value", name);
            query.setMaxResults(1);
            FunctionInfoDAO functionInfoDAO = query.uniqueResult();
            return functionInfoDAO == null ? null : functionInfoDAO.toFunctionInfo();
        } catch (Exception e) {
            LOGGER.error("Error getting function", e);
            return null;
        }
    }

    public void deleteFunction(String name, Boolean force) {
        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction tx = session.beginTransaction();
            Query<FunctionInfoDAO> query = session.createQuery("FROM FunctionInfoDAO WHERE fullName = :value", FunctionInfoDAO.class);
            query.setParameter("value", name);
            query.setMaxResults(1);
            FunctionInfoDAO functionInfoDAO = query.uniqueResult();
            if (functionInfoDAO != null) {
                session.remove(functionInfoDAO);
                tx.commit();
                LOGGER.info("Deleted function: {}", functionInfoDAO.getName());
            } else {
                throw new BaseException(ErrorCode.NOT_FOUND, "Function not found: " + name);
            }
        } catch (Exception e) {
            LOGGER.error("Error deleting function", e);
        }
    }
}
