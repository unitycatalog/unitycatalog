package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.utils.ValidationUtils;
import lombok.Getter;
import org.hibernate.query.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class SchemaRepository {
    @Getter
    public static final SchemaRepository instance = new SchemaRepository();
    @Getter
    public static final CatalogRepository catalogRepository = CatalogRepository.getInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRepository.class);
    private static final SessionFactory sessionFactory = HibernateUtil.getSessionFactory();

    private SchemaRepository() {}

    public SchemaInfo createSchema(CreateSchema createSchema) {
        ValidationUtils.validateSqlObjectName(createSchema.getName());
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                if (getSchemaDAO(session, createSchema.getCatalogName(), createSchema.getName()) != null) {
                    throw new BaseException(ErrorCode.ALREADY_EXISTS,
                            "Schema already exists: " + createSchema.getName());
                }
                CatalogInfoDAO catalogDAO = catalogRepository
                        .getCatalogDAO(session, createSchema.getCatalogName());
                SchemaInfoDAO schemaInfo = new SchemaInfoDAO();
                schemaInfo.setId(UUID.randomUUID());
                schemaInfo.setName(createSchema.getName());
                schemaInfo.setCatalogId(catalogDAO.getId());
                schemaInfo.setComment(createSchema.getComment());
                schemaInfo.setCreatedAt(new Date());
                schemaInfo.setUpdatedAt(null);
                session.persist(schemaInfo);
                tx.commit();
                SchemaInfo toReturn = SchemaInfoDAO.toSchemaInfo(schemaInfo);
                addNamespaceData(toReturn, createSchema.getCatalogName());
                return toReturn;
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    private void addNamespaceData(SchemaInfo schemaInfo, String catalogName) {
        schemaInfo.setCatalogName(catalogName);
        schemaInfo.setFullName(catalogName + "." + schemaInfo.getName());
    }

    private SchemaInfo convertFromDAO(SchemaInfoDAO schemaInfoDAO, String fullName) {
        String catalogName = fullName.split("\\.")[0];
        SchemaInfo schemaInfo = SchemaInfoDAO.toSchemaInfo(schemaInfoDAO);
        addNamespaceData(schemaInfo, catalogName);
        return schemaInfo;
    }

    public SchemaInfoDAO getSchemaDAO(Session session, UUID catalogId, String schemaName) {
        Query<SchemaInfoDAO> query = session
                .createQuery("FROM SchemaInfoDAO WHERE name = :name and catalogId = :catalogId", SchemaInfoDAO.class);
        query.setParameter("name", schemaName);
        query.setParameter("catalogId", catalogId);
        query.setMaxResults(1);
        return query.uniqueResult();
    }

    public SchemaInfoDAO getSchemaDAO(Session session, String catalogName, String schemaName) {
        CatalogInfoDAO catalog = catalogRepository.getCatalogDAO(session, catalogName);
        if (catalog == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + catalogName);
        }
        return getSchemaDAO(session, catalog.getId(), schemaName);
    }

    public SchemaInfoDAO getSchemaDAO(Session session, String fullName) {
        String[] namespace = fullName.split("\\.");
        return getSchemaDAO(session, namespace[0], namespace[1]);
    }

    public ListSchemasResponse listSchemas(String catalogName, Optional<Integer> maxResults,
                                           Optional<String> pageToken) {
        try (Session session = sessionFactory.openSession()) {
            // TODO: Implement pagination and filtering if required
            // For now, returning all schemas without pagination
            CatalogInfoDAO catalog = catalogRepository.getCatalogDAO(session, catalogName);
            if (catalog == null) {
                throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + catalogName);
            }
            ListSchemasResponse response = new ListSchemasResponse();
            Query<SchemaInfoDAO> query = session
                    .createQuery("FROM SchemaInfoDAO WHERE catalogId = :value", SchemaInfoDAO.class);
            query.setParameter("value", catalog.getId());
            response.setSchemas(query.list().stream().map(SchemaInfoDAO::toSchemaInfo)
                            .peek(x -> addNamespaceData(x, catalogName))
                    .collect(Collectors.toList()));
            return response;
        }
    }

    public SchemaInfo getSchema(String fullName) {
        try (Session session = sessionFactory.openSession()) {
            SchemaInfoDAO schemaInfo = getSchemaDAO(session, fullName);
            if (schemaInfo == null) {
                throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + fullName);
            }
            return convertFromDAO(schemaInfo, fullName);
        }
    }

    public SchemaInfo updateSchema(String fullName, UpdateSchema updateSchema) {
        ValidationUtils.validateSqlObjectName(updateSchema.getNewName());
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                SchemaInfoDAO schemaInfo = getSchemaDAO(session, fullName);
                if (schemaInfo == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND,
                            "Schema not found: " + fullName);
                }
                if (updateSchema.getNewName() != null) {
                    if (getSchemaDAO(session, fullName.split("\\.")[0], updateSchema
                            .getNewName()) != null) {
                        throw new BaseException(ErrorCode.ALREADY_EXISTS,
                                "Schema already exists: " + updateSchema.getNewName());
                    }
                }
                // Update the schema with new values
                if (updateSchema.getComment() != null) {
                    schemaInfo.setComment(updateSchema.getComment());
                }
                if (updateSchema.getNewName() != null) {
                    schemaInfo.setName(updateSchema.getNewName());
                }
                schemaInfo.setUpdatedAt(new Date());
                session.merge(schemaInfo);
                tx.commit();
                return convertFromDAO(schemaInfo, fullName);
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public void deleteSchema(String fullName) {
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                SchemaInfoDAO schemaInfo = getSchemaDAO(session, fullName);
                if (schemaInfo != null) {
                    session.remove(schemaInfo);
                    tx.commit();
                } else {
                    throw new BaseException(ErrorCode.NOT_FOUND,
                            "Schema not found: " + fullName);
                }
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }
}
