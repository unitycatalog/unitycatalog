package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.ValidationUtils;
import lombok.Getter;
import org.hibernate.query.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class SchemaRepository {
    @Getter
    public static final SchemaRepository INSTANCE = new SchemaRepository();
    @Getter
    public static final CatalogRepository CATALOG_REPOSITORY = CatalogRepository.getINSTANCE();
    private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();

    private SchemaRepository() {}

    public SchemaInfo createSchema(CreateSchema createSchema) {
        ValidationUtils.validateSqlObjectName(createSchema.getName());
        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                if (getSchemaDAO(session, createSchema.getCatalogName(), createSchema.getName()) != null) {
                    throw new BaseException(ErrorCode.ALREADY_EXISTS,
                            "Schema already exists: " + createSchema.getName());
                }
                CatalogInfoDAO catalogDAO = CATALOG_REPOSITORY
                        .getCatalogDAO(session, createSchema.getCatalogName());
                SchemaInfo schemaInfo = new SchemaInfo()
                        .schemaId(UUID.randomUUID().toString())
                        .name(createSchema.getName())
                        .catalogName(createSchema.getCatalogName())
                        .comment(createSchema.getComment())
                        .createdAt(System.currentTimeMillis())
                        .properties(createSchema.getProperties());
                SchemaInfoDAO schemaInfoDAO = SchemaInfoDAO.from(schemaInfo);
                schemaInfoDAO.setCatalogId(catalogDAO.getId());
                PropertyDAO.from(schemaInfo.getProperties(), schemaInfoDAO.getId(), Constants.SCHEMA)
                        .forEach(session::persist);
                session.persist(schemaInfoDAO);
                tx.commit();
                addNamespaceData(schemaInfo, createSchema.getCatalogName());
                return schemaInfo;
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
        CatalogInfoDAO catalog = CATALOG_REPOSITORY.getCatalogDAO(session, catalogName);
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
        try (Session session = SESSION_FACTORY.openSession()) {
            ListSchemasResponse response = new ListSchemasResponse();
            session.setDefaultReadOnly(true);
            Transaction tx = session.beginTransaction();
            // TODO: Implement pagination and filtering if required
            // For now, returning all schemas without pagination
            try {
                CatalogInfoDAO catalog = CATALOG_REPOSITORY.getCatalogDAO(session, catalogName);
                if (catalog == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + catalogName);
                }

                Query<SchemaInfoDAO> query = session
                        .createQuery("FROM SchemaInfoDAO WHERE catalogId = :value", SchemaInfoDAO.class);
                query.setParameter("value", catalog.getId());
                response.setSchemas(query.list().stream()
                        .map(SchemaInfoDAO::toSchemaInfo)
                        .peek(x -> addNamespaceData(x, catalogName))
                        .map(s -> attachProperties(s, session))
                        .collect(Collectors.toList()));
                tx.commit();
                return response;
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public SchemaInfo attachProperties(SchemaInfo schemaInfo, Session session) {
        List<PropertyDAO> propertyDAOList = PropertyRepository.findProperties(
                session, UUID.fromString(schemaInfo.getSchemaId()), Constants.SCHEMA);
        schemaInfo.setProperties(PropertyDAO.toMap(propertyDAOList));
        return schemaInfo;
    }

    public SchemaInfo getSchema(String fullName) {
        try (Session session = SESSION_FACTORY.openSession()) {
            session.setDefaultReadOnly(true);
            Transaction tx = session.beginTransaction();
            SchemaInfoDAO schemaInfoDAO;
            try {
                schemaInfoDAO = getSchemaDAO(session, fullName);
                if (schemaInfoDAO == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + fullName);
                }
                tx.commit();
                SchemaInfo schemaInfo = convertFromDAO(schemaInfoDAO, fullName);
                return attachProperties(schemaInfo, session);
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public SchemaInfo updateSchema(String fullName, UpdateSchema updateSchema) {
        ValidationUtils.validateSqlObjectName(updateSchema.getNewName());
        try (Session session = SESSION_FACTORY.openSession()) {
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
        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                SchemaInfoDAO schemaInfo = getSchemaDAO(session, fullName);
                if (schemaInfo != null) {
                    session.remove(schemaInfo);
                    PropertyRepository.findProperties(session, schemaInfo.getId(), Constants.SCHEMA)
                            .forEach(session::remove);
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
