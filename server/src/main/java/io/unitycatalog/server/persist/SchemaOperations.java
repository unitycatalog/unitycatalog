package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateSchema;
import io.unitycatalog.server.model.ListSchemasResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.utils.ValidationUtils;
import lombok.Getter;
import org.hibernate.query.Query;
import io.unitycatalog.server.model.UpdateSchema;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class SchemaOperations {
    @Getter
    public static final SchemaOperations instance = new SchemaOperations();
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaOperations.class);
    private static final SessionFactory sessionFactory = HibernateUtil.getSessionFactory();

    private SchemaOperations() {}

    public SchemaInfo createSchema(CreateSchema createSchema) {
        ValidationUtils.validateSqlObjectName(createSchema.getName());
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            SchemaInfoDAO schemaInfo = new SchemaInfoDAO();
            schemaInfo.setSchemaId(UUID.randomUUID());
            schemaInfo.setName(createSchema.getName());
            schemaInfo.setCatalogName(createSchema.getCatalogName());
            schemaInfo.setComment(createSchema.getComment());
            //schemaInfo.setProperties(createSchema.getProperties());
            schemaInfo.setFullName(createSchema.getCatalogName() + "." + createSchema.getName());
            schemaInfo.setCreatedAt(new Date());
            schemaInfo.setUpdatedAt(null);
            session.persist(schemaInfo);
            tx.commit();
            return SchemaInfoDAO.toSchemaInfo(schemaInfo);
        } catch (Exception e) {
            LOGGER.error("Error creating schema", e);
            return null;
        }
    }

    public ListSchemasResponse listSchemas(String catalogName, Optional<Integer> maxResults, Optional<String> pageToken) {
        try (Session session = sessionFactory.openSession()) {
            // TODO: Implement pagination and filtering if required
            // For now, returning all schemas without pagination
            session.beginTransaction();
            ListSchemasResponse response = new ListSchemasResponse();
            Query<SchemaInfoDAO> query = session.createQuery("FROM SchemaInfoDAO WHERE catalogName = :value", SchemaInfoDAO.class);
            query.setParameter("value", catalogName);
            response.setSchemas(query.list().stream().map(SchemaInfoDAO::toSchemaInfo).collect(Collectors.toList()));
            return response;
        } catch (Exception e) {
            LOGGER.error("Error listing schemas", e);
            return null;
        }
    }

    public SchemaInfo getSchema(String fullName) {
        try (Session session = sessionFactory.openSession()) {
            session.beginTransaction();
            return getSchema(session, fullName);
        } catch (Exception e) {
            LOGGER.error("Error getting schema", e);
            return null;
        }
    }

    public SchemaInfoDAO getSchemaInfoDAO(Session session, String fullName) {
        Query<SchemaInfoDAO> query = session.createQuery("FROM SchemaInfoDAO WHERE fullName = :value", SchemaInfoDAO.class);
        query.setParameter("value", fullName);
        query.setMaxResults(1);
        return query.uniqueResult();
    }

    public SchemaInfo getSchema(Session session, String fullName) {
        SchemaInfoDAO schemaInfo = getSchemaInfoDAO(session, fullName);
        if (schemaInfo == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + fullName);
        }
        return SchemaInfoDAO.toSchemaInfo(schemaInfo);
    }

    public SchemaInfo updateSchema(String fullName, UpdateSchema updateSchema) {
        ValidationUtils.validateSqlObjectName(updateSchema.getNewName());
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            SchemaInfoDAO schemaInfo = getSchemaInfoDAO(session, fullName);

            // Update the schema with new values
            if (updateSchema.getComment() != null) {
                schemaInfo.setComment(updateSchema.getComment());
            }
            if (updateSchema.getNewName() != null) {
                schemaInfo.setName(updateSchema.getNewName());
                schemaInfo.setFullName(schemaInfo.getCatalogName() + "." + updateSchema.getNewName());
            }
            schemaInfo.setUpdatedAt(new Date());
            session.merge(schemaInfo);
            tx.commit();
            return SchemaInfoDAO.toSchemaInfo(schemaInfo);
        } catch (Exception e) {
            LOGGER.error("Error updating schema", e);
            return null;
        }
    }

    public void deleteSchema(String fullName) {
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            SchemaInfoDAO schemaInfo = getSchemaInfoDAO(session, fullName);
            if (schemaInfo != null) {
                session.remove(schemaInfo);
                tx.commit();
            } else {
                throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + fullName);
            }
        } catch (Exception e) {
            LOGGER.error("Error deleting schema", e);
        }
    }
}
