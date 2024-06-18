package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.*;
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
    public static final SchemaRepository instance = new SchemaRepository();
    @Getter
    public static final CatalogRepository catalogRepository = CatalogRepository.getInstance();
    private static TableRepository tableRepository = TableRepository.getInstance();
    private static VolumeRepository volumeRepository = VolumeRepository.getInstance();
    private static FunctionRepository functionRepository = FunctionRepository.getInstance();

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
            ListSchemasResponse response = new ListSchemasResponse();
            session.setDefaultReadOnly(true);
            Transaction tx = session.beginTransaction();
            // TODO: Implement pagination and filtering if required
            // For now, returning all schemas without pagination
            try {
                CatalogInfoDAO catalog = catalogRepository.getCatalogDAO(session, catalogName);
                if (catalog == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + catalogName);
                }
                response.setSchemas(listSchemas(session, catalog.getId(), maxResults)
                        .stream().map(SchemaInfoDAO::toSchemaInfo)
                        .peek(x -> addNamespaceData(x, catalogName))
                        .collect(Collectors.toList()));
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
            return response;
        }
    }

    public List<SchemaInfoDAO> listSchemas(Session session, UUID catalogId, Optional<Integer> maxResults) {
        Query<SchemaInfoDAO> query = session
                .createQuery("FROM SchemaInfoDAO WHERE catalogId = :value", SchemaInfoDAO.class);
        maxResults.ifPresent(query::setMaxResults);
        query.setParameter("value", catalogId);
        return query.list();
    }

    public SchemaInfo getSchema(String fullName) {
        try (Session session = sessionFactory.openSession()) {
            session.setDefaultReadOnly(true);
            Transaction tx = session.beginTransaction();
            SchemaInfoDAO schemaInfo = null;
            try {
                schemaInfo = getSchemaDAO(session, fullName);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
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

    public void deleteSchema(String fullName, boolean force) {
        try (Session session = sessionFactory.openSession()) {
            String[] namespace = fullName.split("\\.");
            if (namespace.length != 2) {
                throw new BaseException(ErrorCode.INVALID_ARGUMENT,
                        "Invalid schema name: " + fullName);
            }
            CatalogInfoDAO catalog = catalogRepository.getCatalogDAO(session, namespace[0]);
            if (catalog == null) {
                throw new BaseException(ErrorCode.NOT_FOUND,
                        "Catalog not found: " + namespace[0]);
            }
            Transaction tx = session.beginTransaction();
            try {
                deleteSchema(session, catalog.getId(), namespace[1] ,force);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public void deleteSchema(Session session, UUID catalogId, String schemaName, boolean force) {
        SchemaInfoDAO schemaInfo = getSchemaDAO(session, catalogId, schemaName);
        if (schemaInfo != null) {

            // handle tables
            List<TableInfoDAO> tables = tableRepository.
                    listTables(session, schemaInfo.getId(), 100, null);
            if (tables != null && !tables.isEmpty() && !force) {
                throw new BaseException(ErrorCode.FAILED_PRECONDITION,
                        "Cannot delete schema with tables: " + schemaName);
            } else if (tables != null){
                for (TableInfoDAO table : tables) {
                    tableRepository.deleteTable(session, schemaInfo.getId(), table.getName());
                }
            }

            // handle volumes
            List<VolumeInfoDAO> volumes = volumeRepository
                    .listVolumes(session, schemaInfo.getId(), Optional.of(100), Optional.empty());
            if (volumes != null && !volumes.isEmpty() && !force) {
                throw new BaseException(ErrorCode.FAILED_PRECONDITION,
                        "Cannot delete schema with volumes: " + schemaName);
            } else if (volumes != null) {
                for (VolumeInfoDAO volume : volumes) {
                    volumeRepository.deleteVolume(session, schemaInfo.getId(), volume.getName());
                }
            }

            // handle functions
            List<FunctionInfoDAO> functions = functionRepository
                    .listFunctions(session, schemaInfo.getId(), Optional.of(100), Optional.empty());
            if (functions != null && !functions.isEmpty() && !force) {
                throw new BaseException(ErrorCode.FAILED_PRECONDITION,
                        "Cannot delete schema with functions: " + schemaName);
            } else if (functions != null) {
                for (FunctionInfoDAO function : functions) {
                    functionRepository.deleteFunction(session, schemaInfo.getId(), function.getName());
                }
            }

            session.remove(schemaInfo);
        } else {
            throw new BaseException(ErrorCode.NOT_FOUND,
                    "Schema not found: " + schemaName);
        }
    }
}
