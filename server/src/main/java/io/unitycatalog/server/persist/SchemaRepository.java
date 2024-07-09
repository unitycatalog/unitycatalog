package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateSchema;
import io.unitycatalog.server.model.ListSchemasResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.UpdateSchema;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.ValidationUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class SchemaRepository {
    public static final SchemaRepository INSTANCE = new SchemaRepository();
    public static final CatalogRepository CATALOG_REPOSITORY = CatalogRepository.getInstance();
    private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();

    private static TableRepository tableRepository = TableRepository.getInstance();
    private static VolumeRepository volumeRepository = VolumeRepository.getInstance();
    private static FunctionRepository functionRepository = FunctionRepository.getInstance();

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRepository.class);

    private SchemaRepository() {}

    public static SchemaRepository getInstance() {
        return INSTANCE;
    }

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
        try (Session session = sessionFactory.openSession()) {
            session.setDefaultReadOnly(true);
            Transaction tx = session.beginTransaction();
            // TODO: Implement pagination and filtering if required
            // For now, returning all schemas without pagination
            try {
                CatalogInfoDAO catalog = CATALOG_REPOSITORY.getCatalogDAO(session, catalogName);
                if (catalog == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Catalog not found: " + catalogName);
                }
                ListSchemasResponse response = listSchemas(session, catalog.getId(), catalogName, maxResults, pageToken);
                tx.commit();
                return response;
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public ListSchemasResponse listSchemas(Session session, UUID catalogId, String catalogName,
                                           Optional<Integer> maxResults, Optional<String> pageToken) {
        ListSchemasResponse response = new ListSchemasResponse();
        Query<SchemaInfoDAO> query = session
                .createQuery("FROM SchemaInfoDAO WHERE catalogId = :value", SchemaInfoDAO.class);
        maxResults.ifPresent(query::setMaxResults);
        query.setParameter("value", catalogId);
        response.setSchemas(query.list().stream()
                .map(SchemaInfoDAO::toSchemaInfo)
                .peek(x -> addNamespaceData(x, catalogName))
                .map(s -> RepositoryUtils.attachProperties(s, s.getSchemaId(), Constants.SCHEMA, session))
                .collect(Collectors.toList()));
        return response;
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
                return RepositoryUtils.attachProperties(
                        schemaInfo,
                        schemaInfo.getSchemaId(),
                        Constants.SCHEMA,
                        session);
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
                deleteSchema(session, catalog.getId(), namespace[0], namespace[1] ,force);
                tx.commit();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public void processChildTables(Session session, UUID schemaId, String catalogName, String schemaName, boolean force) {
        // first check if there are any child tables
        List<TableInfo> tables = tableRepository
                .listTables(session, schemaId, catalogName, schemaName, Optional.of(1),
                        Optional.empty(), true, true).getTables();
        if (tables != null && !tables.isEmpty()) {
            if (!force) {
                throw new BaseException(ErrorCode.FAILED_PRECONDITION,
                        "Cannot delete schema with tables");
            }
            String nextToken = null;
            do {
                ListTablesResponse listTablesResponse = tableRepository
                        .listTables(session, schemaId, catalogName, schemaName, Optional.empty(),
                                Optional.ofNullable(nextToken), true, true);
                for (TableInfo tableInfo : listTablesResponse.getTables()) {
                    tableRepository.deleteTable(session, schemaId, tableInfo.getName());
                }
                nextToken = listTablesResponse.getNextPageToken();
            } while (nextToken != null);
        }
    }

    public void processChildVolumes(Session session, UUID schemaId, String catalogName, String schemaName,
                                    boolean force) {
        // first check if there are any child volumes
        List<VolumeInfo> volumes = volumeRepository
                .listVolumes(session, schemaId, catalogName, schemaName,
                        Optional.of(1), Optional.empty()).getVolumes();
        if (volumes != null && !volumes.isEmpty()) {
            if (!force) {
                throw new BaseException(ErrorCode.FAILED_PRECONDITION,
                        "Cannot delete schema with volumes");
            }
            String nextToken = null;
            do {
                ListVolumesResponseContent listVolumesResponse = volumeRepository
                        .listVolumes(session, schemaId, catalogName, schemaName,
                                Optional.empty(), Optional.ofNullable(nextToken));
                for (VolumeInfo volumeInfo : listVolumesResponse.getVolumes()) {
                    volumeRepository.deleteVolume(session, schemaId, volumeInfo.getName());
                }
                nextToken = listVolumesResponse.getNextPageToken();
            } while (nextToken != null);
        }
    }

    public void processChildFunctions(Session session, UUID schemaId, String catalogName, String schemaName,
                                      boolean force) {
        // first check if there are any child functions
        List<FunctionInfo> functions = functionRepository
                .listFunctions(session, schemaId, catalogName, schemaName,
                        Optional.of(1), Optional.empty()).getFunctions();
        if (functions != null && !functions.isEmpty()) {
            if (!force) {
                throw new BaseException(ErrorCode.FAILED_PRECONDITION,
                        "Cannot delete schema with functions");
            }
            String nextToken = null;
            do {
                ListFunctionsResponse listFunctionsResponse = functionRepository
                        .listFunctions(session, schemaId, catalogName, schemaName,
                                Optional.empty(), Optional.ofNullable(nextToken));
                for (FunctionInfo functionInfo : listFunctionsResponse.getFunctions()) {
                    functionRepository.deleteFunction(session, schemaId, functionInfo.getName());
                }
                nextToken = listFunctionsResponse.getNextPageToken();
            } while (nextToken != null);
        }
    }

    public void deleteSchema(Session session, UUID catalogId, String catalogName, String schemaName, boolean force) {
        SchemaInfoDAO schemaInfo = getSchemaDAO(session, catalogId, schemaName);
        if (schemaInfo != null) {
            processChildTables(session, schemaInfo.getId(), catalogName, schemaName, force);
            processChildVolumes(session, schemaInfo.getId(), catalogName, schemaName, force);
            processChildFunctions(session, schemaInfo.getId(), catalogName, schemaName, force);
            session.remove(schemaInfo);
            PropertyRepository.findProperties(session, schemaInfo.getId(), Constants.SCHEMA)
                    .forEach(session::remove);
        } else {
            throw new BaseException(ErrorCode.NOT_FOUND,
                    "Schema not found: " + schemaName);
        }
    }
}
