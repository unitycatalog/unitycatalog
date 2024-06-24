package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.utils.ValidationUtils;
import lombok.Getter;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class VolumeRepository {

    @Getter
    public static final VolumeRepository instance = new VolumeRepository();
    public static final SchemaRepository schemaRepository = SchemaRepository.getInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(VolumeRepository.class);
    private static final SessionFactory sessionFactory = HibernateUtil.getSessionFactory();

    private VolumeRepository() {}

    public VolumeInfo createVolume(CreateVolumeRequestContent createVolumeRequest) {
        ValidationUtils.validateSqlObjectName(createVolumeRequest.getName());
        String volumeFullName = createVolumeRequest.getCatalogName() + "." + createVolumeRequest.getSchemaName() + "." + createVolumeRequest.getName();
        VolumeInfo volumeInfo = new VolumeInfo();
        volumeInfo.setVolumeId(UUID.randomUUID().toString());
        volumeInfo.setCatalogName(createVolumeRequest.getCatalogName());
        volumeInfo.setSchemaName(createVolumeRequest.getSchemaName());
        volumeInfo.setName(createVolumeRequest.getName());
        volumeInfo.setComment(createVolumeRequest.getComment());
        volumeInfo.setFullName(volumeFullName);
        volumeInfo.setCreatedAt(System.currentTimeMillis());
        volumeInfo.setVolumeType(createVolumeRequest.getVolumeType());
        if (VolumeType.MANAGED.equals(createVolumeRequest.getVolumeType())) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Managed volume creation is not supported");
        }
        if (createVolumeRequest.getStorageLocation() == null) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Storage location is required for external volume");
        }
        volumeInfo.setStorageLocation(createVolumeRequest.getStorageLocation());
        VolumeInfoDAO volumeInfoDAO = VolumeInfoDAO.from(volumeInfo);
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                SchemaInfoDAO schemaInfoDAO = schemaRepository.getSchemaDAO(session,
                        createVolumeRequest.getCatalogName(), createVolumeRequest.getSchemaName());
                if (schemaInfoDAO == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND,
                            "Schema not found: " + createVolumeRequest.getCatalogName() + "." + createVolumeRequest.getSchemaName());
                }
                if (getVolumeDAO(session, createVolumeRequest.getCatalogName(), createVolumeRequest.getSchemaName(), createVolumeRequest.getName()) != null) {
                    throw new BaseException(ErrorCode.ALREADY_EXISTS, "Volume already exists: " + volumeFullName);
                }
                volumeInfoDAO.setSchemaId(schemaInfoDAO.getId());
                session.persist(volumeInfoDAO);
                tx.commit();
                LOGGER.info("Added volume: {}", volumeInfo.getName());
                return convertFromDAO(volumeInfoDAO, createVolumeRequest.getCatalogName(), createVolumeRequest.getSchemaName());
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public VolumeInfo getVolume(String fullName) {
        try (Session session = sessionFactory.openSession()) {
            String[] namespace = fullName.split("\\.");
            if (namespace.length != 3) {
                throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid volume name: " + fullName);
            }
            String catalogName = namespace[0];
            String schemaName = namespace[1];
            String volumeName = namespace[2];
            return convertFromDAO(getVolumeDAO(session, catalogName, schemaName, volumeName),
                    catalogName, schemaName);
        } catch (Exception e) {
            LOGGER.error("Error getting volume", e);
            return null;
        }
    }

    public VolumeInfoDAO getVolumeDAO(Session session, String catalogName, String schemaName, String volumeName) {
        SchemaInfoDAO schemaInfo = schemaRepository.getSchemaDAO(session, catalogName, schemaName);
        if (schemaInfo == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + catalogName + "." + schemaName);
        }
        Query<VolumeInfoDAO> query = session.createQuery(
                "FROM VolumeInfoDAO WHERE name = :name and schemaId = :schemaId", VolumeInfoDAO.class);
        query.setParameter("name", volumeName);
        query.setParameter("schemaId", schemaInfo.getId());
        query.setMaxResults(1);
        return query.uniqueResult();
    }

    public VolumeInfo getVolumeById(String volumeId) {
        try (Session session = sessionFactory.openSession()) {
            session.setDefaultReadOnly(true);
            Transaction tx = session.beginTransaction();
            try {
                Query<VolumeInfoDAO> query = session.createQuery("FROM VolumeInfoDAO WHERE id = :value", VolumeInfoDAO.class);
                query.setParameter("value", UUID.fromString(volumeId));
                query.setMaxResults(1);
                VolumeInfoDAO volumeInfoDAO = query.uniqueResult();
                tx.commit();
                return volumeInfoDAO.toVolumeInfo();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public ListVolumesResponseContent listVolumes(String catalogName, String schemaName, Optional<Integer> maxResults, Optional<String> pageToken, Optional<Boolean> includeBrowse) {
        ListVolumesResponseContent responseContent = new ListVolumesResponseContent();
        try (Session session = sessionFactory.openSession()) {
            session.setDefaultReadOnly(true);
            Transaction tx = session.beginTransaction();
            try {
                SchemaInfoDAO schemaInfo = schemaRepository.getSchemaDAO(session, catalogName, schemaName);
                if (schemaInfo == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Schema not found: " + catalogName + "." + schemaName);
                }
                String queryString = "from VolumeInfoDAO v where v.schemaId = :schemaId";
                Query<VolumeInfoDAO> query = session.createQuery(queryString, VolumeInfoDAO.class);
                query.setParameter("schemaId", schemaInfo.getId());
                maxResults.ifPresent(query::setMaxResults);
                if (pageToken.isPresent()) {
                    // Perform pagination logic here if needed
                    // Example: query.setFirstResult(startIndex);
                }
                responseContent.setVolumes(query.list().stream()
                        .map(x -> convertFromDAO(x, catalogName, schemaName))
                        .collect(Collectors.toList()));
                tx.commit();
                return responseContent;
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    private VolumeInfo convertFromDAO(VolumeInfoDAO volumeInfoDAO, String catalogName, String schemaName) {
        VolumeInfo volumeInfo = volumeInfoDAO.toVolumeInfo();
        volumeInfo.setCatalogName(catalogName);
        volumeInfo.setSchemaName(schemaName);
        volumeInfo.setFullName(catalogName + "." + schemaName + "." + volumeInfo.getName());
        return volumeInfo;
    }

    public VolumeInfo updateVolume(String name, UpdateVolumeRequestContent updateVolumeRequest) {
        ValidationUtils.validateSqlObjectName(updateVolumeRequest.getNewName());
        String[] namespace =name.split("\\.");
        String catalog = namespace[0], schema = namespace[1], volume = namespace[2];
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                VolumeInfoDAO volumeInfo = getVolumeDAO(session, catalog, schema, volume);
                if (volumeInfo == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Volume not found: " + name);
                }
                if (updateVolumeRequest.getNewName() != null) {
                    VolumeInfoDAO existingVolume = getVolumeDAO(session, catalog, schema,
                            updateVolumeRequest.getNewName());
                    if (existingVolume != null) {
                        throw new BaseException(ErrorCode.ALREADY_EXISTS,
                                "Volume already exists: " + updateVolumeRequest.getNewName());
                    }
                }
                if (updateVolumeRequest.getNewName() != null) {
                    volumeInfo.setName(updateVolumeRequest.getNewName());
                }
                if (updateVolumeRequest.getComment() != null) {
                    volumeInfo.setComment(updateVolumeRequest.getComment());
                }
                volumeInfo.setUpdatedAt(new Date());
                session.merge(volumeInfo);
                tx.commit();
                LOGGER.info("Updated volume: {}", volumeInfo.getName());
                return convertFromDAO(volumeInfo, catalog, schema);
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }

    public void deleteVolume(String name) {
        try (Session session = sessionFactory.openSession()) {
            String[] namespace = name.split("\\.");
            if (namespace.length != 3) {
                throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid volume name: " + name);
            }
            String catalog = namespace[0], schema = namespace[1], volume = namespace[2];
            Transaction tx = session.beginTransaction();
            try {
                VolumeInfoDAO volumeInfoDAO = getVolumeDAO(session, catalog, schema, volume);
                if (volumeInfoDAO == null) {
                    throw new BaseException(ErrorCode.NOT_FOUND, "Volume not found: " + name);
                }
                if (VolumeType.MANAGED.getValue().equals(volumeInfoDAO.getVolumeType())) {
                    try {
                        FileUtils.deleteDirectory(volumeInfoDAO.getStorageLocation());
                    } catch (Exception e) {
                        LOGGER.error("Error deleting volume directory", e);
                    }
                }
                session.remove(volumeInfoDAO);
                tx.commit();
                LOGGER.info("Deleted volume: {}", volumeInfoDAO.getName());
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }
}