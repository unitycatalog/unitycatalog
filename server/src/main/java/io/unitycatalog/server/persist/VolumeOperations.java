package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.converters.VolumeInfoConverter;
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

public class VolumeOperations {

    @Getter
    public static final VolumeOperations instance = new VolumeOperations();
    private static final Logger LOGGER = LoggerFactory.getLogger(VolumeOperations.class);
    private static final SessionFactory sessionFactory = HibernateUtil.getSessionFactory();

    private VolumeOperations() {}

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
        VolumeInfoDAO volumeInfoDAO = VolumeInfoConverter.toDAO(volumeInfo);
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            session.persist(volumeInfoDAO);
            tx.commit();
            LOGGER.info("Added volume: {}", volumeInfo.getName());
            return VolumeInfoConverter.fromDAO(volumeInfoDAO);
        } catch (Exception e) {
            LOGGER.error("Error adding volume", e);
            return null;
        }
    }

    public VolumeInfo getVolume(String fullName) {
        try (Session session = sessionFactory.openSession()) {
            session.beginTransaction();
            Query<VolumeInfoDAO> query = session.createQuery("FROM VolumeInfoDAO WHERE fullName = :value", VolumeInfoDAO.class);
            query.setParameter("value", fullName);
            query.setMaxResults(1);
            return VolumeInfoConverter.fromDAO(query.uniqueResult());
        } catch (Exception e) {
            LOGGER.error("Error getting volume", e);
            return null;
        }
    }

    public VolumeInfo getVolumeById(String volumeId) {
        try (Session session = sessionFactory.openSession()) {
            session.beginTransaction();
            Query<VolumeInfoDAO> query = session.createQuery("FROM VolumeInfoDAO WHERE volumeId = :value", VolumeInfoDAO.class);
            query.setParameter("value", volumeId);
            query.setMaxResults(1);
            return VolumeInfoConverter.fromDAO(query.uniqueResult());
        } catch (Exception e) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Volume not found: " + volumeId);
        }
    }

    public ListVolumesResponseContent listVolumes(String catalogName, String schemaName, Optional<Integer> maxResults, Optional<String> pageToken, Optional<Boolean> includeBrowse) {
        ListVolumesResponseContent responseContent = new ListVolumesResponseContent();
        try (Session session = sessionFactory.openSession()) {
            session.beginTransaction();

            String queryString = "from VolumeInfoDAO v where v.catalogName = :catalogName and v.schemaName = :schemaName";
            Query<VolumeInfoDAO> query = session.createQuery(queryString, VolumeInfoDAO.class);
            query.setParameter("catalogName", catalogName);
            query.setParameter("schemaName", schemaName);

            maxResults.ifPresent(query::setMaxResults);

            if (pageToken.isPresent()) {
                // Perform pagination logic here if needed
                // Example: query.setFirstResult(startIndex);
            }

            responseContent.setVolumes(query.list().stream()
                    .map(VolumeInfoConverter::fromDAO).collect(Collectors.toList()));
            return responseContent;
        } catch (Exception e) {
            LOGGER.error("Error listing volumes", e);
            return null;
        }
    }

    public VolumeInfo updateVolume(String name, UpdateVolumeRequestContent updateVolumeRequest) {
        ValidationUtils.validateSqlObjectName(updateVolumeRequest.getNewName());
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            VolumeInfoDAO volumeInfo = VolumeInfoConverter.toDAO(getVolume(name));
            if (volumeInfo != null) {
                if (updateVolumeRequest.getNewName() != null) {
                    volumeInfo.setName(updateVolumeRequest.getNewName());
                    String fullName = volumeInfo.getCatalogName() + "." + volumeInfo.getSchemaName() + "." + updateVolumeRequest.getNewName();
                    volumeInfo.setFullName(fullName);
                }
                if (updateVolumeRequest.getComment() != null) {
                    volumeInfo.setComment(updateVolumeRequest.getComment());
                }
                volumeInfo.setUpdatedAt(new Date());
                session.merge(volumeInfo);
                tx.commit();
                LOGGER.info("Updated volume: {}", volumeInfo.getName());
            }
            return VolumeInfoConverter.fromDAO(volumeInfo);
        } catch (Exception e) {
            LOGGER.error("Error updating volume", e);
            return null;
        }
    }

    public void deleteVolume(String name) {
        try (Session session = sessionFactory.openSession()) {
            Transaction tx = session.beginTransaction();
            VolumeInfo volumeInfo = getVolume(name);
            VolumeInfoDAO volumeInfoDAO = VolumeInfoConverter.toDAO(volumeInfo);
            if (volumeInfoDAO != null) {
                if (VolumeType.MANAGED.getValue().equals(volumeInfoDAO.getVolumeType())) {
                    try {
                        FileUtils.deleteDirectory(volumeInfo.getStorageLocation());
                    } catch (Exception e) {
                        LOGGER.error("Error deleting volume directory", e);
                    }
                }
                session.remove(volumeInfoDAO);
                tx.commit();
                LOGGER.info("Deleted volume: {}", volumeInfo.getName());
            } else {
                throw new BaseException(ErrorCode.NOT_FOUND, "Volume not found: " + name);
            }
        } catch (Exception e) {
            LOGGER.error("Error deleting volume", e);
        }
    }
}
