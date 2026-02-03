package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateVolumeRequestContent;
import io.unitycatalog.server.model.ListVolumesResponseContent;
import io.unitycatalog.server.model.UpdateVolumeRequestContent;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.model.VolumeType;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VolumeRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(VolumeRepository.class);
  private final Repositories repositories;
  private final SessionFactory sessionFactory;
  private final FileOperations fileOperations;
  private static final PagedListingHelper<VolumeInfoDAO> LISTING_HELPER =
      new PagedListingHelper<>(VolumeInfoDAO.class);

  public VolumeRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
    this.fileOperations = repositories.getFileOperations();
  }

  public VolumeInfo createVolume(CreateVolumeRequestContent createVolumeRequest) {
    ValidationUtils.validateSqlObjectName(createVolumeRequest.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          RepositoryUtils.CatalogAndSchemaDao catalogAndSchemaDao =
              RepositoryUtils.getCatalogAndSchemaDaoOrThrow(
                  session,
                  createVolumeRequest.getCatalogName(),
                  createVolumeRequest.getSchemaName());
          UUID schemaId = catalogAndSchemaDao.schemaInfoDAO().getId();
          if (getVolumeDAO(session, schemaId, createVolumeRequest.getName()) != null) {
            throw new BaseException(
                ErrorCode.ALREADY_EXISTS,
                "Volume already exists: " + createVolumeRequest.getName());
          }

          UUID volumeId = UUID.randomUUID();
          NormalizedURL storageLocation;
          if (createVolumeRequest.getVolumeType() == VolumeType.MANAGED) {
            if (createVolumeRequest.getStorageLocation() != null) {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT,
                  "Storage location should not be specified for managed volume");
            }
            NormalizedURL parentStorageLocation =
                ExternalLocationUtils.getManagedStorageLocation(catalogAndSchemaDao);
            storageLocation =
                ExternalLocationUtils.getManagedLocationForVolume(parentStorageLocation, volumeId);
          } else {
            // EXTERNAL volume.
            if (createVolumeRequest.getStorageLocation() == null) {
              throw new BaseException(
                  ErrorCode.INVALID_ARGUMENT, "Storage location is required for external volume");
            }
            storageLocation = NormalizedURL.from(createVolumeRequest.getStorageLocation());
            ExternalLocationUtils.validateNotOverlapWithManagedStorage(session, storageLocation);
          }
          Date now = new Date();

          VolumeInfoDAO volumeInfoDAO =
              VolumeInfoDAO.builder()
                  .id(volumeId)
                  .name(createVolumeRequest.getName())
                  .schemaId(schemaId)
                  .comment(createVolumeRequest.getComment())
                  .storageLocation(storageLocation.toString())
                  .owner(callerId)
                  .createdAt(now)
                  .createdBy(callerId)
                  .updatedAt(now)
                  .updatedBy(callerId)
                  .volumeType(createVolumeRequest.getVolumeType().getValue())
                  .build();
          session.persist(volumeInfoDAO);
          LOGGER.info("Added volume: {}", volumeInfoDAO.getName());
          return volumeInfoDAO.toVolumeInfo(
              createVolumeRequest.getCatalogName(), createVolumeRequest.getSchemaName());
        },
        "Failed to create volume",
        /* readOnly = */ false);
  }

  public VolumeInfo getVolume(String fullName) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String[] namespace = fullName.split("\\.");
          if (namespace.length != 3) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid volume name: " + fullName);
          }
          String catalogName = namespace[0];
          String schemaName = namespace[1];
          String volumeName = namespace[2];
          return getVolumeDAO(session, catalogName, schemaName, volumeName)
              .toVolumeInfo(catalogName, schemaName);
        },
        "Failed to get volume",
        /* readOnly = */ true);
  }

  public VolumeInfoDAO getVolumeDAO(
      Session session, String catalogName, String schemaName, String volumeName) {
    UUID schemaId =
        repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalogName, schemaName);
    return getVolumeDAO(session, schemaId, volumeName);
  }

  public VolumeInfoDAO getVolumeDAO(Session session, UUID schemaId, String volumeName) {
    Query<VolumeInfoDAO> query =
        session.createQuery(
            "FROM VolumeInfoDAO WHERE name = :name and schemaId = :schemaId", VolumeInfoDAO.class);
    query.setParameter("name", volumeName);
    query.setParameter("schemaId", schemaId);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public VolumeInfo getVolumeById(String volumeId) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          VolumeInfoDAO volumeInfoDAO = session.get(VolumeInfoDAO.class, UUID.fromString(volumeId));
          if (volumeInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Volume not found: " + volumeId);
          }
          RepositoryUtils.CatalogAndSchemaNames catalogAndSchemaNames =
              RepositoryUtils.getCatalogAndSchemaNames(session, volumeInfoDAO.getSchemaId());
          VolumeInfo volumeInfo =
              volumeInfoDAO.toVolumeInfo(
                  catalogAndSchemaNames.catalogName(), catalogAndSchemaNames.schemaName());
          return volumeInfo;
        },
        "Failed to get volume by ID",
        /* readOnly = */ true);
  }

  /**
   * Return the list of volumes in ascending order of volume name.
   *
   * @param catalogName
   * @param schemaName
   * @param maxResults
   * @param pageToken
   * @param includeBrowse
   * @return
   */
  public ListVolumesResponseContent listVolumes(
      String catalogName,
      String schemaName,
      Optional<Integer> maxResults,
      Optional<String> pageToken,
      Optional<Boolean> includeBrowse) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UUID schemaId =
              repositories
                  .getSchemaRepository()
                  .getSchemaIdOrThrow(session, catalogName, schemaName);
          return listVolumes(session, schemaId, catalogName, schemaName, maxResults, pageToken);
        },
        "Failed to list volumes",
        /* readOnly = */ true);
  }

  public ListVolumesResponseContent listVolumes(
      Session session,
      UUID schemaId,
      String catalogName,
      String schemaName,
      Optional<Integer> maxResults,
      Optional<String> pageToken) {
    List<VolumeInfoDAO> volumeInfoDAOList =
        LISTING_HELPER.listEntity(session, maxResults, pageToken, schemaId);
    String nextPageToken = LISTING_HELPER.getNextPageToken(volumeInfoDAOList, maxResults);
    List<VolumeInfo> result = new ArrayList<>();
    for (VolumeInfoDAO volumeInfoDAO : volumeInfoDAOList) {
      VolumeInfo volumeInfo = volumeInfoDAO.toVolumeInfo(catalogName, schemaName);
      result.add(volumeInfo);
    }
    return new ListVolumesResponseContent().volumes(result).nextPageToken(nextPageToken);
  }

  public VolumeInfo updateVolume(String name, UpdateVolumeRequestContent updateVolumeRequest) {
    if (updateVolumeRequest.getNewName() != null) {
      ValidationUtils.validateSqlObjectName(updateVolumeRequest.getNewName());
    }
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    String[] namespace = name.split("\\.");
    String catalog = namespace[0], schema = namespace[1], volume = namespace[2];

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          VolumeInfoDAO volumeInfo = getVolumeDAO(session, catalog, schema, volume);
          if (volumeInfo == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Volume not found: " + name);
          }
          if (updateVolumeRequest.getNewName() != null) {
            VolumeInfoDAO existingVolume =
                getVolumeDAO(session, catalog, schema, updateVolumeRequest.getNewName());
            if (existingVolume != null) {
              throw new BaseException(
                  ErrorCode.ALREADY_EXISTS,
                  "Volume already exists: " + updateVolumeRequest.getNewName());
            }
          }
          if (updateVolumeRequest.getNewName() == null
              && updateVolumeRequest.getComment() == null) {
            return volumeInfo.toVolumeInfo(catalog, schema);
          }
          if (updateVolumeRequest.getNewName() != null) {
            volumeInfo.setName(updateVolumeRequest.getNewName());
          }
          if (updateVolumeRequest.getComment() != null) {
            volumeInfo.setComment(updateVolumeRequest.getComment());
          }
          volumeInfo.setUpdatedAt(new Date());
          volumeInfo.setUpdatedBy(callerId);
          session.merge(volumeInfo);
          LOGGER.info("Updated volume: {}", volumeInfo.getName());
          return volumeInfo.toVolumeInfo(catalog, schema);
        },
        "Failed to update volume",
        /* readOnly = */ false);
  }

  public void deleteVolume(String name) {
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String[] namespace = name.split("\\.");
          if (namespace.length != 3) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid volume name: " + name);
          }
          String catalog = namespace[0], schema = namespace[1], volume = namespace[2];
          SchemaInfoDAO schemaInfo =
              repositories.getSchemaRepository().getSchemaDaoOrThrow(session, catalog, schema);
          deleteVolume(session, schemaInfo.getId(), volume);
          return null;
        },
        "Failed to delete volume",
        /* readOnly = */ false);
  }

  public void deleteVolume(Session session, UUID schemaId, String volumeName) {
    VolumeInfoDAO volumeInfoDAO = getVolumeDAO(session, schemaId, volumeName);
    if (volumeInfoDAO == null) {
      throw new BaseException(ErrorCode.NOT_FOUND, "Volume not found: " + volumeName);
    }
    if (VolumeType.MANAGED.getValue().equals(volumeInfoDAO.getVolumeType())) {
      try {
        fileOperations.deleteDirectory(volumeInfoDAO.getStorageLocation());
      } catch (Exception e) {
        LOGGER.error("Error deleting volume directory", e);
      }
    }
    session.remove(volumeInfoDAO);
    LOGGER.info("Deleted volume: {}", volumeInfoDAO.getName());
  }
}
