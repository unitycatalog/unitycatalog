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
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VolumeRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(VolumeRepository.class);
  private final Repositories repositories;
  private final SessionFactory sessionFactory;
  private static final PagedListingHelper<VolumeInfoDAO> LISTING_HELPER =
      new PagedListingHelper<>(VolumeInfoDAO.class);

  public VolumeRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
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
                ErrorCode.RESOURCE_ALREADY_EXISTS,
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
            ValidationUtils.checkArgument(
                !storageLocation.isCloudStorageRoot(),
                "External volume storage location must include a non-empty path prefix: %s",
                createVolumeRequest.getStorageLocation());
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
        /* readOnly= */ false);
  }

  public VolumeInfo getVolume(String fullName) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String[] namespace =
              ValidationUtils.splitFullName(fullName, 3, "Invalid volume name: " + fullName);
          String catalogName = namespace[0];
          String schemaName = namespace[1];
          String volumeName = namespace[2];
          VolumeInfoDAO volumeInfoDAO = getVolumeDAO(session, catalogName, schemaName, volumeName);
          if (volumeInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "Volume not found: " + fullName);
          }
          return volumeInfoDAO.toVolumeInfo(catalogName, schemaName);
        },
        "Failed to get volume",
        /* readOnly= */ true);
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
        /* readOnly= */ true);
  }

  /**
   * Return the list of volumes in ascending order of volume name.
   *
   * @param catalogName
   * @param schemaName
   * @param maxResults
   * @param pageToken
   * @return
   */
  public ListVolumesResponseContent listVolumes(
      String catalogName,
      String schemaName,
      Optional<Integer> maxResults,
      Optional<String> pageToken) {
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
        /* readOnly= */ true);
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
    String[] namespace = ValidationUtils.splitFullName(name, 3, "Invalid volume name: " + name);
    String catalog = namespace[0], schema = namespace[1], volume = namespace[2];

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          VolumeInfoDAO volumeInfo = getVolumeDAO(session, catalog, schema, volume);
          if (volumeInfo == null) {
            throw volumeNotFound(name);
          }
          if (updateVolumeRequest.getNewName() != null) {
            VolumeInfoDAO existingVolume =
                getVolumeDAO(session, catalog, schema, updateVolumeRequest.getNewName());
            if (existingVolume != null) {
              throw new BaseException(
                  ErrorCode.RESOURCE_ALREADY_EXISTS,
                  "Volume already exists: " + updateVolumeRequest.getNewName());
            }
          }
          if (updateVolumeRequest.getNewName() == null
              && updateVolumeRequest.getComment() == null) {
            return volumeInfo.toVolumeInfo(catalog, schema);
          }
          Map<String, Object> key = Map.of("name", volume);
          VolumeInfoDAO locked =
              RepositoryUtils.lockByKey(
                  session,
                  VolumeInfoDAO.class,
                  volumeInfo.getId(),
                  key,
                  "volume " + name,
                  () -> volumeNotFound(name));
          Map<String, Object> changes = new HashMap<>();
          if (updateVolumeRequest.getNewName() != null) {
            changes.put("name", updateVolumeRequest.getNewName());
          }
          if (updateVolumeRequest.getComment() != null) {
            changes.put("comment", updateVolumeRequest.getComment());
          }
          changes.put("updatedAt", new Date());
          changes.put("updatedBy", callerId);
          RepositoryUtils.updateByKey(session, locked, key, changes, () -> volumeNotFound(name));
          LOGGER.info("Updated volume: {}", locked.getName());
          return locked.toVolumeInfo(catalog, schema);
        },
        "Failed to update volume",
        /* readOnly= */ false);
  }

  public void deleteVolume(String name) {
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String[] namespace =
              ValidationUtils.splitFullName(name, 3, "Invalid volume name: " + name);
          String catalog = namespace[0], schema = namespace[1], volume = namespace[2];
          SchemaInfoDAO schemaInfo =
              repositories.getSchemaRepository().getSchemaDaoOrThrow(session, catalog, schema);
          deleteVolume(session, schemaInfo.getId(), volume);
          return null;
        },
        "Failed to delete volume",
        /* readOnly= */ false);
  }

  public void deleteVolume(Session session, UUID schemaId, String volumeName) {
    VolumeInfoDAO found = getVolumeDAO(session, schemaId, volumeName);
    if (found == null) {
      throw volumeNotFound(volumeName);
    }
    deleteVolume(session, found.getId(), volumeName, () -> volumeNotFound(volumeName));
  }

  /**
   * Deletes the volume {@code volumeId} named {@code volumeName}; {@code notFound} is thrown if no
   * such row exists, for example because it was renamed or dropped since the caller found it.
   */
  void deleteVolume(
      Session session, UUID volumeId, String volumeName, Supplier<BaseException> notFound) {
    Map<String, Object> key = Map.of("name", volumeName);
    VolumeInfoDAO volumeInfoDAO =
        RepositoryUtils.lockByKey(
            session, VolumeInfoDAO.class, volumeId, key, "volume " + volumeName, notFound);
    if (VolumeType.MANAGED.getValue().equals(volumeInfoDAO.getVolumeType())) {
      queueManagedVolumeCleanup(session, volumeInfoDAO);
    }
    RepositoryUtils.deleteByKey(session, volumeInfoDAO, key, notFound);
    LOGGER.info("Deleted volume: {}", volumeInfoDAO.getName());
  }

  private static BaseException volumeNotFound(String name) {
    return new BaseException(ErrorCode.NOT_FOUND, "Volume not found: " + name);
  }

  /**
   * Queues a storage cleanup task so the background worker reclaims a dropped managed volume's
   * files. The task is created in the caller's transaction, so the row removal and task creation
   * commit or roll back together. This applies to every storage scheme, including local, matching
   * how managed-table drops are handled; filesystem deletion is never done synchronously in the
   * drop transaction.
   */
  private void queueManagedVolumeCleanup(Session session, VolumeInfoDAO volumeInfoDAO) {
    repositories
        .getStorageCleanupTaskRepository()
        .create(
            session,
            ManagedResourceType.VOLUME,
            volumeInfoDAO.getId(),
            volumeInfoDAO.getName(),
            volumeInfoDAO.getStorageLocation());
  }
}
