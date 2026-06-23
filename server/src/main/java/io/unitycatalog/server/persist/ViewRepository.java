package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.CreateView;
import io.unitycatalog.server.model.ListViewsResponse;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.model.ViewInfo;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(ViewRepository.class);
  private final SessionFactory sessionFactory;
  private final Repositories repositories;
  private static final PagedListingHelper<TableInfoDAO> LISTING_HELPER =
      new PagedListingHelper<>(
          TableInfoDAO.class, (cb, root) -> cb.equal(root.get("type"), TableType.VIEW.toString()));

  public ViewRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
  }

  public ViewInfo getViewById(String viewId) {
    LOGGER.debug("Getting view by id: {}", viewId);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          TableInfoDAO tableInfoDAO = session.get(TableInfoDAO.class, UUID.fromString(viewId));
          if (!isView(tableInfoDAO)) {
            throw new BaseException(ErrorCode.NOT_FOUND, "View not found: " + viewId);
          }
          SchemaInfoDAO schemaInfoDAO =
              session.get(SchemaInfoDAO.class, tableInfoDAO.getSchemaId());
          if (schemaInfoDAO == null) {
            throw new BaseException(
                ErrorCode.NOT_FOUND, "Schema not found: " + tableInfoDAO.getSchemaId());
          }
          CatalogInfoDAO catalogInfoDAO =
              session.get(CatalogInfoDAO.class, schemaInfoDAO.getCatalogId());
          if (catalogInfoDAO == null) {
            throw new BaseException(
                ErrorCode.NOT_FOUND, "Catalog not found: " + schemaInfoDAO.getCatalogId());
          }
          ViewInfo viewInfo =
              tableInfoDAO.toViewInfo(true, catalogInfoDAO.getName(), schemaInfoDAO.getName());
          return viewInfo;
        },
        "Failed to get view by ID",
        /* readOnly = */ true);
  }

  public ViewInfo getView(String fullName) {
    LOGGER.debug("Getting view: {}", fullName);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String[] parts = fullName.split("\\.");
          if (parts.length != 3) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid view name: " + fullName);
          }
          String catalogName = parts[0];
          String schemaName = parts[1];
          String viewName = parts[2];
          TableInfoDAO tableInfoDAO = findView(session, catalogName, schemaName, viewName);
          if (!isView(tableInfoDAO)) {
            throw new BaseException(ErrorCode.NOT_FOUND, "View not found: " + fullName);
          }
          ViewInfo viewInfo = tableInfoDAO.toViewInfo(true, catalogName, schemaName);
          RepositoryUtils.attachProperties(
              viewInfo, viewInfo.getViewId(), Constants.TABLE, session);
          return viewInfo;
        },
        "Failed to get view",
        /* readOnly = */ true);
  }

  private TableInfoDAO findView(
      Session session, String catalogName, String schemaName, String tableName) {
    UUID schemaId = getSchemaId(session, catalogName, schemaName);
    return findBySchemaIdAndName(session, schemaId, tableName);
  }

  private boolean isView(TableInfoDAO tableInfoDAO) {
    return tableInfoDAO != null && (TableType.VIEW.toString().equals(tableInfoDAO.getType()));
  }

  public ViewInfo createView(CreateView createView) {
    ValidationUtils.validateSqlObjectName(createView.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    List<ColumnInfo> columnInfos =
        createView.getColumns().stream()
            .map(c -> c.typeText(c.getTypeText().toLowerCase(Locale.ROOT)))
            .collect(Collectors.toList());
    Long createTime = System.currentTimeMillis();
    ViewInfo viewInfo =
        new ViewInfo()
            .viewId(UUID.randomUUID().toString())
            .name(createView.getName())
            .catalogName(createView.getCatalogName())
            .schemaName(createView.getSchemaName())
            .viewType(createView.getViewType())
            .sqlQuery(createView.getSqlQuery())
            .columns(columnInfos)
            .comment(createView.getComment())
            .properties(createView.getProperties())
            .owner(callerId)
            .createdAt(createTime)
            .createdBy(callerId)
            .updatedAt(createTime)
            .updatedBy(callerId);
    String fullName = getViewFullName(viewInfo);
    LOGGER.debug("Creating view: {}", fullName);

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String catalogName = viewInfo.getCatalogName();
          String schemaName = viewInfo.getSchemaName();
          UUID schemaId = getSchemaId(session, catalogName, schemaName);

          // Check if table already exists
          TableInfoDAO existingView = findBySchemaIdAndName(session, schemaId, viewInfo.getName());
          if (existingView != null) {
            throw new BaseException(ErrorCode.ALREADY_EXISTS, "View already exists: " + fullName);
          }
          TableInfoDAO tableInfoDAO = TableInfoDAO.from(viewInfo, schemaId);
          tableInfoDAO.setSchemaId(schemaId);
          // create columns
          tableInfoDAO
              .getColumns()
              .forEach(
                  c -> {
                    c.setId(UUID.randomUUID());
                    c.setTable(tableInfoDAO);
                  });
          // create properties
          PropertyDAO.from(viewInfo.getProperties(), tableInfoDAO.getId(), Constants.TABLE)
              .forEach(session::persist);
          session.persist(tableInfoDAO);
          return viewInfo;
        },
        "Error creating view: " + fullName,
        /* readOnly = */ false);
  }

  public TableInfoDAO findBySchemaIdAndName(Session session, UUID schemaId, String name) {
    String hql =
        "FROM TableInfoDAO t WHERE t.schemaId = :schemaId AND t.name = :name AND t.type = '"
            + TableType.VIEW
            + "'";
    Query<TableInfoDAO> query = session.createQuery(hql, TableInfoDAO.class);
    query.setParameter("schemaId", schemaId);
    query.setParameter("name", name);
    LOGGER.debug("Finding table by schemaId: {} and name: {}", schemaId, name);
    return query.uniqueResult(); // Returns null if no result is found
  }

  private String getViewFullName(ViewInfo viewInfo) {
    return viewInfo.getCatalogName() + "." + viewInfo.getSchemaName() + "." + viewInfo.getName();
  }

  public UUID getSchemaId(Session session, String catalogName, String schemaName) {
    SchemaInfoDAO schemaInfo =
        repositories.getSchemaRepository().getSchemaDaoOrThrow(session, catalogName, schemaName);
    return schemaInfo.getId();
  }

  /**
   * Return the list of views in ascending order of view name.
   *
   * @param catalogName
   * @param schemaName
   * @param maxResults
   * @param pageToken
   * @param omitProperties
   * @param omitColumns
   * @return
   */
  public ListViewsResponse listViews(
      String catalogName,
      String schemaName,
      Optional<Integer> maxResults,
      Optional<String> pageToken,
      Boolean omitProperties,
      Boolean omitColumns) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UUID schemaId = getSchemaId(session, catalogName, schemaName);
          return listViews(
              session,
              schemaId,
              catalogName,
              schemaName,
              maxResults,
              pageToken,
              omitProperties,
              omitColumns);
        },
        "Failed to list views",
        /* readOnly = */ true);
  }

  public ListViewsResponse listViews(
      Session session,
      UUID schemaId,
      String catalogName,
      String schemaName,
      Optional<Integer> maxResults,
      Optional<String> pageToken,
      Boolean omitProperties,
      Boolean omitColumns) {
    List<TableInfoDAO> tableInfoDAOList =
        LISTING_HELPER.listEntity(session, maxResults, pageToken, schemaId);
    String nextPageToken = LISTING_HELPER.getNextPageToken(tableInfoDAOList, maxResults);
    List<ViewInfo> result = new ArrayList<>();
    for (TableInfoDAO tableInfoDAO : tableInfoDAOList) {
      ViewInfo viewInfo = tableInfoDAO.toViewInfo(!omitColumns, catalogName, schemaName);
      if (!omitProperties) {
        RepositoryUtils.attachProperties(viewInfo, viewInfo.getViewId(), Constants.TABLE, session);
      }
      result.add(viewInfo);
    }
    return new ListViewsResponse().views(result).nextPageToken(nextPageToken);
  }

  public void deleteView(String fullName) {
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String[] parts = fullName.split("\\.");
          if (parts.length != 3) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid view name: " + fullName);
          }
          String catalogName = parts[0];
          String schemaName = parts[1];
          String tableName = parts[2];
          UUID schemaId = getSchemaId(session, catalogName, schemaName);
          deleteView(session, schemaId, tableName);
          return null;
        },
        "Failed to delete view",
        /* readOnly = */ false);
  }

  public void deleteView(Session session, UUID schemaId, String viewName) {
    TableInfoDAO tableInfoDAO = findBySchemaIdAndName(session, schemaId, viewName);
    if (!isView(tableInfoDAO)) {
      throw new BaseException(ErrorCode.NOT_FOUND, "View not found: " + viewName);
    }
    PropertyRepository.findProperties(session, tableInfoDAO.getId(), Constants.TABLE)
        .forEach(session::remove);
    session.remove(tableInfoDAO);
  }
}
