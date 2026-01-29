package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.CreateView;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.model.ViewInfo;
import io.unitycatalog.server.model.ViewRepresentation;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.ViewRepresentationDAO;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.util.List;
import java.util.Locale;
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

  public ViewRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
  }

  public ViewInfo createView(CreateView createView) {
    ValidationUtils.validateSqlObjectName(createView.getName());

    String callerId = IdentityUtils.findPrincipalEmailAddress();
    Long createTime = System.currentTimeMillis();
    String fullName = getViewFullName(createView);
    LOGGER.debug("Creating view: {}", fullName);

    List<ColumnInfo> columnInfos =
        createView.getColumns().stream()
            .map(c -> c.typeText(c.getTypeText().toLowerCase(Locale.ROOT)))
            .collect(Collectors.toList());

    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          String catalogName = createView.getCatalogName();
          String schemaName = createView.getSchemaName();
          UUID schemaId =
              repositories
                  .getSchemaRepository()
                  .getSchemaIdOrThrow(session, catalogName, schemaName);

          // Check for existing table OR view with the same name (use AnyType to include views)
          TableInfoDAO existingEntity =
              repositories
                  .getTableRepository()
                  .findBySchemaIdAndNameAnyType(session, schemaId, createView.getName());
          if (existingEntity != null) {
            throw new BaseException(
                ErrorCode.ALREADY_EXISTS, "View or table already exists: " + fullName);
          }

          if (createView.getViewDefinition() == null || createView.getViewDefinition().isEmpty()) {
            throw new BaseException(ErrorCode.INVALID_ARGUMENT, "No view definition provided");
          }

          String tableID = UUID.randomUUID().toString();
          TableInfo tableInfo =
              new TableInfo()
                  .name(createView.getName())
                  .catalogName(createView.getCatalogName())
                  .schemaName(createView.getSchemaName())
                  .tableType(TableType.VIEW)
                  .dataSourceFormat(DataSourceFormat.DELTA)
                  .columns(columnInfos)
                  .comment(createView.getComment())
                  .properties(createView.getProperties())
                  .owner(callerId)
                  .createdAt(createTime)
                  .createdBy(callerId)
                  .updatedAt(createTime)
                  .updatedBy(callerId)
                  .storageLocation(null) // Views don't have storage locations
                  .tableId(tableID);

          TableInfoDAO tableInfoDAO =
              repositories.getTableRepository().persistTableInfoDAO(session, tableInfo, schemaId);

          List<ViewRepresentationDAO> viewRepresentations =
              createView.getViewDefinition().stream()
                  .map(
                      vr ->
                          ViewRepresentationDAO.builder()
                              .id(UUID.randomUUID())
                              .table(tableInfoDAO)
                              .dialect(vr.getDialect())
                              .sql(vr.getSql())
                              .build())
                  .collect(Collectors.toList());

          tableInfoDAO.setViewRepresentations(viewRepresentations);
          session.merge(tableInfoDAO);

          return toViewInfo(tableInfoDAO, createView.getCatalogName(), createView.getSchemaName());
        },
        "Error creating view: " + fullName,
        /* readOnly = */ false);
  }

  /**
   * Retrieves a view by its full name.
   *
   * @param fullName the full name of the view (catalog.schema.view)
   * @return the TableInfo for the view
   */
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
          if (tableInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "View not found: " + fullName);
          }

          ViewInfo viewInfo = toViewInfo(tableInfoDAO, catalogName, schemaName);
          RepositoryUtils.attachProperties(
              viewInfo, viewInfo.getViewId(), Constants.TABLE, session);
          return viewInfo;
        },
        "Failed to get view",
        /* readOnly = */ true);
  }

  /**
   * Lists all views in a schema.
   *
   * @param catalogName the catalog name
   * @param schemaName the schema name
   * @return list of ViewInfo objects
   */
  public List<ViewInfo> listViews(String catalogName, String schemaName) {
    LOGGER.debug("Listing views in {}.{}", catalogName, schemaName);
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UUID schemaId =
              repositories
                  .getSchemaRepository()
                  .getSchemaIdOrThrow(session, catalogName, schemaName);

          // TODO[t.wiest] proper listing; Only return id + name without view definition?
          String hql =
              "FROM TableInfoDAO t WHERE t.schemaId = :schemaId AND t.type = :type ORDER BY t.name ASC";
          Query<TableInfoDAO> query = session.createQuery(hql, TableInfoDAO.class);
          query.setParameter("schemaId", schemaId);
          query.setParameter("type", TableType.VIEW.getValue());

          List<TableInfoDAO> tableInfoDAOs = query.list();
          return tableInfoDAOs.stream()
              .map(
                  tableInfoDAO -> {
                    ViewInfo viewInfo = toViewInfo(tableInfoDAO, catalogName, schemaName);
                    RepositoryUtils.attachProperties(
                        viewInfo, viewInfo.getViewId(), Constants.TABLE, session);
                    return viewInfo;
                  })
              .collect(Collectors.toList());
        },
        "Failed to list views",
        /* readOnly = */ true);
  }

  /**
   * Finds a view by catalog, schema, and view name. Delegates to TableRepository for the actual
   * query since views are stored as TableInfoDAO records.
   *
   * @param session the Hibernate session
   * @param catalogName the catalog name
   * @param schemaName the schema name
   * @param viewName the view name
   * @return the TableInfoDAO for the view, or null if not found
   */
  private TableInfoDAO findView(
      Session session, String catalogName, String schemaName, String viewName) {
    UUID schemaId =
        repositories.getSchemaRepository().getSchemaIdOrThrow(session, catalogName, schemaName);
    return repositories
        .getTableRepository()
        .findBySchemaIdAndNameWithType(session, schemaId, viewName, TableType.VIEW);
  }

  /**
   * Deletes a view by its full name. Delegates the actual deletion to TableRepository since views
   * are stored as TableInfoDAO records.
   *
   * @param fullName the full name of the view (catalog.schema.view)
   */
  public void deleteView(String fullName) {
    LOGGER.debug("Deleting view: {}", fullName);
    TransactionManager.executeWithTransaction(
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
          if (tableInfoDAO == null) {
            throw new BaseException(ErrorCode.NOT_FOUND, "View not found: " + fullName);
          }

          repositories.getTableRepository().deleteTableInfoDAO(session, tableInfoDAO);
          return null;
        },
        "Failed to delete view",
        /* readOnly = */ false);
  }

  private ViewInfo toViewInfo(TableInfoDAO tableInfoDAO, String catalogName, String schemaName) {
    ViewInfo viewInfo = new ViewInfo();
    viewInfo.setViewId(tableInfoDAO.getId().toString());
    viewInfo.setName(tableInfoDAO.getName());
    viewInfo.setCatalogName(catalogName);
    viewInfo.setSchemaName(schemaName);
    viewInfo.setColumns(ColumnInfoDAO.toList(tableInfoDAO.getColumns()));
    List<ViewRepresentationDAO> reps = tableInfoDAO.getViewRepresentations();
    viewInfo.setRepresentations(
        reps.stream()
            .map(r -> new ViewRepresentation().dialect(r.getDialect()).sql(r.getSql()))
            .collect(Collectors.toList()));
    viewInfo.setComment(tableInfoDAO.getComment());
    viewInfo.setOwner(tableInfoDAO.getOwner());
    viewInfo.setCreatedAt(
        tableInfoDAO.getCreatedAt() != null ? tableInfoDAO.getCreatedAt().getTime() : null);
    viewInfo.setCreatedBy(tableInfoDAO.getCreatedBy());
    viewInfo.setUpdatedAt(
        tableInfoDAO.getUpdatedAt() != null ? tableInfoDAO.getUpdatedAt().getTime() : null);
    viewInfo.setUpdatedBy(tableInfoDAO.getUpdatedBy());
    return viewInfo;
  }

  private String getViewFullName(CreateView createView) {
    return createView.getCatalogName()
        + "."
        + createView.getSchemaName()
        + "."
        + createView.getName();
  }
}
