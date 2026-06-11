package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.delta.model.DeltaCreateTableRequest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.utils.ColumnUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Converts a {@link DeltaCreateTableRequest} (with typed Delta columns and kebab-case field names)
 * into the UC {@link CreateTable} (with UC {@link ColumnInfo}s and partition-index-per-column). The
 * server holds path params for catalog and schema; the rest comes from the request body.
 *
 * <p>Required-field checks (name, location, columns, protocol, table-type) apply to all tables. The
 * full UC catalog-managed contract ({@link UcManagedDeltaContract}) applies only to MANAGED tables
 * -- including MANAGED_SHALLOW_CLONE, which is stored as a MANAGED table carrying a base-table-id
 * (see {@link DeltaTableTypes}); EXTERNAL tables skip contract validation but still go through the
 * same {@link DeltaPropertyMapper} projection, so derived {@code delta.feature.*} and {@code
 * clusteringColumns} entries override any client-supplied values under those keys.
 */
public final class DeltaCreateTableMapper {

  private DeltaCreateTableMapper() {}

  /**
   * Result of mapping a {@link DeltaCreateTableRequest}: the assembled UC {@link CreateTable}
   * together with the validated, normalized UniForm Iceberg fields ({@code Optional.empty()} when
   * no UniForm metadata was supplied) and the base table UUID for shallow clones ({@code
   * Optional.empty()} for non-clone tables). Callers thread both straight to {@code
   * TableRepository.createTableForDelta} so the metadata-location is normalized exactly once at the
   * request boundary and the base table is resolved inside the create transaction.
   */
  public record Result(
      CreateTable createTable,
      Optional<DeltaUniformUtils.UniformIcebergFields> uniformIcebergFields,
      Optional<UUID> baseTableId) {}

  public static Result toCreateTable(
      String catalog,
      String schema,
      DeltaCreateTableRequest req,
      ServerProperties serverProperties) {
    if (req == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Request body is required.");
    }
    if (req.getName() == null || req.getName().isBlank()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table name is required.");
    }
    if (req.getLocation() == null || req.getLocation().isBlank()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table location is required.");
    }
    ColumnUtils.validateStructType(req.getColumns(), "columns");

    if (req.getTableType() == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "table-type is required.");
    }
    TableType tableType = DeltaTableTypes.toStoredTableType(req.getTableType());
    Optional<UUID> baseTableId = validateBaseTableId(req);
    if (req.getProtocol() == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "protocol is required.");
    }
    if (req.getLastCommitTimestampMs() == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "last-commit-timestamp-ms is required.");
    }

    // MANAGED-only: full UC catalog-managed contract (protocol versions + features + reader-subset
    // + domain-metadata consistency + properties). EXTERNAL tables get a pass: UC mirrors what the
    // client wrote; the Delta log is the source of truth.
    if (tableType == TableType.MANAGED) {
      UcManagedDeltaContract.validate(
          req.getProtocol(), req.getDomainMetadata(), req.getProperties(), serverProperties);
    }

    // Uniform property/block consistency mirrors the addCommit-time check (shared via
    // DeltaUniformUtils) so a table never starts in a state the next commit would reject.
    DeltaUniformUtils.validateConsistency(req.getProperties(), req.getUniform() != null);
    Optional<DeltaUniformUtils.UniformIcebergFields> uniformFields =
        DeltaUniformUtils.getUniformFields(req.getUniform());
    DeltaUniformUtils.validateCreate(uniformFields, NormalizedURL.from(req.getLocation()));

    List<ColumnInfo> columns = ColumnUtils.toColumnInfos(req.getColumns().getFields());
    ColumnUtils.applyPartitionColumns(columns, req.getPartitionColumns());

    CreateTable createTable =
        new CreateTable()
            .name(req.getName())
            .catalogName(catalog)
            .schemaName(schema)
            .tableType(tableType)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .columns(columns)
            .comment(req.getComment())
            .storageLocation(req.getLocation())
            .properties(DeltaPropertyMapper.buildStoredProperties(req));
    return new Result(createTable, uniformFields, baseTableId);
  }

  /**
   * base-table-id must only be present for shallow-clone types; all other types must not include it
   * or the table will be stored as a shallow clone.
   */
  private static Optional<UUID> validateBaseTableId(DeltaCreateTableRequest req) {
    if (DeltaTableTypes.isShallowClone(req.getTableType())) {
      if (req.getBaseTableId() == null) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "base-table-id is required for table-type " + req.getTableType() + ".");
      }
      return Optional.of(req.getBaseTableId());
    }
    if (req.getBaseTableId() != null) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "base-table-id must not be set for table-type " + req.getTableType() + ".");
    }
    return Optional.empty();
  }
}
