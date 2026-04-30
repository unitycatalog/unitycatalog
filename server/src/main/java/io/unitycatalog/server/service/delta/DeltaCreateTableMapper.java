package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.delta.model.CreateTableRequest;
import io.unitycatalog.server.delta.model.StructField;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.utils.ColumnUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Converts a Delta REST Catalog {@link CreateTableRequest} (with typed Delta columns and kebab-case
 * field names) into the UC {@link CreateTable} (with UC {@link ColumnInfo}s and
 * partition-index-per-column). The server holds path params for catalog and schema; the rest comes
 * from the request body.
 *
 * <p>Required-field checks (name, location, columns, protocol, table-type, data-source-format) and
 * the DELTA-only format rule apply to all tables. Domain-metadata vs feature consistency and the
 * full UC catalog-managed contract ({@link UcManagedDeltaContract}) apply only to MANAGED tables;
 * for EXTERNAL tables UC mirrors what the client wrote with the Delta log as the source of truth.
 */
public final class DeltaCreateTableMapper {

  private DeltaCreateTableMapper() {}

  /**
   * Result of mapping a {@link CreateTableRequest}: the assembled UC {@link CreateTable} together
   * with the validated, normalized UniForm Iceberg fields ({@code Optional.empty()} when no
   * UniForm metadata was supplied). Callers thread the uniform fields straight to {@code
   * TableRepository.createTableForDelta} so the metadata-location is normalized exactly once at
   * the request boundary.
   */
  public record Result(
      CreateTable createTable,
      Optional<DeltaUniformUtils.UniformIcebergFields> uniformIcebergFields) {}

  public static Result toCreateTable(String catalog, String schema, CreateTableRequest req) {
    if (req == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Request body is required.");
    }
    if (req.getName() == null || req.getName().isBlank()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table name is required.");
    }
    if (req.getLocation() == null || req.getLocation().isBlank()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table location is required.");
    }
    if (req.getColumns() == null || req.getColumns().getFields() == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Table columns are required.");
    }

    TableType tableType = toUCTableType(req.getTableType());
    if (req.getProtocol() == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "protocol is required.");
    }

    // MANAGED-only: full UC catalog-managed contract (protocol versions + features + reader-subset
    // + domain-metadata consistency + properties). EXTERNAL tables get a pass: UC mirrors what the
    // client wrote; the Delta log is the source of truth.
    if (tableType == TableType.MANAGED) {
      UcManagedDeltaContract.validate(
          req.getProtocol(), req.getDomainMetadata(), req.getProperties());
    }

    // Uniform property/block consistency mirrors the addCommit-time check (shared via
    // DeltaUniformUtils) so a table never starts in a state the next commit would reject.
    DeltaUniformUtils.validateConsistency(req.getProperties(), req.getUniform() != null);
    Optional<DeltaUniformUtils.UniformIcebergFields> uniformFields =
        DeltaUniformUtils.getUniformFields(req.getUniform());
    DeltaUniformUtils.validateCreate(uniformFields, NormalizedURL.from(req.getLocation()));

    List<ColumnInfo> columns = new ArrayList<>();
    List<StructField> fields = req.getColumns().getFields();
    for (int i = 0; i < fields.size(); i++) {
      columns.add(ColumnUtils.toColumnInfo(fields.get(i), i));
    }
    ColumnUtils.applyPartitionColumns(columns, req.getPartitionColumns());

    CreateTable createTable =
        new CreateTable()
            .name(req.getName())
            .catalogName(catalog)
            .schemaName(schema)
            .tableType(tableType)
            .dataSourceFormat(toUCDataSourceFormat(req.getDataSourceFormat()))
            .columns(columns)
            .comment(req.getComment())
            .storageLocation(req.getLocation())
            .properties(
                DeltaPropertyMapper.mergeDerivedWithClient(
                    req.getProtocol(), req.getDomainMetadata(), req.getProperties()));
    return new Result(createTable, uniformFields);
  }

  private static TableType toUCTableType(io.unitycatalog.server.delta.model.TableType type) {
    if (type == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "table-type is required.");
    }
    return switch (type) {
      case MANAGED -> TableType.MANAGED;
      case EXTERNAL -> TableType.EXTERNAL;
    };
  }

  private static DataSourceFormat toUCDataSourceFormat(
      io.unitycatalog.server.delta.model.DataSourceFormat format) {
    if (format == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "data-source-format is required.");
    }
    // Only DELTA is accepted.
    if (format == io.unitycatalog.server.delta.model.DataSourceFormat.DELTA) {
      return DataSourceFormat.DELTA;
    }
    throw new BaseException(
        ErrorCode.INVALID_ARGUMENT, "Unsupported data-source-format: " + format.getValue());
  }
}
