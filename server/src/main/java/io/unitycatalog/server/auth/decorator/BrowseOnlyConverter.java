package io.unitycatalog.server.auth.decorator;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ExternalLocationInfo;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.model.RegisteredModelInfo;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.VolumeInfo;

/** Explicit field allowlists for native API discovery responses. */
public final class BrowseOnlyConverter {
  private BrowseOnlyConverter() {}

  /** Returns catalog discovery fields, including managed storage paths. */
  public static CatalogInfo toBrowseOnly(CatalogInfo info) {
    return new BrowseCatalogInfo()
        .name(info.getName())
        .comment(info.getComment())
        .properties(info.getProperties())
        .owner(info.getOwner())
        .createdAt(info.getCreatedAt())
        .createdBy(info.getCreatedBy())
        .updatedAt(info.getUpdatedAt())
        .updatedBy(info.getUpdatedBy())
        .id(info.getId())
        .storageRoot(info.getStorageRoot())
        .storageLocation(info.getStorageLocation())
        .browseOnly(true);
  }

  /** Returns schema discovery fields, including managed storage paths. */
  public static SchemaInfo toBrowseOnly(SchemaInfo info) {
    return new BrowseSchemaInfo()
        .name(info.getName())
        .catalogName(info.getCatalogName())
        .comment(info.getComment())
        .properties(info.getProperties())
        .fullName(info.getFullName())
        .owner(info.getOwner())
        .createdAt(info.getCreatedAt())
        .createdBy(info.getCreatedBy())
        .updatedAt(info.getUpdatedAt())
        .updatedBy(info.getUpdatedBy())
        .schemaId(info.getSchemaId())
        .storageRoot(info.getStorageRoot())
        .storageLocation(info.getStorageLocation())
        .browseOnly(true);
  }

  /** Returns table discovery fields without storage details, properties, or view definitions. */
  public static TableInfo toBrowseOnly(TableInfo info) {
    return new BrowseTableInfo()
        .name(info.getName())
        .catalogName(info.getCatalogName())
        .schemaName(info.getSchemaName())
        .tableType(info.getTableType())
        .dataSourceFormat(info.getDataSourceFormat())
        .columns(
            info.getColumns() == null
                ? null
                : info.getColumns().stream().map(BrowseOnlyConverter::toBrowseColumn).toList())
        .comment(info.getComment())
        .owner(info.getOwner())
        .createdAt(info.getCreatedAt())
        .createdBy(info.getCreatedBy())
        .updatedAt(info.getUpdatedAt())
        .updatedBy(info.getUpdatedBy())
        .tableId(info.getTableId())
        .properties(null)
        .browseOnly(true);
  }

  /** Returns function discovery fields without the function body. */
  public static FunctionInfo toBrowseOnly(FunctionInfo info) {
    return new BrowseFunctionInfo()
        .name(info.getName())
        .catalogName(info.getCatalogName())
        .schemaName(info.getSchemaName())
        .inputParams(info.getInputParams())
        .dataType(info.getDataType())
        .fullDataType(info.getFullDataType())
        .returnParams(info.getReturnParams())
        .routineBody(info.getRoutineBody())
        .routineDependencies(info.getRoutineDependencies())
        .parameterStyle(info.getParameterStyle())
        .isDeterministic(info.getIsDeterministic())
        .sqlDataAccess(info.getSqlDataAccess())
        .isNullCall(info.getIsNullCall())
        .securityType(info.getSecurityType())
        .specificName(info.getSpecificName())
        .comment(info.getComment())
        .properties(info.getProperties())
        .fullName(info.getFullName())
        .owner(info.getOwner())
        .createdAt(info.getCreatedAt())
        .createdBy(info.getCreatedBy())
        .updatedAt(info.getUpdatedAt())
        .updatedBy(info.getUpdatedBy())
        .functionId(info.getFunctionId())
        .externalLanguage(info.getExternalLanguage())
        .browseOnly(true);
  }

  /** Returns model discovery fields, including its storage path. */
  public static RegisteredModelInfo toBrowseOnly(RegisteredModelInfo info) {
    return new BrowseRegisteredModelInfo()
        .name(info.getName())
        .catalogName(info.getCatalogName())
        .schemaName(info.getSchemaName())
        .storageLocation(info.getStorageLocation())
        .fullName(info.getFullName())
        .comment(info.getComment())
        .owner(info.getOwner())
        .createdAt(info.getCreatedAt())
        .createdBy(info.getCreatedBy())
        .updatedAt(info.getUpdatedAt())
        .updatedBy(info.getUpdatedBy())
        .id(info.getId())
        .browseOnly(true);
  }

  /** Returns volume discovery fields, including its storage path. */
  public static VolumeInfo toBrowseOnly(VolumeInfo info) {
    return new BrowseVolumeInfo()
        .catalogName(info.getCatalogName())
        .schemaName(info.getSchemaName())
        .name(info.getName())
        .comment(info.getComment())
        .owner(info.getOwner())
        .createdAt(info.getCreatedAt())
        .createdBy(info.getCreatedBy())
        .updatedAt(info.getUpdatedAt())
        .updatedBy(info.getUpdatedBy())
        .volumeId(info.getVolumeId())
        .volumeType(info.getVolumeType())
        .storageLocation(info.getStorageLocation())
        .fullName(info.getFullName())
        .browseOnly(true);
  }

  /** Returns external location discovery fields without credential references. */
  public static ExternalLocationInfo toBrowseOnly(ExternalLocationInfo info) {
    return new BrowseExternalLocationInfo()
        .name(info.getName())
        .id(info.getId())
        .url(info.getUrl())
        .comment(info.getComment())
        .owner(info.getOwner())
        .createdAt(info.getCreatedAt())
        .createdBy(info.getCreatedBy())
        .updatedAt(info.getUpdatedAt())
        .updatedBy(info.getUpdatedBy())
        .browseOnly(true);
  }

  private static ColumnInfo toBrowseColumn(ColumnInfo info) {
    return new ColumnInfo()
        .name(info.getName())
        .typeText(info.getTypeText())
        .typeJson(info.getTypeJson())
        .typeName(info.getTypeName())
        .typePrecision(info.getTypePrecision())
        .typeScale(info.getTypeScale())
        .typeIntervalType(info.getTypeIntervalType())
        .position(info.getPosition())
        .comment(info.getComment())
        .nullable(info.getNullable())
        .partitionIndex(info.getPartitionIndex());
  }

  // Discovery objects omit unset fields so excluded fields do not appear as null.
  @JsonInclude(NON_NULL)
  private static final class BrowseCatalogInfo extends CatalogInfo {}

  @JsonInclude(NON_NULL)
  private static final class BrowseSchemaInfo extends SchemaInfo {}

  @JsonInclude(NON_NULL)
  private static final class BrowseTableInfo extends TableInfo {}

  @JsonInclude(NON_NULL)
  private static final class BrowseFunctionInfo extends FunctionInfo {}

  @JsonInclude(NON_NULL)
  private static final class BrowseRegisteredModelInfo extends RegisteredModelInfo {}

  @JsonInclude(NON_NULL)
  private static final class BrowseVolumeInfo extends VolumeInfo {}

  @JsonInclude(NON_NULL)
  private static final class BrowseExternalLocationInfo extends ExternalLocationInfo {}
}
