package io.unitycatalog.server.persist.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.FileUtils;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.utils.ThrowingFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class TableInfoConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableInfoConverter.class);

    public static TableInfo convertFromCreateRequest(CreateTable createTable) {
        if (createTable == null) {
            return null;
        }
        return new TableInfo()
                .name(createTable.getName())
                .catalogName(createTable.getCatalogName())
                .schemaName(createTable.getSchemaName())
                .tableType(createTable.getTableType())
                .dataSourceFormat(createTable.getDataSourceFormat())
                .columns(createTable.getColumns())
                .storageLocation(createTable.getStorageLocation())
                .comment(createTable.getComment())
                .properties(createTable.getProperties());
    }


    public static TableInfo convertToDTO(TableInfoDAO dao) {
        if (dao == null) {
            return null;
        }
        return new TableInfo()
                .name(dao.getName())
                .tableType(TableType.valueOf(dao.getType()))
                .dataSourceFormat(DataSourceFormat.valueOf(dao.getDataSourceFormat()))
                .storageLocation(FileUtils.convertRelativePathToURI(dao.getUrl()))
                .comment(dao.getComment())
                .createdAt(dao.getCreatedAt() != null ? dao.getCreatedAt().getTime() : null)
                .updatedAt(dao.getUpdatedAt() != null ? dao.getUpdatedAt().getTime() : null)
                .tableId(dao.getId().toString());
    }

    public static List<ColumnInfo> convertColumnsToDTO(List<ColumnInfoDAO> daoColumns) {
        if (daoColumns == null) {
            return new ArrayList<>();
        }
        return daoColumns.stream().map(ThrowingFunction.handleException(TableInfoConverter::convertColumnToDTO,
                JsonProcessingException.class)).collect(Collectors.toList());
    }

    private static ColumnInfo convertColumnToDTO(ColumnInfoDAO dao) {
        return new ColumnInfo()
                .name(dao.getName())
                .typeText(dao.getTypeText() != null ? dao.getTypeText().toLowerCase(Locale.ROOT) : null)
                .typeJson(dao.getTypeJson())
                .typeName(ColumnTypeName.valueOf(dao.getTypeName()))
                .typePrecision(Optional.ofNullable(dao.getTypePrecision()).orElse(0))
                .typeScale(Optional.ofNullable(dao.getTypeScale()).orElse(0))
                .typeIntervalType(dao.getTypeIntervalType())
                .position((int) dao.getOrdinalPosition())
                .comment(dao.getComment())
                .nullable(dao.isNullable())
                .partitionIndex(dao.getPartitionIndex() != null ? dao.getPartitionIndex().intValue() : null);
    }

    private static Map<String, String> convertProperties(TableInfoDAO dao) {
        Map<String, String> properties = new HashMap<>();
        // Convert properties if they are stored in a serialized form
        return properties;
    }

    public static TableInfoDAO convertToDAO(TableInfo dto) {
        if (dto == null) {
            return null;
        }

        return TableInfoDAO.builder()
                .name(dto.getName())
                .comment(dto.getComment())
                .createdAt(dto.getCreatedAt() != null ? new Date(dto.getCreatedAt()) : new Date())
                .updatedAt(dto.getUpdatedAt() != null ? new Date(dto.getUpdatedAt()) : new Date())
                .columnCount(dto.getColumns() != null ? dto.getColumns().size() : 0)
                .url(dto.getStorageLocation() != null ? dto.getStorageLocation() : null)
                .type(dto.getTableType().toString())
                .dataSourceFormat(dto.getDataSourceFormat().toString())
                .build();
    }

    public static List<ColumnInfoDAO> convertColumnsListToDAO(TableInfo tableInfo, TableInfoDAO tableInfoDAO) {
        if (tableInfo.getColumns() == null) {
            return null;
        }
        return tableInfo.getColumns().stream()
                .map(TableInfoConverter::convertColumnToDAO)
                .peek(x -> x.setTableId(tableInfoDAO))
                .collect(Collectors.toList());
    }

    public static Map<String, String> convertPropertiesToMap(List<PropertyDAO> propertyDAOList) {
        if (propertyDAOList == null) {
            return null;
        }
        Map<String, String> properties = new HashMap<>();
        for (PropertyDAO propertyDAO : propertyDAOList) {
            properties.put(propertyDAO.getKey(), propertyDAO.getValue());
        }
        return properties;
    }

    public static List<PropertyDAO> convertPropertiesToDAOList(TableInfo dto, String tableId) {
        if (dto.getProperties() == null) {
            return List.of();
        }
        List<PropertyDAO> propertyDAOList = new ArrayList<>();
        for (Map.Entry<String, String> entry : dto.getProperties().entrySet()) {
            propertyDAOList.add(PropertyDAO.builder()
                    .key(entry.getKey())
                    .value(entry.getValue())
                    .entityId(UUID.fromString(tableId))
                    .entityType("table")
                    .build());
        }
        return propertyDAOList;
    }

    private static ColumnInfoDAO convertColumnToDAO(ColumnInfo column) {
        if (column == null) {
            return null;
        }
        return ColumnInfoDAO.builder()
                .name(column.getName())
                .typeText(column.getTypeText())
                .typeJson(column.getTypeJson())
                .typeName(column.getTypeName().toString())
                .typePrecision(column.getTypePrecision())
                .typeScale(column.getTypeScale())
                .typeIntervalType(column.getTypeIntervalType())
                .ordinalPosition(column.getPosition().shortValue())
                .comment(column.getComment())
                .nullable(Optional.ofNullable(column.getNullable()).orElse(false))
                .partitionIndex(column.getPartitionIndex() != null ? column.getPartitionIndex().shortValue() : null)
                .build();
    }


}
