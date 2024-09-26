package io.unitycatalog.server.auth.decorator;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.FUNCTION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.REGISTERED_MODEL;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.model.SecurableType.VOLUME;

import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.model.RegisteredModelInfo;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.FunctionRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.ModelRepository;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.VolumeRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class KeyMapperUtil {
  public static Map<SecurableType, Object> mapResourceKeys(
      Map<SecurableType, Object> resourceKeys) {
    Map<SecurableType, Object> resourceIds = new HashMap<>();

    if (resourceKeys.containsKey(CATALOG)
        && resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(TABLE)) {
      String fullName =
          resourceKeys.get(CATALOG)
              + "."
              + resourceKeys.get(SCHEMA)
              + "."
              + resourceKeys.get(TABLE);
      TableInfo table = TableRepository.getInstance().getTable(fullName);
      resourceIds.put(TABLE, UUID.fromString(table.getTableId()));
    }

    // If only TABLE is specified, assuming its value is a full table name (including catalog and
    // schema)
    if (!resourceKeys.containsKey(CATALOG)
        && !resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(TABLE)) {
      String fullName = (String) resourceKeys.get(TABLE);
      // If the full name contains a dot, we assume it's a full name, otherwise we assume it's an id
      TableInfo table =
          fullName.contains(".")
              ? TableRepository.getInstance().getTable(fullName)
              : TableRepository.getInstance().getTableById(fullName);
      String fullSchemaName = table.getCatalogName() + "." + table.getSchemaName();
      SchemaInfo schema = SchemaRepository.getInstance().getSchema(fullSchemaName);
      CatalogInfo catalog = CatalogRepository.getInstance().getCatalog(table.getCatalogName());
      resourceIds.put(TABLE, UUID.fromString(table.getTableId()));
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(CATALOG)
        && resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(VOLUME)) {
      String fullName =
          resourceKeys.get(CATALOG)
              + "."
              + resourceKeys.get(SCHEMA)
              + "."
              + resourceKeys.get(VOLUME);
      VolumeInfo volume = VolumeRepository.getInstance().getVolume(fullName);
      resourceIds.put(VOLUME, UUID.fromString(volume.getVolumeId()));
    }

    // If only VOLUME is specified, assuming its value is a full volume name (including catalog and
    // schema)
    if (!resourceKeys.containsKey(CATALOG)
        && !resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(VOLUME)) {
      String fullName = (String) resourceKeys.get(VOLUME);
      // If the full name contains a dot, we assume it's a full name, otherwise we assume it's an id
      VolumeInfo volume =
          (fullName.contains("."))
              ? VolumeRepository.getInstance().getVolume(fullName)
              : VolumeRepository.getInstance().getVolumeById(fullName);
      String fullSchemaName = volume.getCatalogName() + "." + volume.getSchemaName();
      SchemaInfo schema = SchemaRepository.getInstance().getSchema(fullSchemaName);
      CatalogInfo catalog = CatalogRepository.getInstance().getCatalog(volume.getCatalogName());
      resourceIds.put(VOLUME, UUID.fromString(volume.getVolumeId()));
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(CATALOG)
        && resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(FUNCTION)) {
      String fullName =
          resourceKeys.get(CATALOG)
              + "."
              + resourceKeys.get(SCHEMA)
              + "."
              + resourceKeys.get(FUNCTION);
      FunctionInfo function = FunctionRepository.getInstance().getFunction(fullName);
      resourceIds.put(FUNCTION, UUID.fromString(function.getFunctionId()));
    }

    // If only FUNCTION is specified, assuming its value is a full volume name (including catalog
    // and schema)
    if (!resourceKeys.containsKey(CATALOG)
        && !resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(FUNCTION)) {
      String fullName = (String) resourceKeys.get(FUNCTION);
      FunctionInfo function = FunctionRepository.getInstance().getFunction(fullName);
      String fullSchemaName = function.getCatalogName() + "." + function.getSchemaName();
      SchemaInfo schema = SchemaRepository.getInstance().getSchema(fullSchemaName);
      CatalogInfo catalog = CatalogRepository.getInstance().getCatalog(function.getCatalogName());
      resourceIds.put(FUNCTION, UUID.fromString(function.getFunctionId()));
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(CATALOG)
        && resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(REGISTERED_MODEL)) {
      String fullName =
          resourceKeys.get(CATALOG)
              + "."
              + resourceKeys.get(SCHEMA)
              + "."
              + resourceKeys.get(REGISTERED_MODEL);
      RegisteredModelInfo model = ModelRepository.getInstance().getRegisteredModel(fullName);
      resourceIds.put(REGISTERED_MODEL, UUID.fromString(model.getId()));
    }

    // If only REGISTERED_MODEL is specified, assuming its value is a full volume name (including
    // catalog and schema)
    if (!resourceKeys.containsKey(CATALOG)
        && !resourceKeys.containsKey(SCHEMA)
        && resourceKeys.containsKey(REGISTERED_MODEL)) {
      String fullName = (String) resourceKeys.get(REGISTERED_MODEL);
      RegisteredModelInfo model = ModelRepository.getInstance().getRegisteredModel(fullName);
      String fullSchemaName = model.getCatalogName() + "." + model.getSchemaName();
      SchemaInfo schema = SchemaRepository.getInstance().getSchema(fullSchemaName);
      CatalogInfo catalog = CatalogRepository.getInstance().getCatalog(model.getCatalogName());
      resourceIds.put(REGISTERED_MODEL, UUID.fromString(model.getId()));
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(CATALOG) && resourceKeys.containsKey(SCHEMA)) {
      String fullName = resourceKeys.get(CATALOG) + "." + resourceKeys.get(SCHEMA);
      SchemaInfo schema = SchemaRepository.getInstance().getSchema(fullName);
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
    }

    // if only SCHEMA is specified, assuming its value is a full schema name (including catalog)
    if (!resourceKeys.containsKey(CATALOG) && resourceKeys.containsKey(SCHEMA)) {
      String fullName = (String) resourceKeys.get(SCHEMA);
      SchemaInfo schema = SchemaRepository.getInstance().getSchema(fullName);
      CatalogInfo catalog = CatalogRepository.getInstance().getCatalog(schema.getCatalogName());
      resourceIds.put(SCHEMA, UUID.fromString(schema.getSchemaId()));
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(CATALOG)) {
      String fullName = (String) resourceKeys.get(CATALOG);
      CatalogInfo catalog = CatalogRepository.getInstance().getCatalog(fullName);
      resourceIds.put(CATALOG, UUID.fromString(catalog.getId()));
    }

    if (resourceKeys.containsKey(METASTORE)) {
      resourceIds.put(METASTORE, MetastoreRepository.getInstance().getMetastoreId());
    }

    return resourceIds;
  }
}
