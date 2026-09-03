package io.unitycatalog.server.sharing;

import io.opensharing.asset.storage.StoragePaths;
import io.opensharing.catalog.AccessMode;
import io.opensharing.catalog.AssetAccessDeniedException;
import io.opensharing.catalog.AssetLookup;
import io.opensharing.catalog.AssetNotFoundException;
import io.opensharing.catalog.AssetType;
import io.opensharing.catalog.CatalogCaller;
import io.opensharing.catalog.CatalogConnector;
import io.opensharing.catalog.CatalogException;
import io.opensharing.catalog.CloudProvider;
import io.opensharing.catalog.CredentialRequest;
import io.opensharing.catalog.ResolvedAsset;
import io.opensharing.catalog.StorageCredentialKeys;
import io.opensharing.catalog.StorageCredentials;
import io.opensharing.catalog.TableFormat;
import io.opensharing.catalog.UnsupportedAssetTypeException;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-process {@link CatalogConnector} for OpenSharing embedded in Unity Catalog OSS.
 *
 * <p>Replaces the HTTP {@code UnityCatalogConnector} with direct calls to UC repositories and
 * {@link StorageCredentialVendor}.
 */
public final class UnityCatalogEmbeddedConnector implements CatalogConnector {

  public static final String NAME = "unity";

  private static final Logger LOGGER = LoggerFactory.getLogger(UnityCatalogEmbeddedConnector.class);
  private static final int PAGE_SIZE = 50;
  private static final int MAX_PAGES = 200;
  private static final Set<String> VENDABLE_SCHEMES =
      Set.of("s3", "s3a", "s3n", "abfs", "abfss", "wasb", "wasbs", "gs");

  private final TableRepository tableRepository;
  private final Repositories repositories;
  private final StorageCredentialVendor storageCredentialVendor;

  public UnityCatalogEmbeddedConnector(Repositories repositories) {
    this.repositories = repositories;
    this.tableRepository = repositories.getTableRepository();
    this.storageCredentialVendor = repositories.getStorageCredentialVendor();
    LOGGER.info("Using in-process Unity Catalog connector for OpenSharing");
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public ResolvedAsset resolveAsset(AssetLookup lookup, CatalogCaller caller) {
    return switch (lookup.type()) {
      case TABLE -> table(lookup, caller);
      case SCHEMA -> schema(lookup, caller);
      case VOLUME, MODEL, SKILL ->
          throw new UnsupportedAssetTypeException(
              "the "
                  + NAME
                  + " catalog connector resolves tables and schemas, not a "
                  + lookup.type());
    };
  }

  private ResolvedAsset table(AssetLookup lookup, CatalogCaller caller) {
    String fullName = qualifiedName(lookup, 3, "catalog.schema.table");
    TableInfo info = ask(lookup, caller, () -> tableRepository.getTable(fullName));
    String refused = unshareable(info);
    if (refused != null) {
      throw new UnsupportedAssetTypeException("'" + lookup.identifier() + "' " + refused);
    }
    return resolved(fullName, info);
  }

  private ResolvedAsset schema(AssetLookup lookup, CatalogCaller caller) {
    String fullName = qualifiedName(lookup, 2, "catalog.schema");
    SchemaInfo info =
        ask(lookup, caller, () -> repositories.getSchemaRepository().getSchema(fullName));
    return ResolvedAsset.builder(AssetType.SCHEMA, lookup.identifier())
        .catalogAssetId(info.getSchemaId())
        .build();
  }

  @Override
  public List<ResolvedAsset> listChildren(AssetLookup parent, CatalogCaller caller) {
    if (parent.type() != AssetType.SCHEMA) {
      throw new UnsupportedAssetTypeException(
          "the " + NAME + " catalog only lists the contents of a SCHEMA, not a " + parent.type());
    }
    String[] parts = qualifiedName(parent, 2, "catalog.schema").split("\\.");
    String catalog = parts[0];
    String schema = parts[1];
    List<ResolvedAsset> tables = new ArrayList<>();
    String pageToken = null;
    for (int page = 0; page < MAX_PAGES; page++) {
      String token = pageToken;
      ListTablesResponse response =
          ask(
              parent,
              caller,
              () ->
                  tableRepository.listTables(
                      catalog,
                      schema,
                      Optional.of(PAGE_SIZE),
                      Optional.ofNullable(token),
                      true,
                      true));
      collect(parent, response, tables);
      pageToken = response.getNextPageToken();
      if (pageToken == null || pageToken.isBlank() || pageToken.equals(token)) {
        return tables;
      }
    }
    throw new CatalogException(
        "'"
            + parent.identifier()
            + "' holds more tables than this server will list for one shared schema; share its "
            + "tables individually");
  }

  @Override
  public List<StorageCredentials> getStorageCredentials(
      CredentialRequest request, CatalogCaller caller) {
    if (request.assetType() != AssetType.TABLE) {
      throw new UnsupportedAssetTypeException(
          "the "
              + NAME
              + " catalog connector vends credentials for a TABLE, not a "
              + request.assetType());
    }
    if (isBlank(request.storageLocation())) {
      throw new CatalogException(
          "asset '" + request.identifier() + "' has no storage location to scope credentials to");
    }
    if (isBlank(request.catalogAssetId())) {
      throw new CatalogException(
          "Unity Catalog mints credentials for a table id, and none is recorded for '"
              + request.identifier()
              + "'");
    }
    AssetLookup lookup = AssetLookup.of(request.assetType(), request.identifier());
    TemporaryCredentials minted =
        ask(
            lookup,
            caller,
            () -> {
              var info =
                  tableRepository.getStorageLocationForTableOrStagingTable(
                      UUID.fromString(request.catalogAssetId()));
              return storageCredentialVendor.vendCredential(
                  info.url(), Set.of(CredentialContext.Privilege.SELECT));
            });
    StorageCredentials vended = toStorageCredentials(request, minted);
    return vended == null ? List.of() : List.of(vended);
  }

  private void collect(AssetLookup parent, ListTablesResponse page, List<ResolvedAsset> into) {
    if (page.getTables() == null) {
      return;
    }
    Set<String> listed =
        into.stream().map(ResolvedAsset::identifier).collect(java.util.stream.Collectors.toSet());
    for (TableInfo info : page.getTables()) {
      String identifier = fullName(info);
      String refused =
          identifier == null
              ? "is named only in part by the catalog"
              : listed.contains(identifier)
                  ? "was listed by an earlier page already"
                  : unshareable(info);
      if (refused != null) {
        LOGGER.debug(
            "Leaving '{}' out of the tables of '{}': it {}",
            identifier == null ? info.getName() : identifier,
            parent.identifier(),
            refused);
        continue;
      }
      into.add(resolved(identifier, info));
    }
  }

  private static String unshareable(TableInfo info) {
    if (isBlank(info.getStorageLocation())) {
      String type =
          info.getTableType() == null ? "a table type" : "a " + info.getTableType().getValue();
      return "is "
          + type
          + " with no storage location in Unity Catalog,"
          + " so there is nothing to point a recipient at";
    }
    if (format(info.getDataSourceFormat()) == null) {
      String stated =
          info.getDataSourceFormat() == null
              ? "of no stated format"
              : info.getDataSourceFormat().getValue();
      return "is " + stated + " in Unity Catalog, and this server shares Delta and Parquet tables";
    }
    return null;
  }

  private static ResolvedAsset resolved(String identifier, TableInfo info) {
    return ResolvedAsset.builder(AssetType.TABLE, identifier)
        .catalogAssetId(info.getTableId())
        .storageLocation(info.getStorageLocation())
        .format(format(info.getDataSourceFormat()))
        .partitionColumns(partitionColumns(info))
        .subtype(info.getTableType() == null ? null : info.getTableType().getValue())
        .accessModes(directoryAccess(info))
        .build();
  }

  private static Set<AccessMode> directoryAccess(TableInfo info) {
    return VENDABLE_SCHEMES.contains(schemeOf(info.getStorageLocation()))
        ? Set.of(AccessMode.DIR)
        : Set.of();
  }

  private static String schemeOf(String location) {
    if (isBlank(location)) {
      return "";
    }
    int end = location.indexOf(':');
    return end < 0 ? "" : location.substring(0, end).toLowerCase(Locale.ROOT);
  }

  private static TableFormat format(DataSourceFormat dataSourceFormat) {
    if (dataSourceFormat == null) {
      return null;
    }
    return switch (dataSourceFormat) {
      case DELTA -> TableFormat.DELTA;
      case PARQUET -> TableFormat.PARQUET;
      default -> null;
    };
  }

  private static List<String> partitionColumns(TableInfo info) {
    if (info.getColumns() == null) {
      return List.of();
    }
    return info.getColumns().stream()
        .filter(column -> column.getPartitionIndex() != null && column.getPartitionIndex() >= 0)
        .sorted(Comparator.comparingInt(ColumnInfo::getPartitionIndex))
        .map(ColumnInfo::getName)
        .toList();
  }

  private static String fullName(TableInfo info) {
    if (info.getCatalogName() == null || info.getSchemaName() == null || info.getName() == null) {
      return null;
    }
    return info.getCatalogName() + "." + info.getSchemaName() + "." + info.getName();
  }

  private static StorageCredentials toStorageCredentials(
      CredentialRequest request, TemporaryCredentials minted) {
    Instant expiration =
        minted.getExpirationTime() == null || minted.getExpirationTime() == 0L
            ? null
            : Instant.ofEpochMilli(minted.getExpirationTime());
    if (minted.getAwsTempCredentials() != null) {
      var aws = minted.getAwsTempCredentials();
      return stated(
          request,
          new StorageCredentials(
              prefix(request, minted),
              CloudProvider.AWS,
              values(
                  StorageCredentialKeys.ACCESS_KEY_ID, aws.getAccessKeyId(),
                  StorageCredentialKeys.SECRET_ACCESS_KEY, aws.getSecretAccessKey(),
                  StorageCredentialKeys.SESSION_TOKEN, aws.getSessionToken()),
              expiration));
    }
    if (minted.getAzureUserDelegationSas() != null) {
      return stated(
          request,
          new StorageCredentials(
              prefix(request, minted),
              CloudProvider.AZURE,
              values(
                  StorageCredentialKeys.SAS_TOKEN,
                  minted.getAzureUserDelegationSas().getSasToken()),
              expiration));
    }
    if (minted.getGcpOauthToken() != null) {
      return stated(
          request,
          new StorageCredentials(
              prefix(request, minted),
              CloudProvider.GCP,
              values(StorageCredentialKeys.OAUTH_TOKEN, minted.getGcpOauthToken().getOauthToken()),
              expiration));
    }
    if (StoragePaths.isLocal(request.storageLocation())) {
      return null;
    }
    LOGGER.error(
        "Unity Catalog vended no credentials for '{}' on {}",
        request.identifier(),
        request.storageLocation());
    throw new CatalogException("Unity Catalog vended no credentials for this table");
  }

  private static StorageCredentials stated(
      CredentialRequest request, StorageCredentials credentials) {
    if (credentials.credentials().isEmpty()) {
      throw new CatalogException("Unity Catalog minted a credential with nothing in it");
    }
    return credentials;
  }

  private static String prefix(CredentialRequest request, TemporaryCredentials minted) {
    String url = minted.getUrl();
    return !isBlank(url) && request.storageLocation().startsWith(url)
        ? url
        : request.storageLocation();
  }

  private static Map<String, String> values(String... keysAndValues) {
    Map<String, String> result = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      if (!isBlank(keysAndValues[i + 1])) {
        result.put(keysAndValues[i], keysAndValues[i + 1]);
      }
    }
    return result;
  }

  private static String qualifiedName(AssetLookup lookup, int parts, String shape) {
    String[] segments = lookup.identifier().split("\\.");
    if (segments.length != parts) {
      throw new CatalogException("expected " + shape + " but got '" + lookup.identifier() + "'");
    }
    return lookup.identifier();
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private <T> T ask(AssetLookup lookup, CatalogCaller caller, Supplier<T> request) {
    try {
      return request.get();
    } catch (BaseException e) {
      throw mapException(lookup, caller, e);
    }
  }

  private static CatalogException mapException(
      AssetLookup lookup, CatalogCaller caller, BaseException e) {
    ErrorCode code = e.getErrorCode();
    if (code == ErrorCode.TABLE_NOT_FOUND
        || code == ErrorCode.SCHEMA_NOT_FOUND
        || code == ErrorCode.CATALOG_NOT_FOUND) {
      return new AssetNotFoundException(lookup);
    }
    if (code == ErrorCode.PERMISSION_DENIED || code == ErrorCode.UNAUTHENTICATED) {
      return new AssetAccessDeniedException(lookup, caller);
    }
    return new CatalogException(e.getErrorMessage(), e);
  }
}
