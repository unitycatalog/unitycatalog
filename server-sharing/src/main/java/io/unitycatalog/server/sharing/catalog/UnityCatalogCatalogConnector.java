package io.unitycatalog.server.sharing.catalog;

import static io.unitycatalog.server.auth.AuthorizeExpressions.GET_SCHEMA;
import static io.unitycatalog.server.auth.AuthorizeExpressions.GET_TABLE;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;

import io.opensharing.catalog.AccessMode;
import io.opensharing.catalog.AssetAccessDeniedException;
import io.opensharing.catalog.AssetLookup;
import io.opensharing.catalog.AssetNotFoundException;
import io.opensharing.catalog.AssetType;
import io.opensharing.catalog.CatalogAuthorizationException;
import io.opensharing.catalog.CatalogCaller;
import io.opensharing.catalog.CatalogConnector;
import io.opensharing.catalog.CatalogException;
import io.opensharing.catalog.CatalogPrincipal;
import io.opensharing.catalog.CloudProvider;
import io.opensharing.catalog.CredentialRequest;
import io.opensharing.catalog.ResolvedAsset;
import io.opensharing.catalog.StorageCredentialKeys;
import io.opensharing.catalog.StorageCredentials;
import io.opensharing.catalog.TableFormat;
import io.opensharing.catalog.UnsupportedAssetTypeException;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.security.UnityCatalogIdentityService;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** In-process OpenSharing connector owned and hosted by Unity Catalog. */
public final class UnityCatalogCatalogConnector implements CatalogConnector {

  public static final String NAME = "unity";
  private static final Logger LOGGER =
      LoggerFactory.getLogger(UnityCatalogCatalogConnector.class);
  private static final int PAGE_SIZE = 100;
  private static final int MAX_PAGES = 10_000;
  private static final Set<String> VENDABLE_SCHEMES =
      Set.of("s3", "s3a", "s3n", "abfs", "abfss", "wasb", "wasbs", "gs");

  private final TableRepository tables;
  private final SchemaRepository schemas;
  private final MetastoreRepository metastore;
  private final StorageCredentialVendor credentialVendor;
  private final UnityCatalogIdentityService identities;
  private final UnityCatalogAuthorizer authorizer;
  private final KeyMapper keyMapper;
  private final UnityAccessEvaluator evaluator;

  public UnityCatalogCatalogConnector(
      Repositories repositories,
      UnityCatalogAuthorizer authorizer,
      UnityCatalogIdentityService identities,
      StorageCredentialVendor credentialVendor) {
    this.tables = repositories.getTableRepository();
    this.schemas = repositories.getSchemaRepository();
    this.metastore = repositories.getMetastoreRepository();
    this.credentialVendor = credentialVendor;
    this.identities = identities;
    this.authorizer = authorizer;
    this.keyMapper = new KeyMapper(repositories);
    try {
      this.evaluator = new UnityAccessEvaluator(authorizer);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("failed to initialize Unity Catalog authorization", e);
    }
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public CatalogPrincipal authorize(String bearerToken, String privilege) {
    try {
      UnityCatalogIdentityService.Identity identity = identities.authenticate(bearerToken);
      if (privilege != null && !privilege.isBlank()) {
        Privileges requested;
        try {
          requested = Privileges.valueOf(privilege);
        } catch (IllegalArgumentException e) {
          throw new CatalogAuthorizationException("unknown Unity Catalog privilege: " + privilege);
        }
        if (!authorizer.authorizeAny(
            identity.id(), metastore.getMetastoreId(), Privileges.OWNER, requested)) {
          throw new CatalogAuthorizationException(
              "'" + identity.name() + "' does not have " + privilege);
        }
      }
      return new CatalogPrincipal(identity.id().toString(), identity.name());
    } catch (CatalogAuthorizationException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new CatalogAuthorizationException("Unity Catalog rejected the bearer token");
    }
  }

  @Override
  public ResolvedAsset resolveAsset(AssetLookup lookup, CatalogCaller caller) {
    return switch (lookup.type()) {
      case TABLE -> resolveTable(lookup, caller);
      case SCHEMA -> resolveSchema(lookup, caller);
      default ->
          throw new UnsupportedAssetTypeException(
              "the Unity Catalog connector cannot resolve a " + lookup.type());
    };
  }

  private ResolvedAsset resolveTable(AssetLookup lookup, CatalogCaller caller) {
    qualifiedName(lookup, 3, "catalog.schema.table");
    try {
      TableInfo table = tables.getTable(lookup.identifier());
      requireAllowed(lookup, caller, GET_TABLE, TABLE, lookup.identifier());
      String refused = unshareable(table);
      if (refused != null) {
        throw new UnsupportedAssetTypeException("'" + lookup.identifier() + "' " + refused);
      }
      return resolved(table);
    } catch (RuntimeException e) {
      throw mapFailure(lookup, caller, e);
    }
  }

  private ResolvedAsset resolveSchema(AssetLookup lookup, CatalogCaller caller) {
    qualifiedName(lookup, 2, "catalog.schema");
    try {
      SchemaInfo schema = schemas.getSchema(lookup.identifier());
      requireAllowed(lookup, caller, GET_SCHEMA, SCHEMA, lookup.identifier());
      return ResolvedAsset.builder(AssetType.SCHEMA, lookup.identifier())
          .catalogAssetId(schema.getSchemaId())
          .build();
    } catch (RuntimeException e) {
      throw mapFailure(lookup, caller, e);
    }
  }

  @Override
  public List<ResolvedAsset> listChildren(AssetLookup parent, CatalogCaller caller) {
    if (parent.type() != AssetType.SCHEMA) {
      throw new UnsupportedAssetTypeException(
          "the Unity Catalog connector only lists a SCHEMA, not a " + parent.type());
    }
    String[] name = qualifiedName(parent, 2, "catalog.schema").split("\\.");
    resolveSchema(parent, caller);

    List<ResolvedAsset> result = new ArrayList<>();
    Set<String> listed = new java.util.HashSet<>();
    String pageToken = null;
    for (int page = 0; page < MAX_PAGES; page++) {
      String previous = pageToken;
      ListTablesResponse response;
      try {
        response =
            tables.listTables(
                name[0],
                name[1],
                Optional.of(PAGE_SIZE),
                Optional.ofNullable(pageToken),
                false,
                false);
      } catch (RuntimeException e) {
        throw mapFailure(parent, caller, e);
      }
      if (response.getTables() != null) {
        for (TableInfo table : response.getTables()) {
          String fullName =
              table.getCatalogName() + "." + table.getSchemaName() + "." + table.getName();
          if (!listed.add(fullName)
              || unshareable(table) != null
              || !allowed(caller, GET_TABLE, TABLE, fullName)) {
            LOGGER.debug("Leaving unshareable table '{}' out of schema '{}'", fullName, parent);
            continue;
          }
          result.add(resolved(table));
        }
      }
      pageToken = response.getNextPageToken();
      if (pageToken == null || pageToken.isBlank() || pageToken.equals(previous)) {
        return result;
      }
    }
    throw new CatalogException(
        "'" + parent.identifier() + "' holds too many tables to list in one shared schema");
  }

  @Override
  public List<StorageCredentials> getStorageCredentials(
      CredentialRequest request, CatalogCaller caller) {
    if (request.assetType() != AssetType.TABLE) {
      throw new UnsupportedAssetTypeException(
          "the Unity Catalog connector vends credentials for TABLE, not " + request.assetType());
    }
    AssetLookup lookup = AssetLookup.of(AssetType.TABLE, request.identifier());
    try {
      TableInfo table = tables.getTable(request.identifier());
      requireAllowed(lookup, caller, GET_TABLE, TABLE, request.identifier());
      if (request.catalogAssetId() != null
          && !request.catalogAssetId().equals(table.getTableId())) {
        throw new CatalogException(
            "the recorded table id for '" + request.identifier() + "' is stale");
      }
      NormalizedURL tableLocation = NormalizedURL.from(table.getStorageLocation());
      NormalizedURL requestedLocation = NormalizedURL.from(request.storageLocation());
      if (!tableLocation.equals(requestedLocation)) {
        throw new CatalogException(
            "storage location is not the current location of '" + request.identifier() + "'");
      }
      TemporaryCredentials minted =
          credentialVendor.vendCredential(tableLocation, CredentialContext.READ_ONLY);
      if (hasNoCredential(minted)) {
        if ("file".equals(tableLocation.toUri().getScheme())) {
          return List.of();
        }
        throw new CatalogException("Unity Catalog vended no credentials for this table");
      }
      return List.of(credentials(request, minted));
    } catch (RuntimeException e) {
      throw mapFailure(lookup, caller, e);
    }
  }

  private void requireAllowed(
      AssetLookup lookup,
      CatalogCaller caller,
      String expression,
      io.unitycatalog.server.model.SecurableType type,
      String resource) {
    if (!allowed(caller, expression, type, resource)) {
      throw new AssetAccessDeniedException(lookup, caller);
    }
  }

  private boolean allowed(
      CatalogCaller caller,
      String expression,
      io.unitycatalog.server.model.SecurableType type,
      String resource) {
    UnityCatalogIdentityService.Identity identity = identity(caller);
    Map<io.unitycatalog.server.model.SecurableType, UUID> ids =
        new HashMap<>(keyMapper.mapResourceKeys(Map.of(type, resource)));
    ids.put(METASTORE, metastore.getMetastoreId());
    return evaluator.evaluate(identity.id(), expression, ids, Map.of());
  }

  private UnityCatalogIdentityService.Identity identity(CatalogCaller caller) {
    return switch (caller.credential()) {
      case CatalogCaller.Credential.BearerToken token -> identities.authenticate(token.token());
      case CatalogCaller.Credential.OnBehalfOf onBehalfOf ->
          identities.onBehalfOf(onBehalfOf.catalogUserId(), caller.name());
      case CatalogCaller.Credential.None ignored ->
          throw new CatalogAuthorizationException(
              "Unity Catalog requires a bearer or trusted on-behalf-of identity");
    };
  }

  private static ResolvedAsset resolved(TableInfo table) {
    String identifier =
        table.getCatalogName() + "." + table.getSchemaName() + "." + table.getName();
    return ResolvedAsset.builder(AssetType.TABLE, identifier)
        .catalogAssetId(table.getTableId())
        .storageLocation(table.getStorageLocation())
        .format(format(table.getDataSourceFormat()))
        .partitionColumns(partitionColumns(table))
        .subtype(table.getTableType() == null ? null : table.getTableType().getValue())
        .accessModes(directoryAccess(table.getStorageLocation()))
        .build();
  }

  private static String unshareable(TableInfo table) {
    if (table.getStorageLocation() == null || table.getStorageLocation().isBlank()) {
      return "has no storage location";
    }
    if (format(table.getDataSourceFormat()) == null) {
      return "has unsupported format " + table.getDataSourceFormat();
    }
    return null;
  }

  private static TableFormat format(DataSourceFormat format) {
    if (format == null) {
      return null;
    }
    return switch (format) {
      case DELTA -> TableFormat.DELTA;
      case PARQUET -> TableFormat.PARQUET;
      default -> null;
    };
  }

  private static List<String> partitionColumns(TableInfo table) {
    if (table.getColumns() == null) {
      return List.of();
    }
    return table.getColumns().stream()
        .filter(column -> column.getPartitionIndex() != null && column.getPartitionIndex() >= 0)
        .sorted(Comparator.comparingInt(ColumnInfo::getPartitionIndex))
        .map(ColumnInfo::getName)
        .toList();
  }

  private static Set<AccessMode> directoryAccess(String location) {
    return VENDABLE_SCHEMES.contains(scheme(location))
        ? Set.of(AccessMode.DIR)
        : Set.of();
  }

  private static String scheme(String location) {
    if (location == null) {
      return "";
    }
    int colon = location.indexOf(':');
    return colon < 0 ? "" : location.substring(0, colon).toLowerCase(Locale.ROOT);
  }

  private static StorageCredentials credentials(
      CredentialRequest request, TemporaryCredentials minted) {
    Instant expiration =
        minted.getExpirationTime() == null || minted.getExpirationTime() == 0
            ? null
            : Instant.ofEpochMilli(minted.getExpirationTime());
    String prefix =
        minted.getUrl() != null && request.storageLocation().startsWith(minted.getUrl())
            ? minted.getUrl()
            : request.storageLocation();
    if (minted.getAwsTempCredentials() != null) {
      AwsCredentials aws = minted.getAwsTempCredentials();
      return stated(
          new StorageCredentials(
              prefix,
              CloudProvider.AWS,
              values(
                  StorageCredentialKeys.ACCESS_KEY_ID,
                  aws.getAccessKeyId(),
                  StorageCredentialKeys.SECRET_ACCESS_KEY,
                  aws.getSecretAccessKey(),
                  StorageCredentialKeys.SESSION_TOKEN,
                  aws.getSessionToken()),
              expiration));
    }
    if (minted.getAzureUserDelegationSas() != null) {
      AzureUserDelegationSAS azure = minted.getAzureUserDelegationSas();
      return stated(
          new StorageCredentials(
              prefix,
              CloudProvider.AZURE,
              values(StorageCredentialKeys.SAS_TOKEN, azure.getSasToken()),
              expiration));
    }
    if (minted.getGcpOauthToken() != null) {
      GcpOauthToken gcp = minted.getGcpOauthToken();
      return stated(
          new StorageCredentials(
              prefix,
              CloudProvider.GCP,
              values(StorageCredentialKeys.OAUTH_TOKEN, gcp.getOauthToken()),
              expiration));
    }
    throw new CatalogException("Unity Catalog returned an unsupported credential");
  }

  private static boolean hasNoCredential(TemporaryCredentials credentials) {
    return credentials.getAwsTempCredentials() == null
        && credentials.getAzureUserDelegationSas() == null
        && credentials.getGcpOauthToken() == null;
  }

  private static StorageCredentials stated(StorageCredentials credentials) {
    if (credentials.credentials().isEmpty()) {
      throw new CatalogException("Unity Catalog minted an empty credential");
    }
    return credentials;
  }

  private static Map<String, String> values(String... keysAndValues) {
    Map<String, String> values = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      if (keysAndValues[i + 1] != null && !keysAndValues[i + 1].isBlank()) {
        values.put(keysAndValues[i], keysAndValues[i + 1]);
      }
    }
    return values;
  }

  private RuntimeException mapFailure(
      AssetLookup lookup, CatalogCaller caller, RuntimeException failure) {
    if (failure instanceof CatalogException catalogFailure) {
      return catalogFailure;
    }
    if (failure instanceof BaseException ucFailure) {
      if (ucFailure.getErrorCode() == ErrorCode.NOT_FOUND
          || ucFailure.getErrorCode() == ErrorCode.TABLE_NOT_FOUND
          || ucFailure.getErrorCode() == ErrorCode.SCHEMA_NOT_FOUND) {
        return new AssetNotFoundException(lookup);
      }
      if (ucFailure.getErrorCode() == ErrorCode.PERMISSION_DENIED
          || ucFailure.getErrorCode() == ErrorCode.UNAUTHENTICATED) {
        return new AssetAccessDeniedException(lookup, caller);
      }
      return new CatalogException(ucFailure.getMessage(), ucFailure);
    }
    return failure;
  }

  private static String qualifiedName(AssetLookup lookup, int parts, String shape) {
    String[] split = lookup.identifier().trim().split("\\.", -1);
    if (split.length != parts || Arrays.stream(split).anyMatch(String::isBlank)) {
      throw new IllegalArgumentException(
          "'" + lookup.identifier() + "' is not a Unity Catalog name; expected " + shape);
    }
    return lookup.identifier().trim();
  }
}
