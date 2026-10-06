package io.unitycatalog.server.sharing.catalog;

import static io.unitycatalog.server.auth.AuthorizeExpressions.GET_SCHEMA;
import static io.unitycatalog.server.auth.AuthorizeExpressions.GET_TABLE;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.TABLE;

import io.opensharing.auth.AuthContext;
import io.opensharing.auth.Privilege;
import io.opensharing.auth.UserContext;
import io.opensharing.catalog.Asset;
import io.opensharing.catalog.AssetLocation;
import io.opensharing.catalog.AssetPage;
import io.opensharing.catalog.AssetType;
import io.opensharing.catalog.CatalogConnector;
import io.opensharing.catalog.CloudProvider;
import io.opensharing.catalog.CredentialRequest;
import io.opensharing.catalog.ResolvedAsset;
import io.opensharing.catalog.StorageCredentials;
import io.opensharing.catalog.TableProperties;
import io.opensharing.exception.AssetAccessDeniedException;
import io.opensharing.exception.AssetNotFoundException;
import io.opensharing.exception.CatalogAuthorizationException;
import io.opensharing.exception.CatalogException;
import io.opensharing.exception.UnsupportedAssetTypeException;
import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-process OpenSharing connector owned and hosted by Unity Catalog. It reads UC's repositories,
 * authorizes with UC's own access expressions, and vends credentials with UC's credential vendor,
 * so a shared asset is visible to exactly the users UC lets read it.
 */
public final class UnityCatalogCatalogConnector implements CatalogConnector {

  public static final String NAME = "unity";
  private static final Logger LOGGER =
      LoggerFactory.getLogger(UnityCatalogCatalogConnector.class);

  private final TableRepository tables;
  private final SchemaRepository schemas;
  private final MetastoreRepository metastore;
  private final UserRepository users;
  private final StorageCredentialVendor credentialVendor;
  private final UnityCatalogAuthorizer authorizer;
  private final KeyMapper keyMapper;
  private final UnityAccessEvaluator evaluator;

  public UnityCatalogCatalogConnector(
      Repositories repositories, UnityCatalogAuthorizer authorizer) {
    this.tables = repositories.getTableRepository();
    this.schemas = repositories.getSchemaRepository();
    this.metastore = repositories.getMetastoreRepository();
    this.users = repositories.getUserRepository();
    this.credentialVendor = repositories.getStorageCredentialVendor();
    this.authorizer = authorizer;
    this.keyMapper = repositories.getKeyMapper();
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

  /** Sharing privileges are granted on the metastore, and metastore owners hold them all. */
  @Override
  public UserContext authorize(AuthContext auth, Privilege privilege) {
    User user = user(auth);
    if (privilege != null
        && !authorizer.authorizeAny(
            UUID.fromString(user.getId()),
            metastore.getMetastoreId(),
            Privileges.OWNER,
            privilege(privilege))) {
      throw new CatalogAuthorizationException(
          "'" + user.getEmail() + "' does not have " + privilege);
    }
    return UserContext.fromUserIdAndName(user.getId(), user.getEmail());
  }

  @Override
  public ResolvedAsset resolveAsset(Asset asset, AuthContext auth) {
    return switch (asset.type()) {
      case TABLE -> resolveTable(asset, auth);
      case SCHEMA -> resolveSchema(asset, auth);
    };
  }

  private ResolvedAsset resolveTable(Asset asset, AuthContext auth) {
    qualifiedName(asset, 3, "catalog.schema.table");
    try {
      TableInfo table = tables.getTable(asset.fullName());
      requireAllowed(asset, auth, GET_TABLE, TABLE);
      String refused = unshareable(table);
      if (refused != null) {
        throw new UnsupportedAssetTypeException("'" + asset.fullName() + "' " + refused);
      }
      return resolved(table);
    } catch (RuntimeException e) {
      throw mapFailure(asset, auth, e);
    }
  }

  private ResolvedAsset resolveSchema(Asset asset, AuthContext auth) {
    qualifiedName(asset, 2, "catalog.schema");
    try {
      SchemaInfo schema = schemas.getSchema(asset.fullName());
      requireAllowed(asset, auth, GET_SCHEMA, SCHEMA);
      return ResolvedAsset.builder(AssetType.SCHEMA, asset.fullName())
          .catalogAssetId(schema.getSchemaId())
          .build();
    } catch (RuntimeException e) {
      throw mapFailure(asset, auth, e);
    }
  }

  /**
   * Lists one page of a schema's tables with UC's own paging. Tables the caller may not read, or
   * that OpenSharing cannot serve, are left out, so a page may hold fewer than {@code maxResults}.
   */
  @Override
  public AssetPage listChildren(
      Asset parent, int maxResults, String pageToken, AuthContext auth) {
    if (parent.type() != AssetType.SCHEMA) {
      throw new UnsupportedAssetTypeException(
          "the Unity Catalog connector only lists a SCHEMA, not a " + parent.type());
    }
    String[] name = qualifiedName(parent, 2, "catalog.schema").split("\\.");
    resolveSchema(parent, auth);
    ListTablesResponse response;
    try {
      response =
          tables.listTables(
              name[0],
              name[1],
              Optional.of(maxResults),
              Optional.ofNullable(pageToken),
              false,
              false);
    } catch (RuntimeException e) {
      throw mapFailure(parent, auth, e);
    }
    List<ResolvedAsset> assets = new ArrayList<>();
    if (response.getTables() != null) {
      for (TableInfo table : response.getTables()) {
        String fullName = fullName(table);
        if (unshareable(table) != null || !allowed(auth, GET_TABLE, TABLE, fullName)) {
          LOGGER.debug("Leaving unshareable table '{}' out of schema '{}'", fullName, parent);
          continue;
        }
        assets.add(resolved(table));
      }
    }
    String next = response.getNextPageToken();
    return new AssetPage(assets, next == null || next.isBlank() ? null : next);
  }

  /** UC vends credentials for a table's own storage location only, for its default lifetime. */
  @Override
  public List<StorageCredentials> getStorageCredentials(
      CredentialRequest request, AuthContext auth) {
    Asset asset = new Asset(request.assetType(), request.assetFullName());
    if (asset.type() != AssetType.TABLE) {
      throw new UnsupportedAssetTypeException(
          "the Unity Catalog connector vends credentials for TABLE, not " + asset.type());
    }
    try {
      TableInfo table = tables.getTable(asset.fullName());
      requireAllowed(asset, auth, GET_TABLE, TABLE);
      if (request.catalogAssetId() != null
          && !request.catalogAssetId().equals(table.getTableId())) {
        throw new CatalogException("the recorded table id for '" + asset.fullName() + "' is stale");
      }
      NormalizedURL tableLocation = NormalizedURL.from(table.getStorageLocation());
      String requested = request.storageLocation();
      if (requested != null && !tableLocation.equals(NormalizedURL.from(requested))) {
        throw new CatalogException(
            "storage location is not the current location of '" + asset.fullName() + "'");
      }
      TemporaryCredentials minted =
          credentialVendor.vendCredential(tableLocation, CredentialContext.READ_ONLY);
      if (hasNoCredential(minted)) {
        if ("file".equals(tableLocation.toUri().getScheme())) {
          return List.of();
        }
        throw new CatalogException("Unity Catalog vended no credentials for this table");
      }
      String location = requested != null ? requested : table.getStorageLocation();
      return List.of(credentials(location, minted));
    } catch (RuntimeException e) {
      throw mapFailure(asset, auth, e);
    }
  }

  private void requireAllowed(
      Asset asset, AuthContext auth, String expression, SecurableType type) {
    if (!allowed(auth, expression, type, asset.fullName())) {
      throw new AssetAccessDeniedException(asset, auth.user());
    }
  }

  private boolean allowed(
      AuthContext auth, String expression, SecurableType type, String resource) {
    UUID principal = UUID.fromString(user(auth).getId());
    Map<SecurableType, UUID> ids = new HashMap<>(keyMapper.mapResourceKeys(Map.of(type, resource)));
    ids.put(METASTORE, metastore.getMetastoreId());
    return evaluator.evaluate(principal, expression, ids, Map.of());
  }

  // UC authenticates callers itself, so OpenSharing names them by the UC user id it was given.
  private User user(AuthContext auth) {
    String userId = auth.user().userId();
    if (userId == null) {
      throw new CatalogAuthorizationException("Unity Catalog requires a UC user id");
    }
    User user;
    try {
      user = users.getUser(userId);
    } catch (BaseException e) {
      throw new CatalogAuthorizationException("'" + userId + "' is not a Unity Catalog user");
    }
    if (user.getState() != User.StateEnum.ENABLED) {
      throw new CatalogAuthorizationException("'" + user.getEmail() + "' is not enabled");
    }
    return user;
  }

  private static Privileges privilege(Privilege privilege) {
    return switch (privilege) {
      case CREATE_SHARE -> Privileges.CREATE_SHARE;
      case CREATE_RECIPIENT -> Privileges.CREATE_RECIPIENT;
    };
  }

  private static ResolvedAsset resolved(TableInfo table) {
    Map<String, String> attributes = new LinkedHashMap<>();
    if (table.getTableType() != null) {
      attributes.put("subtype", table.getTableType().getValue());
    }
    return ResolvedAsset.builder(AssetType.TABLE, fullName(table))
        .catalogAssetId(table.getTableId())
        .location(new AssetLocation(table.getStorageLocation(), List.of()))
        .additionalProperties(
            new TableProperties(io.opensharing.catalog.DataSourceFormat.DELTA, attributes))
        .build();
  }

  private static String fullName(TableInfo table) {
    return table.getCatalogName() + "." + table.getSchemaName() + "." + table.getName();
  }

  private static String unshareable(TableInfo table) {
    if (table.getStorageLocation() == null || table.getStorageLocation().isBlank()) {
      return "has no storage location";
    }
    if (table.getDataSourceFormat() != DataSourceFormat.DELTA) {
      return "has unsupported format " + table.getDataSourceFormat();
    }
    return null;
  }

  private static StorageCredentials credentials(String location, TemporaryCredentials minted) {
    Instant expiration =
        minted.getExpirationTime() == null || minted.getExpirationTime() == 0
            ? null
            : Instant.ofEpochMilli(minted.getExpirationTime());
    String url = minted.getUrl();
    String prefix = url != null && location.startsWith(url) ? url : location;
    if (minted.getAwsTempCredentials() != null) {
      AwsCredentials aws = minted.getAwsTempCredentials();
      return stated(
          new StorageCredentials(
              prefix,
              CloudProvider.AWS,
              values(
                  StorageCredentials.ACCESS_KEY_ID,
                  aws.getAccessKeyId(),
                  StorageCredentials.SECRET_ACCESS_KEY,
                  aws.getSecretAccessKey(),
                  StorageCredentials.SESSION_TOKEN,
                  aws.getSessionToken()),
              expiration));
    }
    if (minted.getAzureUserDelegationSas() != null) {
      AzureUserDelegationSAS azure = minted.getAzureUserDelegationSas();
      return stated(
          new StorageCredentials(
              prefix,
              CloudProvider.AZURE,
              values(StorageCredentials.SAS_TOKEN, azure.getSasToken()),
              expiration));
    }
    if (minted.getGcpOauthToken() != null) {
      GcpOauthToken gcp = minted.getGcpOauthToken();
      return stated(
          new StorageCredentials(
              prefix,
              CloudProvider.GCP,
              values(StorageCredentials.OAUTH_TOKEN, gcp.getOauthToken()),
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

  private static RuntimeException mapFailure(
      Asset asset, AuthContext auth, RuntimeException failure) {
    if (failure instanceof CatalogException catalogFailure) {
      return catalogFailure;
    }
    if (failure instanceof BaseException ucFailure) {
      if (ucFailure.getErrorCode() == ErrorCode.NOT_FOUND
          || ucFailure.getErrorCode() == ErrorCode.TABLE_NOT_FOUND
          || ucFailure.getErrorCode() == ErrorCode.SCHEMA_NOT_FOUND) {
        return new AssetNotFoundException(asset);
      }
      if (ucFailure.getErrorCode() == ErrorCode.PERMISSION_DENIED
          || ucFailure.getErrorCode() == ErrorCode.UNAUTHENTICATED) {
        return new AssetAccessDeniedException(asset, auth.user());
      }
      return new CatalogException(ucFailure.getMessage(), ucFailure);
    }
    return failure;
  }

  private static String qualifiedName(Asset asset, int parts, String shape) {
    String[] split = asset.fullName().trim().split("\\.", -1);
    if (split.length != parts || Arrays.stream(split).anyMatch(String::isBlank)) {
      throw new IllegalArgumentException(
          "'" + asset.fullName() + "' is not a Unity Catalog name; expected " + shape);
    }
    return asset.fullName().trim();
  }
}
