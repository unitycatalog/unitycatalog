package io.unitycatalog.server.sharing.catalog;

import static io.unitycatalog.server.auth.AuthorizeExpressions.VEND_TABLE_CREDENTIAL;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.TABLE;

import io.opensharing.auth.AuthContext;
import io.opensharing.auth.Privilege;
import io.opensharing.auth.UserContext;
import io.opensharing.catalog.Asset;
import io.opensharing.catalog.AssetLocation;
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
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.CredentialUse;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * In-process OpenSharing connector owned and hosted by Unity Catalog. It reads UC's repositories,
 * authorizes with UC's own access expressions, and vends credentials with UC's credential vendor,
 * so a table can be shared, and read through a share, only by a user UC lets read its data.
 */
public final class UnityCatalogCatalogConnector implements CatalogConnector {

  public static final String NAME = "unity";

  /** Reading a table's data: what UC asks before vending it read-only table credentials. */
  private static final Map<String, Object> READ = Map.of("operation", "READ");

  private final TableRepository tables;
  private final MetastoreRepository metastore;
  private final UserRepository users;
  private final StorageCredentialVendor credentialVendor;
  private final Map<NormalizedURL, S3StorageConfig> s3Buckets;
  private final KeyMapper keyMapper;
  private final UnityAccessEvaluator evaluator;

  public UnityCatalogCatalogConnector(
      Repositories repositories,
      UnityCatalogAuthorizer authorizer,
      ServerProperties serverProperties) {
    this.tables = repositories.getTableRepository();
    this.metastore = repositories.getMetastoreRepository();
    this.users = repositories.getUserRepository();
    this.credentialVendor = repositories.getStorageCredentialVendor();
    this.s3Buckets = serverProperties.getS3Configurations();
    this.keyMapper = repositories.getKeyMapper();
    try {
      this.evaluator =
          new UnityAccessEvaluator(authorizer, serverProperties.isStrictPrivilegeChecksEnabled());
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("failed to initialize Unity Catalog authorization", e);
    }
  }

  @Override
  public String name() {
    return NAME;
  }

  /**
   * Checks only that the caller is an enabled UC user. {@code OpenSharingProviderService} checks
   * every sharing privilege OpenSharing asks for here, before calling it: CREATE SHARE on create,
   * USE SHARE on getting a share, and USE SHARE, per item, on listing shares, which calls {@code
   * listAll} and asks nothing. Calling OpenSharing where it asks for another needs a check there
   * first.
   */
  @Override
  public UserContext authorize(AuthContext auth, Privilege privilege) {
    User user = user(auth);
    return UserContext.fromUserIdAndName(user.getId(), user.getEmail());
  }

  /** Resolves a table the caller may read the data of. Shares hold only tables. */
  @Override
  public ResolvedAsset resolveAsset(Asset asset, AuthContext auth) {
    if (asset.type() != AssetType.TABLE) {
      throw new UnsupportedAssetTypeException(
          "the Unity Catalog connector resolves TABLE, not " + asset.type());
    }
    qualifiedName(asset);
    try {
      TableInfo table = tables.getTable(asset.fullName());
      requireReadable(asset, auth);
      String refused = unshareable(table);
      if (refused != null) {
        throw new UnsupportedAssetTypeException("'" + asset.fullName() + "' " + refused);
      }
      return ResolvedAsset.builder(AssetType.TABLE, fullName(table))
          .catalogAssetId(table.getTableId())
          .location(new AssetLocation(table.getStorageLocation(), List.of()))
          .additionalProperties(
              new TableProperties(io.opensharing.catalog.DataSourceFormat.DELTA, attributes(table)))
          .build();
    } catch (RuntimeException e) {
      throw mapFailure(asset, auth, e);
    }
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
      requireReadable(asset, auth);
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
          credentialVendor.vendCredential(
              tableLocation, CredentialContext.READ_ONLY, CredentialUse.CLIENT);
      if (hasNoCredential(minted)) {
        if ("file".equals(tableLocation.toUri().getScheme())) {
          return List.of();
        }
        throw new CatalogException("Unity Catalog vended no credentials for this table");
      }
      String location = requested != null ? requested : table.getStorageLocation();
      return List.of(credentials(location, minted, region(tableLocation)));
    } catch (RuntimeException e) {
      throw mapFailure(asset, auth, e);
    }
  }

  private void requireReadable(Asset asset, AuthContext auth) {
    UUID principal;
    try {
      principal = UUID.fromString(user(auth).getId());
    } catch (CatalogAuthorizationException e) {
      // Recipients read as the share's owner, who has lost access to the table once deleted or
      // disabled, as when their SELECT is revoked.
      throw new AssetAccessDeniedException(asset, auth.user());
    }
    Map<SecurableType, UUID> ids =
        new HashMap<>(keyMapper.mapResourceKeys(Map.of(TABLE, asset.fullName())));
    ids.put(METASTORE, metastore.getMetastoreId());
    if (!evaluator.evaluate(principal, VEND_TABLE_CREDENTIAL, ids, READ)) {
      throw new AssetAccessDeniedException(asset, auth.user());
    }
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
      if (e.getErrorCode() != ErrorCode.NOT_FOUND) {
        throw new CatalogException("failed to look up Unity Catalog user '" + userId + "'", e);
      }
      throw new CatalogAuthorizationException("'" + userId + "' is not a Unity Catalog user");
    }
    if (user.getState() != User.StateEnum.ENABLED) {
      throw new CatalogAuthorizationException("'" + user.getEmail() + "' is not enabled");
    }
    return user;
  }

  // The region of a bucket configured in server.properties; UC does not know it otherwise.
  private String region(NormalizedURL location) {
    S3StorageConfig bucket = s3Buckets.get(location.getStorageBase());
    return bucket == null ? null : bucket.getRegion();
  }

  private static Map<String, String> attributes(TableInfo table) {
    Map<String, String> attributes = new LinkedHashMap<>();
    if (table.getTableType() != null) {
      attributes.put("subtype", table.getTableType().getValue());
    }
    return attributes;
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

  private static StorageCredentials credentials(
      String location, TemporaryCredentials minted, String region) {
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
                  aws.getSessionToken(),
                  StorageCredentials.REGION,
                  region),
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
          || ucFailure.getErrorCode() == ErrorCode.TABLE_NOT_FOUND) {
        return new AssetNotFoundException(asset);
      }
      if (ucFailure.getErrorCode() == ErrorCode.PERMISSION_DENIED
          || ucFailure.getErrorCode() == ErrorCode.UNAUTHENTICATED) {
        return new AssetAccessDeniedException(asset, auth.user());
      }
      return new CatalogException(ucFailure.getMessage(), ucFailure);
    }
    if (failure instanceof IllegalArgumentException invalid) {
      // The caller's table name is checked before this is reached, so this is about UC's own
      // data, such as a stored storage location, and not a bad request.
      return new CatalogException(invalid.getMessage(), invalid);
    }
    return failure;
  }

  private static void qualifiedName(Asset asset) {
    String[] split = asset.fullName().trim().split("\\.", -1);
    if (split.length != 3 || Arrays.stream(split).anyMatch(String::isBlank)) {
      throw new IllegalArgumentException(
          "'"
              + asset.fullName()
              + "' is not a Unity Catalog table name; expected"
              + " catalog.schema.table");
    }
  }
}
