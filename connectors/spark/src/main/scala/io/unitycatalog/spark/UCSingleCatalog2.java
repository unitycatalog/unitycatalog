package io.unitycatalog.spark;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.shaded.com.google.common.base.Preconditions;
import org.apache.hadoop.shaded.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.catalog.CatalogUtils;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UCSingleCatalog2 implements TableCatalog, SupportsNamespaces {
  private static final Logger LOG = LoggerFactory.getLogger(UCSingleCatalog2.class);

  private ApiClient apiClient = null;
  private TemporaryCredentialsApi tempCredApi = null;
  private volatile TableCatalog delegate = null;

  static ThreadLocal<Boolean> LOAD_DELTA_CATALOG = ThreadLocal.withInitial(() -> true);
  static ThreadLocal<Boolean> DELTA_CATALOG_LOADED = ThreadLocal.withInitial(() -> false);

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    String uriStr = options.get("uri");
    Preconditions.checkArgument(uriStr != null && !uriStr.isEmpty(),
        "uri must be specified for Unity Catalog '%s'", name);
    URI uri = URI.create(uriStr);

    apiClient = new ApiClient()
        .setHost(uri.getHost())
        .setPort(uri.getPort())
        .setScheme(uri.getScheme());

    String token = options.get("token");
    if (token != null && !token.isEmpty()) {
      apiClient = apiClient.setRequestInterceptor(
          request -> request.header("Authorization", "Bearer " + token)
      );
    }

    tempCredApi = new TemporaryCredentialsApi(apiClient);
    UCProxy2 proxy = new UCProxy2(apiClient, tempCredApi);
    proxy.initialize(name, options);
    if (LOAD_DELTA_CATALOG.get()) {
      try {
        delegate = (TableCatalog) Class
            .forName("org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getDeclaredConstructor()
            .newInstance();
        ((DelegatingCatalogExtension) delegate).setDelegateCatalog(proxy);

        DELTA_CATALOG_LOADED.set(true);
      } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException |
               IllegalAccessException | InvocationTargetException e) {
        LOG.warn("DeltaCatalog is not available in the classpath", e);
        delegate = proxy;
      }
    } else {
      delegate = proxy;
    }
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return delegate.listTables(namespace);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return delegate.loadTable(ident);
  }

  @Override
  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    return delegate.loadTable(ident, version);
  }

  @Override
  public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    return delegate.loadTable(ident, timestamp);
  }

  @Override
  public boolean tableExists(Identifier ident) {
    return delegate.tableExists(ident);
  }

  @Override
  public Table createTable(
      Identifier ident,
      org.apache.spark.sql.connector.catalog.Column[] columns,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
    boolean hasExternalClause = properties.containsKey(TableCatalog.PROP_EXTERNAL);
    boolean hasLocationClause = properties.containsKey(TableCatalog.PROP_LOCATION);

    if (hasExternalClause && !hasLocationClause) {
      throw new RuntimeException("Cannot create EXTERNAL TABLE without location.");
    }

    boolean isPathTable = ident.namespace().length == 1 && new Path(ident.name()).isAbsolute();
    // If both EXTERNAL and LOCATION are not specified in the CREATE TABLE command, and the table is
    // not a path table like parquet.`/file/path`, we generate the UC-managed table location here.
    if (!hasExternalClause && !hasLocationClause && !isPathTable) {
      Map<String, String> newProps = new HashMap<>(properties);
      // TODO: here we use a fake location for managed table, we should generate table location
      //       properly when Unity Catalog supports creating managed table.
      newProps.put(TableCatalog.PROP_LOCATION, properties.get("__FAKE_PATH__"));
      // `PROP_IS_MANAGED_LOCATION` is used to indicate that the table location is not
      // user-specified but system-generated, which is exactly the case here.
      newProps.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true");
      return delegate.createTable(ident, columns, partitions, newProps);
    } else if (hasLocationClause) {
      String location = properties.get(TableCatalog.PROP_LOCATION);
      assert location != null;

      TemporaryCredentials cred;
      try {
        cred = tempCredApi.generateTemporaryPathCredentials(
            new GenerateTemporaryPathCredential()
                .url(location)
                .operation(PathOperation.PATH_CREATE_TABLE)
        );
      } catch (ApiException e) {
        throw new RuntimeException(e);
      }

      Map<String, String> newProps = new HashMap<>(properties);
      Map<String, String> credentialProps = UCSingleCatalog2.generateCredentialProps(
          CatalogUtils.stringToURI(location).getScheme(), cred
      );
      newProps.putAll(credentialProps);

      // TODO: Delta requires the options to be set twice in the properties, with and without the
      //       `option.` prefix. We should revisit this in Delta.
      String prefix = TableCatalog.OPTION_PREFIX;
      credentialProps.forEach((k, v) -> newProps.put(prefix + k, v));
      return delegate.createTable(ident, columns, partitions, newProps);
    } else {
      // TODO: for path-based tables, Spark should generate a location property using the qualified
      //       path string.
      return delegate.createTable(ident, columns, partitions, properties);
    }
  }

  @Override
  public Table createTable(Identifier ident, StructType schema,
                           Transform[] partitions, Map<String, String> properties) {
    throw new UnsupportedOperationException("Deprecated `createTable` should not be called");
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) {
    throw new UnsupportedOperationException("Altering a table is not supported yet");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return delegate.dropTable(ident);
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) {
    throw new UnsupportedOperationException("Rename table is not supported yet");
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return ((DelegatingCatalogExtension) delegate).listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return ((DelegatingCatalogExtension) delegate).listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    return ((DelegatingCatalogExtension) delegate).loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    ((DelegatingCatalogExtension) delegate).createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    ((DelegatingCatalogExtension) delegate).alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    return ((DelegatingCatalogExtension) delegate).dropNamespace(namespace, cascade);
  }

  private static Map<String, String> generateCredentialProps(
      String scheme,
      TemporaryCredentials temporaryCredentials) {
    if (scheme.equals("s3")) {
      AwsCredentials awsCredentials = temporaryCredentials.getAwsTempCredentials();

      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
      builder.put("fs.s3a.access.key", awsCredentials.getAccessKeyId());
      builder.put("fs.s3a.secret.key", awsCredentials.getSecretAccessKey());
      builder.put("fs.s3a.session.token", awsCredentials.getSessionToken());
      builder.put("fs.s3a.path.style.access", "true");
      builder.put("fs.s3.impl.disable.cache", "true");
      builder.put("fs.s3a.impl.disable.cache", "true");
      return builder.build();

    } else if (scheme.equals("gs")) {
      GcpOauthToken gcsCredentials = temporaryCredentials.getGcpOauthToken();

      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
      builder.put(GcsVendedTokenProvider.ACCESS_TOKEN_KEY, gcsCredentials.getOauthToken());
      builder.put(GcsVendedTokenProvider.ACCESS_TOKEN_EXPIRATION_KEY,
          temporaryCredentials.getExpirationTime().toString());
      builder.put("fs.gs.create.items.conflict.check.enable", "false");
      builder.put("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER");
      builder.put("fs.gs.auth.access.token.provider", GcsVendedTokenProvider.class.getName());
      builder.put("fs.gs.impl.disable.cache", "true");
      return builder.build();

    } else if (scheme.equals("abfs") || scheme.equals("abfss")) {
      AzureUserDelegationSAS azCredentials = temporaryCredentials.getAzureUserDelegationSas();

      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
      builder.put(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, "SAS");
      builder.put(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, "true");
      builder.put(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, AbfsVendedTokenProvider.class.getName());
      builder.put(AbfsVendedTokenProvider.ACCESS_TOKEN_KEY, azCredentials.getSasToken());
      builder.put("fs.abfs.impl.disable.cache", "true");
      builder.put("fs.abfss.impl.disable.cache", "true");
      return builder.build();

    } else {
      return ImmutableMap.of();
    }
  }
}
