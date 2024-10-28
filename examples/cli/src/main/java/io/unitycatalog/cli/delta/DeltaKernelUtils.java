package io.unitycatalog.cli.delta;

import static io.unitycatalog.cli.utils.CliUtils.EMPTY;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import de.vandermeer.asciitable.AsciiTable;
import io.delta.kernel.*;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterable;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.security.AccessControlException;

/**
 * Utility class to create and read Delta tables. The create method creates a Delta table with the
 * given schema at the given path. The create method just initializes the delta log and does not
 * write any data to the table. The read method reads the data from the Delta table The code has
 * evolved from examples provided in <a
 * href="https://github.com/delta-io/delta/tree/master/kernel/examples/kernel-examples/src/main/java/io/delta/kernel/examples">Delta
 * examples</a>
 */
public class DeltaKernelUtils {

  public static String createDeltaTable(
      String tablePath, List<ColumnInfo> columns, TemporaryCredentials temporaryCredentials) {
    tablePath = tablePath.replace("s3://", "s3a://");
    URI tableUri = URI.create(tablePath);
    Configuration conf = getHDFSConfiguration(tableUri, temporaryCredentials);
    Engine engine = DefaultEngine.create(conf);
    Table table = Table.forPath(engine, tableUri.toString());
    // construct the schema
    StructType tableSchema = getSchema(columns);
    TransactionBuilder txnBuilder =
        table.createTransactionBuilder(engine, "UnityCatalogCli", Operation.CREATE_TABLE);
    // Set the schema of the new table on the transaction builder
    txnBuilder = txnBuilder.withSchema(engine, tableSchema);
    // Build the transaction
    Transaction txn = txnBuilder.build(engine);
    // create an empty table
    TransactionCommitResult commitResult = txn.commit(engine, CloseableIterable.emptyIterable());
    if (commitResult.getVersion() >= 0) {
      System.out.println("Table created successfully at: " + tableUri);
    } else {
      throw new RuntimeException("Table creation failed");
    }
    return EMPTY;
  }

  public static String substituteSchemeForS3(String tablePath) {
    return tablePath.replace("s3://", "s3a://");
  }

  public static FileSystem getFileSystem(URI tablePathURI, Configuration conf) {
    try {
      return FileSystem.get(tablePathURI, conf);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to get file system", e);
    }
  }

  public static Configuration getHDFSConfiguration(
      URI tablePathUri, TemporaryCredentials temporaryCredentials) {
    Configuration conf = new Configuration();

    String scheme = tablePathUri.getScheme();
    if (scheme == null) {
      throw new IllegalArgumentException("URI scheme is missing");
    }

    System.out.println(temporaryCredentials);

    switch (scheme) {
      case "s3":
      case "s3a":
        if (temporaryCredentials.getAwsTempCredentials() == null) {
          throw new IllegalArgumentException("AWS temporary credentials are missing");
        }
        conf.set(
            "fs.s3a.access.key", temporaryCredentials.getAwsTempCredentials().getAccessKeyId());
        conf.set(
            "fs.s3a.secret.key", temporaryCredentials.getAwsTempCredentials().getSecretAccessKey());
        conf.set(
            "fs.s3a.session.token", temporaryCredentials.getAwsTempCredentials().getSessionToken());
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.path.style.access", "true");
        break;

      case "gs":
        if (temporaryCredentials.getGcpOauthToken() == null) {
          throw new IllegalArgumentException("GCP OAuth token is missing");
        }
        conf.set(
            GcsVendedTokenProvider.ACCESS_TOKEN_KEY,
            temporaryCredentials.getGcpOauthToken().getOauthToken());
        conf.set(
            GcsVendedTokenProvider.ACCESS_TOKEN_EXPIRATION_KEY,
            temporaryCredentials.getExpirationTime().toString());
        conf.set("fs.gs.create.items.conflict.check.enable", "false");
        conf.set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER");
        conf.set("fs.gs.impl.disable.cache", "true");
        conf.set("fs.gs.auth.access.token.provider", GcsVendedTokenProvider.class.getName());
        break;

      case "abfs":
      case "abfss":
        String accountName = null;
        String authority = tablePathUri.getAuthority();
        if (authority != null && authority.contains(".dfs.core.windows.net")) {
          accountName = authority.split("\\.")[0].split("@")[1];
        }
        System.out.println(accountName);
        if (accountName == null) {
          throw new IllegalArgumentException("Unable to extract account name from URI");
        }
        if (temporaryCredentials.getAzureUserDelegationSas() == null) {
          throw new IllegalArgumentException("Azure User Delegation SAS token is missing");
        }
        conf.set("fs.azure.account.auth.type", "SAS");
        conf.set("fs.azure.account.hns.enabled", "true");
        conf.set(
            AbfsVendedTokenProvider.ACCESS_TOKEN_KEY,
            temporaryCredentials.getAzureUserDelegationSas().getSasToken());
        conf.set("fs.abfs.impl.disable.cache", "true");
        conf.set("fs.abfss.impl.disable.cache", "true");
        conf.set("fs.azure.sas.token.provider.type", AbfsVendedTokenProvider.class.getName());
        break;

      case "file":
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        break;

      default:
        throw new IllegalArgumentException("Unsupported URI scheme: " + scheme);
    }

    return conf;
  }

  public static String readDeltaTable(
      String tablePath, TemporaryCredentials temporaryCredentials, int maxResults) {
    Configuration conf = getHDFSConfiguration(URI.create(tablePath), temporaryCredentials);
    Engine engine = DefaultEngine.create(conf);
    try {
      Table table = Table.forPath(engine, substituteSchemeForS3(tablePath));
      Snapshot snapshot = table.getLatestSnapshot(engine);
      StructType readSchema = snapshot.getSchema(engine);
      Object[] schema =
          readSchema.fields().stream()
              .map(x -> x.getName() + "(" + x.getDataType().toString() + ")")
              .toArray(String[]::new);
      AsciiTable at = new AsciiTable();
      at.addRule();
      at.addRow(schema);
      at.addRule();
      // might need to prune it later
      ScanBuilder scanBuilder = snapshot.getScanBuilder(engine).withReadSchema(engine, readSchema);
      List<Row> rowData =
          DeltaKernelReadUtils.readData(engine, readSchema, scanBuilder.build(), maxResults);
      for (Row row : rowData) {
        Object[] rowValues =
            IntStream.range(0, schema.length)
                .mapToObj(colOrdinal -> DeltaKernelReadUtils.getValue(row, colOrdinal))
                .toArray();
        at.addRow(rowValues);
        at.addRule();
      }
      return at.render();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to read delta table", e);
    }
  }

  // TODO : INTERVAL, CHAR and NULL, ARRAY, MAP, STRUCT
  public static StructType getSchema(List<ColumnInfo> columns) {
    StructType structType = new StructType();
    for (ColumnInfo column : columns) {
      DataType dataType = getDataType(column);
      structType = structType.add(column.getName(), dataType);
    }
    return structType;
  }

  public static DataType getDataType(ColumnInfo column) {
    if (column.getTypeName() == null) {
      throw new IllegalArgumentException("Column type is missing: " + column.getName());
    }
    return findBasicTypeFromString(column.getTypeName().toString());
  }

  public static DataType findBasicTypeFromString(String typeText) {
    DataType dataType = null;
    switch (typeText) {
      case "INT":
        dataType = IntegerType.INTEGER;
        break;
      case "STRING":
      case "DOUBLE":
      case "BOOLEAN":
      case "LONG":
      case "FLOAT":
      case "SHORT":
      case "BYTE":
      case "DATE":
      case "TIMESTAMP":
      case "TIMESTAMP_NTZ":
      case "BINARY":
      case "DECIMAL":
        dataType = BasePrimitiveType.createPrimitive(typeText.toLowerCase(Locale.ROOT));
        break;
      default:
        throw new IllegalArgumentException("Unsupported basic data type: " + typeText);
    }
    return dataType;
  }

  public static class AbfsVendedTokenProvider implements SASTokenProvider {
    private Configuration conf;
    protected static final String ACCESS_TOKEN_KEY = "fs.azure.sas.fixed.token";

    @Override
    public String getSASToken(String account, String fileSystem, String path, String operation)
        throws IOException, AccessControlException {
      return conf.get(ACCESS_TOKEN_KEY);
    }

    @Override
    public void initialize(Configuration configuration, String accountName) throws IOException {
      this.conf = configuration;
    }
  }

  public static class GcsVendedTokenProvider implements AccessTokenProvider {
    protected static final String ACCESS_TOKEN_KEY = "fs.gs.auth.access.token.credential";
    protected static final String ACCESS_TOKEN_EXPIRATION_KEY =
        "fs.gs.auth.access.token.expiration";
    private Configuration conf;

    @Override
    public AccessToken getAccessToken() {
      return new AccessToken(
          conf.get(ACCESS_TOKEN_KEY), Long.parseLong(conf.get(ACCESS_TOKEN_EXPIRATION_KEY)));
    }

    @Override
    public void refresh() throws IOException {
      throw new IOException("Temporary token, not refreshable");
    }

    @Override
    public void setConf(Configuration configuration) {
      conf = configuration;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }
  }
}
