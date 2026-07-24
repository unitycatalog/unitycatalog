package io.unitycatalog.hadoop.internal;

import io.unitycatalog.hadoop.internal.auth.AzureCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import java.util.Map;

/** ABFS realization of the {@link CredPropsBaseTest} matrix. */
class AzureCredPropsTest extends CredPropsBaseTest {

  private static final String CUSTOM_ABFS_IMPL = "com.example.CustomAbfsFileSystem";
  private static final String CUSTOM_ABFSS_IMPL = "com.example.CustomAbfssFileSystem";
  private static final String ABFS_FS = "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem";
  private static final String ABFSS_FS = "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem";
  private static final String ABFS_AFS = "org.apache.hadoop.fs.azurebfs.Abfs";
  private static final String ABFSS_AFS = "org.apache.hadoop.fs.azurebfs.Abfss";
  private static final String VENDED_PROVIDER =
      "io.unitycatalog.hadoop.internal.auth.AbfsVendedTokenProvider";

  @Override
  String scheme() {
    return "abfss";
  }

  @Override
  String location() {
    return "abfss://container@account.dfs.core.windows.net/data";
  }

  @Override
  GenericCredential vendedCred(Long expirationMillis) {
    return new AzureCredential("sas", expirationMillis);
  }

  @Override
  String initExpirationKey() {
    return UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME;
  }

  @Override
  Map<String, String> constructorKeys() {
    return props(
        UCHadoopConfConstants.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME,
        "SAS",
        UCHadoopConfConstants.FS_AZURE_ACCOUNT_IS_HNS_ENABLED,
        "true",
        "fs.abfs.impl.disable.cache",
        "true",
        "fs.abfss.impl.disable.cache",
        "true");
  }

  @Override
  Map<String, String> implOverrideKeys() {
    return props(
        "fs.abfs.impl", CRED_SCOPED_FS,
        "fs.abfs.impl.original", ABFS_FS,
        "fs.abfss.impl", CRED_SCOPED_FS,
        "fs.abfss.impl.original", ABFSS_FS,
        "fs.AbstractFileSystem.abfs.impl", CRED_SCOPED_AFS,
        "fs.AbstractFileSystem.abfs.impl.original", ABFS_AFS,
        "fs.AbstractFileSystem.abfss.impl", CRED_SCOPED_AFS,
        "fs.AbstractFileSystem.abfss.impl.original", ABFSS_AFS);
  }

  @Override
  Map<String, String> staticCredKeys() {
    return props("fs.azure.sas.fixed.token", "sas");
  }

  @Override
  Map<String, String> renewableCredKeys() {
    return props(
        UCHadoopConfConstants.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE,
        VENDED_PROVIDER,
        UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN,
        "sas");
  }

  @Override
  Map<String, String> customImplSeed() {
    return props("fs.abfs.impl", CUSTOM_ABFS_IMPL, "fs.abfss.impl", CUSTOM_ABFSS_IMPL);
  }

  @Override
  Map<String, String> customImplOriginals() {
    return props(
        "fs.abfs.impl.original", CUSTOM_ABFS_IMPL, "fs.abfss.impl.original", CUSTOM_ABFSS_IMPL);
  }
}
