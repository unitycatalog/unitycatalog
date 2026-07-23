package io.unitycatalog.hadoop.internal.props;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.AzureCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import org.apache.hadoop.conf.Configuration;

/** Builds ABFS credential properties. */
final class AbfsCredPropsBuilder extends CredPropsBuilder {
  private static final String ABFS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.unitycatalog.hadoop.internal.auth.AbfsVendedTokenProvider";
  private static final String ABFS_FIXED_SAS_TOKEN_KEY = "fs.azure.sas.fixed.token";

  AbfsCredPropsBuilder() {
    set(UCHadoopConfConstants.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, "SAS");
    set(UCHadoopConfConstants.FS_AZURE_ACCOUNT_IS_HNS_ENABLED, "true");
    set("fs.abfs.impl.disable.cache", "true");
    set("fs.abfss.impl.disable.cache", "true");
  }

  @Override
  protected void writeImplOverrides(Configuration conf) {
    saveAndOverride(
        conf,
        "fs.abfs.impl",
        "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem",
        CRED_SCOPED_FS_CLASS);
    saveAndOverride(
        conf,
        "fs.abfss.impl",
        "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
        CRED_SCOPED_FS_CLASS);
    saveAndOverride(
        conf,
        "fs.AbstractFileSystem.abfs.impl",
        "org.apache.hadoop.fs.azurebfs.Abfs",
        CRED_SCOPED_AFS_CLASS);
    saveAndOverride(
        conf,
        "fs.AbstractFileSystem.abfss.impl",
        "org.apache.hadoop.fs.azurebfs.Abfss",
        CRED_SCOPED_AFS_CLASS);
  }

  @Override
  protected void applyVendedProviderKeys() {
    set(UCHadoopConfConstants.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, ABFS_VENDED_TOKEN_PROVIDER_CLASS);
  }

  @Override
  public CredPropsBuilder writeCredKeys(String prefix, boolean renewable, GenericCredential cred) {
    AzureCredential azure = (AzureCredential) cred;
    if (renewable) {
      set(prefix, UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN, azure.sasToken());
      // Expiration may be absent (e.g. a static token provider), so write the key only when set.
      if (azure.expirationTimeMillis() != null) {
        set(
            prefix,
            UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME,
            String.valueOf(azure.expirationTimeMillis()));
      }
    } else {
      set(prefix, ABFS_FIXED_SAS_TOKEN_KEY, azure.sasToken());
    }
    return this;
  }
}
