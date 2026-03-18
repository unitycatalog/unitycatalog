package io.unitycatalog.spark.auth;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link CredPropsUtil} saves the original {@code fs.<scheme>.impl} values under
 * {@code fs.<scheme>.impl.original} before overriding them with {@link
 * io.unitycatalog.spark.fs.CredScopedFileSystem}, so that the real delegate can be restored in
 * {@code CredScopedFileSystem#newFileSystem}.
 */
class CredPropsUtilTest {

  private static final String CUSTOM_S3_IMPL = "com.example.CustomS3FileSystem";
  private static final String CUSTOM_GS_IMPL = "com.example.CustomGcsFileSystem";
  private static final String CUSTOM_ABFS_IMPL = "com.example.CustomAbfsFileSystem";
  private static final String CUSTOM_ABFSS_IMPL = "com.example.CustomAbfssFileSystem";

  @Test
  void s3OriginalImplPreservedFromExistingProps() {
    Map<String, String> existingProps =
        Map.of("fs.s3.impl", CUSTOM_S3_IMPL, "fs.s3a.impl", CUSTOM_S3_IMPL);

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            existingProps,
            "s3",
            "http://uc",
            null,
            "tid",
            TableOperation.READ_WRITE,
            s3Creds());

    assertThat(props.get("fs.s3.impl.original")).isEqualTo(CUSTOM_S3_IMPL);
    assertThat(props.get("fs.s3a.impl.original")).isEqualTo(CUSTOM_S3_IMPL);
  }

  @Test
  void s3DefaultOriginalImplWhenNotInExistingProps() {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            Collections.emptyMap(),
            "s3",
            "http://uc",
            null,
            "tid",
            TableOperation.READ_WRITE,
            s3Creds());

    assertThat(props.get("fs.s3.impl.original"))
        .isEqualTo("org.apache.hadoop.fs.s3a.S3AFileSystem");
    assertThat(props.get("fs.s3a.impl.original"))
        .isEqualTo("org.apache.hadoop.fs.s3a.S3AFileSystem");
  }

  @Test
  void gsOriginalImplPreservedFromExistingProps() {
    Map<String, String> existingProps = Map.of("fs.gs.impl", CUSTOM_GS_IMPL);

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            existingProps,
            "gs",
            "http://uc",
            null,
            "tid",
            TableOperation.READ_WRITE,
            gcsCreds());

    assertThat(props.get("fs.gs.impl.original")).isEqualTo(CUSTOM_GS_IMPL);
  }

  @Test
  void abfsOriginalImplPreservedFromExistingProps() {
    Map<String, String> existingProps =
        Map.of("fs.abfs.impl", CUSTOM_ABFS_IMPL, "fs.abfss.impl", CUSTOM_ABFSS_IMPL);

    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            existingProps,
            "abfs",
            "http://uc",
            null,
            "tid",
            TableOperation.READ_WRITE,
            abfsCreds());

    assertThat(props.get("fs.abfs.impl.original")).isEqualTo(CUSTOM_ABFS_IMPL);
    assertThat(props.get("fs.abfss.impl.original")).isEqualTo(CUSTOM_ABFSS_IMPL);
  }

  @Test
  void gsDefaultOriginalImplWhenNotInExistingProps() {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            Collections.emptyMap(),
            "gs",
            "http://uc",
            null,
            "tid",
            TableOperation.READ_WRITE,
            gcsCreds());

    assertThat(props.get("fs.gs.impl.original"))
        .isEqualTo("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
  }

  @Test
  void abfsDefaultOriginalImplWhenNotInExistingProps() {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            true,
            Collections.emptyMap(),
            "abfs",
            "http://uc",
            null,
            "tid",
            TableOperation.READ_WRITE,
            abfsCreds());

    assertThat(props.get("fs.abfs.impl.original"))
        .isEqualTo("org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
    assertThat(props.get("fs.abfss.impl.original"))
        .isEqualTo("org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");
  }

  @Test
  void originalImplNotSetWhenCredScopedFsDisabled() {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            false,
            Collections.emptyMap(),
            "s3",
            "http://uc",
            null,
            "tid",
            TableOperation.READ_WRITE,
            s3Creds());

    assertThat(props).doesNotContainKey("fs.s3.impl.original");
    assertThat(props).doesNotContainKey("fs.s3a.impl.original");
  }

  private static TemporaryCredentials s3Creds() {
    return new TemporaryCredentials()
        .awsTempCredentials(
            new AwsCredentials().accessKeyId("ak").secretAccessKey("sk").sessionToken("st"));
  }

  private static TemporaryCredentials gcsCreds() {
    return new TemporaryCredentials()
        .gcpOauthToken(new GcpOauthToken().oauthToken("token"))
        .expirationTime(Long.MAX_VALUE);
  }

  private static TemporaryCredentials abfsCreds() {
    return new TemporaryCredentials()
        .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("sas"));
  }
}
