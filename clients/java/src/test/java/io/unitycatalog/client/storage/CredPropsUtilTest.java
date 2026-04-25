package io.unitycatalog.client.storage;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CredPropsUtilTest {

  @Test
  void createRenewableS3TableCredentialProps() {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            true,
            false,
            Collections.emptyMap(),
            "s3",
            "http://uc",
            staticTokenProvider(),
            "table-id",
            TableOperation.READ,
            s3Creds());

    assertThat(props)
        .containsEntry(
            UCHadoopConf.S3A_CREDENTIALS_PROVIDER, AwsVendedTokenProvider.class.getName())
        .containsEntry(UCHadoopConf.UC_URI_KEY, "http://uc")
        .containsEntry(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY, "table")
        .containsEntry(UCHadoopConf.UC_TABLE_ID_KEY, "table-id")
        .containsEntry(UCHadoopConf.UC_TABLE_OPERATION_KEY, "READ")
        .containsEntry(UCHadoopConf.S3A_INIT_ACCESS_KEY, "ak")
        .containsEntry(UCHadoopConf.S3A_INIT_SECRET_KEY, "sk")
        .containsEntry(UCHadoopConf.S3A_INIT_SESSION_TOKEN, "st")
        .containsEntry(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME, "1234")
        .containsEntry(UCHadoopConf.UC_AUTH_TYPE, "static")
        .containsEntry(UCHadoopConf.UC_AUTH_TOKEN_KEY, "token");
  }

  @Test
  void createStaticS3TableCredentialProps() {
    Map<String, String> props =
        CredPropsUtil.createTableCredProps(
            false,
            false,
            Collections.emptyMap(),
            "s3",
            "http://uc",
            null,
            "table-id",
            TableOperation.READ,
            s3Creds());

    assertThat(props)
        .containsEntry("fs.s3a.access.key", "ak")
        .containsEntry("fs.s3a.secret.key", "sk")
        .containsEntry("fs.s3a.session.token", "st")
        .doesNotContainKey(UCHadoopConf.S3A_CREDENTIALS_PROVIDER)
        .doesNotContainKey(UCHadoopConf.UC_URI_KEY);
  }

  private static TokenProvider staticTokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "token"));
  }

  private static TemporaryCredentials s3Creds() {
    return new TemporaryCredentials()
        .awsTempCredentials(
            new AwsCredentials().accessKeyId("ak").secretAccessKey("sk").sessionToken("st"))
        .expirationTime(1234L);
  }
}
