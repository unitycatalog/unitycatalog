package io.unitycatalog.spark.auth;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.UCHadoopConf;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class CredPropsUtilTest {

  private static final String TEST_URI = "http://localhost:8080";
  private static final String TEST_TOKEN = "test-token";
  private static final String TEST_TABLE_ID = "test-table-id";
  private static final String TEST_PATH = "s3://test-bucket/test-path";
  private static final String CUSTOM_ENDPOINT = "https://s3.custom-endpoint.example.com";

  private TemporaryCredentials createAwsCredentials(boolean withEndpoint, boolean withExpiration) {
    AwsCredentials awsCred = new AwsCredentials();
    awsCred.setAccessKeyId("test-access-key");
    awsCred.setSecretAccessKey("test-secret-key");
    awsCred.setSessionToken("test-session-token");

    TemporaryCredentials tempCred = new TemporaryCredentials();
    tempCred.setAwsTempCredentials(awsCred);

    if (withExpiration) {
      tempCred.setExpirationTime(System.currentTimeMillis() + 3600000L); // 1 hour from now
    }

    if (withEndpoint) {
      tempCred.setEndpointUrl(CUSTOM_ENDPOINT);
    }

    return tempCred;
  }

  @Test
  public void testCreateTableCredProps_FixedCredential_WithoutCustomEndpoint() {
    // Test fixed credentials (renewCredEnabled = false) without custom endpoint
    TemporaryCredentials tempCreds = createAwsCredentials(false, false);

    Map<String, String> props = CredPropsUtil.createTableCredProps(
        false, // renewCredEnabled
        "s3",
        TEST_URI,
        TEST_TOKEN,
        TEST_TABLE_ID,
        TableOperation.READ,
        tempCreds
    );

    // Verify basic S3 properties
    assertThat(props).containsEntry("fs.s3a.access.key", "test-access-key");
    assertThat(props).containsEntry("fs.s3a.secret.key", "test-secret-key");
    assertThat(props).containsEntry("fs.s3a.session.token", "test-session-token");

    // Verify common S3 properties
    assertThat(props).containsEntry("fs.s3a.path.style.access", "true");
    assertThat(props).containsEntry("fs.s3.impl.disable.cache", "true");
    assertThat(props).containsEntry("fs.s3a.impl.disable.cache", "true");

    // Verify no custom endpoint is set
    assertThat(props).doesNotContainKey("fs.s3a.endpoint");

    // Verify no credential provider is set (fixed credentials)
    assertThat(props).doesNotContainKey(UCHadoopConf.S3A_CREDENTIALS_PROVIDER);
  }

  @Test
  public void testCreateTableCredProps_FixedCredential_WithCustomEndpoint() {
    // Test fixed credentials (renewCredEnabled = false) with custom endpoint
    TemporaryCredentials tempCreds = createAwsCredentials(true, false);

    Map<String, String> props = CredPropsUtil.createTableCredProps(
        false, // renewCredEnabled
        "s3",
        TEST_URI,
        TEST_TOKEN,
        TEST_TABLE_ID,
        TableOperation.READ,
        tempCreds
    );

    // Verify basic S3 properties
    assertThat(props).containsEntry("fs.s3a.access.key", "test-access-key");
    assertThat(props).containsEntry("fs.s3a.secret.key", "test-secret-key");
    assertThat(props).containsEntry("fs.s3a.session.token", "test-session-token");

    // Verify custom endpoint is set
    assertThat(props).containsEntry("fs.s3a.endpoint", CUSTOM_ENDPOINT);

    // Verify no credential provider is set (fixed credentials)
    assertThat(props).doesNotContainKey(UCHadoopConf.S3A_CREDENTIALS_PROVIDER);
  }

  @Test
  public void testCreateTableCredProps_TempCredential_WithoutCustomEndpoint() {
    // Test temporary credentials (renewCredEnabled = true) without custom endpoint
    TemporaryCredentials tempCreds = createAwsCredentials(false, true);

    Map<String, String> props = CredPropsUtil.createTableCredProps(
        true, // renewCredEnabled
        "s3",
        TEST_URI,
        TEST_TOKEN,
        TEST_TABLE_ID,
        TableOperation.READ,
        tempCreds
    );

    // Verify credential provider is configured
    assertThat(props).containsEntry(UCHadoopConf.S3A_CREDENTIALS_PROVIDER,
        AwsVendedTokenProvider.class.getName());

    // Verify Unity Catalog configuration
    assertThat(props).containsEntry(UCHadoopConf.UC_URI_KEY, TEST_URI);
    assertThat(props).containsEntry(UCHadoopConf.UC_TOKEN_KEY, TEST_TOKEN);
    assertThat(props).containsKey(UCHadoopConf.UC_CREDENTIALS_UID_KEY);

    // Verify table-based credential configuration
    assertThat(props).containsEntry(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    assertThat(props).containsEntry(UCHadoopConf.UC_TABLE_ID_KEY, TEST_TABLE_ID);
    assertThat(props).containsEntry(UCHadoopConf.UC_TABLE_OPERATION_KEY,
        TableOperation.READ.getValue());

    // Verify initial credentials
    assertThat(props).containsEntry(UCHadoopConf.S3A_INIT_ACCESS_KEY, "test-access-key");
    assertThat(props).containsEntry(UCHadoopConf.S3A_INIT_SECRET_KEY, "test-secret-key");
    assertThat(props).containsEntry(UCHadoopConf.S3A_INIT_SESSION_TOKEN, "test-session-token");
    assertThat(props).containsKey(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME);

    // Verify no custom endpoint is set
    assertThat(props).doesNotContainKey("fs.s3a.endpoint");

    // Verify common S3 properties
    assertThat(props).containsEntry("fs.s3a.path.style.access", "true");
    assertThat(props).containsEntry("fs.s3.impl.disable.cache", "true");
    assertThat(props).containsEntry("fs.s3a.impl.disable.cache", "true");
  }

  @Test
  public void testCreateTableCredProps_TempCredential_WithCustomEndpoint() {
    // Test temporary credentials (renewCredEnabled = true) with custom endpoint
    TemporaryCredentials tempCreds = createAwsCredentials(true, true);

    Map<String, String> props = CredPropsUtil.createTableCredProps(
        true, // renewCredEnabled
        "s3",
        TEST_URI,
        TEST_TOKEN,
        TEST_TABLE_ID,
        TableOperation.READ,
        tempCreds
    );

    // Verify credential provider is configured
    assertThat(props).containsEntry(UCHadoopConf.S3A_CREDENTIALS_PROVIDER,
        AwsVendedTokenProvider.class.getName());

    // Verify Unity Catalog configuration
    assertThat(props).containsEntry(UCHadoopConf.UC_URI_KEY, TEST_URI);
    assertThat(props).containsEntry(UCHadoopConf.UC_TOKEN_KEY, TEST_TOKEN);
    assertThat(props).containsKey(UCHadoopConf.UC_CREDENTIALS_UID_KEY);

    // Verify table-based credential configuration
    assertThat(props).containsEntry(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    assertThat(props).containsEntry(UCHadoopConf.UC_TABLE_ID_KEY, TEST_TABLE_ID);
    assertThat(props).containsEntry(UCHadoopConf.UC_TABLE_OPERATION_KEY,
        TableOperation.READ.getValue());

    // Verify initial credentials
    assertThat(props).containsEntry(UCHadoopConf.S3A_INIT_ACCESS_KEY, "test-access-key");
    assertThat(props).containsEntry(UCHadoopConf.S3A_INIT_SECRET_KEY, "test-secret-key");
    assertThat(props).containsEntry(UCHadoopConf.S3A_INIT_SESSION_TOKEN, "test-session-token");
    assertThat(props).containsKey(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME);

    // Verify custom endpoint is set
    assertThat(props).containsEntry("fs.s3a.endpoint", CUSTOM_ENDPOINT);

    // Verify common S3 properties
    assertThat(props).containsEntry("fs.s3a.path.style.access", "true");
    assertThat(props).containsEntry("fs.s3.impl.disable.cache", "true");
    assertThat(props).containsEntry("fs.s3a.impl.disable.cache", "true");
  }

  @Test
  public void testCreatePathCredProps_FixedCredential_WithoutCustomEndpoint() {
    // Test fixed credentials (renewCredEnabled = false) without custom endpoint
    TemporaryCredentials tempCreds = createAwsCredentials(false, false);

    Map<String, String> props = CredPropsUtil.createPathCredProps(
        false, // renewCredEnabled
        "s3",
        TEST_URI,
        TEST_TOKEN,
        TEST_PATH,
        PathOperation.PATH_READ,
        tempCreds
    );

    // Verify basic S3 properties
    assertThat(props).containsEntry("fs.s3a.access.key", "test-access-key");
    assertThat(props).containsEntry("fs.s3a.secret.key", "test-secret-key");
    assertThat(props).containsEntry("fs.s3a.session.token", "test-session-token");

    // Verify common S3 properties
    assertThat(props).containsEntry("fs.s3a.path.style.access", "true");
    assertThat(props).containsEntry("fs.s3.impl.disable.cache", "true");
    assertThat(props).containsEntry("fs.s3a.impl.disable.cache", "true");

    // Verify no custom endpoint is set
    assertThat(props).doesNotContainKey("fs.s3a.endpoint");

    // Verify no credential provider is set (fixed credentials)
    assertThat(props).doesNotContainKey(UCHadoopConf.S3A_CREDENTIALS_PROVIDER);
  }

  @Test
  public void testCreatePathCredProps_FixedCredential_WithCustomEndpoint() {
    // Test fixed credentials (renewCredEnabled = false) with custom endpoint
    TemporaryCredentials tempCreds = createAwsCredentials(true, false);

    Map<String, String> props = CredPropsUtil.createPathCredProps(
        false, // renewCredEnabled
        "s3",
        TEST_URI,
        TEST_TOKEN,
        TEST_PATH,
        PathOperation.PATH_READ,
        tempCreds
    );

    // Verify basic S3 properties
    assertThat(props).containsEntry("fs.s3a.access.key", "test-access-key");
    assertThat(props).containsEntry("fs.s3a.secret.key", "test-secret-key");
    assertThat(props).containsEntry("fs.s3a.session.token", "test-session-token");

    // Verify custom endpoint is set
    assertThat(props).containsEntry("fs.s3a.endpoint", CUSTOM_ENDPOINT);

    // Verify no credential provider is set (fixed credentials)
    assertThat(props).doesNotContainKey(UCHadoopConf.S3A_CREDENTIALS_PROVIDER);

    // Verify common S3 properties
    assertThat(props).containsEntry("fs.s3a.path.style.access", "true");
    assertThat(props).containsEntry("fs.s3.impl.disable.cache", "true");
    assertThat(props).containsEntry("fs.s3a.impl.disable.cache", "true");
  }

  @Test
  public void testCreatePathCredProps_TempCredential_WithoutCustomEndpoint() {
    // Test temporary credentials (renewCredEnabled = true) without custom endpoint
    TemporaryCredentials tempCreds = createAwsCredentials(false, true);

    Map<String, String> props = CredPropsUtil.createPathCredProps(
        true, // renewCredEnabled
        "s3",
        TEST_URI,
        TEST_TOKEN,
        TEST_PATH,
        PathOperation.PATH_READ,
        tempCreds
    );

    // Verify credential provider is configured
    assertThat(props).containsEntry(UCHadoopConf.S3A_CREDENTIALS_PROVIDER,
        AwsVendedTokenProvider.class.getName());

    // Verify Unity Catalog configuration
    assertThat(props).containsEntry(UCHadoopConf.UC_URI_KEY, TEST_URI);
    assertThat(props).containsEntry(UCHadoopConf.UC_TOKEN_KEY, TEST_TOKEN);
    assertThat(props).containsKey(UCHadoopConf.UC_CREDENTIALS_UID_KEY);

    // Verify path-based credential configuration
    assertThat(props).containsEntry(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE);
    assertThat(props).containsEntry(UCHadoopConf.UC_PATH_KEY, TEST_PATH);
    assertThat(props).containsEntry(UCHadoopConf.UC_PATH_OPERATION_KEY,
        PathOperation.PATH_READ.getValue());

    // Verify initial credentials
    assertThat(props).containsEntry(UCHadoopConf.S3A_INIT_ACCESS_KEY, "test-access-key");
    assertThat(props).containsEntry(UCHadoopConf.S3A_INIT_SECRET_KEY, "test-secret-key");
    assertThat(props).containsEntry(UCHadoopConf.S3A_INIT_SESSION_TOKEN, "test-session-token");
    assertThat(props).containsKey(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME);

    // Verify no custom endpoint is set
    assertThat(props).doesNotContainKey("fs.s3a.endpoint");

    // Verify common S3 properties
    assertThat(props).containsEntry("fs.s3a.path.style.access", "true");
    assertThat(props).containsEntry("fs.s3.impl.disable.cache", "true");
    assertThat(props).containsEntry("fs.s3a.impl.disable.cache", "true");
  }

  @Test
  public void testCreatePathCredProps_TempCredential_WithCustomEndpoint() {
    // Test temporary credentials (renewCredEnabled = true) with custom endpoint
    TemporaryCredentials tempCreds = createAwsCredentials(true, true);

    Map<String, String> props = CredPropsUtil.createPathCredProps(
        true, // renewCredEnabled
        "s3",
        TEST_URI,
        TEST_TOKEN,
        TEST_PATH,
        PathOperation.PATH_READ,
        tempCreds
    );

    // Verify credential provider is configured
    assertThat(props).containsEntry(UCHadoopConf.S3A_CREDENTIALS_PROVIDER,
        AwsVendedTokenProvider.class.getName());

    // Verify Unity Catalog configuration
    assertThat(props).containsEntry(UCHadoopConf.UC_URI_KEY, TEST_URI);
    assertThat(props).containsEntry(UCHadoopConf.UC_TOKEN_KEY, TEST_TOKEN);
    assertThat(props).containsKey(UCHadoopConf.UC_CREDENTIALS_UID_KEY);

    // Verify path-based credential configuration
    assertThat(props).containsEntry(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE);
    assertThat(props).containsEntry(UCHadoopConf.UC_PATH_KEY, TEST_PATH);
    assertThat(props).containsEntry(UCHadoopConf.UC_PATH_OPERATION_KEY,
        PathOperation.PATH_READ.getValue());

    // Verify initial credentials
    assertThat(props).containsEntry(UCHadoopConf.S3A_INIT_ACCESS_KEY, "test-access-key");
    assertThat(props).containsEntry(UCHadoopConf.S3A_INIT_SECRET_KEY, "test-secret-key");
    assertThat(props).containsEntry(UCHadoopConf.S3A_INIT_SESSION_TOKEN, "test-session-token");
    assertThat(props).containsKey(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME);

    // Verify custom endpoint is set
    assertThat(props).containsEntry("fs.s3a.endpoint", CUSTOM_ENDPOINT);

    // Verify common S3 properties
    assertThat(props).containsEntry("fs.s3a.path.style.access", "true");
    assertThat(props).containsEntry("fs.s3.impl.disable.cache", "true");
    assertThat(props).containsEntry("fs.s3a.impl.disable.cache", "true");
  }
}

