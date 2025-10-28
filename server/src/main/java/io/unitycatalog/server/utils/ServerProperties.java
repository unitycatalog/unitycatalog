package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSStorageConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerProperties {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerProperties.class);
  private final Properties properties = new Properties(generateDefaults());

  @Getter
  public enum Property {
    SERVER_ENV("server.env"),
    AUTHORIZATION_ENABLED("server.authorization", "disable"),
    AUTHORIZATION_URL("server.authorization-url"),
    TOKEN_URL("server.token-url"),
    CLIENT_ID("server.client-id"),
    CLIENT_SECRET("server.client-secret"),
    REDIRECT_PORT("server.redirect-port"),
    COOKIE_TIMEOUT("server.cookie-timeout", "P5D"),
    MANAGED_TABLE_ENABLED("server.managed-table.enabled", "false"),
    MODEL_STORAGE_ROOT("storage-root.models", "file:///tmp/ucroot"),
    TABLE_STORAGE_ROOT("storage-root.tables", "file:///tmp/ucroot"),
    AWS_S3_ACCESS_KEY("aws.s3.accessKey"),
    AWS_S3_SECRET_KEY("aws.s3.secretKey"),
    AWS_S3_SESSION_TOKEN("aws.s3.sessionToken"),
    AWS_REGION("aws.region"),
    AWS_S3_ENDPOINT_URL("aws.s3.endpointUrl");
    // The is not an exhaustive list. Some property keys like s3.bucketPath.0 with a numbering
    // suffix is not included. They are only accessed internally from functions like
    // getS3Configurations.

    private final String key;
    private final String defaultValue;

    Property(String key, String defaultValue) {
      this.key = key;
      this.defaultValue = defaultValue;
    }

    Property(String key) {
      this(key, null);
    }
  }

  public ServerProperties() {}

  public ServerProperties(String propertiesFile) {
    readPropertiesFromFile(propertiesFile);
  }

  public ServerProperties(Properties inputProperties) {
    properties.putAll(inputProperties);
  }

  // Load properties from a configuration file
  private static Properties generateDefaults() {
    Properties defaults = new Properties();
    Arrays.stream(Property.values())
        .filter(property -> property.defaultValue != null)
        .forEach(property -> defaults.setProperty(property.key, property.defaultValue));
    return defaults;
  }

  // Load properties from a configuration file
  private void readPropertiesFromFile(String propertiesFile) {
    Path path = Paths.get(propertiesFile);
    if (path.toFile().exists()) {
      try (InputStream input = Files.newInputStream(path)) {
        properties.load(input);
        LOGGER.debug("Server properties loaded successfully: {}", path);
      } catch (IOException ex) {
        LOGGER.error("Exception during loading properties", ex);
      }
    }
  }

  public Map<String, S3StorageConfig> getS3Configurations() {
    Map<String, S3StorageConfig> s3BucketConfigMap = new HashMap<>();
    int i = 0;
    while (true) {
      String bucketPath = getProperty("s3.bucketPath." + i);
      String region = getProperty("s3.region." + i);
      String awsRoleArn = getProperty("s3.awsRoleArn." + i);
      String accessKey = getProperty("s3.accessKey." + i);
      String secretKey = getProperty("s3.secretKey." + i);
      String sessionToken = getProperty("s3.sessionToken." + i);
      String credentialsGenerator = getProperty("s3.credentialsGenerator." + i);
      String endpointUrl = getProperty("s3.endpointUrl." + i);
      if ((bucketPath == null || region == null || awsRoleArn == null)
          && (accessKey == null || secretKey == null || sessionToken == null)) {
        break;
      }
      S3StorageConfig s3StorageConfig =
          S3StorageConfig.builder()
              .bucketPath(bucketPath)
              .region(region)
              .awsRoleArn(awsRoleArn)
              .accessKey(accessKey)
              .secretKey(secretKey)
              .sessionToken(sessionToken)
              .credentialGenerator(credentialsGenerator)
              .endpointUrl(endpointUrl)
              .build();
      s3BucketConfigMap.put(bucketPath, s3StorageConfig);
      i++;
    }

    return s3BucketConfigMap;
  }

  public Map<String, String> getGcsConfigurations() {
    Map<String, String> gcsConfigMap = new HashMap<>();
    int i = 0;
    while (true) {
      String bucketPath = getProperty("gcs.bucketPath." + i);
      String jsonKeyFilePath = getProperty("gcs.jsonKeyFilePath." + i);
      if (bucketPath == null || jsonKeyFilePath == null) {
        break;
      }
      gcsConfigMap.put(bucketPath, jsonKeyFilePath);
      i++;
    }

    return gcsConfigMap;
  }

  public Map<String, ADLSStorageConfig> getAdlsConfigurations() {
    Map<String, ADLSStorageConfig> adlsConfigMap = new HashMap<>();

    int i = 0;
    while (true) {
      String storageAccountName = getProperty("adls.storageAccountName." + i);
      String tenantId = getProperty("adls.tenantId." + i);
      String clientId = getProperty("adls.clientId." + i);
      String clientSecret = getProperty("adls.clientSecret." + i);
      String testMode = getProperty("adls.testMode." + i);
      if (storageAccountName == null
          || tenantId == null
          || clientId == null
          || clientSecret == null) {
        break;
      }
      adlsConfigMap.put(
          storageAccountName,
          ADLSStorageConfig.builder()
              .storageAccountName(storageAccountName)
              .tenantId(tenantId)
              .clientId(clientId)
              .clientSecret(clientSecret)
              .testMode(testMode != null && testMode.equalsIgnoreCase("true"))
              .build());
      i++;
    }

    return adlsConfigMap;
  }

  /**
   * Get a property value by key.
   *
   * <p>The key can be one of the following (in that order) before looking it up in the server
   * properties:
   *
   * <ol>
   *   <li>System property
   *   <li>Environment variable
   *   <li>server.properties file
   * </ol>
   *
   * <p>If it's not set, the default value is returned.
   */
  public String get(Property property) {
    return getProperty(property.key);
  }

  /** Get a property value by key name. */
  private String getProperty(String key) {
    if (System.getProperty(key) != null) {
      return System.getProperty(key);
    }
    if (System.getenv().containsKey(key)) {
      return System.getenv(key);
    }
    // Finally try properties. If not found in properties, this would return default value or null
    // if no default value.
    return properties.getProperty(key);
  }

  public void set(Property property, String value) {
    properties.setProperty(property.key, value);
  }

  private boolean isTrueOrEnable(String value) {
    return value != null && (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("enable"));
  }

  public boolean isAuthorizationEnabled() {
    return isTrueOrEnable(get(Property.AUTHORIZATION_ENABLED));
  }

  /**
   * Check if experimental MANAGED table feature is enabled. This method throws BaseException with
   * ErrorCode.INVALID_ARGUMENT if it's disabled.
   */
  public void checkManagedTableEnabled() {
    if (!isTrueOrEnable(get(Property.MANAGED_TABLE_ENABLED))) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "MANAGED table is an experimental feature and is currently disabled. "
              + "To enable it, set 'server.managed-table.enabled=true' in server.properties");
    }
  }
}
