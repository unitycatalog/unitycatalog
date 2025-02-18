package io.unitycatalog.server.utils;

import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSStorageConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerProperties {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerProperties.class);
  private final Properties properties;

  public enum Property {
    MODEL_STORAGE_ROOT("storage-root.models");

    private final String key;

    Property(String key) {
      this.key = key;
    }

    public String getKey() {
      return key;
    }
  }

  public ServerProperties() {
    this(new Properties());
  }

  public ServerProperties(String propertiesFile) {
    this(readPropertiesFromFile(propertiesFile));
  }

  public ServerProperties(Properties properties) {
    this.properties = properties;
  }

  // Load properties from a configuration file
  private static Properties readPropertiesFromFile(String propertiesFile) {
    Path path = Paths.get(propertiesFile);
    Properties propertiesFromFile = new Properties();
    if (path.toFile().exists()) {
      try (InputStream input = Files.newInputStream(path)) {
        propertiesFromFile.load(input);
        LOGGER.debug("Server properties loaded successfully: {}", path);
      } catch (IOException ex) {
        LOGGER.error("Exception during loading properties", ex);
      }
    }
    return propertiesFromFile;
  }

  public Map<String, S3StorageConfig> getS3Configurations() {
    Map<String, S3StorageConfig> s3BucketConfigMap = new HashMap<>();
    int i = 0;
    while (true) {
      String bucketPath = properties.getProperty("s3.bucketPath." + i);
      String region = properties.getProperty("s3.region." + i);
      String awsRoleArn = properties.getProperty("s3.awsRoleArn." + i);
      String accessKey = properties.getProperty("s3.accessKey." + i);
      String secretKey = properties.getProperty("s3.secretKey." + i);
      String sessionToken = properties.getProperty("s3.sessionToken." + i);
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
      String bucketPath = properties.getProperty("gcs.bucketPath." + i);
      String jsonKeyFilePath = properties.getProperty("gcs.jsonKeyFilePath." + i);
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
      String storageAccountName = properties.getProperty("adls.storageAccountName." + i);
      String tenantId = properties.getProperty("adls.tenantId." + i);
      String clientId = properties.getProperty("adls.clientId." + i);
      String clientSecret = properties.getProperty("adls.clientSecret." + i);
      String testMode = properties.getProperty("adls.testMode." + i);
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
   * </ol>
   */
  public String getProperty(String key) {
    if (System.getProperty(key) != null) return System.getProperty(key);
    if (System.getenv().containsKey(key)) return System.getenv(key);
    return properties.getProperty(key);
  }

  public String get(Property property) {
    return getProperty(property.getKey());
  }

  public void set(Property property, String value) {
    properties.setProperty(property.getKey(), value);
  }

  /**
   * Get a property value by key with a default value
   *
   * @see Properties#getProperty(String key, String defaultValue)
   */
  public String getProperty(String key, String defaultValue) {
    String val = getProperty(key);
    return (val == null) ? defaultValue : val;
  }

  public boolean isAuthorizationEnabled() {
    String authorization = getProperty("server.authorization", "disable");
    return authorization.equalsIgnoreCase("enable");
  }
}
