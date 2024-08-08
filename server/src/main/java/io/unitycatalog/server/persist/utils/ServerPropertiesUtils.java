package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSStorageConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerPropertiesUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerPropertiesUtils.class);
  @Getter private static final ServerPropertiesUtils instance = new ServerPropertiesUtils();
  private final Properties properties;

  private ServerPropertiesUtils() {
    properties = new Properties();
    loadProperties();
  }

  // Load properties from a configuration file
  private void loadProperties() {
    try (InputStream input = Files.newInputStream(Paths.get("etc/conf/server.properties"))) {
      properties.load(input);
      LOGGER.debug("Properties loaded successfully");
    } catch (IOException ex) {
      LOGGER.error("Exception during loading properties", ex);
    }
  }

  // Get a property value by key
  public String getProperty(String key) {
    if (System.getProperty(key) != null) return System.getProperty(key);
    if (System.getenv().containsKey(key)) return System.getenv(key);
    return properties.getProperty(key);
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
      if (bucketPath == null || region == null || awsRoleArn == null) {
        break;
      }
      S3StorageConfig s3StorageConfig =
          S3StorageConfig.builder()
              .bucketPath(bucketPath)
              .region(region)
              .awsRoleArn(awsRoleArn)
              .accessKey(accessKey)
              .secretKey(secretKey)
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
              .build());
      i++;
    }

    return adlsConfigMap;
  }

  // Get a property value by key with a default value
  public String getProperty(String key, String defaultValue) {
    return properties.getProperty(key, defaultValue);
  }
}
