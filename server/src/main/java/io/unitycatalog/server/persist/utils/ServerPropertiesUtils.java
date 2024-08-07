package io.unitycatalog.server.persist.utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
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
    Path path = Paths.get("etc/conf/server.properties");
    if (!path.toFile().exists()) {
      LOGGER.error("Properties file not found: {}", path);
      return;
    }
    try (InputStream input = Files.newInputStream(path)) {
      properties.load(input);
      LOGGER.debug("Properties loaded successfully");
      int i = 0;
      while (true) {
        String bucketPath = properties.getProperty("s3.bucketPath." + i);
        String accessKey = properties.getProperty("s3.accessKey." + i);
        String secretKey = properties.getProperty("s3.secretKey." + i);
        String sessionToken = properties.getProperty("s3.sessionToken." + i);
        if (bucketPath == null || accessKey == null || secretKey == null || sessionToken == null) {
          break;
        }
        S3BucketConfig s3BucketConfig =
            new S3BucketConfig(bucketPath, accessKey, secretKey, sessionToken);
        properties.put(bucketPath, s3BucketConfig);
        i++;
      }
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

  public S3BucketConfig getS3BucketConfig(String s3Path) {
    URI uri = URI.create(s3Path);
    String bucketPath = uri.getScheme() + "://" + uri.getHost();
    return (S3BucketConfig) properties.get(bucketPath);
  }

  // Get a property value by key with a default value
  public String getProperty(String key, String defaultValue) {
    return properties.getProperty(key, defaultValue);
  }

  @Getter
  @Setter
  @AllArgsConstructor
  public static class S3BucketConfig {
    private final String bucketPath;
    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;
  }
}
