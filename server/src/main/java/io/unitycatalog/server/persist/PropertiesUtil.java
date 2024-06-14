package io.unitycatalog.server.persist;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class PropertiesUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtil.class);
    @Getter
    private static final PropertiesUtil instance = new PropertiesUtil();
    private final Properties properties;

    private PropertiesUtil() {
        properties = new Properties();
        loadProperties();
    }

    // Load properties from a configuration file
    private void loadProperties() {
        String serverPropertiesPath = getServerPropertiesPath();
        try (InputStream input = Files.newInputStream(Paths.get(serverPropertiesPath))) {
            properties.load(input);
            LOGGER.debug("Properties loaded successfully, properties-path={}", serverPropertiesPath);
            int i = 0;
            while (true) {
                String bucketPath = properties.getProperty("s3.bucketPath." + i);
                String accessKey = properties.getProperty("s3.accessKey." + i);
                String secretKey = properties.getProperty("s3.secretKey." + i);
                String sessionToken = properties.getProperty("s3.sessionToken." + i);
                if (bucketPath == null || accessKey == null || secretKey == null || sessionToken == null) {
                    break;
                }
                S3BucketConfig s3BucketConfig = new S3BucketConfig(bucketPath, accessKey, secretKey, sessionToken);
                properties.put(bucketPath, s3BucketConfig);
                i++;
            }
        } catch (IOException ex) {
            LOGGER.error("Exception during loading properties, properties-path=" + serverPropertiesPath, ex);
        }
    }

    private String getServerPropertiesPath() {
        String serverPropertiesKey = "SERVER_PROPERTIES_FILE";
        if (System.getProperty(serverPropertiesKey) != null)
            return System.getProperty(serverPropertiesKey);
        if (System.getenv().containsKey(serverPropertiesKey))
            return System.getenv(serverPropertiesKey);
        return "etc/conf/server.properties";
    }

    // Get a property value by key
    public String getProperty(String key) {
        if (System.getProperty(key) != null)
            return System.getProperty(key);
        if (System.getenv().containsKey(key))
            return System.getenv(key);
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

    public String getLogPropertiesPath(String instance) {
        String getServerPropertiesFile = getProperty(instance.toUpperCase() + "_LOG4J_CONFIGURATION_FILE");
        if (getServerPropertiesFile != null) {
            return getServerPropertiesFile;
        } else {
            return "etc/conf/" + instance + ".log4j2.properties";
        }
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