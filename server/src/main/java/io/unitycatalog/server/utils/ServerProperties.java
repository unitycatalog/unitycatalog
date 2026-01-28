package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSStorageConfig;
import io.unitycatalog.server.service.credential.gcp.GcsStorageConfig;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.IntPredicate;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerProperties {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerProperties.class);
  private final Properties properties = new Properties(generateDefaults());

  /** Validator interface for property values */
  private interface PropertyValidator {
    /**
     * Validates a property value
     *
     * @param key the property key
     * @param value the property value to validate
     * @throws BaseException if validation fails
     */
    void validate(String key, String value);
  }

  /** Validator for enum-like properties with a fixed set of allowed values */
  private static class EnumValidator implements PropertyValidator {
    private final Set<String> allowedValues;
    private final boolean caseInsensitive;

    EnumValidator(boolean caseInsensitive, String... allowedValues) {
      this.caseInsensitive = caseInsensitive;
      this.allowedValues = new HashSet<>();
      for (String value : allowedValues) {
        this.allowedValues.add(caseInsensitive ? value.toLowerCase() : value);
      }
    }

    @Override
    public void validate(String key, String value) {
      String checkValue = caseInsensitive ? value.toLowerCase() : value;
      if (!allowedValues.contains(checkValue)) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            String.format(
                "Invalid value '%s' for property '%s'. Allowed values: %s",
                value, key, allowedValues));
      }
    }
  }

  /** Validator for boolean properties */
  private static class BooleanValidator extends EnumValidator {
    BooleanValidator() {
      super(/* caseInsensitive= */ true, "true", "false");
    }
  }

  /** Validator for URL properties */
  private static class UrlValidator implements PropertyValidator {
    @Override
    public void validate(String key, String value) {
      try {
        new URL(value).toURI();
      } catch (MalformedURLException | URISyntaxException | IllegalArgumentException e) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            String.format("Invalid URL '%s' for property '%s': %s", value, key, e.getMessage()));
      }
    }
  }

  /**
   * Validator for storage path properties that can be either URLs or file paths. This is more
   * lenient than UrlValidator as it accepts both proper URLs (e.g., file:///tmp/path) and plain
   * file paths (e.g., /tmp/path).
   */
  private static class StoragePathValidator implements PropertyValidator {
    @Override
    public void validate(String key, String value) {
      try {
        NormalizedURL.from(value);
      } catch (Exception e) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            String.format(
                "Invalid storage path '%s' for property '%s': %s", value, key, e.getMessage()));
      }
    }
  }

  /** Validator for integer properties with a custom condition */
  private static class IntegerValidator implements PropertyValidator {
    private final IntPredicate condition;
    private final String conditionDescription;

    /**
     * Creates an IntegerValidator with a custom condition.
     *
     * @param condition the condition that the integer value must satisfy
     * @param conditionDescription human-readable description of the condition for error messages
     */
    IntegerValidator(IntPredicate condition, String conditionDescription) {
      this.condition = condition;
      this.conditionDescription = conditionDescription;
    }

    @Override
    public void validate(String key, String value) {
      try {
        int intValue = Integer.parseInt(value);
        if (!condition.test(intValue)) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT,
              String.format(
                  "Invalid value '%s' for property '%s'. Expected %s",
                  value, key, conditionDescription));
        }
      } catch (NumberFormatException e) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            String.format("Invalid value '%s' for property '%s'. Expected an integer", value, key));
      }
    }
  }

  /** Validator for positive integer properties */
  private static class PositiveIntegerValidator extends IntegerValidator {
    PositiveIntegerValidator() {
      super(i -> i > 0, "a positive integer (> 0)");
    }
  }

  /**
   * Validator for the string format of {@code java.time.Duration}. Check function {@Duration.parse}
   * for all the accepted forms.
   */
  private static class DurationValidator implements PropertyValidator {
    @Override
    public void validate(String key, String value) {
      try {
        Duration.parse(value);
      } catch (DateTimeParseException e) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            String.format("Invalid value '%s' for property '%s': %s", value, key, e.getMessage()));
      }
    }
  }

  /** No-op validator that accepts any value */
  private static class NoOpValidator implements PropertyValidator {
    @Override
    public void validate(String key, String value) {
      // Accept any value
    }
  }

  private static final BooleanValidator BOOLEAN_VALIDATOR = new BooleanValidator();
  private static final UrlValidator URL_VALIDATOR = new UrlValidator();
  private static final StoragePathValidator STORAGE_PATH_VALIDATOR = new StoragePathValidator();
  private static final PositiveIntegerValidator POSITIVE_INTEGER_VALIDATOR =
      new PositiveIntegerValidator();
  private static final NoOpValidator NOOP_VALIDATOR = new NoOpValidator();
  private static final DurationValidator DURATION_VALIDATOR = new DurationValidator();

  @Getter
  public enum Property {
    SERVER_ENV("server.env", "dev", new EnumValidator(true, "dev", "prod", "test")),
    AUTHORIZATION_ENABLED(
        "server.authorization", "disable", new EnumValidator(true, "enable", "disable")),
    AUTHORIZATION_URL("server.authorization-url", URL_VALIDATOR),
    TOKEN_URL("server.token-url", URL_VALIDATOR),
    CLIENT_ID("server.client-id"),
    CLIENT_SECRET("server.client-secret"),
    REDIRECT_PORT("server.redirect-port", POSITIVE_INTEGER_VALIDATOR),
    COOKIE_TIMEOUT("server.cookie-timeout", "P5D", DURATION_VALIDATOR),
    MANAGED_TABLE_ENABLED("server.managed-table.enabled", "false", BOOLEAN_VALIDATOR),
    MODEL_STORAGE_ROOT("storage-root.models", "file:///tmp/ucroot", STORAGE_PATH_VALIDATOR),
    TABLE_STORAGE_ROOT("storage-root.tables", "file:///tmp/ucroot", STORAGE_PATH_VALIDATOR),
    AWS_MASTER_ROLE_ARN("aws.masterRoleArn"),
    AWS_S3_ACCESS_KEY("aws.s3.accessKey"),
    AWS_S3_SECRET_KEY("aws.s3.secretKey"),
    AWS_S3_SESSION_TOKEN("aws.s3.sessionToken"),
    AWS_REGION("aws.region"),
    UNIFORM_ICEBERG_ENABLED(
        "server.uniform-iceberg.enabled",
        "false",
        BOOLEAN_VALIDATOR);
    // The is not an exhaustive list. Some property keys like s3.bucketPath.0 with a numbering
    // suffix is not included. They are only accessed internally from functions like
    // getS3Configurations.

    private final String key;
    private final String defaultValue;
    private final PropertyValidator validator;

    Property(String key, String defaultValue, PropertyValidator validator) {
      this.key = key;
      this.defaultValue = defaultValue;
      this.validator = validator;
      // Default values are always valid. They are validated by
      // ServerPropertiesTest.testDefaultPropertyValues
    }

    Property(String key, PropertyValidator validator) {
      this(key, null, validator);
    }

    Property(String key) {
      this(key, null, NOOP_VALIDATOR);
    }
  }

  public ServerProperties() {
    // This call is still useful after the default values being validated by constructors of
    // Property because it also validates values picked up from system property and env.
    validateProperties();
  }

  public ServerProperties(String propertiesFile) {
    readPropertiesFromFile(propertiesFile);
    validateProperties();
  }

  public ServerProperties(Properties inputProperties) {
    properties.putAll(inputProperties);
    validateProperties();
  }

  /**
   * Validates all registered properties against their validators. This method is called during
   * initialization to fail fast if any property has an invalid value. It also validates any
   * override value from system property and env if set.
   *
   * @throws BaseException if any property value is invalid
   */
  private void validateProperties() {
    for (Property property : Property.values()) {
      String value = get(property);
      if (value == null) {
        // null means not set.
        continue;
      }
      property.validator.validate(property.key, value);
    }
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
        // Remove empty values. In server.properties these lines are for demonstration purpose.
        properties
            .entrySet()
            .removeIf(
                entry -> entry.getValue() == null || entry.getValue().toString().trim().isEmpty());
        LOGGER.debug("Server properties loaded successfully: {}", path);
      } catch (IOException ex) {
        LOGGER.error("Exception during loading properties", ex);
      }
    }
  }

  public Map<NormalizedURL, S3StorageConfig> getS3Configurations() {
    Map<NormalizedURL, S3StorageConfig> s3BucketConfigMap = new HashMap<>();
    int i = 0;
    while (true) {
      String bucketPath = getProperty("s3.bucketPath." + i);
      String region = getProperty("s3.region." + i);
      String awsRoleArn = getProperty("s3.awsRoleArn." + i);
      String accessKey = getProperty("s3.accessKey." + i);
      String secretKey = getProperty("s3.secretKey." + i);
      String sessionToken = getProperty("s3.sessionToken." + i);
      String credentialsGenerator = getProperty("s3.credentialsGenerator." + i);
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
              .credentialsGenerator(credentialsGenerator)
              .build();
      s3BucketConfigMap.put(NormalizedURL.from(bucketPath), s3StorageConfig);
      i++;
    }

    return s3BucketConfigMap;
  }

  public Map<NormalizedURL, GcsStorageConfig> getGcsConfigurations() {
    Map<NormalizedURL, GcsStorageConfig> gcsConfigMap = new HashMap<>();
    int i = 0;
    while (true) {
      String bucketPath = getProperty("gcs.bucketPath." + i);
      if (bucketPath == null) {
        break;
      }
      String jsonKeyFilePath = getProperty("gcs.jsonKeyFilePath." + i);
      String credentialsGenerator = getProperty("gcs.credentialsGenerator." + i);
      gcsConfigMap.put(
          NormalizedURL.from(bucketPath),
          GcsStorageConfig.builder()
              .bucketPath(bucketPath)
              .jsonKeyFilePath(jsonKeyFilePath)
              .credentialsGenerator(credentialsGenerator)
              .build());
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
      String credentialsGenerator = getProperty("adls.credentialsGenerator." + i);
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
              .credentialsGenerator(credentialsGenerator)
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
    // Validate before setting
    property.validator.validate(property.key, value);
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
