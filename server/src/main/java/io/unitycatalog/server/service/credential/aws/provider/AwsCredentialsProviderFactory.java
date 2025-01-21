package io.unitycatalog.server.service.credential.aws.provider;

import com.amazonaws.util.StringUtils;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.ServerProperties;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class AwsCredentialsProviderFactory {

  public Map<String, AwsCredentialsProvider> loadProviders(ServerProperties serverProperties) {
    final Map<String, AwsCredentialsProviderConfig> providerConfigurations =
        serverProperties.getAwsCredentialProviderConfigurations();

    // Create a cache of providers from configuration
    final Map<String, AwsCredentialsProvider> providersCache =
        providerConfigurations.entrySet().stream()
            .collect(
                Collectors.toMap(Map.Entry::getKey, entry -> createProvider(entry.getValue())));

    log.info("Loaded providers {}", providersCache.keySet());

    final Map<String, AwsCredentialsProvider> providers = new HashMap<>();

    // Map providers to buckets
    serverProperties
        .getS3Configurations()
        .forEach(
            (bucket, s3Configuration) -> {
              final String sessionToken = s3Configuration.getSessionToken();
              final String roleArn = s3Configuration.getAwsRoleArn();
              final String providerName = s3Configuration.getProvider();
              final String accessKey = s3Configuration.getAccessKey();
              final String secretKey = s3Configuration.getSecretKey();
              final String region = s3Configuration.getRegion();
              AwsCredentialsProvider provider;

              log.info("Configuring {} with provider {}", bucket, providerName);

              // If no provider name given - fall back to provider configuration from s3
              // configuration
              if (providerName == null || providerName.isBlank()) {
                // Session token present -> Static credentials provider
                if (sessionToken != null && !sessionToken.isBlank()) {
                  final String staticProviderName =
                      "static-provider-" + StringUtils.fromInteger(sessionToken.hashCode());
                  provider = providersCache.get(staticProviderName);
                  if (provider == null) {
                    final AwsCredentialsProviderConfig providerConfiguration =
                        new AwsCredentialsProviderConfig();
                    providerConfiguration.put(
                        AwsCredentialsProviderConfig.PROVIDER_CLASS,
                        StaticCredentialsProvider.class.getCanonicalName());
                    providerConfiguration.put(
                        AwsCredentialsProviderConfig.ACCESS_KEY_ID, accessKey);
                    providerConfiguration.put(
                        AwsCredentialsProviderConfig.SECRET_ACCESS_KEY, secretKey);
                    providerConfiguration.put(
                        StaticCredentialsProvider.SESSION_TOKEN, sessionToken);
                    provider = createProvider(providerConfiguration);
                    providersCache.put(
                        staticProviderName,
                        provider); // Is same session key will be found provider will be reused
                  }
                } else if (roleArn != null && !roleArn.isBlank()) {
                  final String stsProviderName =
                      "sts-provider-" + StringUtils.fromInteger(roleArn.hashCode());
                  provider = providersCache.get(stsProviderName);
                  if (provider == null) {
                    final AwsCredentialsProviderConfig providerConfiguration =
                        new AwsCredentialsProviderConfig();
                    providerConfiguration.put(
                        AwsCredentialsProviderConfig.PROVIDER_CLASS,
                        StsCredentialsProvider.class.getCanonicalName());
                    providerConfiguration.put(
                        AwsCredentialsProviderConfig.ACCESS_KEY_ID, accessKey);
                    providerConfiguration.put(
                        AwsCredentialsProviderConfig.SECRET_ACCESS_KEY, secretKey);
                    providerConfiguration.put(StsCredentialsProvider.ROLE_ARN, roleArn);
                    providerConfiguration.put(StsCredentialsProvider.REGION, region);
                    provider = createProvider(providerConfiguration);
                    providersCache.put(
                        stsProviderName,
                        provider); // Is same role arn will be found provider will be reused
                  }
                } else {
                  throw new BaseException(
                      ErrorCode.FAILED_PRECONDITION,
                      "No credentials provider configured for bucket "
                          + bucket
                          + ". Please provide either credentials provider reference or in-place configuration");
                }
              } else {
                provider = providersCache.get(providerName);
                if (provider == null) {
                  throw new BaseException(
                      ErrorCode.FAILED_PRECONDITION,
                      "Provider " + providerName + " is not defined");
                }
              }

              providers.put(bucket, provider);
            });

    return providers;
  }

  private AwsCredentialsProvider createProvider(AwsCredentialsProviderConfig configuration) {
    final String providerClass = configuration.getProviderClass();
    try {
      final Class<?> clz = this.getClass().getClassLoader().loadClass(providerClass);
      if (!AwsCredentialsProvider.class.isAssignableFrom(clz)) {
        throw new BaseException(
            ErrorCode.FAILED_PRECONDITION,
            providerClass
                + " is not a subclass of "
                + AwsCredentialsProvider.class.getCanonicalName());
      }
      @SuppressWarnings("unchecked")
      final Class<? extends AwsCredentialsProvider> providerClz =
          (Class<? extends AwsCredentialsProvider>) clz;
      final AwsCredentialsProvider provider = providerClz.getDeclaredConstructor().newInstance();
      return provider.configure(configuration);
    } catch (ClassNotFoundException e) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "Aws credentials provider class " + providerClass + " is not found.");
    } catch (NoSuchMethodException e) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION, "No default constructor in class " + providerClass);
    } catch (InvocationTargetException | InstantiationException e) {
      throw new BaseException(
          ErrorCode.INTERNAL, "Class " + providerClass + " could not be instantiated.");
    } catch (IllegalAccessException e) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "Default constructor of class " + providerClass + " is not accessible.");
    }
  }
}
