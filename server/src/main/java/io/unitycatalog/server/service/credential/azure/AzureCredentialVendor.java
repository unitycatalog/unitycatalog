package io.unitycatalog.server.service.credential.azure;

import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AzureCredentialVendor {
  private final Map<String, ADLSStorageConfig> adlsConfigurations;
  private final Map<String, AzureCredentialsGenerator> credGenerators = new ConcurrentHashMap<>();

  public AzureCredentialVendor(ServerProperties serverProperties) {
    this.adlsConfigurations = serverProperties.getAdlsConfigurations();
  }

  public AzureCredential vendAzureCredential(CredentialContext ctx) {
    AzureCredentialsGenerator generator =
        credGenerators.compute(
            ctx.getStorageBase(),
            (storageBase, credGenerator) ->
                credGenerator == null
                    ? createAzureCredentialsGenerator(storageBase)
                    : credGenerator);

    return generator.generate(ctx);
  }

  private AzureCredentialsGenerator createAzureCredentialsGenerator(String storageBase) {
    ADLSLocationUtils.ADLSLocationParts locParts = ADLSLocationUtils.parseLocation(storageBase);
    ADLSStorageConfig config = adlsConfigurations.get(locParts.accountName());

    if (config == null) {
      return new AzureCredentialsGenerator.DatalakeCredentialsGenerator(null);
    }

    if (config.getCredentialsGenerator() != null) {
      try {
        return (AzureCredentialsGenerator)
            Class.forName(config.getCredentialsGenerator())
                .getDeclaredConstructor(ADLSStorageConfig.class)
                .newInstance(config);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else if (config.isTestMode()) {
      return new AzureCredentialsGenerator.StaticAzureCredentialsGenerator(config);
    } else {
      return new AzureCredentialsGenerator.DatalakeCredentialsGenerator(config);
    }
  }
}
