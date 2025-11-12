package io.unitycatalog.server.service.credential.azure;

import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AzureCredentialVendor {
  private final Map<String, ADLSStorageConfig> adlsConfigurations;
  private final Map<String, AzureCredentialGenerator> credGenerators = new ConcurrentHashMap<>();

  public AzureCredentialVendor(ServerProperties serverProperties) {
    this.adlsConfigurations = serverProperties.getAdlsConfigurations();
  }

  public AzureCredential vendAzureCredential(CredentialContext ctx) {
    AzureCredentialGenerator generator =
        credGenerators.compute(
            ctx.getStorageBase(),
            (storageBase, credGenerator) ->
                credGenerator == null
                    ? createAzureCredentialsGenerator(storageBase)
                    : credGenerator);

    return generator.generate(ctx);
  }

  private AzureCredentialGenerator createAzureCredentialsGenerator(String storageBase) {
    ADLSLocationUtils.ADLSLocationParts locParts = ADLSLocationUtils.parseLocation(storageBase);
    ADLSStorageConfig config = adlsConfigurations.get(locParts.accountName());

    if (config == null) {
      return new AzureCredentialGenerator.DatalakeCredentialGenerator(null);
    }

    if (config.getCredentialGenerator() != null) {
      try {
        return (AzureCredentialGenerator)
            Class.forName(config.getCredentialGenerator())
                .getDeclaredConstructor(ADLSStorageConfig.class)
                .newInstance(config);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else if (config.isTestMode()) {
      return new AzureCredentialGenerator.StaticAzureCredentialGenerator(config);
    } else {
      return new AzureCredentialGenerator.DatalakeCredentialGenerator(config);
    }
  }
}
