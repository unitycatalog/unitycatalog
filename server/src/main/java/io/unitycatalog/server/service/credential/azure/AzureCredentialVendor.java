package io.unitycatalog.server.service.credential.azure;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageLimits;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class AzureCredentialVendor {
  private final Map<String, ADLSStorageConfig> adlsConfigurations;
  private final Map<NormalizedURL, AzureCredentialGenerator> credGenerators =
      new ConcurrentHashMap<>();

  public AzureCredentialVendor(ServerProperties serverProperties) {
    this.adlsConfigurations = serverProperties.getAdlsConfigurations();
  }

  /**
   * Checks the Azure Blob Storage limits that objects at each location would hit: the blob name
   * length and the number of path segments.
   *
   * @return why the locations can't be used on Azure, or empty
   */
  public Optional<String> limitViolation(CredentialContext ctx, StorageLimits.Extent extent) {
    return StorageLimits.objectNameViolation(
            ctx.getLocations(), "Azure blob", StorageLimits.Unit.UTF16_CODE_UNITS, extent)
        .or(() -> StorageLimits.azurePathSegmentViolation(ctx.getLocations(), extent));
  }

  public AzureCredential vendAzureCredential(CredentialContext ctx) {
    ctx.getCredentialDAO()
        .ifPresent(
            c -> {
              throw new BaseException(
                  ErrorCode.UNIMPLEMENTED,
                  "Storage credential/external location for Azure is not supported yet.");
            });
    AzureCredentialGenerator generator =
        credGenerators.computeIfAbsent(ctx.getStorageBase(), this::createAzureCredentialGenerator);

    return generator.generate(ctx);
  }

  private AzureCredentialGenerator createAzureCredentialGenerator(NormalizedURL storageBase) {
    ADLSLocationUtils.ADLSLocationParts locParts = ADLSLocationUtils.parseLocation(storageBase);
    ADLSStorageConfig config = adlsConfigurations.get(locParts.accountName());

    if (config == null) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION, "Azure storage account configuration not found.");
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
