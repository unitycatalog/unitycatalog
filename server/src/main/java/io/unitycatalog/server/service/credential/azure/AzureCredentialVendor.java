package io.unitycatalog.server.service.credential.azure;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AzureServicePrincipalRequest;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Vends Azure user delegation SAS tokens for ADLS storage.
 *
 * <p>A path under an external location is vended with the location's storage credential: the server
 * signs in as the credential's service principal. Like the AWS master role assuming the storage
 * role, this happens on every vend, so a rotated client secret is used right away. Any other path
 * falls back to the legacy per-account {@code adls.*} configuration in server.properties, whose
 * generators are kept per storage account.
 */
public class AzureCredentialVendor {
  private final Map<String, ADLSStorageConfig> adlsConfigurations;
  private final Map<NormalizedURL, AzureCredentialGenerator> credGenerators =
      new ConcurrentHashMap<>();
  private final Function<AzureServicePrincipalRequest, AzureCredentialGenerator>
      servicePrincipalGenerators;

  public AzureCredentialVendor(ServerProperties serverProperties) {
    this(
        serverProperties,
        AzureCredentialGenerator.DatalakeCredentialGenerator::forServicePrincipal);
  }

  /**
   * Builds the generator for a storage credential's service principal with {@code
   * servicePrincipalGenerators}; tests pass one that does not call Azure.
   */
  AzureCredentialVendor(
      ServerProperties serverProperties,
      Function<AzureServicePrincipalRequest, AzureCredentialGenerator> servicePrincipalGenerators) {
    this.adlsConfigurations = serverProperties.getAdlsConfigurations();
    this.servicePrincipalGenerators = servicePrincipalGenerators;
  }

  public AzureCredential vendAzureCredential(CredentialContext ctx) {
    AzureCredentialGenerator generator =
        ctx.getCredentialDAO()
            .map(
                credential ->
                    servicePrincipalGenerators.apply(credential.getAzureServicePrincipal()))
            .orElseGet(
                () ->
                    credGenerators.computeIfAbsent(
                        ctx.getStorageBase(), this::createAzureCredentialGenerator));
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
