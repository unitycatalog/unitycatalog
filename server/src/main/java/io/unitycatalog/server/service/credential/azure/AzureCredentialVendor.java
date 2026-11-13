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
 * Vends user delegation SAS tokens for ADLS. A path under an external location is vended as its
 * credential's service principal; any other path uses the {@code adls.*} configuration of its
 * storage account.
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
        servicePrincipal ->
            new AzureCredentialGenerator.DatalakeCredentialGenerator(
                servicePrincipal.getDirectoryId(),
                servicePrincipal.getApplicationId(),
                servicePrincipal.getClientSecret()));
  }

  /**
   * @param servicePrincipalGenerators makes the generator for a credential's service principal;
   *     tests pass one that does not call Azure
   */
  public AzureCredentialVendor(
      ServerProperties serverProperties,
      Function<AzureServicePrincipalRequest, AzureCredentialGenerator> servicePrincipalGenerators) {
    this.adlsConfigurations = serverProperties.getAdlsConfigurations();
    this.servicePrincipalGenerators = servicePrincipalGenerators;
  }

  public AzureCredential vendAzureCredential(CredentialContext ctx) {
    // A credential's generator is made on every vend, as the AWS vendor assumes the role on every
    // vend, so a changed service principal applies to the next request.
    AzureCredentialGenerator generator =
        ctx.getCredentialDAO()
            .map(dao -> servicePrincipalGenerators.apply(dao.getAzureServicePrincipal()))
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
