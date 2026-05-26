package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.CredentialsResponse;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.DeltaStorageCredentialUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;

/** Adapts the UC Delta staging table credentials SDK API for Hadoop token providers. */
final class UCDeltaStagingTableCredentialFetcher implements GenericCredentialFetcher {

  private final TemporaryCredentialsApi api;
  private final UUID stagingTableId;
  private final String stagingTableLocation;

  UCDeltaStagingTableCredentialFetcher(Configuration conf, TemporaryCredentialsApi api) {
    Preconditions.checkNotNull(api, "Temporary credentials API is required");
    this.api = api;
    this.stagingTableId =
        UUID.fromString(require(conf, UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_ID_KEY));
    this.stagingTableLocation =
        require(conf, UCHadoopConfConstants.UC_DELTA_STAGING_TABLE_LOCATION_KEY);
  }

  @Override
  public GenericCredential createCredential() throws ApiException {
    CredentialsResponse response = api.getStagingTableCredentials(stagingTableId);
    Preconditions.checkArgument(
        response != null,
        "UC Delta API returned no credentials response for staging table '%s'.",
        stagingTableId);

    return new GenericCredential(
        DeltaStorageCredentialUtil.toTemporaryCredentials(
            DeltaStorageCredentialUtil.selectForLocation(
                stagingTableLocation, response.getStorageCredentials())));
  }

  private static String require(Configuration conf, String key) {
    String value = conf.get(key);
    Preconditions.checkArgument(
        value != null && !value.isEmpty(),
        "The required '%s' is not set in hadoop configuration",
        key);
    return value;
  }
}
