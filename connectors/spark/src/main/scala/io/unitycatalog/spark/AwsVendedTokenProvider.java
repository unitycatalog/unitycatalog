package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.*;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.net.URI;

public class AwsVendedTokenProvider implements AwsCredentialsProvider {
    // The time remaining until expiration, we will try to renew the credential before the expiration time.
    private static final long DEFAULT_RENEWAL_LEAD_TIME = 30 * 1000;

    private final Configuration conf;
    private final URI uri;
    private final String token;

    private volatile long renewalLeadTime = DEFAULT_RENEWAL_LEAD_TIME;

    private volatile AwsS3Credentials awsS3Credentials;
    private volatile ApiClient lazyApiClient = null;

    /**
     * Constructor for the hadoop's CredentialProviderListFactory#buildAWSProviderList to initialize.
     */
    public AwsVendedTokenProvider(URI ignored, Configuration conf) {
        this.conf = conf;
        this.uri = URI.create(conf.get(Constants.UNITY_CATALOG_URI));
        this.token = conf.get(Constants.UNITY_CATALOG_TOKEN);
    }

    private ApiClient apiClient() {
        if(lazyApiClient == null) {
            synchronized (this) {
                if(lazyApiClient == null) {
                    lazyApiClient = ApiClientFactory.createApiClient(uri, token);
                }
            }
        }

        return lazyApiClient;
    }

    @Override
    public AwsCredentials resolveCredentials() {
        if(awsS3Credentials == null || awsS3Credentials.readyToRenew()) {
            synchronized (this) {
                if(awsS3Credentials == null || awsS3Credentials.readyToRenew()) {
                    try {
                        awsS3Credentials = createS3Credentials();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        return awsS3Credentials.awsSessionCredentials;
    }

    // For testing purpose only.
    void setRenewalLeadTime(long renewalLeadTime) {
        this.renewalLeadTime = renewalLeadTime;
    }

    protected TemporaryCredentialsApi temporaryCredentialsApi() {
        return new TemporaryCredentialsApi(apiClient());
    }

    private AwsS3Credentials createS3Credentials() throws ApiException {
        TemporaryCredentialsApi tempCredApi = temporaryCredentialsApi();

        // Generate the temporary credential via requesting UnityCatalog.
        TemporaryCredentials tempCred;
        String type = conf.get(Constants.UNITY_CATALOG_CREDENTIALS_TYPE);
        // TODO We will need to retry the temporary credential request if any recoverable failure, for better robust.
        if (Constants.UNITY_CATALOG_PATH_CREDENTIALS_TYPE.equals(type)) {
            String path = conf.get(Constants.UNITY_CATALOG_PATH);
            String pathOperation = conf.get(Constants.UNITY_CATALOG_PATH_OPERATION);

            tempCred = tempCredApi.generateTemporaryPathCredentials(
                    new GenerateTemporaryPathCredential()
                            .url(path)
                            .operation(PathOperation.fromValue(pathOperation))
            );
        } else if(Constants.UNITY_CATALOG_TABLE_CREDENTIALS_TYPE.equals(type)) {
            String table = conf.get(Constants.UNITY_CATALOG_TABLE);
            String tableOperation = conf.get(Constants.UNITY_CATALOG_TABLE_OPERATION);

            tempCred = tempCredApi.generateTemporaryTableCredentials(
                    new GenerateTemporaryTableCredential()
                            .tableId(table)
                            .operation(TableOperation.fromValue(tableOperation))
            );
        } else {
            throw new IllegalArgumentException("Unsupported unity catalog temporary credentials type: " + type);
        }

        return new AwsS3Credentials(tempCred);
    }

    private class AwsS3Credentials {
        private final Long expiresTimeMillis;
        private final AwsSessionCredentials awsSessionCredentials;

        AwsS3Credentials(TemporaryCredentials tempCred) {
            assert tempCred.getAwsTempCredentials() != null;

            this.expiresTimeMillis = tempCred.getExpirationTime();
            this.awsSessionCredentials = AwsSessionCredentials.builder()
                    .accessKeyId(tempCred.getAwsTempCredentials().getAccessKeyId())
                    .secretAccessKey(tempCred.getAwsTempCredentials().getSecretAccessKey())
                    .sessionToken(tempCred.getAwsTempCredentials().getSessionToken())
                    .build();
        }

        public boolean readyToRenew() {
            System.out.println("expiresTimeMillis: " + expiresTimeMillis);
            System.out.println("targetTime: " + (System.currentTimeMillis() + renewalLeadTime));
            return expiresTimeMillis == null || expiresTimeMillis <= System.currentTimeMillis() + renewalLeadTime;
        }
    }
}
