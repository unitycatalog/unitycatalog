package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.TemporaryCredentials;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.net.URI;

public class AwsVendedTokenProvider implements AwsCredentialsProvider {
    private final Configuration conf;
    private final URI uri;
    private final String token;

    private volatile AwsS3Credentials awsS3Credentials;
    private volatile ApiClient lazyApiClient = null;

    /**
     * Constructor for the hadoop's CredentialProviderListFactory#buildAWSProviderList to initialize.
     */
    public AwsVendedTokenProvider(URI ignored, Configuration conf) {
        this.conf = conf;
        this.uri = URI.create(conf.get("fs.s3a.unitycatalog.uri"));
        this.token = conf.get("fs.s3a.unitycatalog.token");
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

        return awsS3Credentials;
    }

    private AwsS3Credentials createS3Credentials() throws ApiException {
        TemporaryCredentialsApi tempCredApi = new TemporaryCredentialsApi(apiClient());

        // Get the temp credential request.
        String jsonRequest = conf.get("fs.s3a.unitycatalog.credential.request");
        TempCredentialRequest request = TempCredentialRequest.deserialize(jsonRequest);
        TemporaryCredentials cred = request.generate(tempCredApi);

        return new AwsS3Credentials(cred);
    }

    private static class AwsS3Credentials implements AwsCredentials {
        private final String accessKeyId;
        private final String secretAccessKey;
        private final String sessionToken;
        private final Long expiresTimeMillis;

        AwsS3Credentials(TemporaryCredentials credentials) {
            assert credentials.getAwsTempCredentials() != null;

            this.accessKeyId = credentials.getAwsTempCredentials().getAccessKeyId();
            this.secretAccessKey = credentials.getAwsTempCredentials().getSecretAccessKey();
            this.sessionToken = credentials.getAwsTempCredentials().getSessionToken();
            this.expiresTimeMillis = credentials.getExpirationTime();
        }

        @Override
        public String accessKeyId() {
            return accessKeyId;
        }

        @Override
        public String secretAccessKey() {
            return secretAccessKey;
        }

        public boolean readyToRenew() {
            if(expiresTimeMillis == null || expiresTimeMillis <= System.currentTimeMillis() + 30 * 1000) {
                return true;
            } else{
                return false;
            }
        }
    }
}
