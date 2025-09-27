package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.TemporaryCredentials;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.net.URI;
import java.net.URISyntaxException;

public class AwsVendedTokenProvider implements AwsCredentialsProvider {
    private final Configuration conf;

    private volatile AwsS3Credentials awsS3Credentials;

    /**
     * Constructor for the hadoop's CredentialProviderListFactory#buildAWSProviderList to initialize.
     */
    public AwsVendedTokenProvider(URI ignored, Configuration conf) {
        this.conf = conf;
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

    private AwsS3Credentials createS3Credentials() throws URISyntaxException, ApiException {
        URI uri = new URI(conf.get("fs.s3a.unitycatalog.uri"));
        String token = conf.get("fs.s3a.unitycatalog.token");

        ApiClient apiClient = ApiClientFactory.createApiClient(uri, token);
        TemporaryCredentialsApi tempCredApi = new TemporaryCredentialsApi(apiClient);

        // Get the temp credential request.
        String jsonRequest = conf.get("fs.s3a.unitycatalog.request");
        TempCredentialRequest generalRequest = TempCredentialRequest.deserialize(jsonRequest);

        switch (generalRequest.type()) {
            case PATH: {
                TempCredentialRequest.TempPathCredentialRequest request = (TempCredentialRequest.TempPathCredentialRequest) generalRequest;
                TemporaryCredentials cred = tempCredApi.generateTemporaryPathCredentials(
                        new GenerateTemporaryPathCredential()
                                .url(request.path())
                                .operation(request.operation())
                );

                return new AwsS3Credentials(cred);
            }

            case TABLE: {
                TempCredentialRequest.TempTableCredentialRequest request = (TempCredentialRequest.TempTableCredentialRequest) generalRequest;
                TemporaryCredentials cred = tempCredApi.generateTemporaryTableCredentials(
                        new GenerateTemporaryTableCredential()
                                .tableId(request.tableId())
                                .operation(request.operation())
                );

                return new AwsS3Credentials(cred);
            }

            default: {
                throw new IllegalStateException("Unsupported temporary credential type " + generalRequest.type());
            }
        }
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
