package io.unitycatalog.spark;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class AwsVendedTokenProvider implements AwsCredentialsProvider {
    @Override
    public AwsCredentials resolveCredentials() {
        return null;
    }
}
