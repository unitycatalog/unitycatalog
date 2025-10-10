package io.unitycatalog.spark;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/**
 * AWS credentials provider for vended temporary tokens from Unity Catalog.
 * This provider implements AWSCredentialsProvider to supply temporary AWS credentials
 * (access key, secret key, and session token) that are vended by Unity Catalog
 * for table-level access control.
 */
public class AwsVendedTokenProvider extends Configured implements AWSCredentialsProvider {
    protected static final String ACCESS_KEY_ID = "fs.s3a.access.key.credential";
    protected static final String SECRET_ACCESS_KEY = "fs.s3a.secret.key.credential";
    protected static final String SESSION_TOKEN = "fs.s3a.session.token.credential";

    private Configuration conf;
    private AWSSessionCredentials credentials;

    public AwsVendedTokenProvider() {
        super();
    }

    public AwsVendedTokenProvider(Configuration conf) {
        super(conf);
        this.conf = conf;
        refresh();
    }

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        this.conf = conf;
        refresh();
    }

    @Override
    public AWSCredentials getCredentials() {
        if (credentials == null) {
            refresh();
        }
        return credentials;
    }

    @Override
    public void refresh() {
        if (conf == null) {
            throw new IllegalStateException("Configuration not set for AwsVendedTokenProvider");
        }

        String accessKeyId = conf.get(ACCESS_KEY_ID);
        String secretAccessKey = conf.get(SECRET_ACCESS_KEY);
        String sessionToken = conf.get(SESSION_TOKEN);

        if (accessKeyId == null || secretAccessKey == null || sessionToken == null) {
            throw new IllegalArgumentException(
                "AWS credentials not found in configuration. Required keys: " +
                ACCESS_KEY_ID + ", " + SECRET_ACCESS_KEY + ", " + SESSION_TOKEN
            );
        }

        credentials = new AWSSessionCredentials() {
            @Override
            public String getSessionToken() {
                return sessionToken;
            }

            @Override
            public String getAWSAccessKeyId() {
                return accessKeyId;
            }

            @Override
            public String getAWSSecretKey() {
                return secretAccessKey;
            }
        };
    }
}