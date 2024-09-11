package io.unitycatalog.spark;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.time.Instant;

public class GcsVendedTokenProvider implements AccessTokenProvider {
    protected static final String ACCESS_TOKEN_KEY = "fs.gs.auth.access.token.credential";
    protected static final String ACCESS_TOKEN_EXPIRATION_KEY = "fs.gs.auth.access.token.expiration";
    private Configuration conf;

    @Override
    public AccessToken getAccessToken() {
        return new AccessToken(
                conf.get(ACCESS_TOKEN_KEY),
                Instant.ofEpochMilli(Long.parseLong(conf.get(ACCESS_TOKEN_EXPIRATION_KEY)))
        );
    }

    @Override
    public void refresh() throws IOException {
        throw new IOException("Temporary token, not refreshable");
    }

    @Override
    public void setConf(Configuration configuration) {
        conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
