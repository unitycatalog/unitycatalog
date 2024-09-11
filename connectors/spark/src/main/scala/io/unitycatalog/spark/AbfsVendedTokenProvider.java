package io.unitycatalog.spark;

import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;

public class AbfsVendedTokenProvider implements SASTokenProvider {
    private Configuration conf;
    protected static final String ACCESS_TOKEN_KEY = "fs.azure.sas.fixed.token";

    @Override
    public String getSASToken(String account, String fileSystem, String path, String operation) throws IOException, AccessControlException {
        return conf.get(ACCESS_TOKEN_KEY);
    }

    @Override
    public void initialize(Configuration configuration, String accountName) throws IOException {
        this.conf = configuration;
    }
}
