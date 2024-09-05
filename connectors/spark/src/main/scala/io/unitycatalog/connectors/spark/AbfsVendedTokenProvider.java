package io.unitycatalog.connectors.spark;

import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;

import static java.lang.String.format;

public class AbfsVendedTokenProvider implements SASTokenProvider {
    private Configuration conf;


    @Override
    public String getSASToken(String account, String fileSystem, String path, String operation) throws IOException, AccessControlException {
        return conf.get("fs.azure.sas.fixed.token");
    }

    @Override
    public void initialize(Configuration configuration, String accountName) throws IOException {
        this.conf = configuration;
    }

}
