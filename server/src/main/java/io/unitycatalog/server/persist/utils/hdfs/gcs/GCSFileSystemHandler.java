package io.unitycatalog.server.persist.utils.hdfs.gcs;

import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.persist.utils.hdfs.AbstractFileSystemHandler;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSFileSystemHandler extends AbstractFileSystemHandler {

  private final Configuration conf = new Configuration();
  private static final Logger LOGGER = LoggerFactory.getLogger(GCSFileSystemHandler.class);

  public GCSFileSystemHandler(String storageRoot, String projectId, GcpOauthToken gcpOauthToken) {
    super(storageRoot);

    // Set the implementation class for GCS
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");

    // Set the project ID for GCS
    conf.set("google.cloud.auth.service.account.project.id", projectId);

    // Provide a reference to the access token provider implementation
    conf.set("google.cloud.auth.type", "ACCESS_TOKEN_PROVIDER");
    conf.set("fs.gs.auth.access.token.provider.impl", GCSAccessTokenProvider.class.getName());
    conf.set("google.cloud.auth.access.token.value", gcpOauthToken.getOauthToken());
  }

  @Override
  protected Configuration getHadoopConfiguration() {
    return conf;
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }
}
