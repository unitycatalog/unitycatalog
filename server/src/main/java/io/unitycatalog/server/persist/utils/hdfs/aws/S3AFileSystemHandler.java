package io.unitycatalog.server.persist.utils.hdfs.aws;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.persist.utils.hdfs.AbstractFileSystemHandler;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3AFileSystemHandler extends AbstractFileSystemHandler {

  private final Configuration conf;
  private static final Logger LOGGER = LoggerFactory.getLogger(S3AFileSystemHandler.class);

  public S3AFileSystemHandler(String storageRoot, AwsCredentials awsCredentials) {
    super(storageRoot);
    conf = new Configuration();
    conf.set("fs.s3a.access.key", awsCredentials.getAccessKeyId());
    conf.set("fs.s3a.secret.key", awsCredentials.getSecretAccessKey());
    conf.set("fs.s3a.session.token", awsCredentials.getSessionToken());
    // Use s3a for both s3 and s3a schemes
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.connection.ssl.enabled", "true");
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
