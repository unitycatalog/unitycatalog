package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.persist.PropertiesUtil;
import io.unitycatalog.server.utils.TemporaryCredentialUtils;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.Map;

public class FileIOFactory {

  private static final String S3 = "s3";

  public FileIOFactory() {
  }

  // TODO: Cache fileIOs
  public FileIO getFileIO(URI tableLocationUri) {
    switch (tableLocationUri.getScheme()) {
      case S3: return getS3FileIO(tableLocationUri);
      default: return null;
    }
  }

  public S3FileIO getS3FileIO(URI tableLocationUri) {
    // FIXME!! - proper credential vending and region settings
    AwsCredentials credentials = TemporaryCredentialUtils.findS3BucketConfig(tableLocationUri.toString());
    String region = PropertiesUtil.getInstance().getProperty("aws.region");

    S3FileIO s3FileIO = new S3FileIO(() ->
      S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(DefaultCredentialsProvider.create())
        //.credentialsProvider(StaticCredentialsProvider.create(
        //  AwsSessionCredentials.create(
        //    credentials.getAccessKeyId(),credentials.getSecretAccessKey(),credentials.getSessionToken())))
        .forcePathStyle(false)
        .build());

    s3FileIO.initialize(Map.of());

    return s3FileIO;
  }
}
