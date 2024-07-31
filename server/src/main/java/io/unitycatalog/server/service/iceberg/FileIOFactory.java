package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.persist.utils.ServerPropertiesUtils;
import io.unitycatalog.server.utils.TemporaryCredentialUtils;
import java.net.URI;
import java.util.Map;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class FileIOFactory {

  private static final String S3 = "s3";

  public FileIOFactory() {}

  // TODO: Cache fileIOs
  public FileIO getFileIO(URI tableLocationUri) {
    switch (tableLocationUri.getScheme()) {
      case S3:
        return getS3FileIO(tableLocationUri);
        // TODO: should we default/fallback to HadoopFileIO ?
      default:
        return new SimpleLocalFileIO();
    }
  }

  protected S3FileIO getS3FileIO(URI tableLocationUri) {
    String region =
        ServerPropertiesUtils.getInstance().getProperty("aws.region", System.getenv("AWS_REGION"));

    // FIXME!! - proper credential vending and region settings
    S3FileIO s3FileIO =
        new S3FileIO(() -> getS3Client(getAwsCredentialsProvider(tableLocationUri), region));

    s3FileIO.initialize(Map.of());

    return s3FileIO;
  }

  protected S3Client getS3Client(AwsCredentialsProvider awsCredentialsProvider, String region) {
    return S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(awsCredentialsProvider)
        .forcePathStyle(false)
        .build();
  }

  private AwsCredentialsProvider getAwsCredentialsProvider(URI tableLocationUri) {
    try {
      AwsCredentials credentials =
          TemporaryCredentialUtils.findS3BucketConfig(tableLocationUri.toString());
      return StaticCredentialsProvider.create(
          AwsSessionCredentials.create(
              credentials.getAccessKeyId(),
              credentials.getSecretAccessKey(),
              credentials.getSessionToken()));
    } catch (BaseException e) {
      return DefaultCredentialsProvider.create();
    }
  }
}
