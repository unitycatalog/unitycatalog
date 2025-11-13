package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.utils.Validate;

public class AwsUtils {

  public static AwsCredentialsProvider getAwsCredentialsProvider(
      String accessKeyId, String secretAccessKey, String sessionToken) {
    try {
      return StaticCredentialsProvider.create(
          AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken));
    } catch (BaseException e) {
      return DefaultCredentialsProvider.create();
    }
  }

  public static AwsCredentialsProvider getAwsCredentialsProvider(Credentials awsCredentials) {
    return getAwsCredentialsProvider(
        awsCredentials.accessKeyId(),
        awsCredentials.secretAccessKey(),
        awsCredentials.sessionToken());
  }

  public static S3Client getS3Client(
      AwsCredentialsProvider awsCredentialsProvider, String region, String endpointUrl) {
    return S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(awsCredentialsProvider)
        .endpointOverride(
            endpointUrl != null && !endpointUrl.isEmpty() ? URI.create(endpointUrl) : null)
        .forcePathStyle(false)
        .build();
  }

  public static boolean doesObjectExist(S3Client s3Client, String bucketName, String key) {
    try {
      Validate.notEmpty(bucketName, "The bucket name must not be null or an empty string.", "");
      s3Client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(key).build());
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    }
  }
}
