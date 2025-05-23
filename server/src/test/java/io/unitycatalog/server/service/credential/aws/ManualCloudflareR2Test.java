package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.services.sts.model.Credentials;

public class ManualCloudflareR2Test {
  public static void main(String[] args) {
    // Set this to match your s3.bucketPath.N in your properties file
    String bucketPath = "s3://unity-oss";

    // Load server properties (ensure your properties file is set up)
    ServerProperties serverProperties = new ServerProperties();
    AwsCredentialVendor vendor = new AwsCredentialVendor(serverProperties);

    // Get the S3 config for the bucket path
    Map<String, S3StorageConfig> configMap = serverProperties.getS3Configurations();
    S3StorageConfig s3Config = configMap.get(bucketPath);
    if (s3Config == null) {
      System.err.println("No S3 config found for: " + bucketPath);
      return;
    }

    // Use realistic privileges for your test
    Set<CredentialContext.Privilege> privileges =
        Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE);
    CredentialContext context = CredentialContext.create(URI.create(bucketPath), privileges);

    try {
      Credentials creds = vendor.vendCloudflareR2Credentials(s3Config, context);
      System.out.println("AccessKeyId: " + creds.accessKeyId());
      System.out.println("SecretAccessKey: " + creds.secretAccessKey());
      System.out.println("SessionToken: " + creds.sessionToken());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
