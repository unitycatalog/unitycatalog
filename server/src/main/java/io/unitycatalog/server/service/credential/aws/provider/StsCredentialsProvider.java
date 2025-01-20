package io.unitycatalog.server.service.credential.aws.provider;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.aws.AwsPolicyGenerator;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import java.time.Duration;
import java.util.UUID;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.Credentials;

public class StsCredentialsProvider implements AwsCredentialsProvider {

  public static final String ROLE_ARN = "rolearn";
  public static final String REGION = "region";

  private String roleArn;
  private StsClient stsClient;

  @Override
  public AwsCredentialsProvider configure(AwsCredentialsProviderConfig configuration) {
    this.roleArn = configuration.get(ROLE_ARN);
    this.stsClient = getStsClient(configuration);
    return this;
  }

  @Override
  public AwsCredentials generateCredentials(
      CredentialContext context, S3StorageConfig s3StorageConfig) {
    Credentials credentials;

    // TODO: Update this with relevant user/role type info once available
    String roleSessionName = "uc-%s".formatted(UUID.randomUUID());
    String awsPolicy =
        AwsPolicyGenerator.generatePolicy(context.getPrivileges(), context.getLocations());

    credentials =
        stsClient
            .assumeRole(
                r ->
                    r.roleArn(roleArn)
                        .policy(awsPolicy)
                        .roleSessionName(roleSessionName)
                        .durationSeconds((int) Duration.ofHours(1).toSeconds()))
            .credentials();

    return new AwsCredentials()
        .accessKeyId(credentials.accessKeyId())
        .secretAccessKey(credentials.secretAccessKey())
        .sessionToken(credentials.sessionToken())
        .endpoint(s3StorageConfig.getEndpoint());
  }

  private StsClient getStsClient(AwsCredentialsProviderConfig config) {
    software.amazon.awssdk.auth.credentials.AwsCredentialsProvider credentialsProvider;
    final String accessKey = config.get(AwsCredentialsProviderConfig.ACCESS_KEY_ID);
    final String secretKey = config.get(AwsCredentialsProviderConfig.SECRET_ACCESS_KEY);
    if (secretKey != null && !accessKey.isEmpty()) {
      credentialsProvider =
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(
                  accessKey, secretKey));
    } else {
      credentialsProvider = DefaultCredentialsProvider.create();
    }

    // TODO: should we try and set the region to something configurable or specific to the server
    // instead?
    return StsClient.builder()
        .credentialsProvider(credentialsProvider)
        .region(Region.of(config.get(REGION)))
        .build();
  }
}
