package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.service.credential.CredentialContext;
import java.time.Duration;
import java.util.UUID;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.Credentials;

public interface CredentialsGenerator {
  Credentials generate(CredentialContext ctx);

  class StaticCredentialsGenerator implements CredentialsGenerator {
    private final String accessKeyId;
    private final String secretKey;
    private final String sessionToken;

    public StaticCredentialsGenerator(String accessKeyId, String secretKey, String sessionToken) {
      this.accessKeyId = accessKeyId;
      this.secretKey = secretKey;
      this.sessionToken = sessionToken;
    }

    @Override
    public Credentials generate(CredentialContext ctx) {
      return Credentials.builder()
          .accessKeyId(accessKeyId)
          .secretAccessKey(secretKey)
          .sessionToken(sessionToken)
          .build();
    }
  }

  class StsCredentialsGenerator implements CredentialsGenerator {
    private final StsClient stsClient;
    private final String awsRoleArn;

    public StsCredentialsGenerator(String accessKey, String secretKey, String awsRoleArn) {
      this.stsClient =
          StsClient.builder()
              .credentialsProvider(
                  StaticCredentialsProvider.create(
                      AwsBasicCredentials.create(accessKey, secretKey)))
              .build();
      this.awsRoleArn = awsRoleArn;
    }

    public StsCredentialsGenerator(String awsRoleArn) {
      this.stsClient =
          StsClient.builder().credentialsProvider(DefaultCredentialsProvider.create()).build();
      this.awsRoleArn = awsRoleArn;
    }

    @Override
    public Credentials generate(CredentialContext ctx) {
      String awsPolicy = AwsPolicyGenerator.generatePolicy(ctx.getPrivileges(), ctx.getLocations());
      String roleSessionName = "uc-%s".formatted(UUID.randomUUID());

      return stsClient
          .assumeRole(
              r ->
                  r.roleArn(awsRoleArn)
                      .policy(awsPolicy)
                      .roleSessionName(roleSessionName)
                      .durationSeconds((int) Duration.ofHours(1).toSeconds()))
          .credentials();
    }
  }
}
