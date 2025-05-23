package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONObject;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.Credentials;

public class AwsCredentialVendor {

  private final Map<String, S3StorageConfig> s3Configurations;

  public AwsCredentialVendor(ServerProperties serverProperties) {
    this.s3Configurations = serverProperties.getS3Configurations();
    System.out.println("----------------");
    System.out.println("Loaded S3 config keys: " + s3Configurations.keySet());
  }

  public Credentials vendAwsCredentials(CredentialContext context) {
    S3StorageConfig s3StorageConfig = s3Configurations.get(context.getStorageBase());

    System.out.println("----------------");
    System.out.println(s3StorageConfig);
    System.out.println(context.getStorageBase());
    System.out.flush();

    if (s3StorageConfig == null) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "S3 bucket configuration not found.");
    }

    // Detect Cloudflare R2 endpoint
    String endpoint = s3StorageConfig.getEndpoint();
    if (endpoint != null
        && endpoint.matches("^https://[a-zA-Z0-9_-]+\\.r2\\.cloudflarestorage\\.com/?$")) {
      return vendCloudflareR2Credentials(s3StorageConfig, context);
    }

    if (s3StorageConfig.getSessionToken() != null && !s3StorageConfig.getSessionToken().isEmpty()) {
      // if a session token was supplied, then we will just return static session credentials
      return Credentials.builder()
          .accessKeyId(s3StorageConfig.getAccessKey())
          .secretAccessKey(s3StorageConfig.getSecretKey())
          .sessionToken(s3StorageConfig.getSessionToken())
          .build();
    }

    // TODO: cache sts client
    StsClient stsClient = getStsClientForStorageConfig(s3StorageConfig);

    // TODO: Update this with relevant user/role type info once available
    String roleSessionName = "uc-%s".formatted(UUID.randomUUID());
    String awsPolicy =
        AwsPolicyGenerator.generatePolicy(context.getPrivileges(), context.getLocations());

    return stsClient
        .assumeRole(
            r ->
                r.roleArn(s3StorageConfig.getAwsRoleArn())
                    .policy(awsPolicy)
                    .roleSessionName(roleSessionName)
                    .durationSeconds((int) Duration.ofHours(1).toSeconds()))
        .credentials();
  }

  public Credentials vendCloudflareR2Credentials(
      S3StorageConfig s3StorageConfig, CredentialContext context) {
    // Extract account_id from
    String endpoint = s3StorageConfig.getEndpoint();
    Pattern pattern =
        Pattern.compile("^https://([a-zA-Z0-9_-]+)\\.r2\\.cloudflarestorage\\.com/?$");
    Matcher matcher = pattern.matcher(endpoint);
    String accountId = null;
    if (matcher.find()) {
      accountId = matcher.group(1);
    } else {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Invalid R2 endpoint format.");
    }

    // Prepare permissions
    String permission = "read,write";
    if (context.getPrivileges() != null && !context.getPrivileges().isEmpty()) {
      boolean canRead = context.getPrivileges().contains(CredentialContext.Privilege.SELECT);
      boolean canWrite = context.getPrivileges().contains(CredentialContext.Privilege.UPDATE);
      if (canRead && canWrite) {
        permission = "object-read-write";
      } else if (canRead) {
        permission = "object-read-only";
      } else if (canWrite) {
        permission = "object-read-write";
      }
    }

    // Prepare API request
    String apiKey = s3StorageConfig.getSessionToken();
    String parentAccessKeyId = s3StorageConfig.getAccessKey();
    // Remove scheme if present (e.g., "s3://bucket-name" -> "bucket-name")
    String bucketPath = s3StorageConfig.getBucketPath();
    String bucket = bucketPath.replaceFirst("^[a-zA-Z0-9+.-]+://", "");

    try {
      HttpClient client = HttpClient.newHttpClient();
      JSONObject body = new JSONObject();
      body.put("parentAccessKeyId", parentAccessKeyId);
      body.put("bucket", bucket);
      body.put("permission", permission);

      System.out.println("Cloudflare R2 Debug:");
      System.out.println("accountId: " + accountId);
      System.out.println("bucketPath: " + bucketPath);
      System.out.println("bucket: " + bucket);
      System.out.println("permission: " + permission);
      System.out.println("parentAccessKeyId: " + parentAccessKeyId);
      System.out.println("apiKey: " + apiKey);
      String r2Url =
          "https://api.cloudflare.com/client/v4/accounts/"
              + accountId
              + "/r2/temp-access-credentials";
      System.out.println("URL: " + r2Url);
      System.out.println("Request Body: " + body.toString());

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(
                  URI.create(
                      "https://api.cloudflare.com/client/v4/accounts/"
                          + accountId
                          + "/r2/temp-access-credentials"))
              .header("Authorization", "Bearer " + apiKey)
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
              .build();

      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      JSONObject json = new JSONObject(response.body());

      if (response.statusCode() != 200 || !json.optBoolean("success", false)) {
        throw new BaseException(
            ErrorCode.FAILED_PRECONDITION,
            "Failed to get R2 temporary credentials: " + response.body());
      }

      JSONObject creds = json.getJSONObject("result");

      return Credentials.builder()
          .accessKeyId(creds.optString("accessKeyId"))
          .secretAccessKey(creds.optString("secretAccessKey"))
          .sessionToken(creds.optString("sessionToken"))
          .build();
    } catch (Exception e) {
      e.printStackTrace();
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION, "Error fetching R2 credentials: " + e.getMessage());
    }
  }

  private StsClient getStsClientForStorageConfig(S3StorageConfig s3StorageConfig) {
    AwsCredentialsProvider credentialsProvider;
    if (s3StorageConfig.getSecretKey() != null && !s3StorageConfig.getAccessKey().isEmpty()) {
      credentialsProvider =
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(
                  s3StorageConfig.getAccessKey(), s3StorageConfig.getSecretKey()));
    } else {
      credentialsProvider = DefaultCredentialsProvider.create();
    }

    // TODO: should we try and set the region to something configurable or specific to the server
    // instead?
    return StsClient.builder()
        .credentialsProvider(credentialsProvider)
        .region(Region.of(s3StorageConfig.getRegion()))
        .build();
  }
}
