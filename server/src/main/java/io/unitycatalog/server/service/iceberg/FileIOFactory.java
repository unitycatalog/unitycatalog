package io.unitycatalog.server.service.iceberg;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.utils.ServerPropertiesUtils;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.CredentialOperations;
import lombok.SneakyThrows;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.unitycatalog.server.utils.Constants.URI_SCHEME_GS;
import static io.unitycatalog.server.utils.Constants.URI_SCHEME_S3;

public class FileIOFactory {

  private CredentialOperations credentialOps;

  public FileIOFactory(CredentialOperations credentialOps) {
    this.credentialOps = credentialOps;
  }

  // TODO: Cache fileIOs
  public FileIO getFileIO(URI tableLocationUri) {
    return switch (tableLocationUri.getScheme()) {
      case URI_SCHEME_GS -> getGCSFileIO(tableLocationUri);
      case URI_SCHEME_S3 -> getS3FileIO(tableLocationUri);
      // TODO: should we default/fallback to HadoopFileIO ?
      default -> new SimpleLocalFileIO();
    };
  }

  @SneakyThrows
  protected GCSFileIO getGCSFileIO(URI tableLocationUri) {
    CredentialContext credentialContext = getCredentialContextFromTableLocation(tableLocationUri);
    AccessToken gcpToken = credentialOps.vendGcpToken(credentialContext);

    // NOTE: when fileio caching is implemented, need to set/deal with expiry here
    Map<String, String> properties =
      Map.of(
        GCPProperties.GCS_OAUTH2_TOKEN, gcpToken.getTokenValue());

    GCSFileIO result = new GCSFileIO();
    result.initialize(properties);
    return result;
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
      CredentialContext context = getCredentialContextFromTableLocation(tableLocationUri);
      AwsSessionCredentials credential = credentialOps.vendAwsCredential(context);
      return StaticCredentialsProvider.create(credential);
    } catch (BaseException e) {
      return DefaultCredentialsProvider.create();
    }
  }

  private CredentialContext getCredentialContextFromTableLocation(URI tableLocationUri) {
    // FIXME!! privileges are just defaulted to READ only here
    return CredentialContext.builder().storageBasePath(tableLocationUri.getPath())
      .privileges(Set.of(CredentialContext.Privilege.SELECT)).locations(List.of(tableLocationUri.getRawPath())).build();
  }
}
