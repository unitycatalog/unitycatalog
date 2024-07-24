package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.GenerateTemporaryVolumeCredential;
import io.unitycatalog.server.model.GenerateTemporaryVolumeCredentialResponse;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.persist.VolumeRepository;
import io.unitycatalog.server.persist.utils.ServerPropertiesUtils;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;

import java.net.URI;
import java.util.Map;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryVolumeCredentialsService {

  private static final VolumeRepository VOLUME_REPOSITORY = VolumeRepository.getInstance();

  @Post("")
  public HttpResponse generateTemporaryTableCredential(
      GenerateTemporaryVolumeCredential generateTemporaryVolumeCredential) {
    String volumeId = generateTemporaryVolumeCredential.getVolumeId();
    if (volumeId.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Volume ID is required.");
    }
    VolumeInfo volumeInfo = VOLUME_REPOSITORY.getVolumeById(volumeId);
    String volumePath = volumeInfo.getStorageLocation();
    if (volumePath == null || volumePath.isEmpty()) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Volume storage location not found.");
    }
    if (volumePath.startsWith("s3://")) {
      Map<String, S3StorageConfig> s3Configs = ServerPropertiesUtils.getInstance().getS3Configurations();
      S3StorageConfig s3StorageConfig = s3Configs.get(volumePath);

      return HttpResponse.ofJson(
          new GenerateTemporaryVolumeCredentialResponse()
              .awsTempCredentials(new AwsCredentials()
                .accessKeyId(s3StorageConfig.getAccessKey())
                .secretAccessKey(s3StorageConfig.getSecretKey())
                .sessionToken(s3StorageConfig.getSessionToken())));
    } else {
      // return empty credentials for local file system
      return HttpResponse.ofJson(new GenerateTemporaryVolumeCredential());
    }
  }
}
