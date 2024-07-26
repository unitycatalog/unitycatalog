package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.GenerateTemporaryVolumeCredential;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.persist.VolumeRepository;
import io.unitycatalog.server.service.credential.CredentialOperations;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryVolumeCredentialsService {

  private static final VolumeRepository VOLUME_REPOSITORY = VolumeRepository.getInstance();

  private final CredentialOperations credentialOps;

  public TemporaryVolumeCredentialsService(CredentialOperations credentialOps) {
    this.credentialOps = credentialOps;
  }

  @Post("")
  public HttpResponse generateTemporaryTableCredential(
      GenerateTemporaryVolumeCredential generateTemporaryVolumeCredential) {
    String volumeId = generateTemporaryVolumeCredential.getVolumeId();
    if (volumeId.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Volume ID is required.");
    }
    VolumeInfo volumeInfo = VOLUME_REPOSITORY.getVolumeById(volumeId);

    return HttpResponse.ofJson(credentialOps.vendCredentialForVolume(volumeInfo));
  }
}
