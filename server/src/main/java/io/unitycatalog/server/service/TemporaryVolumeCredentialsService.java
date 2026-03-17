package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.GenerateTemporaryVolumeCredential;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.model.VolumeOperation;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.VolumeRepository;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;

import java.util.Collections;
import java.util.Set;

import static io.unitycatalog.server.model.SecurableType.VOLUME;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryVolumeCredentialsService {
  private final VolumeRepository volumeRepository;
  private final StorageCredentialVendor storageCredentialVendor;

  public TemporaryVolumeCredentialsService(StorageCredentialVendor storageCredentialVendor,
                                           Repositories repositories) {
    this.storageCredentialVendor = storageCredentialVendor;
    this.volumeRepository = repositories.getVolumeRepository();
  }

  @Post("")
  @AuthorizeExpression("""
      #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      (#operation == 'READ_VOLUME'
        ? #authorizeAny(#principal, #volume, OWNER, READ_VOLUME)
        : #authorize(#principal, #volume, OWNER))
      """)
  public HttpResponse generateTemporaryVolumeCredential(
      @AuthorizeResourceKey(value = VOLUME, key = "volume_id")
      @AuthorizeKey(key = "operation")
      GenerateTemporaryVolumeCredential generateTemporaryVolumeCredential) {
    String volumeId = generateTemporaryVolumeCredential.getVolumeId();
    if (volumeId.isEmpty()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Volume ID is required.");
    }
    VolumeInfo volumeInfo = volumeRepository.getVolumeById(volumeId);
    return HttpResponse.ofJson(
        storageCredentialVendor.vendCredential(
            NormalizedURL.from(volumeInfo.getStorageLocation()),
            volumeOperationToPrivileges(generateTemporaryVolumeCredential.getOperation())));
  }

  private Set<CredentialContext.Privilege> volumeOperationToPrivileges(
      VolumeOperation volumeOperation) {
    return switch (volumeOperation) {
      case READ_VOLUME -> Set.of(SELECT);
      case WRITE_VOLUME -> Set.of(SELECT, UPDATE);
      case UNKNOWN_VOLUME_OPERATION -> Collections.emptySet();
    };
  }

}
