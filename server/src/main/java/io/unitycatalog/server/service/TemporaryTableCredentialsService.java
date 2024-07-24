package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.GenerateTemporaryTableCredential;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.service.credential.CredentialOperations;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryTableCredentialsService {

  private static final TableRepository TABLE_REPOSITORY = TableRepository.getInstance();

  private CredentialOperations credentialOps;

  public TemporaryTableCredentialsService(CredentialOperations credentialOps) {
    this.credentialOps = credentialOps;
  }

  @Post("")
  public HttpResponse generateTemporaryTableCredential(
      GenerateTemporaryTableCredential generateTemporaryTableCredential) {
    String tableId = generateTemporaryTableCredential.getTableId();
    TableInfo tableInfo = TABLE_REPOSITORY.getTableById(tableId);

    // TODO: check if table exists

    return HttpResponse.ofJson(credentialOps.vendCredentialForTable(tableInfo));
  }
}
