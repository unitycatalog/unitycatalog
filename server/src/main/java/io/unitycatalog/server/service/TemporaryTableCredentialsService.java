package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.GenerateTemporaryTableCredential;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableOperation;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.CredentialOperations;

import java.util.Collections;
import java.util.Set;

import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryTableCredentialsService {

  private static final TableRepository TABLE_REPOSITORY = TableRepository.getInstance();

  private final CredentialOperations credentialOps;

  public TemporaryTableCredentialsService(CredentialOperations credentialOps) {
    this.credentialOps = credentialOps;
  }

  @Post("")
  public HttpResponse generateTemporaryTableCredential(
      GenerateTemporaryTableCredential generateTemporaryTableCredential) {
    String tableId = generateTemporaryTableCredential.getTableId();
    TableInfo tableInfo = TABLE_REPOSITORY.getTableById(tableId);
    return HttpResponse.ofJson(credentialOps
            .vendCredential(tableInfo.getStorageLocation(),
                    tableOperationToPrivileges(generateTemporaryTableCredential.getOperation()))
            .toTableCredentialResponse());
  }

  private Set<CredentialContext.Privilege> tableOperationToPrivileges(TableOperation tableOperation) {
    return switch (tableOperation) {
      case READ -> Set.of(SELECT);
      case READ_WRITE -> Set.of(SELECT, UPDATE);
      case UNKNOWN_TABLE_OPERATION -> Collections.emptySet();
    };
  }
}
