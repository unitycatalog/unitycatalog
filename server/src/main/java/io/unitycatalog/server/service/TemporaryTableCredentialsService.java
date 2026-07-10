package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.AuthorizeExpressions;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.GenerateTemporaryTableCredential;
import io.unitycatalog.server.model.TableOperation;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.TableRepository.TableStorageLocationInfo;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.utils.ServerProperties;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryTableCredentialsService {
  private final TableRepository tableRepository;
  private final StorageCredentialVendor storageCredentialVendor;
  private final ServerProperties serverProperties;

  public TemporaryTableCredentialsService(StorageCredentialVendor storageCredentialVendor,
                                          Repositories repositories,
                                          ServerProperties serverProperties) {
    this.storageCredentialVendor = storageCredentialVendor;
    this.tableRepository = repositories.getTableRepository();
    this.serverProperties = serverProperties;
  }

  @Post("")
  @AuthorizeExpression(AuthorizeExpressions.VEND_TABLE_CREDENTIAL)
  public HttpResponse generateTemporaryTableCredential(
      @AuthorizeResourceKey(value = TABLE, key = "table_id")
      @AuthorizeKey(key = "operation")
      GenerateTemporaryTableCredential generateTemporaryTableCredential) {
    String tableId = generateTemporaryTableCredential.getTableId();
    TableStorageLocationInfo info =
        tableRepository.getStorageLocationForTableOrStagingTable(UUID.fromString(tableId));
    serverProperties.checkDeltaApiOnlyForManagedTable(
        info.tableType(),
        "GET /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials"
            + " (or /delta/v1/staging-tables/{table_id}/credentials for unfinalized staging)");
    return HttpResponse.ofJson(storageCredentialVendor.vendCredential(info.url(),
            tableOperationToPrivileges(generateTemporaryTableCredential.getOperation())));
  }

  private Set<CredentialContext.Privilege> tableOperationToPrivileges(
      TableOperation tableOperation) {
    return switch (tableOperation) {
      case READ -> Set.of(SELECT);
      case READ_WRITE -> Set.of(SELECT, UPDATE);
      case UNKNOWN_TABLE_OPERATION -> Collections.emptySet();
    };
  }
}
