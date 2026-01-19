package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.GenerateTemporaryTableCredential;
import io.unitycatalog.server.model.TableOperation;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.utils.NormalizedURL;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryTableCredentialsService {
  private final TableRepository tableRepository;
  private final CloudCredentialVendor cloudCredentialVendor;

  public TemporaryTableCredentialsService(CloudCredentialVendor cloudCredentialVendor,
                                          Repositories repositories) {
    this.cloudCredentialVendor = cloudCredentialVendor;
    this.tableRepository = repositories.getTableRepository();
  }

  @Post("")
  @AuthorizeExpression("""
      #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      (#operation == 'READ'
        ? #authorizeAny(#principal, #table, OWNER, SELECT)
        : (#authorize(#principal, #table, OWNER) ||
           #authorizeAll(#principal, #table, SELECT, MODIFY)))
      """)
  public HttpResponse generateTemporaryTableCredential(
      @AuthorizeResourceKey(value = TABLE, key = "table_id")
      @AuthorizeKey(key = "operation")
      GenerateTemporaryTableCredential generateTemporaryTableCredential) {
    String tableId = generateTemporaryTableCredential.getTableId();
    NormalizedURL storageLocation = tableRepository.getStorageLocationForTableOrStagingTable(
        UUID.fromString(tableId));
    return HttpResponse.ofJson(cloudCredentialVendor.vendCredential(storageLocation,
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

