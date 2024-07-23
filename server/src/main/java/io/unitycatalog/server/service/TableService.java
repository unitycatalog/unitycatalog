package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.persist.TableRepository;
import java.util.Optional;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TableService {

  private static final TableRepository TABLE_REPOSITORY = TableRepository.getInstance();

  public TableService() {}

  @Post("")
  public HttpResponse createTable(CreateTable createTable) {
    assert createTable != null;
    TableInfo createTableResponse = TABLE_REPOSITORY.createTable(createTable);
    return HttpResponse.ofJson(createTableResponse);
  }

  @Get("/{full_name}")
  public HttpResponse getTable(@Param("full_name") String fullName) {
    assert fullName != null;
    TableInfo tableInfo = TABLE_REPOSITORY.getTable(fullName);
    return HttpResponse.ofJson(tableInfo);
  }

  @Get("")
  public HttpResponse listTables(
      @Param("catalog_name") String catalogName,
      @Param("schema_name") String schemaName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken,
      @Param("omit_properties") Optional<Boolean> omitProperties,
      @Param("omit_columns") Optional<Boolean> omitColumns) {
    return HttpResponse.ofJson(
        TABLE_REPOSITORY.listTables(
            catalogName,
            schemaName,
            maxResults,
            pageToken,
            omitProperties.orElse(false),
            omitColumns.orElse(false)));
  }

  @Delete("/{full_name}")
  public HttpResponse deleteTable(@Param("full_name") String fullName) {
    TABLE_REPOSITORY.deleteTable(fullName);
    return HttpResponse.of(HttpStatus.OK);
  }
}
