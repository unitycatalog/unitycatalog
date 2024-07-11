package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateFunctionRequest;
import io.unitycatalog.server.persist.FunctionRepository;
import java.util.Optional;

@ExceptionHandler(GlobalExceptionHandler.class)
public class FunctionService {

  private static final FunctionRepository FUNCTION_REPOSITORY = FunctionRepository.getInstance();

  public FunctionService() {}

  @Post("")
  public HttpResponse createFunction(CreateFunctionRequest createFunctionRequest) {
    return HttpResponse.ofJson(FUNCTION_REPOSITORY.createFunction(createFunctionRequest));
  }

  @Get("")
  public HttpResponse listFunctions(
      @Param("catalog_name") String catalogName,
      @Param("schema_name") String schemaName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    return HttpResponse.ofJson(
        FUNCTION_REPOSITORY.listFunctions(catalogName, schemaName, maxResults, pageToken));
  }

  @Get("/{name}")
  public HttpResponse getFunction(@Param("name") String name) {
    return HttpResponse.ofJson(FUNCTION_REPOSITORY.getFunction(name));
  }

  @Delete("/{name}")
  public HttpResponse deleteFunction(
      @Param("name") String name, @Param("force") Optional<Boolean> force) {
    FUNCTION_REPOSITORY.deleteFunction(name, force.orElse(false));
    return HttpResponse.of(HttpStatus.OK);
  }
}
