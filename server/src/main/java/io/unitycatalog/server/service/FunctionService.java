package io.unitycatalog.server.service;

import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateFunctionRequest;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.persist.FunctionRepository;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.utils.ValidationUtils;

import java.util.Optional;

import static io.unitycatalog.server.utils.ValidationUtils.FUNCTION;
import static io.unitycatalog.server.utils.ValidationUtils.SCHEMA;

@ExceptionHandler(GlobalExceptionHandler.class)
public class FunctionService {

    private static final FunctionRepository FUNCTION_REPOSITORY = FunctionRepository.getInstance();

    public FunctionService() {}

    @Post("")
    public HttpResponse createFunction(CreateFunctionRequest createFunctionRequest) {
        String catalogName = createFunctionRequest.getFunctionInfo().getCatalogName();
        String schemaName = createFunctionRequest.getFunctionInfo().getSchemaName();
        String functionName = createFunctionRequest.getFunctionInfo().getName();
        if (!ValidationUtils.schemaExists(catalogName, schemaName)) {
            return ValidationUtils.entityNotFoundResponse(SCHEMA, catalogName, schemaName);
        }
        if (ValidationUtils.functionExists(catalogName, schemaName, functionName)) {
            return ValidationUtils.entityAlreadyExistsResponse(FUNCTION, catalogName, schemaName, functionName);
        }
        FunctionInfo createdFunction = FUNCTION_REPOSITORY.createFunction(createFunctionRequest);
        return HttpResponse.ofJson(createdFunction);
    }

    @Get("")
    public HttpResponse listFunctions(@Param("catalog_name") String catalogName,
                                      @Param("schema_name") String schemaName,
                                      @Param("max_results") Optional<Integer> maxResults,
                                      @Param("page_token") Optional<String> pageToken) {
        return HttpResponse.ofJson(FUNCTION_REPOSITORY
                .listFunctions(catalogName, schemaName, maxResults, pageToken));
    }

    @Get("/{name}")
    public HttpResponse getFunction(@Param("name") String name) {
        FunctionInfo functionInfo = FUNCTION_REPOSITORY.getFunction(name);
        if (functionInfo != null) {
            return HttpResponse.ofJson(functionInfo);
        }
        return ValidationUtils.entityNotFoundResponse(FUNCTION, name);
    }

    @Delete("/{name}")
    public HttpResponse deleteFunction(@Param("name") String name, @Param("force") Optional<Boolean> force) {
        FUNCTION_REPOSITORY.deleteFunction(name, force.orElse(false));
        return HttpResponse.of(HttpStatus.OK);
    }
}
