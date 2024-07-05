package io.unitycatalog.server.service;

import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateSchema;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.model.UpdateSchema;
import io.unitycatalog.server.persist.SchemaRepository;

import java.util.Optional;

@ExceptionHandler(GlobalExceptionHandler.class)
public class SchemaService {
    private static final SchemaRepository SCHEMA_REPOSITORY = SchemaRepository.getINSTANCE();

    public SchemaService() {}

    @Post("")
    public HttpResponse createSchema(CreateSchema createSchema) {
        return HttpResponse.ofJson(SCHEMA_REPOSITORY.createSchema(createSchema));
    }

    @Get("")
    public HttpResponse listSchemas(
            @Param("catalog_name") String catalogName,
            @Param("max_results") Optional<Integer> maxResults,
            @Param("page_token") Optional<String> pageToken) {
        return HttpResponse.ofJson(SCHEMA_REPOSITORY.listSchemas(catalogName, maxResults, pageToken));
    }

    @Get("/{full_name}")
    public HttpResponse getSchema(@Param("full_name") String fullName) {
        return HttpResponse.ofJson(SCHEMA_REPOSITORY.getSchema(fullName));
    }

    @Patch("/{full_name}")
    public HttpResponse updateSchema(@Param("full_name") String fullName, UpdateSchema updateSchema) {
        return HttpResponse.ofJson(SCHEMA_REPOSITORY.updateSchema(fullName, updateSchema));
    }

    @Delete("/{full_name}")
    public HttpResponse deleteSchema(@Param("full_name") String fullName) {
        SCHEMA_REPOSITORY.deleteSchema(fullName);
        return HttpResponse.of(HttpStatus.OK);
    }
}
