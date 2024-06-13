package io.unitycatalog.server.service;

import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateSchema;
import io.unitycatalog.server.model.SchemaInfo;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.model.UpdateSchema;
import io.unitycatalog.server.persist.SchemaOperations;
import io.unitycatalog.server.utils.ValidationUtils;

import java.util.Optional;

import static io.unitycatalog.server.utils.ValidationUtils.CATALOG;
import static io.unitycatalog.server.utils.ValidationUtils.SCHEMA;

@ExceptionHandler(GlobalExceptionHandler.class)
public class SchemaService {
    private static final SchemaOperations schemaOperations = SchemaOperations.getInstance();

    public SchemaService() {}

    @Post("")
    public HttpResponse createSchema(CreateSchema createSchema) {
        if (!ValidationUtils.catalogExists(createSchema.getCatalogName())) {
            return ValidationUtils.entityNotFoundResponse(CATALOG, createSchema.getCatalogName());
        }
        if (ValidationUtils.schemaExists(createSchema.getCatalogName(), createSchema.getName())) {
            return ValidationUtils.entityAlreadyExistsResponse(SCHEMA, createSchema.getCatalogName(), createSchema.getName());
        }
        SchemaInfo schemaInfo = schemaOperations.createSchema(createSchema);
        return HttpResponse.ofJson(schemaInfo);
    }

    @Get("")
    public HttpResponse listSchemas(
            @Param("catalog_name") String catalogName,
            @Param("max_results") Optional<Integer> maxResults,
            @Param("page_token") Optional<String> pageToken) {
        return HttpResponse.ofJson(schemaOperations.listSchemas(catalogName, maxResults, pageToken));
    }

    @Get("/{full_name}")
    public HttpResponse getSchema(@Param("full_name") String fullName) {
        SchemaInfo schemaInfo = schemaOperations.getSchema(fullName);
        if (schemaInfo != null) {
            return HttpResponse.ofJson(schemaInfo);
        }
        return ValidationUtils.entityNotFoundResponse(SCHEMA, fullName);
    }

    @Patch("/{full_name}")
    public HttpResponse updateSchema(@Param("full_name") String fullName, UpdateSchema updateSchema) {
        SchemaInfo updatedSchema = schemaOperations.updateSchema(fullName, updateSchema);
        if (updatedSchema != null) {
            return HttpResponse.ofJson(updatedSchema);
        }
        return ValidationUtils.entityNotFoundResponse(SCHEMA, fullName);
    }

    @Delete("/{full_name}")
    public HttpResponse deleteSchema(@Param("full_name") String fullName) {
        schemaOperations.deleteSchema(fullName);
        return HttpResponse.of(HttpStatus.OK);
    }
}
