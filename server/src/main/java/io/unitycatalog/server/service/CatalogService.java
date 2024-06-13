package io.unitycatalog.server.service;

import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import com.linecorp.armeria.common.HttpStatus;
import io.unitycatalog.server.persist.CatalogOperations;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.model.CreateCatalog;
import io.unitycatalog.server.model.UpdateCatalog;
import io.unitycatalog.server.utils.ValidationUtils;

import static io.unitycatalog.server.utils.ValidationUtils.CATALOG;

@ExceptionHandler(GlobalExceptionHandler.class)
public class CatalogService {
    private static final CatalogOperations catalogOperations = CatalogOperations.getInstance();

    public CatalogService() {}

    @Post("")
    public HttpResponse createCatalog(CreateCatalog createCatalog) {
        if (ValidationUtils.catalogExists(createCatalog.getName())) {
            return ValidationUtils.entityAlreadyExistsResponse(CATALOG, createCatalog.getName());
        }
        CatalogInfo catalogInfo =  catalogOperations.addCatalog(createCatalog);
        return HttpResponse.ofJson(catalogInfo);
    }

    @Get("")
    public HttpResponse listCatalogs() {
        return HttpResponse.ofJson(catalogOperations.listCatalogs());
    }

    @Get("/{name}")
    public HttpResponse getCatalog(@Param("name") String name) {
        CatalogInfo catalogInfo = catalogOperations.getCatalog(name);
        if (catalogInfo != null) {
            return HttpResponse.ofJson(catalogInfo);
        }
        return ValidationUtils.entityNotFoundResponse(CATALOG, name);
    }

    @Patch("/{name}")
    public HttpResponse updateCatalog(@Param("name") String name, UpdateCatalog updateCatalog) {
        CatalogInfo catalogInfo = catalogOperations.updateCatalog(name, updateCatalog);
        if (catalogInfo != null) {
            return HttpResponse.ofJson(catalogInfo);
        }
        return ValidationUtils.entityAlreadyExistsResponse(CATALOG, name);
    }

    @Delete("/{name}")
    public HttpResponse deleteCatalog(@Param("name") String name) {
        catalogOperations.deleteCatalog(name);
        return HttpResponse.of(HttpStatus.OK);
    }
}
