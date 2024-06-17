package io.unitycatalog.server.service;

import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import com.linecorp.armeria.common.HttpStatus;
import io.unitycatalog.server.persist.CatalogRepository;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.model.CreateCatalog;
import io.unitycatalog.server.model.UpdateCatalog;
import io.unitycatalog.server.utils.ValidationUtils;

import static io.unitycatalog.server.utils.ValidationUtils.CATALOG;

@ExceptionHandler(GlobalExceptionHandler.class)
public class CatalogService {
    private static final CatalogRepository catalogOperations = CatalogRepository.getInstance();

    public CatalogService() {}

    @Post("")
    public HttpResponse createCatalog(CreateCatalog createCatalog) {
        return HttpResponse.ofJson(catalogOperations.addCatalog(createCatalog));
    }

    @Get("")
    public HttpResponse listCatalogs() {
        return HttpResponse.ofJson(catalogOperations.listCatalogs());
    }

    @Get("/{name}")
    public HttpResponse getCatalog(@Param("name") String name) {
        return HttpResponse.ofJson(catalogOperations.getCatalog(name));
    }

    @Patch("/{name}")
    public HttpResponse updateCatalog(@Param("name") String name, UpdateCatalog updateCatalog) {
        return HttpResponse.ofJson(catalogOperations.updateCatalog(name, updateCatalog));
    }

    @Delete("/{name}")
    public HttpResponse deleteCatalog(@Param("name") String name) {
        catalogOperations.deleteCatalog(name);
        return HttpResponse.of(HttpStatus.OK);
    }
}
