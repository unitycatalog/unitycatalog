package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateCatalog;
import io.unitycatalog.server.model.UpdateCatalog;
import io.unitycatalog.server.persist.CatalogRepository;
import java.util.Optional;

@ExceptionHandler(GlobalExceptionHandler.class)
public class CatalogService {
  private static final CatalogRepository CATALOG_REPOSITORY = CatalogRepository.getInstance();

  public CatalogService() {}

  @Post("")
  public HttpResponse createCatalog(CreateCatalog createCatalog) {
    return HttpResponse.ofJson(CATALOG_REPOSITORY.addCatalog(createCatalog));
  }

  @Get("")
  public HttpResponse listCatalogs(
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    return HttpResponse.ofJson(CATALOG_REPOSITORY.listCatalogs(maxResults, pageToken));
  }

  @Get("/{name}")
  public HttpResponse getCatalog(@Param("name") String name) {
    return HttpResponse.ofJson(CATALOG_REPOSITORY.getCatalog(name));
  }

  @Patch("/{name}")
  public HttpResponse updateCatalog(@Param("name") String name, UpdateCatalog updateCatalog) {
    return HttpResponse.ofJson(CATALOG_REPOSITORY.updateCatalog(name, updateCatalog));
  }

  @Delete("/{name}")
  public HttpResponse deleteCatalog(
      @Param("name") String name, @Param("force") Optional<Boolean> force) {
    CATALOG_REPOSITORY.deleteCatalog(name, force.orElse(false));
    return HttpResponse.of(HttpStatus.OK);
  }
}
