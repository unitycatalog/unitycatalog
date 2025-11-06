package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.METASTORE;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Patch;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateExternalLocation;
import io.unitycatalog.server.model.ListExternalLocationsResponse;
import io.unitycatalog.server.model.UpdateExternalLocation;
import io.unitycatalog.server.persist.ExternalLocationRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import java.util.Optional;
import lombok.SneakyThrows;

@ExceptionHandler(GlobalExceptionHandler.class)
public class ExternalLocationService extends AuthorizedService {
  private final ExternalLocationRepository externalLocationRepository;

  @SneakyThrows
  public ExternalLocationService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    super(authorizer, repositories.getUserRepository());
    this.externalLocationRepository = repositories.getExternalLocationRepository();
  }

  @Post("")
  // TODO: Do we add CREATE_EXTERNAL_LOCATION
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeKey(METASTORE)
  public HttpResponse createExternalLocation(CreateExternalLocation createExternalLocation) {
    ExternalLocationDAO externalLocationDAO =
        externalLocationRepository.addExternalLocation(createExternalLocation);
    initializeBasicAuthorization(externalLocationDAO.getId().toString());
    return HttpResponse.ofJson(externalLocationDAO.toExternalLocationInfo());
  }

  @Get("")
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeKey(METASTORE)
  public HttpResponse listExternalLocations(
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    ListExternalLocationsResponse locations =
        externalLocationRepository.listExternalLocations(maxResults, pageToken);
    return HttpResponse.ofJson(locations);
  }

  @Get("/{name}")
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeKey(METASTORE)
  public HttpResponse getExternalLocation(@Param("name") String name) {
    return HttpResponse.ofJson(externalLocationRepository.getExternalLocation(name));
  }

  @Patch("/{name}")
  @AuthorizeExpression("#authorize(#principal, #externalLocation, OWNER)")
  @AuthorizeKey(METASTORE)
  public HttpResponse updateExternalLocation(
      @Param("name") String name, UpdateExternalLocation updateRequest) {
    return HttpResponse.ofJson(
        externalLocationRepository.updateExternalLocation(name, updateRequest));
  }

  @Delete("/{name}")
  @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
  @AuthorizeKey(METASTORE)
  public HttpResponse deleteExternalLocation(@Param("name") String name) {
    ExternalLocationDAO externalLocationDAO =
        externalLocationRepository.deleteExternalLocation(name);
    removeAuthorizations(externalLocationDAO.getId().toString());
    return HttpResponse.of(HttpStatus.OK);
  }
}
