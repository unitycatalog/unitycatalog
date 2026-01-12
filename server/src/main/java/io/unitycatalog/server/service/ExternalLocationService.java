package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CREDENTIAL;
import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
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
import io.unitycatalog.server.auth.annotation.AuthorizeKeys;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateExternalLocation;
import io.unitycatalog.server.model.ExternalLocationInfo;
import io.unitycatalog.server.model.ListExternalLocationsResponse;
import io.unitycatalog.server.model.UpdateExternalLocation;
import io.unitycatalog.server.persist.ExternalLocationRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.SneakyThrows;

@ExceptionHandler(GlobalExceptionHandler.class)
public class ExternalLocationService extends AuthorizedService {
  private final ExternalLocationRepository externalLocationRepository;
  private final MetastoreRepository metastoreRepository;

  @SneakyThrows
  public ExternalLocationService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    super(authorizer, repositories);
    this.externalLocationRepository = repositories.getExternalLocationRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
  }

  @Post("")
  @AuthorizeExpression("""
    #authorize(#principal, #metastore, OWNER) ||
    (#authorize(#principal, #metastore, CREATE_EXTERNAL_LOCATION) &&
     #authorizeAny(#principal, #credential, OWNER, CREATE_EXTERNAL_LOCATION))
    """)
  @AuthorizeKey(METASTORE)
  public HttpResponse createExternalLocation(
      @AuthorizeKeys({@AuthorizeKey(value = CREDENTIAL, key = "credential_name")})
          CreateExternalLocation createExternalLocation) {
    ExternalLocationInfo externalLocationInfo =
        externalLocationRepository.addExternalLocation(createExternalLocation);
    initializeBasicAuthorization(externalLocationInfo.getId());
    return HttpResponse.ofJson(externalLocationInfo);
  }

  private static final String LIST_AND_GET_AUTH_EXPRESSION = """
    #authorize(#principal, #metastore, OWNER) ||
    #authorizeAny(#principal, #external_location, OWNER, READ_FILES, WRITE_FILES,
      CREATE_EXTERNAL_TABLE, CREATE_EXTERNAL_VOLUME, CREATE_MANAGED_STORAGE)
    """;

  @Get("")
  @AuthorizeExpression("#defer")
  public HttpResponse listExternalLocations(
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken) {
    ListExternalLocationsResponse locations =
        externalLocationRepository.listExternalLocations(maxResults, pageToken);
    filterExternalLocations(LIST_AND_GET_AUTH_EXPRESSION, locations.getExternalLocations());
    return HttpResponse.ofJson(locations);
  }

  @Get("/{name}")
  @AuthorizeExpression(LIST_AND_GET_AUTH_EXPRESSION)
  @AuthorizeKey(METASTORE)
  public HttpResponse getExternalLocation(
      @Param("name") @AuthorizeKey(EXTERNAL_LOCATION) String name) {
    return HttpResponse.ofJson(externalLocationRepository.getExternalLocation(name));
  }

  @Patch("/{name}")
  @AuthorizeExpression("""
    #authorize(#principal, #metastore, OWNER) ||
    (#authorize(#principal, #external_location, OWNER) &&
     (#credential == null ||
      #authorizeAny(#principal, #credential, OWNER, CREATE_EXTERNAL_LOCATION)))
    """)
  @AuthorizeKey(METASTORE)
  public HttpResponse updateExternalLocation(
      @Param("name") @AuthorizeKey(EXTERNAL_LOCATION) String name,
      @AuthorizeKeys({@AuthorizeKey(value = CREDENTIAL, key = "credential_name")})
          UpdateExternalLocation updateRequest) {
    return HttpResponse.ofJson(
        externalLocationRepository.updateExternalLocation(name, updateRequest));
  }

  @Delete("/{name}")
  @AuthorizeExpression("""
    #authorize(#principal, #metastore, OWNER) ||
    #authorize(#principal, #external_location, OWNER)
    """)
  @AuthorizeKey(METASTORE)
  public HttpResponse deleteExternalLocation(
      @Param("name") @AuthorizeKey(EXTERNAL_LOCATION) String name,
      @Param("force") Optional<Boolean> force) {
    ExternalLocationDAO externalLocationDAO =
        externalLocationRepository.deleteExternalLocation(name, force.orElse(false));
    removeAuthorizations(externalLocationDAO.getId().toString());
    return HttpResponse.of(HttpStatus.OK);
  }

  /**
   * Filters a list of external locations based on the authorization expression.
   *
   * <p>This method removes external locations from the list that the current principal does not
   * have permission to access according to the provided authorization expression. The filtering is
   * done in-place by removing unauthorized entries from the list.
   *
   * @param expression The authorization expression to evaluate (e.g., checking for READ_FILES,
   *     WRITE_FILES, or other permissions)
   * @param entries The list of external location entries to filter (modified in-place)
   */
  public void filterExternalLocations(String expression, List<ExternalLocationInfo> entries) {
    // TODO: would be nice to move this to filtering in the Decorator response
    UUID principalId = userRepository.findPrincipalId();

    evaluator.filter(
        principalId,
        expression,
        entries,
        externalLocationInfo ->
            Map.of(
                METASTORE,
                metastoreRepository.getMetastoreId(),
                EXTERNAL_LOCATION,
                UUID.fromString(externalLocationInfo.getId())));
  }
}
