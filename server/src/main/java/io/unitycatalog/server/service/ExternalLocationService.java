package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateExternalLocation;
import io.unitycatalog.server.model.ExternalLocationInfo;
import io.unitycatalog.server.model.ListExternalLocationsResponse;
import io.unitycatalog.server.model.UpdateExternalLocation;
import io.unitycatalog.server.persist.ExternalLocationRepository;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.IdentityUtils;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.unitycatalog.server.model.SecurableType.METASTORE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class ExternalLocationService {
    private static final ExternalLocationRepository REPOSITORY = ExternalLocationRepository.getInstance();
    private final UnityCatalogAuthorizer authorizer;
    private final UnityAccessEvaluator evaluator;

    @SneakyThrows
    public ExternalLocationService(UnityCatalogAuthorizer authorizer) {
        this.authorizer = authorizer;
        evaluator = new UnityAccessEvaluator(authorizer);
    }

    @Post("")
    // TODO: Do we add CREATE_EXTERNAL_LOCATION
    @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
    @AuthorizeKey(METASTORE)
    public HttpResponse createExternalLocation(CreateExternalLocation createExternalLocation) {
        ExternalLocationDAO externalLocationDAO = REPOSITORY.addExternalLocation(createExternalLocation);
        initializeAuthorizations(externalLocationDAO.getId());
        return HttpResponse.ofJson(externalLocationDAO.toExternalLocationInfo());
    }

    @Get("")
    @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
    @AuthorizeKey(METASTORE)
    public HttpResponse listExternalLocations(
            @Param("max_results") Optional<Integer> maxResults,
            @Param("page_token") Optional<String> pageToken) {
        ListExternalLocationsResponse locations = REPOSITORY.listExternalLocations(maxResults, pageToken);
        return HttpResponse.ofJson(locations);
    }

    @Get("/{name}")
    @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER)
      """)
    @AuthorizeKey(METASTORE)
    public HttpResponse getExternalLocation(@Param("name") String name) {
        return HttpResponse.ofJson(REPOSITORY.getExternalLocation(name));
    }

    @Patch("/{name}")
    @AuthorizeExpression("""
      #authorize(#principal, #externalLocation, OWNER)
      """)
    @AuthorizeKey(METASTORE)
    public HttpResponse updateExternalLocation(
            @Param("name") String name, UpdateExternalLocation updateRequest) {
        return HttpResponse.ofJson(REPOSITORY.updateExternalLocation(name, updateRequest));
    }

    @Delete("/{name}")
    @AuthorizeExpression("""
      #authorize(#principal, #metastore, OWNER)
      """)
    @AuthorizeKey(METASTORE)
    public HttpResponse deleteExternalLocation(@Param("name") String name) {
        ExternalLocationDAO externalLocationDAO = REPOSITORY.deleteExternalLocation(name);
        removeAuthorizations(externalLocationDAO.getId());
        return HttpResponse.of(HttpStatus.OK);
    }

    private void initializeAuthorizations(UUID externalLocationId) {
        UUID principalId = IdentityUtils.findPrincipalId();
        authorizer.grantAuthorization(
                principalId, externalLocationId, Privileges.OWNER);
    }

    private void removeAuthorizations(UUID externalLocationId) {
        authorizer.clearAuthorizationsForResource(externalLocationId);
    }
}
