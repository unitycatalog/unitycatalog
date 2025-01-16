package io.unitycatalog.server.service;

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
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateVolumeRequestContent;
import io.unitycatalog.server.model.ListVolumesResponseContent;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.UpdateVolumeRequestContent;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.persist.*;
import io.unitycatalog.server.persist.model.Privileges;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.VOLUME;

@ExceptionHandler(GlobalExceptionHandler.class)
public class VolumeService {
  private final VolumeRepository volumeRepository;
  private final SchemaRepository schemaRepository;
  private final CatalogRepository catalogRepository;
  private final MetastoreRepository metastoreRepository;
  private final UserRepository userRepository;

  private final UnityCatalogAuthorizer authorizer;
  private final UnityAccessEvaluator evaluator;

  @SneakyThrows
  public VolumeService(UnityCatalogAuthorizer authorizer, Repositories repositories) {
    this.authorizer = authorizer;
    this.evaluator = new UnityAccessEvaluator(authorizer);
    this.volumeRepository = repositories.getVolumeRepository();
    this.schemaRepository = repositories.getSchemaRepository();
    this.catalogRepository = repositories.getCatalogRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
    this.userRepository = repositories.getUserRepository();
  }

  @Post("")
  // TODO: for now, we are not supporting CREATE VOLUME or CREATE EXTERNAL VOLUME privileges
  @AuthorizeExpression("""
          #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA)
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse createVolume(@AuthorizeKeys({
                                      @AuthorizeKey(value = SCHEMA, key = "schema_name"),
                                      @AuthorizeKey(value = CATALOG, key = "catalog_name")
                                    })
                                   CreateVolumeRequestContent createVolumeRequest) {
    // Throw error if catalog/schema does not exist
    VolumeInfo volumeInfo = volumeRepository.createVolume(createVolumeRequest);
    initializeAuthorizations(volumeInfo);
    return HttpResponse.ofJson(volumeInfo);
  }

  @Get("")
  @AuthorizeExpression("#defer")
  public HttpResponse listVolumes(
      @Param("catalog_name") String catalogName,
      @Param("schema_name") String schemaName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken,
      @Param("include_browse") Optional<Boolean> includeBrowse) {
    ListVolumesResponseContent listVolumesResponse = volumeRepository.listVolumes(
            catalogName, schemaName, maxResults, pageToken, includeBrowse);

    filterVolumes("""
            #authorize(#principal, #metastore, OWNER) ||
            #authorize(#principal, #catalog, OWNER) ||
            (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
            (#authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG) && #authorizeAny(#principal, #volume, OWNER, READ_VOLUME))
            """, listVolumesResponse.getVolumes());

    return HttpResponse.ofJson(listVolumesResponse);
  }

  @Get("/{full_name}")
  @AuthorizeExpression("""
            #authorize(#principal, #metastore, OWNER) ||
            #authorize(#principal, #catalog, OWNER) ||
            (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
            (#authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG) && #authorizeAny(#principal, #volume, OWNER, READ_VOLUME))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse getVolume(
      @Param("full_name") @AuthorizeKey(VOLUME) String fullName,
      @Param("include_browse") Optional<Boolean> includeBrowse) {
    return HttpResponse.ofJson(volumeRepository.getVolume(fullName));
  }

  @Patch("/{full_name}")
  @AuthorizeExpression("""
          (#authorize(#principal, #volume, OWNER) && #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse updateVolume(
      @Param("full_name") @AuthorizeKey(VOLUME) String fullName, UpdateVolumeRequestContent updateVolumeRequest) {
    return HttpResponse.ofJson(volumeRepository.updateVolume(fullName, updateVolumeRequest));
  }

  @Delete("/{full_name}")
  @AuthorizeExpression("""
          #authorize(#principal, #catalog, OWNER) ||
          (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
          (#authorize(#principal, #volume, OWNER) && #authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, USE_SCHEMA))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse deleteVolume(@Param("full_name") @AuthorizeKey(VOLUME) String fullName) {
    VolumeInfo volumeInfo = volumeRepository.getVolume(fullName);
    volumeRepository.deleteVolume(fullName);
    removeAuthorizations(volumeInfo);
    return HttpResponse.of(HttpStatus.OK);
  }

  public void filterVolumes(String expression, List<VolumeInfo> entries) {
    // TODO: would be nice to move this to filtering in the Decorator response
    UUID principalId = userRepository.findPrincipalId();

    evaluator.filter(
            principalId,
            expression,
            entries,
            vi -> {
              CatalogInfo catalogInfo = catalogRepository.getCatalog(vi.getCatalogName());
              SchemaInfo schemaInfo =
                      schemaRepository.getSchema(vi.getCatalogName() + "." + vi.getSchemaName());
              return Map.of(
                      METASTORE,
                      metastoreRepository.getMetastoreId(),
                      CATALOG,
                      UUID.fromString(catalogInfo.getId()),
                      SCHEMA,
                      UUID.fromString(schemaInfo.getSchemaId()),
                      VOLUME,
                      UUID.fromString(vi.getVolumeId()));
            });
  }

  private void initializeAuthorizations(VolumeInfo volumeInfo) {
    SchemaInfo schemaInfo =
            schemaRepository.getSchema(volumeInfo.getCatalogName() + "." + volumeInfo.getSchemaName());
    UUID principalId = userRepository.findPrincipalId();
    // add owner privilege
    authorizer.grantAuthorization(
            principalId, UUID.fromString(volumeInfo.getVolumeId()), Privileges.OWNER);
    // make table a child of the schema
    authorizer.addHierarchyChild(
            UUID.fromString(schemaInfo.getSchemaId()), UUID.fromString(volumeInfo.getVolumeId()));
  }

  private void removeAuthorizations(VolumeInfo volumeInfo) {
    SchemaInfo schemaInfo =
            schemaRepository.getSchema(volumeInfo.getCatalogName() + "." + volumeInfo.getSchemaName());
    // remove any direct authorizations on the table
    authorizer.clearAuthorizationsForResource(UUID.fromString(volumeInfo.getVolumeId()));
    // remove link to the parent schema
    authorizer.removeHierarchyChild(
            UUID.fromString(schemaInfo.getSchemaId()), UUID.fromString(volumeInfo.getVolumeId()));
  }

}
