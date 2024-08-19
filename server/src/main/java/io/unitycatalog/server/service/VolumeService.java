package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeKeys;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.auth.decorator.UnityAccessUtil;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateVolumeRequestContent;
import io.unitycatalog.server.model.ListVolumesResponseContent;
import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.UpdateVolumeRequestContent;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.VolumeRepository;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.unitycatalog.server.model.ResourceType.CATALOG;
import static io.unitycatalog.server.model.ResourceType.METASTORE;
import static io.unitycatalog.server.model.ResourceType.SCHEMA;
import static io.unitycatalog.server.model.ResourceType.TABLE;
import static io.unitycatalog.server.model.ResourceType.VOLUME;

@ExceptionHandler(GlobalExceptionHandler.class)
public class VolumeService {
  private static final VolumeRepository VOLUME_REPOSITORY = VolumeRepository.getInstance();
  private static final SchemaRepository SCHEMA_REPOSITORY = SchemaRepository.getInstance();
  private static final CatalogRepository CATALOG_REPOSITORY = CatalogRepository.getInstance();

  private final UnityCatalogAuthorizer authorizer;
  private final UnityAccessEvaluator evaluator;

  @SneakyThrows
  public VolumeService(UnityCatalogAuthorizer authorizer) {
    this.authorizer = authorizer;
    evaluator = new UnityAccessEvaluator(authorizer);
  }

  @Post("")
  // TODO: for now, we are not supporting CREATE VOLUME or CREATE EXTERNAL VOLUME privileges
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, METASTORE_ADMIN) ||
          #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA)
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse createVolume(@AuthorizeKeys({
                                      @AuthorizeKey(value = SCHEMA, key = "schema_name"),
                                      @AuthorizeKey(value = CATALOG, key = "catalog_name")
                                    })
                                   CreateVolumeRequestContent createVolumeRequest) {
    // Throw error if catalog/schema does not exist
    VolumeInfo volumeInfo = VOLUME_REPOSITORY.createVolume(createVolumeRequest);
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
    ListVolumesResponseContent listVolumesResponse = VOLUME_REPOSITORY.listVolumes(
            catalogName, schemaName, maxResults, pageToken, includeBrowse);

    filterVolumes("""
            #authorize(#principal, #metastore, METASTORE_ADMIN) ||
            #authorize(#principal, #volume, OWNER) ||
            (#authorize(#principal, #volume, READ_VOLUME) && #authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG))
            """, listVolumesResponse.getVolumes());

    return HttpResponse.ofJson(listVolumesResponse);
  }

  @Get("/{full_name}")
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, METASTORE_ADMIN) ||
          (#authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG) && #authorizeAny(#principal, #volume, OWNER, READ_VOLUME))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse getVolume(
      @Param("full_name") @AuthorizeKey(VOLUME) String fullName,
      @Param("include_browse") Optional<Boolean> includeBrowse) {
    return HttpResponse.ofJson(VOLUME_REPOSITORY.getVolume(fullName));
  }

  @Patch("/{full_name}")
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, METASTORE_ADMIN) ||
          (#authorize(#principal, #volume, OWNER) && #authorizeAny(#privilege, #catalog, OWNER, USE_CATALOG) && #authorize(#principal, #schema, USE_SCHEMA))
          """)
  public HttpResponse updateVolume(
      @Param("full_name") String fullName, UpdateVolumeRequestContent updateVolumeRequest) {
    return HttpResponse.ofJson(VOLUME_REPOSITORY.updateVolume(fullName, updateVolumeRequest));
  }

  @Delete("/{full_name}")
  @AuthorizeExpression("""
          #authorize(#principal, #metastore, METASTORE_ADMIN) ||
          (#authorize(#principal, #volume, OWNER) && #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) && #authorize(#principal, #schema, USE_SCHEMA))
          """)
  @AuthorizeKey(METASTORE)
  public HttpResponse deleteVolume(@Param("full_name") @AuthorizeKey(VOLUME) String fullName) {
    VolumeInfo volumeInfo = VOLUME_REPOSITORY.getVolume(fullName);
    VOLUME_REPOSITORY.deleteVolume(fullName);
    removeAuthorizations(volumeInfo);
    return HttpResponse.of(HttpStatus.OK);
  }

  public void filterVolumes(String expression, List<VolumeInfo> entries) {
    // TODO: would be nice to move this to filtering in the Decorator response
    UUID principalId = UnityAccessUtil.findPrincipalId();

    evaluator.filter(
            principalId,
            expression,
            entries,
            vi -> {
              CatalogInfo catalogInfo = CATALOG_REPOSITORY.getCatalog(vi.getCatalogName());
              SchemaInfo schemaInfo =
                      SCHEMA_REPOSITORY.getSchema(vi.getCatalogName() + "." + vi.getSchemaName());
              return Map.of(
                      METASTORE,
                      MetastoreRepository.getInstance().getMetastoreId(),
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
            SCHEMA_REPOSITORY.getSchema(volumeInfo.getCatalogName() + "." + volumeInfo.getSchemaName());
    UUID principalId = UnityAccessUtil.findPrincipalId();
    // add owner privilege
    authorizer.grantAuthorization(
            principalId, UUID.fromString(volumeInfo.getVolumeId()), Privilege.OWNER);
    // make table a child of the schema
    authorizer.addHierarchyChild(
            UUID.fromString(schemaInfo.getSchemaId()), UUID.fromString(volumeInfo.getVolumeId()));
  }

  private void removeAuthorizations(VolumeInfo volumeInfo) {
    SchemaInfo schemaInfo =
            SCHEMA_REPOSITORY.getSchema(volumeInfo.getCatalogName() + "." + volumeInfo.getSchemaName());
    // remove any direct authorizations on the table
    authorizer.clearAuthorizationsForResource(UUID.fromString(volumeInfo.getVolumeId()));
    // remove link to the parent schema
    authorizer.removeHierarchyChild(
            UUID.fromString(schemaInfo.getSchemaId()), UUID.fromString(volumeInfo.getVolumeId()));
  }

}
