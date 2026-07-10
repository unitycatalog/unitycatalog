package io.unitycatalog.server.service;

import static io.unitycatalog.server.model.SecurableType.CATALOG;
import static io.unitycatalog.server.model.SecurableType.EXTERNAL_LOCATION;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.SCHEMA;
import static io.unitycatalog.server.model.SecurableType.VOLUME;

import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.ResponseAuthorizeFilter;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKeys;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateVolumeRequestContent;
import io.unitycatalog.server.model.ListVolumesResponseContent;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.model.UpdateVolumeRequestContent;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.persist.CatalogRepository;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.persist.VolumeRepository;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Optional;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Delete;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Patch;
import com.linecorp.armeria.server.annotation.Post;
import lombok.SneakyThrows;

@ExceptionHandler(GlobalExceptionHandler.class)
public class VolumeService extends AuthorizedService {
  private final VolumeRepository volumeRepository;
  private final SchemaRepository schemaRepository;
  private final CatalogRepository catalogRepository;
  private final MetastoreRepository metastoreRepository;

  @SneakyThrows
  public VolumeService(
      UnityCatalogAuthorizer authorizer,
      Repositories repositories,
      ServerProperties serverProperties) {
    super(authorizer, repositories, serverProperties);
    this.volumeRepository = repositories.getVolumeRepository();
    this.schemaRepository = repositories.getSchemaRepository();
    this.catalogRepository = repositories.getCatalogRepository();
    this.metastoreRepository = repositories.getMetastoreRepository();
  }

  /**
   * Creates a new volume in the specified schema.
   *
   * <p>Authorization requirements:
   *
   * <ul>
   *   <li>Must have OWNER or USE_CATALOG permission on the catalog
   *   <li>Must have either OWNER permission on the schema, or both USE_SCHEMA and CREATE_VOLUME
   *       permissions
   *   <li>For EXTERNAL volumes:
   *       <ul>
   *         <li>The storage location must not overlap with any existing table, volume, or
   *             registered model
   *         <li>If the storage location falls within a registered external location, the user
   *             must have OWNER or CREATE_EXTERNAL_VOLUME permission on that external location
   *         <li>If the storage location does not fall within any registered external location,
   *             the volume can be created without additional external location permissions
   *       </ul>
   *   <li>MANAGED volume creation delegates to catalog and schema for permission. Once the catalog
   *       or schema is allowed to create under an external location with permission
   *       CREATE_MANAGED_STORAGE, all managed entity creations under it no longer need to check for
   *       external location again.
   * </ul>
   *
   * @param createVolumeRequest the volume creation request containing volume metadata and storage
   * @return HTTP response containing the created VolumeInfo
   */
  @Post("")
  @AuthorizeExpression("""
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      (#authorize(#principal, #schema, OWNER) ||
        #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_VOLUME)) &&
      (#volume_type != 'EXTERNAL' ||
        (#no_overlap_with_data_securable &&
          (#external_location == null ||
           #authorizeAny(#principal, #external_location, OWNER, CREATE_EXTERNAL_VOLUME))))
      """)
  public HttpResponse createVolume(
      @AuthorizeResourceKeys({
        @AuthorizeResourceKey(value = SCHEMA, key = "schema_name"),
        @AuthorizeResourceKey(value = CATALOG, key = "catalog_name"),
        @AuthorizeResourceKey(value = EXTERNAL_LOCATION, key = "storage_location")
      })
      @AuthorizeKey(key = "volume_type")
      CreateVolumeRequestContent createVolumeRequest) {
    // Throw error if catalog/schema does not exist
    VolumeInfo volumeInfo = volumeRepository.createVolume(createVolumeRequest);

    SchemaInfo schemaInfo =
        schemaRepository.getSchema(volumeInfo.getCatalogName() + "." + volumeInfo.getSchemaName());
    initializeHierarchicalAuthorization(volumeInfo.getVolumeId(), schemaInfo.getSchemaId());

    return HttpResponse.ofJson(volumeInfo);
  }

  private static final String LIST_AND_GET_AUTH_EXPRESSION = """
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG) &&
          #authorizeAny(#principal, #volume, OWNER, READ_VOLUME))
      """;

  @Get("")
  @AuthorizeExpression(LIST_AND_GET_AUTH_EXPRESSION)
  @ResponseAuthorizeFilter
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse listVolumes(
      @Param("catalog_name") @AuthorizeResourceKey(CATALOG) String catalogName,
      @Param("schema_name") @AuthorizeResourceKey(SCHEMA) String schemaName,
      @Param("max_results") Optional<Integer> maxResults,
      @Param("page_token") Optional<String> pageToken,
      @Param("include_browse") Optional<Boolean> includeBrowse) {
    ListVolumesResponseContent listVolumesResponse = volumeRepository.listVolumes(
        catalogName, schemaName, maxResults, pageToken, includeBrowse);
    applyResponseFilter(SecurableType.VOLUME, listVolumesResponse.getVolumes());
    return HttpResponse.ofJson(listVolumesResponse);
  }

  @Get("/{full_name}")
  @AuthorizeExpression(LIST_AND_GET_AUTH_EXPRESSION)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse getVolume(
      @Param("full_name") @AuthorizeResourceKey(VOLUME) String fullName,
      @Param("include_browse") Optional<Boolean> includeBrowse) {
    return HttpResponse.ofJson(volumeRepository.getVolume(fullName));
  }

  @Patch("/{full_name}")
  @AuthorizeExpression("""
      (#authorize(#principal, #volume, OWNER) &&
          #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
          #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse updateVolume(
      @Param("full_name") @AuthorizeResourceKey(VOLUME) String fullName,
      UpdateVolumeRequestContent updateVolumeRequest) {
    return HttpResponse.ofJson(volumeRepository.updateVolume(fullName, updateVolumeRequest));
  }

  @Delete("/{full_name}")
  @AuthorizeExpression("""
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #volume, OWNER) &&
          #authorize(#principal, #catalog, USE_CATALOG) &&
          #authorize(#principal, #schema, USE_SCHEMA))
      """)
  @AuthorizeResourceKey(METASTORE)
  public HttpResponse deleteVolume(
      @Param("full_name") @AuthorizeResourceKey(VOLUME) String fullName) {
    VolumeInfo volumeInfo = volumeRepository.getVolume(fullName);
    volumeRepository.deleteVolume(fullName);

    SchemaInfo schemaInfo =
        schemaRepository.getSchema(volumeInfo.getCatalogName() + "." + volumeInfo.getSchemaName());
    removeHierarchicalAuthorizations(volumeInfo.getVolumeId(), schemaInfo.getSchemaId());

    return HttpResponse.of(HttpStatus.OK);
  }

}

