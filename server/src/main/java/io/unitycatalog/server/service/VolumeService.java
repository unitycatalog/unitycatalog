package io.unitycatalog.server.service;

import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.UpdateVolumeRequestContent;
import io.unitycatalog.server.utils.ValidationUtils;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.model.CreateVolumeRequestContent;
import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.persist.VolumeOperations;

import java.util.Optional;

@ExceptionHandler(GlobalExceptionHandler.class)
public class VolumeService {
    private static final VolumeOperations volumeOperations = VolumeOperations.getInstance();

    public VolumeService() {}

    @Post("")
    public HttpResponse createVolume(CreateVolumeRequestContent createVolumeRequest) {
        // Throw error if catalog/schema does not exist
        if (!ValidationUtils.schemaExists(createVolumeRequest.getCatalogName(), createVolumeRequest.getSchemaName())) {
            return ValidationUtils.entityNotFoundResponse(ValidationUtils.VOLUME, createVolumeRequest.getCatalogName(), createVolumeRequest.getSchemaName());
        }
        if (ValidationUtils.volumeExists(createVolumeRequest.getCatalogName(), createVolumeRequest.getSchemaName(), createVolumeRequest.getName())) {
            return ValidationUtils.entityAlreadyExistsResponse(ValidationUtils.VOLUME, createVolumeRequest.getCatalogName(), createVolumeRequest.getSchemaName(), createVolumeRequest.getName());
        }
        VolumeInfo volumeInfo = volumeOperations.createVolume(createVolumeRequest);
        return HttpResponse.ofJson(volumeInfo);
    }

    @Get("")
    public HttpResponse listVolumes(@Param("catalog_name") String catalogName,
                                    @Param("schema_name") String schemaName,
                                    @Param("max_results") Optional<Integer> maxResults,
                                    @Param("page_token") Optional<String> pageToken,
                                    @Param("include_browse") Optional<Boolean> includeBrowse) {
        return HttpResponse.ofJson(volumeOperations.listVolumes(catalogName, schemaName, maxResults, pageToken, includeBrowse));
    }

    @Get("/{full_name}")
    public HttpResponse getVolume(@Param("full_name") String fullName,
                                  @Param("include_browse") Optional<Boolean> includeBrowse) {
        VolumeInfo volumeInfo = volumeOperations.getVolume(fullName);
        if (volumeInfo != null) {
            return HttpResponse.ofJson(volumeInfo);
        }
        return ValidationUtils.entityNotFoundResponse(ValidationUtils.VOLUME, fullName);
    }

    @Patch("/{full_name}")
    public HttpResponse updateVolume(@Param("full_name") String fullName,
                                     UpdateVolumeRequestContent updateVolumeRequest) {
        VolumeInfo updatedVolume = volumeOperations.updateVolume(fullName, updateVolumeRequest);
        if (updatedVolume != null) {
            return HttpResponse.ofJson(updatedVolume);
        }
        return ValidationUtils.entityNotFoundResponse(ValidationUtils.VOLUME, fullName);
    }

    @Delete("/{full_name}")
    public HttpResponse deleteVolume(@Param("full_name") String fullName) {
        volumeOperations.deleteVolume(fullName);
        return HttpResponse.of(HttpStatus.OK);
    }
}
