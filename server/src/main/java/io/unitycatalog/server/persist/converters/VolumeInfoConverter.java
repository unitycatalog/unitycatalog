package io.unitycatalog.server.persist.converters;

import io.unitycatalog.server.model.VolumeInfo;
import io.unitycatalog.server.model.VolumeType;
import io.unitycatalog.server.persist.FileUtils;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;

import java.util.Date;
import java.util.UUID;

public class VolumeInfoConverter {

    public static VolumeInfoDAO toDAO(VolumeInfo volumeInfo) {
        if (volumeInfo == null) {
            return null;
        }
        return VolumeInfoDAO.builder()
                .id(UUID.fromString(volumeInfo.getVolumeId()))
                .name(volumeInfo.getName())
                .comment(volumeInfo.getComment())
                .storageLocation(volumeInfo.getStorageLocation())
                .createdAt(volumeInfo.getCreatedAt() != null?  new Date(volumeInfo.getCreatedAt()) : new Date())
                .updatedAt(volumeInfo.getUpdatedAt() != null ? new Date(volumeInfo.getUpdatedAt()) : new Date())
                .volumeType(volumeInfo.getVolumeType().getValue())
                .build();
    }

    public static VolumeInfo fromDAO(VolumeInfoDAO dao) {
        if (dao == null) {
            return null;
        }
        return new VolumeInfo()
                .volumeId(dao.getId().toString())
                .name(dao.getName())
                .comment(dao.getComment())
                .storageLocation(FileUtils.convertRelativePathToURI(dao.getStorageLocation()))
                .createdAt(dao.getCreatedAt().getTime())
                .updatedAt(dao.getUpdatedAt().getTime())
                .volumeType(VolumeType.valueOf(dao.getVolumeType()));
    }

}