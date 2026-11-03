package io.unitycatalog.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.delta.DeltaKernelUtils;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.cli.utils.Constants;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.GenerateTemporaryVolumeCredential;
import io.unitycatalog.client.model.UpdateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeOperation;
import io.unitycatalog.client.model.VolumeType;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.json.JSONObject;

public class VolumeCli {
  private static final ObjectMapper objectMapper = CliUtils.getObjectMapper();
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {
    VolumesApi volumesApi = new VolumesApi(apiClient);
    TemporaryCredentialsApi tempCredApi = new TemporaryCredentialsApi(apiClient);
    String[] subArgs = cmd.getArgs();
    objectWriter = CliUtils.getObjectWriter(cmd);
    String subCommand = subArgs[1];
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = CliUtils.EMPTY;
    switch (subCommand) {
      case CliUtils.CREATE:
        output = createVolume(volumesApi, json);
        break;
      case CliUtils.LIST:
        output = listVolumes(volumesApi, json);
        break;
      case CliUtils.GET:
        output = getVolume(volumesApi, json);
        break;
      case CliUtils.UPDATE:
        output = updateVolume(volumesApi, json);
        break;
      case CliUtils.DELETE:
        output = deleteVolume(volumesApi, json);
        break;
      case CliUtils.READ:
        output = readVolume(volumesApi, tempCredApi, json);
        break;
      case CliUtils.WRITE:
        output = writeRandomFileInVolume(volumesApi, tempCredApi, json);
        break;
      default:
        CliUtils.printEntityHelp(CliUtils.VOLUME);
    }
    CliUtils.postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static String createVolume(VolumesApi volumesApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    CliUtils.resolveFullNameToThreeLevelNamespace(json);
    if (!json.has(CliParams.VOLUME_TYPE.getServerParam())) {
      json.put(CliParams.VOLUME_TYPE.getServerParam(), VolumeType.EXTERNAL.toString());
    }
    CreateVolumeRequestContent createVolumeRequest;
    createVolumeRequest = objectMapper.readValue(json.toString(), CreateVolumeRequestContent.class);
    return objectWriter.writeValueAsString(volumesApi.createVolume(createVolumeRequest));
  }

  private static String listVolumes(VolumesApi volumesApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String catalogName = json.getString(CliParams.CATALOG_NAME.getServerParam());
    String schemaName = json.getString(CliParams.SCHEMA_NAME.getServerParam());
    int maxResults = 100;
    if (json.has(CliParams.MAX_RESULTS.getServerParam())) {
      maxResults = json.getInt(CliParams.MAX_RESULTS.getServerParam());
    }
    String pageToken = null;
    if (json.has(CliParams.PAGE_TOKEN.getServerParam())) {
      pageToken = json.getString(CliParams.PAGE_TOKEN.getServerParam());
    }
    return objectWriter.writeValueAsString(
        volumesApi.listVolumes(catalogName, schemaName, maxResults, pageToken).getVolumes());
  }

  private static String getVolume(VolumesApi volumesApi, JSONObject json)
      throws JsonProcessingException, ApiException {
    String volumeFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    return objectWriter.writeValueAsString(volumesApi.getVolume(volumeFullName));
  }

  private static String writeRandomFileInVolume(
      VolumesApi volumesApi, TemporaryCredentialsApi tempCredApi, JSONObject json)
      throws ApiException {
    String volumeFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    VolumeInfo volumeInfo = volumesApi.getVolume(volumeFullName);
    String volumeLocation = volumeInfo.getStorageLocation();
    Configuration conf =
        DeltaKernelUtils.getHDFSConfiguration(
            new org.apache.hadoop.fs.Path(volumeLocation).toUri(),
            tempCredApi.generateTemporaryVolumeCredentials(
                new GenerateTemporaryVolumeCredential()
                    .volumeId(volumeInfo.getVolumeId())
                    .operation(VolumeOperation.WRITE_VOLUME)));
    FileSystem fs = DeltaKernelUtils.getFileSystem(volumePath(volumeLocation).toUri(), conf);
    try {
      // Generate a random file name
      String randomFileName = UUID.randomUUID() + ".txt";

      if (volumeLocation.endsWith("/")) {
        volumeLocation = volumeLocation.substring(0, volumeLocation.length() - 1);
      }

      // Create a Path object using the S3 directory and the random file name
      org.apache.hadoop.fs.Path filePath =
          new org.apache.hadoop.fs.Path(
              DeltaKernelUtils.substituteSchemeForS3(volumeLocation), randomFileName);

      // Write data to the file
      try (BufferedWriter writer =
          new BufferedWriter(new OutputStreamWriter(fs.create(filePath, true)))) {
        writer.write("This is a test file with random content.");
      }

      System.out.println("File written to: " + filePath);
      return filePath.toString();
    } catch (Exception e) {
      throw new CliException("Failed to write file to volume location " + volumeLocation, e);
    } finally {
      try {
        fs.close();
      } catch (IOException e) {
        throw new CliException("Failed to close file system", e);
      }
    }
  }

  private static String readVolume(
      VolumesApi volumesApi, TemporaryCredentialsApi tempCredApi, JSONObject json)
      throws ApiException {
    String volumeFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    VolumeInfo volumeInfo = volumesApi.getVolume(volumeFullName);
    String volumeLocation = volumeInfo.getStorageLocation();
    String relativePath =
        json.has(CliParams.PATH.getServerParam())
            ? json.getString(CliParams.PATH.getServerParam())
            : "";
    if (!volumeLocation.endsWith("/")) {
      volumeLocation = volumeLocation + "/";
    }
    org.apache.hadoop.fs.Path basePath = volumePath(volumeLocation);
    org.apache.hadoop.fs.Path path = resolvePath(basePath, relativePath);
    Configuration conf =
        DeltaKernelUtils.getHDFSConfiguration(
            new org.apache.hadoop.fs.Path(volumeLocation).toUri(),
            tempCredApi.generateTemporaryVolumeCredentials(
                new GenerateTemporaryVolumeCredential()
                    .volumeId(volumeInfo.getVolumeId())
                    .operation(VolumeOperation.READ_VOLUME)));
    FileSystem fs = DeltaKernelUtils.getFileSystem(basePath.toUri(), conf);
    try {
      if (!fs.exists(path)) {
        throw new CliException("Volume location does not exist " + path);
      }
      FileStatus fileStatus = fs.getFileStatus(path);
      if (fileStatus.isDirectory()) {
        List<String> fileList = new ArrayList<>();
        FileStatus[] fileStatuses = fs.listStatus(path);
        boolean isLocal = Constants.URI_SCHEME_FILE.equals(basePath.toUri().getScheme());
        for (FileStatus status : fileStatuses) {
          URI name = path.toUri().relativize(status.getPath().toUri());
          fileList.add(
              (isLocal ? name.getRawPath() : name.getPath())
                  + " "
                  + (status.isDirectory() ? "[directory]" : "[file]"));
        }
        return String.join("\n", fileList);
      } else if (fileStatus.isFile()) {
        // Read the file contents
        try (InputStream inputStream = fs.open(path);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
          IOUtils.copyBytes(inputStream, outputStream, conf, false);
          return outputStream.toString();
        }
      } else {
        throw new CliException(
            "The specified path is neither a file nor a directory. " + fileStatus.getPath());
      }
    } catch (IOException e) {
      throw new CliException("Failed to read volume location " + path, e);
    } finally {
      try {
        fs.close();
      } catch (IOException e) {
        throw new CliException("Failed to close file system", e);
      }
    }
  }

  private static org.apache.hadoop.fs.Path volumePath(String volumeLocation) {
    String location = DeltaKernelUtils.substituteSchemeForS3(volumeLocation);
    return Constants.SUPPORTED_CLOUD_SCHEMES.contains(
            new org.apache.hadoop.fs.Path(volumeLocation).toUri().getScheme())
        ? new org.apache.hadoop.fs.Path(location)
        : new org.apache.hadoop.fs.Path(URI.create(location));
  }

  private static org.apache.hadoop.fs.Path resolvePath(
      org.apache.hadoop.fs.Path basePath, String relativePath) {
    if (relativePath.isEmpty()) {
      return basePath;
    }
    org.apache.hadoop.fs.Path resolvedPath =
        Constants.URI_SCHEME_FILE.equals(basePath.toUri().getScheme())
            ? localPath(basePath, relativePath)
            : literalPath(basePath, relativePath);
    // Ensure the resolved path stays inside the volume.
    String base = basePath.toUri().getPath().replaceAll("/$", "") + "/";
    String resolved = resolvedPath.toUri().getPath();
    if (!Objects.equals(resolvedPath.toUri().getScheme(), basePath.toUri().getScheme())
        || !Objects.equals(resolvedPath.toUri().getAuthority(), basePath.toUri().getAuthority())
        || !(resolved + "/").startsWith(base)) {
      throw new CliException("Path is outside the volume location. Read failed: " + resolvedPath);
    }
    return resolvedPath;
  }

  private static org.apache.hadoop.fs.Path literalPath(
      org.apache.hadoop.fs.Path basePath, String relativePath) {
    return relativePath.matches("[a-zA-Z][a-zA-Z0-9+.-]*://.*")
        ? new org.apache.hadoop.fs.Path(DeltaKernelUtils.substituteSchemeForS3(relativePath))
        : new org.apache.hadoop.fs.Path(
            basePath, relativePath.startsWith("/") ? relativePath : "./" + relativePath);
  }

  private static org.apache.hadoop.fs.Path localPath(
      org.apache.hadoop.fs.Path basePath, String relativePath) {
    boolean isName =
        !relativePath.matches("[a-zA-Z][a-zA-Z0-9+.-]*://.*") && !relativePath.startsWith("/");
    URI resolved;
    try {
      resolved =
          URI.create(basePath.toUri() + "/").resolve(isName ? "./" + relativePath : relativePath);
    } catch (IllegalArgumentException e) {
      return literalPath(basePath, relativePath);
    }
    if (resolved.getRawQuery() != null || resolved.getRawFragment() != null) {
      return literalPath(basePath, relativePath);
    }
    URI decoded = new org.apache.hadoop.fs.Path(resolved).toUri();
    return new org.apache.hadoop.fs.Path(
        decoded.getScheme(),
        decoded.getAuthority(),
        Paths.get(decoded.getPath()).normalize().toString());
  }

  private static String updateVolume(VolumesApi apiClient, JSONObject json)
      throws JsonProcessingException, ApiException {
    String volumeFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    json.remove(CliParams.FULL_NAME.getServerParam());
    CliUtils.validateUpdateParameters(json, CliUtils.VOLUME, CliUtils.UPDATE);
    UpdateVolumeRequestContent updateVolumeRequest =
        objectMapper.readValue(json.toString(), UpdateVolumeRequestContent.class);
    return objectWriter.writeValueAsString(
        apiClient.updateVolume(volumeFullName, updateVolumeRequest));
  }

  private static String deleteVolume(VolumesApi volumesApi, JSONObject json) throws ApiException {
    String volumeFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    volumesApi.deleteVolume(volumeFullName);
    return CliUtils.EMPTY;
  }
}
