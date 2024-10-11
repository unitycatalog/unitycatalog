package io.unitycatalog.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.delta.DeltaKernelUtils;
import io.unitycatalog.cli.utils.CliException;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.*;
import java.io.*;
import java.net.URI;
import java.util.*;
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
    URI baseURI = URI.create(volumeLocation);
    Configuration conf =
        DeltaKernelUtils.getHDFSConfiguration(
            baseURI,
            tempCredApi
                .generateTemporaryVolumeCredentials(
                    new GenerateTemporaryVolumeCredential()
                        .volumeId(volumeInfo.getVolumeId())
                        .operation(VolumeOperation.WRITE_VOLUME))
                .getAwsTempCredentials());
    FileSystem fs =
        DeltaKernelUtils.getFileSystem(
            URI.create(DeltaKernelUtils.substituteSchemeForS3(volumeLocation)), conf);
    try {
      org.apache.hadoop.fs.Path path =
          new org.apache.hadoop.fs.Path(DeltaKernelUtils.substituteSchemeForS3(baseURI.toString()));
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
    URI baseURI = URI.create(volumeLocation);
    URI relativeURI = resolveURI(baseURI, relativePath);
    Configuration conf =
        DeltaKernelUtils.getHDFSConfiguration(
            relativeURI,
            tempCredApi
                .generateTemporaryVolumeCredentials(
                    new GenerateTemporaryVolumeCredential()
                        .volumeId(volumeInfo.getVolumeId())
                        .operation(VolumeOperation.READ_VOLUME))
                .getAwsTempCredentials());
    URI relativeURIWithS3AScheme =
        URI.create(DeltaKernelUtils.substituteSchemeForS3(relativeURI.toString()));
    FileSystem fs =
        DeltaKernelUtils.getFileSystem(
            URI.create(DeltaKernelUtils.substituteSchemeForS3(volumeLocation)), conf);
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(relativeURIWithS3AScheme);
    try {
      if (!fs.exists(path)) {
        throw new CliException("Volume location does not exist " + path);
      }
      FileStatus fileStatus = fs.getFileStatus(path);
      if (fileStatus.isDirectory()) {
        List<String> fileList = new ArrayList<>();
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus status : fileStatuses) {
          URI entityPath = status.getPath().toUri();
          fileList.add(
              relativeURIWithS3AScheme.relativize(entityPath)
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

  public static URI resolveURI(URI baseURI, String relativePath) {
    URI resolvedURI = baseURI.resolve(relativePath);
    // Ensure the resolved URI is equivalent to or longer than the base URI
    String basePath = baseURI.getPath();
    String resolvedPath = resolvedURI.getPath();

    if (!resolvedPath.startsWith(basePath)) {
      throw new CliException(
          "Resolved URI is outside the base URI path. Read Failed. " + resolvedPath);
    }

    return resolvedURI;
  }

  private static String updateVolume(VolumesApi apiClient, JSONObject json)
      throws JsonProcessingException, ApiException {
    String volumeFullName = json.getString(CliParams.FULL_NAME.getServerParam());
    json.remove(CliParams.FULL_NAME.getServerParam());
    if (json.length() == 0) {
      List<CliParams> optionalParams =
          CliUtils.cliOptions.get(CliUtils.VOLUME).get(CliUtils.UPDATE).getOptionalParams();
      String errorMessage = "No parameters to update, please provide one of:";
      for (CliParams param : optionalParams) {
        errorMessage += "\n  --" + param.val();
      }
      throw new CliException(errorMessage);
    }
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
