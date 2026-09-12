package io.unitycatalog.server.cleanup;

import com.azure.core.util.Context;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.options.DataLakePathDeleteOptions;
import io.unitycatalog.server.service.credential.azure.ADLSLocationUtils;
import io.unitycatalog.server.service.credential.azure.ADLSLocationUtils.ADLSLocationParts;
import io.unitycatalog.server.utils.NormalizedURL;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/** Recursively deletes one exact ADLS task directory. */
public final class ADLSStorageCleanupAdapter implements StorageCleanupAdapter {
  private static final long MAX_TIMEOUT_MILLIS = Duration.ofSeconds(Integer.MAX_VALUE).toMillis();

  private final DataLakeFileSystemClient client;
  private final String location;
  private final String path;
  private final Duration requestTimeout;
  private boolean deleted;

  /** Creates a timeout-configured client for this cleanup attempt. */
  public ADLSStorageCleanupAdapter(
      String sasToken, NormalizedURL location, Duration requestTimeout) {
    this(newClient(sasToken, location, requestTimeout), location, requestTimeout);
  }

  ADLSStorageCleanupAdapter(
      DataLakeFileSystemClient client, NormalizedURL location, Duration requestTimeout) {
    ADLSLocationParts parts = parseLocation(location);
    this.client = Objects.requireNonNull(client, "client");
    this.location = location.toString();
    this.path = parts.path().replaceAll("^/+|/+$", "");
    this.requestTimeout = validateTimeout(requestTimeout);
  }

  @Override
  public List<String> listBatch(int maxFiles) {
    if (maxFiles <= 0) {
      throw new IllegalArgumentException("Cleanup batch size must be positive");
    }
    return deleted ? List.of() : List.of(location);
  }

  @Override
  public void deleteBatch(List<String> locations) {
    if (locations.isEmpty()) {
      return;
    }
    if (locations.size() != 1 || !location.equals(locations.get(0))) {
      throw new IllegalArgumentException("ADLS cleanup can delete only its exact task directory");
    }
    client.deleteDirectoryIfExistsWithResponse(
        path, new DataLakePathDeleteOptions().setIsRecursive(true), requestTimeout, Context.NONE);
    deleted = true;
  }

  private static DataLakeFileSystemClient newClient(
      String sasToken, NormalizedURL location, Duration requestTimeout) {
    ADLSLocationParts parts = parseLocation(location);
    Duration timeout = validateTimeout(requestTimeout);
    Duration tryTimeout = Duration.ofSeconds((timeout.toMillis() + 999) / 1000);
    RequestRetryOptions retryOptions =
        new RequestRetryOptions(null, null, tryTimeout, null, null, null);
    return new DataLakeFileSystemClientBuilder()
        .endpoint("https://" + parts.account() + "/" + parts.container())
        .sasToken(Objects.requireNonNull(sasToken, "sasToken"))
        .retryOptions(retryOptions)
        .buildClient();
  }

  private static ADLSLocationParts parseLocation(NormalizedURL location) {
    ADLSLocationParts parts =
        ADLSLocationUtils.parseLocation(Objects.requireNonNull(location, "location"));
    if (!("abfs".equals(parts.scheme()) || "abfss".equals(parts.scheme()))
        || parts.container() == null
        || parts.container().isBlank()
        || parts.account() == null
        || parts.account().isBlank()
        || parts.accountName() == null
        || parts.accountName().isBlank()
        || parts.path() == null
        || parts.path().chars().allMatch(character -> character == '/')) {
      throw new IllegalArgumentException(
          "ADLS cleanup requires an abfs or abfss directory location");
    }
    return parts;
  }

  private static Duration validateTimeout(Duration requestTimeout) {
    try {
      long millis = Objects.requireNonNull(requestTimeout, "requestTimeout").toMillis();
      if (millis <= 0 || millis > MAX_TIMEOUT_MILLIS) {
        throw new IllegalArgumentException(
            "ADLS request timeout must be between 1 and " + MAX_TIMEOUT_MILLIS + " milliseconds");
      }
      return Duration.ofMillis(millis);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("ADLS request timeout is too large", e);
    }
  }
}
