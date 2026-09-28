package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import io.unitycatalog.server.cleanup.StorageCleanupWorker;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository;
import io.unitycatalog.server.persist.StorageCleanupTaskRepository.Claim;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.utils.CooperativeDeadline;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.io.FileInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@SuppressWarnings("unchecked")
class ADLSPrefixOperationsTest {
  private static final UUID RESOURCE_ID = UUID.randomUUID();
  private static final String DIRECTORY = "root/tables/" + RESOURCE_ID;
  private static final String CONTAINER = "abfss://container@account.dfs.core.windows.net/";
  private static final String PREFIX = CONTAINER + DIRECTORY + "/";
  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

  private final ADLSFileIO fileIO = mock(ADLSFileIO.class);
  private final DataLakeFileSystemClient client = mock(DataLakeFileSystemClient.class);
  private final Clock clock = mock(Clock.class);
  private final CooperativeDeadline deadline = new CooperativeDeadline(clock, NOW.plusSeconds(20));
  private final ADLSPrefixOperations operations =
      new ADLSPrefixOperations(fileIO, PREFIX, deadline);
  private final Map<String, Boolean> paths = new LinkedHashMap<>();
  private final List<String> deleted = new ArrayList<>();
  private Thread expectedThread;
  private Runnable afterDelete = () -> {};

  @BeforeEach
  void setUp() {
    when(clock.instant()).thenReturn(NOW);
    when(fileIO.client(PREFIX)).thenReturn(client);
    paths.put(DIRECTORY, true);
    expectedThread = Thread.currentThread();
    when(client.listPaths(any(), isNull()))
        .thenAnswer(
            invocation -> {
              assertThat(Thread.currentThread()).isSameAs(expectedThread);
              ListPathsOptions options = invocation.getArgument(0);
              assertThat(options.getMaxResults()).isEqualTo(1000);
              String directory = options.getPath();
              if (!paths.containsKey(directory)) {
                throw storageFailure(404);
              }
              List<PathItem> items =
                  paths.entrySet().stream()
                      .filter(entry -> entry.getKey().startsWith(directory + "/"))
                      .filter(
                          entry ->
                              options.isRecursive()
                                  || !entry
                                      .getKey()
                                      .substring(directory.length() + 1)
                                      .contains("/"))
                      .map(entry -> item(entry.getKey(), entry.getValue()))
                      .toList();
              return listing(items.iterator());
            });
    when(client.getFileClient(anyString()))
        .thenAnswer(
            invocation -> {
              String path = invocation.getArgument(0);
              DataLakeFileClient file = mock(DataLakeFileClient.class);
              when(file.deleteIfExists())
                  .thenAnswer(
                      ignored -> {
                        assertThat(Thread.currentThread()).isSameAs(expectedThread);
                        boolean existed = paths.remove(path) != null;
                        deleted.add(path);
                        afterDelete.run();
                        return existed;
                      });
              return file;
            });
    when(client.getDirectoryClient(anyString()))
        .thenAnswer(
            invocation -> {
              String path = invocation.getArgument(0);
              DataLakeDirectoryClient directory = mock(DataLakeDirectoryClient.class);
              when(directory.deleteIfExists())
                  .thenAnswer(
                      ignored -> {
                        assertThat(Thread.currentThread()).isSameAs(expectedThread);
                        if (paths.keySet().stream().anyMatch(name -> name.startsWith(path + "/"))) {
                          throw storageFailure(409);
                        }
                        boolean existed = paths.remove(path) != null;
                        deleted.add(path);
                        return existed;
                      });
              return directory;
            });
  }

  @Test
  void deletesChildrenBeforeParentsAndLeavesSiblingsAlone() {
    paths.put(DIRECTORY + "/nested", true);
    paths.put(DIRECTORY + "/nested/data", false);
    paths.put(DIRECTORY + "/empty", true);
    paths.put(DIRECTORY + "-sibling/data", false);

    operations.deletePrefix(PREFIX);

    assertThat(deleted)
        .containsExactly(
            DIRECTORY + "/nested/data", DIRECTORY + "/nested", DIRECTORY + "/empty", DIRECTORY);
    assertThat(paths).containsOnlyKeys(DIRECTORY + "-sibling/data");
    assertThat(operations.listPrefix(PREFIX)).isEmpty();
    operations.deletePrefix(PREFIX);
    verify(fileIO, never()).deleteFiles(any());
    verify(fileIO, never()).deleteFile(anyString());
    verify(fileIO, never()).deletePrefix(any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"space name", "literal%2Fname", "plus+name", "hash#query?", "資料"})
  void preservesContainerRelativeNamesAndReturnsAbsoluteUrls(String name) {
    String path = DIRECTORY + "/" + name;
    paths.put(path, false);
    FileInfo file = operations.listPrefix(PREFIX).iterator().next();
    assertThat(java.net.URI.create(file.location()).getPath()).isEqualTo("/" + path);
    assertThat(file.location()).startsWith(PREFIX);
    assertThat(file.size()).isEqualTo(1);
    assertThat(file.createdAtMillis()).isZero();

    operations.deletePrefix(PREFIX);

    verify(client).getFileClient(path);
    assertThat(paths).isEmpty();
  }

  @Test
  void handlesMissingDirectoryOnALaterPage() {
    Iterator<PathItem> entries = mock(Iterator.class);
    when(entries.next()).thenReturn(item(DIRECTORY + "/data", false));
    // hasNext may be called repeatedly before next, as allowed by Iterator.
    when(entries.hasNext())
        .thenAnswer(
            ignored -> {
              if (!deleted.isEmpty()) {
                throw storageFailure(404);
              }
              return true;
            });
    doReturn(listing(entries)).when(client).listPaths(any(), isNull());

    operations.deletePrefix(PREFIX);

    assertThat(deleted).containsExactly(DIRECTORY + "/data", DIRECTORY);
  }

  @ParameterizedTest
  @ValueSource(ints = {403, 500})
  void propagatesListingFailures(int status) {
    DataLakeStorageException failure = storageFailure(status);
    doThrow(failure).when(client).listPaths(any(), isNull());
    assertThatThrownBy(() -> operations.deletePrefix(PREFIX)).isSameAs(failure);
    assertThatThrownBy(() -> operations.listPrefix(PREFIX).iterator().hasNext()).isSameAs(failure);
    assertThat(deleted).isEmpty();
  }

  @Test
  void retriesInsteadOfRecursivelyDeletingANewFile() {
    paths.put(DIRECTORY + "/data", false);
    afterDelete = () -> paths.put(DIRECTORY + "/new-file", false);

    assertThatThrownBy(() -> operations.deletePrefix(PREFIX))
        .isInstanceOf(DataLakeStorageException.class);
    assertThat(paths).containsOnlyKeys(DIRECTORY, DIRECTORY + "/new-file");

    afterDelete = () -> {};
    operations.deletePrefix(PREFIX);
    assertThat(paths).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void stopsAfterAFileOnDeadlineOrShutdown(boolean interrupt) {
    paths.put(DIRECTORY + "/first", false);
    paths.put(DIRECTORY + "/second", false);
    afterDelete =
        () -> {
          if (interrupt) {
            Thread.currentThread().interrupt();
          } else {
            when(clock.instant()).thenReturn(NOW.plusSeconds(20));
          }
        };
    try {
      assertThatThrownBy(() -> operations.deletePrefix(PREFIX))
          .isInstanceOf(CancellationException.class);
      assertThat(deleted).containsExactly(DIRECTORY + "/first");
      assertThat(Thread.currentThread().isInterrupted()).isFalse();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void checksDeadlineAfterLazyPageReadBeforeDelete() {
    PagedIterable<PathItem> listing = listing(List.of(item(DIRECTORY + "/data", false)).iterator());
    doAnswer(
            ignored -> {
              when(clock.instant()).thenReturn(NOW.plusSeconds(20));
              return listing;
            })
        .when(client)
        .listPaths(any(), isNull());
    assertThatThrownBy(() -> operations.deletePrefix(PREFIX))
        .isInstanceOf(CancellationException.class);
    assertThat(deleted).isEmpty();
  }

  @Test
  void rejectsMismatchedPrefixesAndExpiredAttemptsBeforeStorageAccess() {
    for (String prefix :
        List.of(CONTAINER, PREFIX + "child/", CONTAINER + DIRECTORY + "-sibling/")) {
      assertThatThrownBy(() -> operations.deletePrefix(prefix))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> operations.listPrefix(prefix))
          .isInstanceOf(IllegalArgumentException.class);
    }
    when(clock.instant()).thenReturn(NOW.plusSeconds(20));
    assertThatThrownBy(() -> operations.deletePrefix(PREFIX))
        .isInstanceOf(CancellationException.class);
    verifyNoInteractions(client);
  }

  @Test
  void rejectsInvalidBoundDirectoriesBeforeStorageAccess() {
    for (String prefix :
        List.of(
            CONTAINER,
            CONTAINER + "%2F/",
            PREFIX + "../",
            PREFIX + "%2E%2E/",
            PREFIX + "?query",
            PREFIX + "#fragment",
            PREFIX.substring(0, PREFIX.length() - 1),
            "s3://bucket/root/")) {
      assertThatThrownBy(() -> new ADLSPrefixOperations(fileIO, prefix, deadline))
          .isInstanceOf(IllegalArgumentException.class);
    }
    verifyNoInteractions(client);
  }

  @Test
  void listingIsLazyAndSkipsDirectories() {
    paths.put(DIRECTORY + "/nested", true);
    Iterable<FileInfo> listing = operations.listPrefix(PREFIX);
    verifyNoInteractions(client);
    assertThat(listing).isEmpty();
  }

  @Test
  void checksDeadlineWhenALazyListingIsOpened() {
    Iterable<FileInfo> listing = operations.listPrefix(PREFIX);
    when(clock.instant()).thenReturn(NOW.plusSeconds(20));
    assertThatThrownBy(listing::iterator).isInstanceOf(CancellationException.class);
    verifyNoInteractions(fileIO, client);
  }

  @Test
  void decodesTheTaskUrlButNotListedNames() {
    String directory = "root with space/tables/" + RESOURCE_ID;
    String prefix = CONTAINER + "root%20with%20space/tables/" + RESOURCE_ID + "/";
    paths.clear();
    paths.put(directory, true);
    paths.put(directory + "/literal%2Fname", false);
    when(fileIO.client(prefix)).thenReturn(client);
    try (ADLSPrefixOperations encoded = new ADLSPrefixOperations(fileIO, prefix, deadline)) {
      assertThat(encoded.listPrefix(prefix).iterator().next().location())
          .isEqualTo(prefix + "literal%252Fname");
      encoded.deletePrefix(prefix);
    }
    assertThat(paths).isEmpty();
    verify(client).getFileClient(directory + "/literal%2Fname");
  }

  @ParameterizedTest
  @ValueSource(strings = {"sibling/data", "../outside", "data/../../outside"})
  void rejectsUnexpectedListingPaths(String suffix) {
    String name =
        suffix.startsWith("sibling") ? DIRECTORY + "-" + suffix : DIRECTORY + "/" + suffix;
    doReturn(listing(List.of(item(name, false)).iterator()))
        .when(client)
        .listPaths(any(), isNull());
    assertThatThrownBy(() -> operations.deletePrefix(PREFIX))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(deleted).isEmpty();
  }

  @Test
  void workerReportsDeleteFailureAndCompletesOnRetry() throws InterruptedException {
    paths.put(DIRECTORY + "/data", false);
    DataLakeFileClient file = mock(DataLakeFileClient.class);
    when(client.getFileClient(DIRECTORY + "/data")).thenReturn(file);
    DataLakeStorageException failure = storageFailure(403);
    when(file.deleteIfExists())
        .thenThrow(failure)
        .thenAnswer(ignored -> paths.remove(DIRECTORY + "/data") != null);
    StorageCleanupTaskRepository repository = mock(StorageCleanupTaskRepository.class);
    UUID token = UUID.randomUUID();
    Claim claim = new Claim(ResourceType.TABLE, RESOURCE_ID, CONTAINER + DIRECTORY, token);
    when(repository.claim(any(), any()))
        .thenReturn(Optional.of(claim), Optional.of(claim), Optional.empty());
    CountDownLatch reported = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(1);
    when(repository.reportFailure(eq(RESOURCE_ID), eq(token), any()))
        .thenAnswer(
            ignored -> {
              verify(repository, never()).finish(any(), any());
              assertThat(paths).containsKey(DIRECTORY + "/data");
              reported.countDown();
              return true;
            });
    when(repository.finish(RESOURCE_ID, token))
        .thenAnswer(
            ignored -> {
              finished.countDown();
              return true;
            });
    FileOperations factory = mock(FileOperations.class);
    when(factory.getCleanupFileIO(eq(NormalizedURL.from(CONTAINER + DIRECTORY)), any()))
        .thenAnswer(
            invocation -> {
              expectedThread = Thread.currentThread();
              return new ADLSPrefixOperations(fileIO, PREFIX, invocation.getArgument(1));
            });

    ServerProperties properties = new ServerProperties();
    properties.set(Property.STORAGE_CLEANUP_POLL_INTERVAL, "PT0.01S");
    properties.set(Property.STORAGE_CLEANUP_LEASE_DURATION, "PT1M");
    properties.set(Property.STORAGE_CLEANUP_ATTEMPT_TIMEOUT, "PT20S");
    properties.set(Property.STORAGE_CLEANUP_INITIAL_DELAY, "PT0.001S");
    properties.set(Property.STORAGE_CLEANUP_RETRY_BACKOFF, "PT30S");

    try (StorageCleanupWorker worker =
        new StorageCleanupWorker(repository, factory, clock, properties)) {
      worker.start();
      assertThat(reported.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
    }
    verify(repository).reportFailure(eq(RESOURCE_ID), eq(token), any());
    verify(repository).finish(RESOURCE_ID, token);
    verify(fileIO, times(2)).close();
    assertThat(paths).isEmpty();
  }

  @Test
  void rejectsUnboundOperationsAndClosesOwner() {
    for (Runnable action :
        List.<Runnable>of(
            () -> operations.newInputFile(PREFIX + "data"),
            () -> operations.newOutputFile(PREFIX + "data"),
            () -> operations.deleteFile(PREFIX + "data"),
            () -> operations.initialize(Map.of()))) {
      assertThatThrownBy(action::run).isInstanceOf(UnsupportedOperationException.class);
    }
    operations.close();
    verify(fileIO).close();
  }

  private static PathItem item(String name, boolean directory) {
    return new PathItem(null, null, 1, null, directory, name, null, null);
  }

  private static PagedIterable<PathItem> listing(Iterator<PathItem> iterator) {
    PagedIterable<PathItem> listing = mock(PagedIterable.class);
    when(listing.iterator()).thenReturn(iterator);
    return listing;
  }

  private static DataLakeStorageException storageFailure(int status) {
    DataLakeStorageException failure = mock(DataLakeStorageException.class);
    when(failure.getStatusCode()).thenReturn(status);
    return failure;
  }
}
