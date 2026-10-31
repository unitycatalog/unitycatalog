package io.unitycatalog.server.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.audit.AuditRecord;
import io.unitycatalog.server.audit.Auditor;
import io.unitycatalog.server.audit.CapturingAuditSink;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.Purpose;
import io.unitycatalog.server.utils.CooperativeDeadline;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.UriScheme;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.UnaryOperator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.hibernate.SessionFactory;

/** Shared helpers for storage-cleanup tests across the repository, worker, and SDK suites. */
public final class StorageCleanupTestSupport {
  private StorageCleanupTestSupport() {}

  /** Fetches the cleanup task keyed by its resource id, or {@code null} if none is queued. */
  public static StorageCleanupTaskDAO findTask(SessionFactory sessionFactory, UUID resourceId) {
    try (var session = sessionFactory.openSession()) {
      return session.get(StorageCleanupTaskDAO.class, resourceId);
    }
  }

  /** Returns all persisted cleanup tasks, including leased and retry-delayed tasks. */
  public static List<StorageCleanupTaskDAO> allTasks(SessionFactory sessionFactory) {
    try (var session = sessionFactory.openSession()) {
      return session.createQuery("FROM StorageCleanupTaskDAO", StorageCleanupTaskDAO.class).list();
    }
  }

  /**
   * Routes audit records to {@link CapturingAuditSink}, starting with none recorded. Also gives the
   * tests' {@code s3://bucket} a region, so building its cleanup FileIO doesn't look the region up.
   */
  public static void captureAuditRecords(Properties serverProperties) {
    serverProperties.setProperty(Property.AUDIT_SINK.getKey(), CapturingAuditSink.class.getName());
    serverProperties.setProperty("s3.bucketPath.0", "s3://bucket");
    serverProperties.setProperty("s3.region.0", "us-west-2");
    serverProperties.setProperty("s3.awsRoleArn.0", "arn:aws:iam::123456789012:role/cleanup-test");
    CapturingAuditSink.reset();
  }

  /**
   * Repositories that audit to the sink in {@code server.audit.sink}, as the server's do. Cloud
   * credentials are placeholders: the tests build cleanup FileIOs but never use them.
   */
  public static Repositories auditedRepositories(
      SessionFactory sessionFactory, ServerProperties serverProperties) {
    CloudCredentialVendor cloud = mock(CloudCredentialVendor.class);
    when(cloud.vendCredential(any()))
        .thenAnswer(
            invocation ->
                switch (invocation.<CredentialContext>getArgument(0).getStorageScheme()) {
                  case S3 ->
                      new TemporaryCredentials()
                          .awsTempCredentials(
                              new AwsCredentials()
                                  .accessKeyId("AK")
                                  .secretAccessKey("SK")
                                  .sessionToken("TK"));
                  case GS ->
                      new TemporaryCredentials().gcpOauthToken(new GcpOauthToken().oauthToken("T"));
                  case ABFS, ABFSS ->
                      new TemporaryCredentials()
                          .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("SAS"));
                  case FILE, NULL -> new TemporaryCredentials();
                });
    return new Repositories(
        sessionFactory,
        serverProperties,
        cloud,
        UnaryOperator.identity(),
        /* metrics= */ null,
        Auditor.fromProperties(serverProperties));
  }

  /**
   * Builds each cloud task's cleanup FileIO, as the worker does, and asserts the {@code CLEANUP}
   * credential it vends is recorded with the task's ID, so it joins the task's {@code
   * QUEUE_CLEANUP} record. Also builds the location's server FileIO and asserts that credential is
   * recorded as {@code SERVER}, without a resource ID. Local tasks are skipped: they vend no
   * credential.
   */
  public static void assertCleanupCredentialsJoinTasks(
      Repositories repositories, SessionFactory sessionFactory) {
    for (StorageCleanupTaskDAO task : allTasks(sessionFactory)) {
      NormalizedURL location = NormalizedURL.from(task.getStorageLocation());
      if (UriScheme.fromURI(location.toUri()) == UriScheme.FILE) {
        continue;
      }
      try (SupportsPrefixOperations ignored =
          repositories
              .getFileOperations()
              .getCleanupFileIO(location, task.getId(), CooperativeDeadline.NO_DEADLINE)) {
        assertThat(CapturingAuditSink.RECORDS)
            .filteredOn(
                r ->
                    r.getPurpose() == Purpose.CLEANUP
                        && location.toString().equals(r.getLocation()))
            .isNotEmpty()
            .allSatisfy(r -> assertThat(r.getResourceId()).isEqualTo(task.getId()));
      }
      try (FileIO ignored =
          repositories.getFileOperations().getFileIO(location, CredentialContext.READ_ONLY)) {
        assertThat(CapturingAuditSink.RECORDS)
            .filteredOn(
                r ->
                    r.getPurpose() == Purpose.SERVER && location.toString().equals(r.getLocation()))
            .isNotEmpty()
            .allSatisfy(r -> assertThat(r.getResourceId()).isNull());
      }
    }
  }

  /**
   * Asserts that every queued task has exactly one {@code QUEUE_CLEANUP} record with its ID and
   * location, and that no record exists for a drop that rolled back or queued no task.
   */
  public static void assertQueueCleanupRecordsMatchTasks(SessionFactory sessionFactory) {
    assertThat(CapturingAuditSink.RECORDS)
        .filteredOn(r -> r.getAction() == AuditRecord.Action.QUEUE_CLEANUP)
        .extracting(AuditRecord::getResourceId, AuditRecord::getLocation)
        .containsExactlyInAnyOrderElementsOf(
            allTasks(sessionFactory).stream()
                .map(task -> tuple(task.getId(), task.getStorageLocation()))
                .toList());
  }

  /**
   * Waits until all paths are absent and the cleanup task is removed, asserting both on timeout.
   */
  public static void awaitCleanup(
      SessionFactory sessionFactory, UUID resourceId, Duration timeout, Path... paths)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (allGone(paths) && findTask(sessionFactory, resourceId) == null) {
        return;
      }
      Thread.sleep(10);
    }
    for (Path path : paths) {
      assertThat(path).doesNotExist();
    }
    assertThat(findTask(sessionFactory, resourceId)).isNull();
  }

  /** Sets a 10 ms poll interval and 1 ms task-eligibility delay for live-worker tests. */
  public static void configureFastCleanup(Properties serverProperties) {
    serverProperties.setProperty(Property.STORAGE_CLEANUP_POLL_INTERVAL.getKey(), "PT0.01S");
    serverProperties.setProperty(Property.STORAGE_CLEANUP_INITIAL_DELAY.getKey(), "PT0.001S");
  }

  private static boolean allGone(Path... paths) {
    for (Path path : paths) {
      if (!Files.notExists(path)) {
        return false;
      }
    }
    return true;
  }
}
