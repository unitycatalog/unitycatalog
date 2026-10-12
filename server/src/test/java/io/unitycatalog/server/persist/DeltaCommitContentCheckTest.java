package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.DeltaCommitRepository.CommitContentCheckRequiredException;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.SimpleLocalFileIO;
import io.unitycatalog.server.utils.NormalizedURL;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Exercises {@link DeltaCommitRepository#verifyContentReplayOrThrowConflict} against a real
 * (in-process, s3mock-backed) {@code S3FileIO}, so the cloud read path -- path construction, {@code
 * newInputFile}/{@code newStream}, and the streamed lockstep comparison -- is covered rather than
 * only the local {@code SimpleLocalFileIO} path the SDK tests use.
 *
 * <p>Like {@code MetadataServiceTest}, {@code getFileIO} is stubbed to return a real {@code
 * S3FileIO}; credential vending itself is not exercised here. One case stubs a {@code
 * SimpleLocalFileIO} instead, for paths the local file system must refuse.
 */
@ExtendWith(S3MockExtension.class)
public class DeltaCommitContentCheckTest {

  @RegisterExtension
  public static final S3MockExtension S3_MOCK = S3MockExtension.builder().silent().build();

  private static final String BUCKET = "test-bucket";

  private final FileOperations fileOperations = mock();
  private final S3Client s3 = S3_MOCK.createS3ClientV2();

  @BeforeAll
  public static void createBucket() {
    // The S3 mock is shared by every test in this class, so the bucket is created once.
    S3_MOCK.createS3ClientV2().createBucket(b -> b.bucket(BUCKET).build());
  }

  @BeforeEach
  public void setUp() {
    when(fileOperations.getFileIO(any())).thenReturn(new S3FileIO(() -> s3));
  }

  /** version 2 -> _delta_log/00000000000000000002.json (matches the %020d.json layout). */
  private static final long VERSION = 2L;

  private static final String STAGED_FILE_NAME = "00000000000000000002.abc-uuid.json";

  @Test
  public void localContentCheckDoesNotResolveAnEscapedOrLinkedStagedPath(@TempDir Path dir)
      throws IOException {
    Path table = dir.resolve("t");
    Path log = Files.createDirectories(table.resolve("_delta_log"));
    Files.writeString(log.resolve("00000000000000000002.json"), "published");
    when(fileOperations.getFileIO(any()))
        .thenAnswer(
            invocation ->
                new SimpleLocalFileIO(
                    Paths.get(((NormalizedURL) invocation.getArgument(0)).toUri())));
    NormalizedURL tableLocation = NormalizedURL.from(table.toUri());

    // "..%2F<v>.json" decodes to "../<v>.json", the published file itself. It must not be read as
    // a replay of the published commit.
    assertUndeterminedBecause(
        tableLocation, "..%2F00000000000000000002.json", ErrorCode.INVALID_ARGUMENT);

    // A linked staged-commit directory holding a matching file is refused, so it is not read as a
    // replay either: the outcome is undetermined.
    Path outside = Files.createDirectories(dir.resolve("outside"));
    Files.writeString(outside.resolve(STAGED_FILE_NAME), "published");
    Files.createSymbolicLink(log.resolve("_staged_commits"), outside);
    assertUndeterminedBecause(tableLocation, STAGED_FILE_NAME, ErrorCode.PERMISSION_DENIED);
  }

  /**
   * Asserts the check reports an undetermined outcome, because the local FileIO refused a path with
   * the given error code.
   */
  private void assertUndeterminedBecause(
      NormalizedURL tableLocation, String stagedFileName, ErrorCode refusal) {
    assertThatThrownBy(
            () ->
                DeltaCommitRepository.verifyContentReplayOrThrowConflict(
                    fileOperations,
                    new CommitContentCheckRequiredException(
                        tableLocation, VERSION, stagedFileName)))
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> {
              assertThat(e.getErrorCode()).isEqualTo(ErrorCode.COMMIT_STATE_UNKNOWN);
              assertThat(e.getCause())
                  .isInstanceOfSatisfying(
                      BaseException.class, c -> assertThat(c.getErrorCode()).isEqualTo(refusal));
            });
  }

  @Test
  public void contentCheckResolvesUnknownReplayAndConflict() {
    // Each case uses a distinct table so object keys never collide.

    // 1. Published file absent (only the staged file exists): the outcome cannot be determined, so
    // it must fail open to a retriable 500 rather than a false conflict.
    putStaged("tbl_unknown", "staged-commit\n");
    assertThatThrownBy(() -> runContentCheck("tbl_unknown"))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.COMMIT_STATE_UNKNOWN);

    // 2. Published and staged files are byte-identical: a recognized replay -> no exception.
    putPublished("tbl_match", "{\"commitInfo\":{\"version\":2}}\n");
    putStaged("tbl_match", "{\"commitInfo\":{\"version\":2}}\n");
    assertThatCode(() -> runContentCheck("tbl_match")).doesNotThrowAnyException();

    // 3. Published and staged files differ: another writer won this version -> 409 conflict.
    putPublished("tbl_conflict", "published-commit\n");
    putStaged("tbl_conflict", "a-different-commit\n");
    assertThatThrownBy(() -> runContentCheck("tbl_conflict"))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.COMMIT_VERSION_CONFLICT);
  }

  private void runContentCheck(String tableName) {
    DeltaCommitRepository.verifyContentReplayOrThrowConflict(
        fileOperations,
        new CommitContentCheckRequiredException(
            NormalizedURL.from("s3://" + BUCKET + "/" + tableName), VERSION, STAGED_FILE_NAME));
  }

  private void putPublished(String tableName, String content) {
    put(tableName + "/_delta_log/00000000000000000002.json", content);
  }

  private void putStaged(String tableName, String content) {
    put(tableName + "/_delta_log/_staged_commits/" + STAGED_FILE_NAME, content);
  }

  private void put(String key, String content) {
    s3.putObject(
        b -> b.bucket(BUCKET).key(key).build(),
        RequestBody.fromString(content, StandardCharsets.UTF_8));
  }
}
