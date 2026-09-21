package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.DeltaCommitRepository.CommitContentCheckRequiredException;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.utils.NormalizedURL;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Exercises {@link DeltaCommitRepository#verifyContentReplayOrThrowConflict} and {@link
 * DeltaCommitRepository#requirePublishedCommitFiles} against a real (in-process, s3mock-backed)
 * {@code S3FileIO}, so the cloud read path -- path construction, {@code newInputFile}/{@code
 * newStream}/{@code exists}, and the streamed lockstep comparison -- is covered rather than only
 * the local {@code SimpleLocalFileIO} path the SDK tests use.
 *
 * <p>Like {@code MetadataServiceTest}, {@code getFileIO} is stubbed to return a real {@code
 * S3FileIO}; credential vending itself is not exercised here.
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
    // The mock server is shared by all tests in this class, so the bucket is created once.
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

  @Test
  public void backfillVerificationSeparatesMissingFilesFromStorageFailures() {
    // Published files for the whole range present: the backfill may proceed.
    putPublished("tbl_published", 1L, "v1\n");
    putPublished("tbl_published", 2L, "v2\n");
    assertThatCode(() -> requirePublishedCommitFiles("tbl_published", 1L, 2L))
        .doesNotThrowAnyException();

    // A gap in the range (v2 was never published) is a client error: retrying cannot help until
    // the file is published, and the DB rows must be kept.
    putPublished("tbl_gap", 1L, "v1\n");
    putPublished("tbl_gap", 3L, "v3\n");
    assertThatThrownBy(() -> requirePublishedCommitFiles("tbl_gap", 1L, 3L))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INVALID_ARGUMENT);

    // Storage itself failed, so existence is undetermined: retriable 500, not a bad request.
    doThrow(new RuntimeException("credential vend failed")).when(fileOperations).getFileIO(any());
    assertThatThrownBy(() -> requirePublishedCommitFiles("tbl_published", 1L, 2L))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.COMMIT_STATE_UNKNOWN);

    // A BaseException from FileIO acquisition (credential / region config) must also fail open to
    // 500, not pass through as the caller's INVALID_ARGUMENT.
    doThrow(new BaseException(ErrorCode.INVALID_ARGUMENT, "No S3 region configured for bucket"))
        .when(fileOperations)
        .getFileIO(any());
    assertThatThrownBy(() -> requirePublishedCommitFiles("tbl_published", 1L, 2L))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.COMMIT_STATE_UNKNOWN);

    // Permanent configuration errors are validated before FileIO acquisition and remain a
    // non-retriable 400 rather than entering the client's transient retry loop.
    doThrow(new BaseException(ErrorCode.INVALID_ARGUMENT, "Managed Delta storage requires region"))
        .when(fileOperations)
        .validateReadAccessConfiguration(any());
    assertThatThrownBy(() -> requirePublishedCommitFiles("tbl_published", 1L, 2L))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.INVALID_ARGUMENT);
  }

  @Test
  public void backfillVerificationInclusiveRangeDoesNotOverflowAtMaxValue() {
    FileIO fileIO = mock();
    InputFile inputFile = mock();
    when(inputFile.exists()).thenReturn(true);
    when(fileIO.newInputFile(anyString())).thenReturn(inputFile);
    when(fileOperations.getFileIO(any())).thenReturn(fileIO);

    assertThatCode(() -> requirePublishedCommitFiles("tbl_max", Long.MAX_VALUE, Long.MAX_VALUE))
        .doesNotThrowAnyException();
  }

  private void requirePublishedCommitFiles(String tableName, long fromVersion, long toVersion) {
    DeltaCommitRepository.requirePublishedCommitFiles(
        fileOperations,
        NormalizedURL.from("s3://" + BUCKET + "/" + tableName),
        fromVersion,
        toVersion);
  }

  private void runContentCheck(String tableName) {
    DeltaCommitRepository.verifyContentReplayOrThrowConflict(
        fileOperations,
        new CommitContentCheckRequiredException(
            NormalizedURL.from("s3://" + BUCKET + "/" + tableName), VERSION, STAGED_FILE_NAME));
  }

  private void putPublished(String tableName, String content) {
    putPublished(tableName, VERSION, content);
  }

  private void putPublished(String tableName, long version, String content) {
    put(String.format("%s/_delta_log/%020d.json", tableName, version), content);
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
