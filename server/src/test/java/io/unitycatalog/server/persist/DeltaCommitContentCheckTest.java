package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.DeltaCommitRepository.BackfillVerificationRequiredException;
import io.unitycatalog.server.persist.DeltaCommitRepository.CommitContentCheckRequiredException;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.SimpleLocalFileIO;
import io.unitycatalog.server.utils.NormalizedURL;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Exercises {@link DeltaCommitRepository#verifyContentReplayOrThrowConflict} and {@link
 * DeltaCommitRepository#requirePublishedCommitFiles} against a real (in-process, s3mock-backed)
 * {@code S3FileIO}, so the cloud read path -- path construction, {@code newInputFile}/{@code
 * newStream}/{@code exists}, and the streamed lockstep comparison -- is covered rather than only
 * the local {@code SimpleLocalFileIO} path the SDK tests use.
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

    // A transient BaseException from FileIO acquisition (e.g. region discovery timed out) is
    // retriable too.
    doThrow(new BaseException(ErrorCode.INTERNAL, "Transient failure resolving the region"))
        .when(fileOperations)
        .getFileIO(any());
    assertThatThrownBy(() -> requirePublishedCommitFiles("tbl_published", 1L, 2L))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.COMMIT_STATE_UNKNOWN);

    // A permanent one (e.g. the bucket's region cannot be resolved) passes through unchanged, so
    // the coordinator does not retry a backfill that can never be verified.
    for (ErrorCode permanent :
        List.of(
            ErrorCode.FAILED_PRECONDITION,
            ErrorCode.PERMISSION_DENIED,
            ErrorCode.INVALID_ARGUMENT)) {
      doThrow(new BaseException(permanent, "permanent FileIO failure"))
          .when(fileOperations)
          .getFileIO(any());
      assertThatThrownBy(() -> requirePublishedCommitFiles("tbl_published", 1L, 2L))
          .isInstanceOf(BaseException.class)
          .extracting(e -> ((BaseException) e).getErrorCode())
          .isEqualTo(permanent);
    }
  }

  @Test
  public void backfillVerificationRereadsTheDatabaseBeforeRejectingAMissingFile() {
    // v2 is gone: another request backfilled through v4 after this one read the range, and log
    // cleanup removed v2. v3 and v4 are still published.
    putPublished("tbl_raced", 3L, "v3\n");
    putPublished("tbl_raced", 4L, "v4\n");
    DeltaCommitRepository repository = new DeltaCommitRepository(null, null, fileOperations);
    BackfillVerificationRequiredException range2To4 = required("tbl_raced", 2L, 4L);

    // The database now has the backfill through v4: success, and no purge is attempted.
    List<Boolean> attempts = new ArrayList<>();
    assertThat(
            repository.<String>withPublishedCommitVerification(
                filesChecked -> {
                  attempts.add(filesChecked);
                  if (attempts.size() == 1) {
                    throw range2To4;
                  }
                  return "done";
                }))
        .isEqualTo("done");
    assertThat(attempts).containsExactly(false, false);

    // Another request backfilled only through v2: v3 and v4 are still needed and present.
    attempts.clear();
    assertThat(
            repository.<String>withPublishedCommitVerification(
                filesChecked -> {
                  attempts.add(filesChecked);
                  if (attempts.size() == 1) {
                    throw range2To4;
                  }
                  if (!filesChecked) {
                    throw required("tbl_raced", 3L, 4L);
                  }
                  return "purged";
                }))
        .isEqualTo("purged");
    assertThat(attempts).containsExactly(false, false, true);

    // Nothing else backfilled: v2 is still needed, so the missing file is the caller's error.
    assertThatThrownBy(
            () ->
                repository.<String>withPublishedCommitVerification(
                    filesChecked -> {
                      throw range2To4;
                    }))
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

  /** A FileIO that can list by prefix, like the {@code ResolvingFileIO} vended for S3 tables. */
  private interface PrefixListingFileIO extends FileIO, SupportsPrefixOperations {}

  @Test
  public void s3HeadForbiddenIsSettledByListingTheKey() {
    String table = "s3://" + BUCKET + "/tbl_forbidden";
    String v1 = table + "/_delta_log/00000000000000000001.json";
    String v2 = table + "/_delta_log/00000000000000000002.json";
    PrefixListingFileIO fileIO = mock();
    when(fileOperations.getFileIO(any())).thenReturn(fileIO);
    InputFile present = mock();
    when(present.exists()).thenReturn(true);
    InputFile forbidden = mock();
    when(forbidden.exists()).thenThrow(s3Error(403));
    when(fileIO.newInputFile(v1)).thenReturn(present);
    when(fileIO.newInputFile(v2)).thenReturn(forbidden);

    // The listing omits v2 (only a look-alike key shares its prefix): v2 is missing, a 400.
    when(fileIO.listPrefix(v2)).thenReturn(List.of(new FileInfo(v2 + ".tmp", 1, 0)));
    assertThatThrownBy(() -> requirePublishedCommitFiles("tbl_forbidden", 1L, 2L))
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> {
              assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
              assertThat(e.getMessage()).contains(v2);
            });

    // The listing has v2: present.
    when(fileIO.listPrefix(v2)).thenReturn(List.of(new FileInfo(v2, 1, 0)));
    assertThatCode(() -> requirePublishedCommitFiles("tbl_forbidden", 1L, 2L))
        .doesNotThrowAnyException();

    // The listing itself fails: existence is undetermined, never "missing".
    when(fileIO.listPrefix(v2)).thenThrow(s3Error(403));
    assertThatThrownBy(() -> requirePublishedCommitFiles("tbl_forbidden", 1L, 2L))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.COMMIT_STATE_UNKNOWN);
  }

  @Test
  public void headFailureOtherThanS3ForbiddenIsNotSettledByListing() {
    PrefixListingFileIO fileIO = mock();
    when(fileOperations.getFileIO(any())).thenReturn(fileIO);
    InputFile failing = mock();
    when(fileIO.newInputFile(anyString())).thenReturn(failing);

    // An S3 500 is transient: retriable, and no listing.
    when(failing.exists()).thenThrow(s3Error(500));
    assertThatThrownBy(() -> requirePublishedCommitFiles("tbl_s3_500", 1L, 1L))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.COMMIT_STATE_UNKNOWN);

    // A 403 from ADLS (not an S3Exception) already means denied there: no listing either.
    DataLakeStorageException adlsForbidden = mock();
    when(adlsForbidden.getStatusCode()).thenReturn(403);
    doThrow(adlsForbidden).when(failing).exists();
    assertThatThrownBy(
            () ->
                DeltaCommitRepository.requirePublishedCommitFiles(
                    fileOperations,
                    NormalizedURL.from("abfss://container@account.dfs.core.windows.net/tbl"),
                    1L,
                    1L))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.COMMIT_STATE_UNKNOWN);

    verify(fileIO, never()).listPrefix(anyString());
  }

  private static S3Exception s3Error(int statusCode) {
    return (S3Exception) S3Exception.builder().statusCode(statusCode).message("s3 error").build();
  }

  private void requirePublishedCommitFiles(String tableName, long fromVersion, long toVersion) {
    DeltaCommitRepository.requirePublishedCommitFiles(
        fileOperations,
        NormalizedURL.from("s3://" + BUCKET + "/" + tableName),
        fromVersion,
        toVersion);
  }

  private static BackfillVerificationRequiredException required(
      String tableName, long fromVersion, long toVersion) {
    return new BackfillVerificationRequiredException(
        NormalizedURL.from("s3://" + BUCKET + "/" + tableName), fromVersion, toVersion);
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
