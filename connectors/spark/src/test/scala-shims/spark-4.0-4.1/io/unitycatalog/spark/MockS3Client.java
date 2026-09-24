package io.unitycatalog.spark;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/**
 * A fake AWS {@link S3Client} for Iceberg's {@code S3FileIO}: serves objects from the local
 * filesystem (an object key is an absolute path, see {@link FakeS3}) and, on every call, checks it
 * was built with the credentials UC vends. Only the operations {@code S3FileIO} uses are
 * overridden; every other S3 operation keeps the SDK default (which throws {@code
 * UnsupportedOperationException}). This is the {@code S3FileIO} analog of the Hadoop-FS {@link
 * CredentialTestFileSystem} the Delta tests use, and it shares mapping and expected credentials
 * with it through {@link FakeS3}.
 */
public class MockS3Client implements S3Client {

  private final String accessKeyId;
  private final String secretKey;
  private final String sessionToken;

  public MockS3Client(String accessKeyId, String secretKey, String sessionToken) {
    this.accessKeyId = accessKeyId;
    this.secretKey = secretKey;
    this.sessionToken = sessionToken;
  }

  @Override
  public PutObjectResponse putObject(PutObjectRequest request, RequestBody body) {
    FakeS3.requireVendedCredentials(accessKeyId, secretKey, sessionToken);
    Path path = FakeS3.toLocalPath(request.key());
    try {
      Files.createDirectories(path.getParent());
      try (InputStream in = body.contentStreamProvider().newStream()) {
        Files.copy(in, path, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return PutObjectResponse.builder().build();
  }

  @Override
  public HeadObjectResponse headObject(HeadObjectRequest request) {
    FakeS3.requireVendedCredentials(accessKeyId, secretKey, sessionToken);
    Path path = FakeS3.toLocalPath(request.key());
    if (!Files.exists(path)) {
      throw noSuchKey(request.key());
    }
    try {
      return HeadObjectResponse.builder().contentLength(Files.size(path)).build();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // Iceberg's S3InputStream reads through this overload (ResponseTransformer.toInputStream()).
  @Override
  public <ReturnT> ReturnT getObject(
      GetObjectRequest request, ResponseTransformer<GetObjectResponse, ReturnT> transformer) {
    FakeS3.requireVendedCredentials(accessKeyId, secretKey, sessionToken);
    Path path = FakeS3.toLocalPath(request.key());
    if (!Files.exists(path)) {
      throw noSuchKey(request.key());
    }
    try {
      byte[] slice = FakeS3.applyRange(Files.readAllBytes(path), request.range());
      GetObjectResponse response =
          GetObjectResponse.builder().contentLength((long) slice.length).build();
      return transformer.transform(
          response, AbortableInputStream.create(new ByteArrayInputStream(slice)));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** A 404 so Iceberg's {@code BaseS3File.exists()} reads it as "absent" rather than rethrowing. */
  private static NoSuchKeyException noSuchKey(String key) {
    return (NoSuchKeyException)
        NoSuchKeyException.builder().statusCode(404).message("No such key: " + key).build();
  }

  @Override
  public String serviceName() {
    return "s3";
  }

  @Override
  public void close() {}
}
