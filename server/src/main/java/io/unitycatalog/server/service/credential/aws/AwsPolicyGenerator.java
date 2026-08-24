package io.unitycatalog.server.service.credential.aws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import lombok.SneakyThrows;
import org.apache.iceberg.exceptions.NotAuthorizedException;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AwsPolicyGenerator {

  static final List<String> SELECT_ACTIONS = List.of("s3:GetO*");
  static final List<String> UPDATE_ACTIONS = List.of(
      "s3:GetO*", "s3:PutO*", "s3:DeleteO*", "s3:*Multipart*");

  // Reading an object encrypted with SSE-KMS requires kms:Decrypt, and writing one additionally
  // requires kms:GenerateDataKey*. Without these the vended session credentials can't touch a
  // table stored in a bucket with SSE-KMS, even when the assumed role itself is allowed to.
  static final List<String> SELECT_KMS_ACTIONS = List.of("kms:Decrypt");
  static final List<String> UPDATE_KMS_ACTIONS = List.of("kms:Decrypt", "kms:GenerateDataKey*");

  static final String POLICY_STATEMENT = """
      Version: 2012-10-17
      Statement: []
      """;

  static final String BUCKET_STATEMENT = """
      Effect: Allow
      Action:
        - s3:ListBucket
      Resource: []
      Condition:
        StringLike:
          "s3:prefix": []
      """;

  static final String OPERATION_STATEMENT = """
      Effect: Allow
      Action: []
      Resource: []
      """;

  // The condition key S3 populates when it calls KMS on the caller's behalf. Its value is the ARN
  // of the object being encrypted or decrypted, or the ARN of the bucket when the bucket has S3
  // Bucket Keys enabled, in which case the data key is shared across the objects in the bucket.
  static final String KMS_ENCRYPTION_CONTEXT_KEY = "kms:EncryptionContext:aws:s3:arn";

  // Unity Catalog doesn't know which KMS key a bucket is configured with, so the resource stays
  // open and the statement is narrowed by condition instead: to KMS calls made through S3, and to
  // the S3 ARNs this policy already grants access to. A session policy can only narrow what the
  // assumed role is already allowed to do, so a role without KMS access still gets none.
  static final String KMS_STATEMENT = """
      Effect: Allow
      Action: []
      Resource:
        - "*"
      Condition:
        StringLike:
          "kms:ViaService": "s3.*.amazonaws.com"
          "%s": []
      """.formatted(KMS_ENCRYPTION_CONTEXT_KEY);

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  // This can support generating a policy across multiple buckets and paths, however, the assumed
  // role the policy is applied to for a scoped-session needs to have access across those buckets
  @SneakyThrows
  public static String generatePolicy(
      Set<CredentialContext.Privilege> privileges,
      List<NormalizedURL> locations) {
    JsonNode policyRoot = loadYaml(POLICY_STATEMENT);
    ArrayNode policyStatement = (ArrayNode) policyRoot.findPath("Statement");
    JsonNode operationsStatement = loadYaml(OPERATION_STATEMENT);
    policyStatement.add(operationsStatement);
    JsonNode kmsStatement = loadYaml(KMS_STATEMENT);

    // Add the appropriate S3 and KMS operations for the privileges requested
    ArrayNode actions = (ArrayNode) operationsStatement.findPath("Action");
    ArrayNode kmsActions = (ArrayNode) kmsStatement.findPath("Action");
    if (privileges.contains(CredentialContext.Privilege.UPDATE)) {
      UPDATE_ACTIONS.forEach(actions::add);
      UPDATE_KMS_ACTIONS.forEach(kmsActions::add);
    } else if (privileges.contains(CredentialContext.Privilege.SELECT)) {
      SELECT_ACTIONS.forEach(actions::add);
      SELECT_KMS_ACTIONS.forEach(kmsActions::add);
    } else {
      throw new NotAuthorizedException(
          String.format("Can't generate policy for unknown privileges '%s' for locations: '%s'",
              privileges, locations));
    }

    ArrayNode kmsEncryptionContexts = (ArrayNode) kmsStatement.findPath(KMS_ENCRYPTION_CONTEXT_KEY);

    // Group each location by s3 bucket it's located in, then for each
    // bucket, add the bucket arn for the listBucket and operations statements,
    // then add each path as a conditional prefix
    getBucketToPathsMap(locations).forEach((bucketName, paths) -> {
      JsonNode listStatement = loadYaml(BUCKET_STATEMENT);
      policyStatement.add(listStatement);

      ArrayNode bucketResource = (ArrayNode) listStatement.findPath("Resource");
      ArrayNode operationsResource = (ArrayNode) operationsStatement.findPath("Resource");
      bucketResource.add(String.format("arn:aws:s3:::%s", bucketName));

      // A bucket with S3 Bucket Keys enabled encrypts under the bucket arn rather than the object
      // arn, so the bucket has to be allowed as an encryption context on its own. That case can't
      // be scoped to a path: the same data key covers every object in the bucket.
      kmsEncryptionContexts.add(String.format("arn:aws:s3:::%s", bucketName));

      ArrayNode conditionalPrefixes = (ArrayNode) listStatement.findPath("s3:prefix");
      paths.forEach(path -> {
        // remove any preceding forward slashes
        String sanitizedPath = escapeIamSpecialCharacters(path.replaceAll("^/+", ""));

        if (sanitizedPath.isEmpty()) {
          conditionalPrefixes.add("*");
          addObjectArn(String.format("arn:aws:s3:::%s/*", bucketName),
              operationsResource, kmsEncryptionContexts);
        } else {
          conditionalPrefixes.add(sanitizedPath);
          conditionalPrefixes.add(sanitizedPath + "/");
          conditionalPrefixes.add(sanitizedPath + "/*");

          addObjectArn(String.format("arn:aws:s3:::%s/%s/*", bucketName, sanitizedPath),
              operationsResource, kmsEncryptionContexts);
          addObjectArn(String.format("arn:aws:s3:::%s/%s", bucketName, sanitizedPath),
              operationsResource, kmsEncryptionContexts);
        }
      });
    });

    // Appended after the per-bucket statements so that the position of the S3 statements
    // within the policy doesn't change
    policyStatement.add(kmsStatement);

    return JSON_MAPPER.writeValueAsString(policyRoot);
  }

  /**
   * Allows an object arn in the S3 operations statement, and allows the same arn as a KMS
   * encryption context so that the KMS permissions cover exactly the objects the policy grants
   * access to.
   */
  private static void addObjectArn(
      String objectArn, ArrayNode operationsResource, ArrayNode kmsEncryptionContexts) {
    operationsResource.add(objectArn);
    kmsEncryptionContexts.add(objectArn);
  }

  /**
   * Makes an S3 path safe to include in an IAM policy.
   *
   * <p>S3 treats {@code *}, {@code ?}, and {@code $} as ordinary characters in object
   * names. IAM gives them special meanings:
   *
   * <ul>
   *   <li>{@code *} matches any number of characters. For example, {@code reports/*}
   *       matches every object under {@code reports/}.
   *   <li>{@code ?} matches exactly one character. For example, {@code file?.txt}
   *       matches {@code file1.txt}.
   *   <li>{@code ${...}} is a policy variable whose value IAM fills in when it evaluates
   *       the policy. For example, {@code ${aws:username}} is replaced with the caller's
   *       IAM username.
   * </ul>
   *
   * <p>Therefore, copying an S3 path directly into a policy could grant access to more
   * objects than the path names. AWS provides {@code ${*}}, {@code ${?}}, and {@code ${$}}
   * to make IAM match the literal {@code *}, {@code ?}, and {@code $} characters instead.
   *
   * <p>We replace {@code $} first because the replacements for {@code *} and {@code ?}
   * also contain a dollar sign.
   *
   * @see <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_variables.html">
   *     AWS documentation for IAM policy variables</a>
   */
  private static String escapeIamSpecialCharacters(String keyPrefix) {
    return keyPrefix.replace("$", "${$}").replace("*", "${*}").replace("?", "${?}");
  }

  private static Map<String, List<String>> getBucketToPathsMap(List<NormalizedURL> locations) {
    return locations.stream()
        .map(NormalizedURL::toUri)
        .collect(Collectors.toMap(
            URI::getHost,
            uri -> new LinkedList<>(List.of(uri.getPath())),
            (map, newPaths) -> {
              map.addAll(newPaths);
              return map;
            }));
  }

  @SneakyThrows
  private static JsonNode loadYaml(String s) {
    return YAML_MAPPER.readTree(s);
  }
}
