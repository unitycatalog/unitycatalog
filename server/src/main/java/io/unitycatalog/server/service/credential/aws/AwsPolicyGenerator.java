package io.unitycatalog.server.service.credential.aws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.unitycatalog.server.service.credential.CredentialContext;
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
  static final List<String> UPDATE_ACTIONS =
    List.of("s3:GetO*", "s3:PutO*", "s3:DeleteO*", "s3:*Multipart*");

  static final String POLICY_STATEMENT =
    """
    Version: 2012-10-17
    Statement: []
    """;

  static final String BUCKET_STATEMENT =
    """
    Effect: Allow
    Action:
      - s3:ListBucket
    Resource: []
    Condition:
      StringLike:
        "s3:prefix": []
    """;

  static final String OPERATION_STATEMENT =
    """
    Effect: Allow
    Action: []
    Resource: []
    """;

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  // This can support generating a policy across multiple buckets and paths, however, the assumed role
  // the policy is applied to for a scoped-session needs to have access across those buckets
  @SneakyThrows
  public static String generatePolicy(Set<CredentialContext.Privilege> privileges, List<String> locations) {
    JsonNode policyRoot = loadYaml(POLICY_STATEMENT);
    ArrayNode policyStatement = (ArrayNode) policyRoot.findPath("Statement");
    JsonNode operationsStatement = loadYaml(OPERATION_STATEMENT);
    policyStatement.add(operationsStatement);

    // Add the appropriate S3 operations for the privileges requested
    ArrayNode actions = (ArrayNode) operationsStatement.findPath("Action");
    if (privileges.contains(CredentialContext.Privilege.UPDATE)) {
      UPDATE_ACTIONS.forEach(actions::add);
    } else if (privileges.contains(CredentialContext.Privilege.SELECT)) {
      SELECT_ACTIONS.forEach(actions::add);
    } else {
      throw new NotAuthorizedException(
        "Can't generate policy for unknown privileges '%s' for locations: '%s'"
          .formatted(privileges, locations));
    }

    // Group each location by s3 bucket it's located in, then for each
    // bucket, add the bucket arn for the listBucket and operations statements,
    // then add each path as a conditional prefix
    getBucketToPathsMap(locations).forEach(
      (bucketName, paths) -> {
        JsonNode listStatement = loadYaml(BUCKET_STATEMENT);
        policyStatement.add(listStatement);

        ArrayNode bucketResource = (ArrayNode) listStatement.findPath("Resource");
        ArrayNode operationsResource = (ArrayNode) operationsStatement.findPath("Resource");
        bucketResource.add("arn:aws:s3:::%s".formatted(bucketName));

        ArrayNode conditionalPrefixes = (ArrayNode) listStatement.findPath("s3:prefix");
        paths.forEach(
          path -> {
            // remove any preceding forward slashes
            // TODO: potentially sanitize/encode the whole path to deal with problematic chars
            String sanitizedPath = path.replaceAll("^/+", "");

            if (sanitizedPath.isEmpty()) {
              conditionalPrefixes.add("*");
              operationsResource.add("arn:aws:s3:::%s/*".formatted(bucketName));
            } else {
              conditionalPrefixes.add(sanitizedPath);
              conditionalPrefixes.add(sanitizedPath + "/");
              conditionalPrefixes.add(sanitizedPath + "/*");

              operationsResource.add("arn:aws:s3:::%s/%s/*".formatted(bucketName, sanitizedPath));
              operationsResource.add("arn:aws:s3:::%s/%s".formatted(bucketName, sanitizedPath));
            }
          });
      });

    return JSON_MAPPER.writeValueAsString(policyRoot);
  }

  private static Map<String, List<String>> getBucketToPathsMap(List<String> locations) {
    return locations.stream()
      .map(URI::create)
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
