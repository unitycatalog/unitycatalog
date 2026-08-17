package io.unitycatalog.server.service.credential.aws;

import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;
import static io.unitycatalog.server.service.credential.aws.AwsPolicyGenerator.BUCKET_STATEMENT;
import static io.unitycatalog.server.service.credential.aws.AwsPolicyGenerator.KMS_ENCRYPTION_CONTEXT_KEY;
import static io.unitycatalog.server.service.credential.aws.AwsPolicyGenerator.KMS_STATEMENT;
import static io.unitycatalog.server.service.credential.aws.AwsPolicyGenerator.OPERATION_STATEMENT;
import static io.unitycatalog.server.service.credential.aws.AwsPolicyGenerator.POLICY_STATEMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class AwsPolicyGeneratorTest {

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  @Test
  public void testPoliciesTemplatesYaml() {
    assertThatNoException().isThrownBy(() -> YAML_MAPPER.readTree(POLICY_STATEMENT));

    assertThatNoException().isThrownBy(() -> YAML_MAPPER.readTree(BUCKET_STATEMENT));

    assertThatNoException().isThrownBy(() -> YAML_MAPPER.readTree(OPERATION_STATEMENT));

    assertThatNoException().isThrownBy(() -> YAML_MAPPER.readTree(KMS_STATEMENT));
  }

  @SneakyThrows
  @Test
  public void testPolicySubstitution() {
    String bucket = "test-bucket";
    String prefix = "%s/%s".formatted(UUID.randomUUID(), UUID.randomUUID());
    NormalizedURL location = NormalizedURL.from("s3://%s/%s".formatted(bucket, prefix));

    String policy = AwsPolicyGenerator.generatePolicy(Set.of(SELECT), List.of(location));

    JsonNode node = JSON_MAPPER.readTree(policy);
    assertThat(node.get("Statement").get(1).get("Resource").get(0).asText())
        .isEqualTo("arn:aws:s3:::" + bucket);

    node.findPath("s3:prefix").forEach(e -> assertThat(e.asText()).startsWith(prefix));
  }

  @Test
  public void testWildcardPathDoesNotProduceBucketWidePolicy() throws Exception {
    String bucket = "victim-bucket";
    String policy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(UPDATE), List.of(NormalizedURL.from("s3://%s/*".formatted(bucket))));

    JsonNode node = JSON_MAPPER.readTree(policy);
    assertThat(node.get("Statement").get(0).get("Resource"))
        .map(JsonNode::asText)
        .containsExactly(
            "arn:aws:s3:::%s/${*}/*".formatted(bucket), "arn:aws:s3:::%s/${*}".formatted(bucket))
        .doesNotContain(
            "arn:aws:s3:::%s/*/*".formatted(bucket), "arn:aws:s3:::%s/*".formatted(bucket));
    assertThat(node.get("Statement").get(1).findPath("s3:prefix"))
        .map(JsonNode::asText)
        .containsExactly("${*}", "${*}/", "${*}/*")
        .doesNotContain("*", "*/", "*/*");
  }

  @ParameterizedTest(name = "{index}: {0} becomes {1}")
  @CsvSource({
    "%2A, ${*}",
    "%2a, ${*}",
    "%3F, ${?}",
    "%3f, ${?}",
    "prefix*middle, prefix${*}middle",
    "prefix%3Fmiddle, prefix${?}middle",
    "%24%7Baws:username%7D, ${$}{aws:username}",
    "$%7B*%7D, ${$}{${*}}"
  })
  public void testPolicyEscapesIamSpecialCharacters(String encodedPath, String expectedPath)
      throws Exception {
    String bucket = "victim-bucket";
    String policy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(UPDATE),
            List.of(NormalizedURL.from("s3://%s/%s".formatted(bucket, encodedPath))));

    JsonNode node = JSON_MAPPER.readTree(policy);
    assertThat(node.get("Statement").get(0).get("Resource"))
        .map(JsonNode::asText)
        .containsExactly(
            "arn:aws:s3:::%s/%s/*".formatted(bucket, expectedPath),
            "arn:aws:s3:::%s/%s".formatted(bucket, expectedPath));
    assertThat(node.get("Statement").get(1).findPath("s3:prefix"))
        .map(JsonNode::asText)
        .containsExactly(expectedPath, expectedPath + "/", expectedPath + "/*");
  }

  @Test
  public void testPolicyLeavesOrdinaryPathUnchanged() throws Exception {
    String policy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(SELECT), List.of(NormalizedURL.from("s3://my-bucket/path1/table1")));

    JsonNode node = JSON_MAPPER.readTree(policy);
    assertThat(node.get("Statement").get(0).get("Resource"))
        .map(JsonNode::asText)
        .containsExactly(
            "arn:aws:s3:::my-bucket/path1/table1/*", "arn:aws:s3:::my-bucket/path1/table1");
    assertThat(node.get("Statement").get(1).findPath("s3:prefix"))
        .map(JsonNode::asText)
        .containsExactly("path1/table1", "path1/table1/", "path1/table1/*");
  }

  @Test
  public void testPolicyPreservesIntentionalBucketRootWildcard() throws Exception {
    String policy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(SELECT), List.of(NormalizedURL.from("s3://my-bucket")));

    JsonNode node = JSON_MAPPER.readTree(policy);
    assertThat(node.get("Statement").get(0).get("Resource"))
        .map(JsonNode::asText)
        .containsExactly("arn:aws:s3:::my-bucket/*");
    assertThat(node.get("Statement").get(1).findPath("s3:prefix"))
        .map(JsonNode::asText)
        .containsExactly("*");
  }

  @Test
  public void testPolicyWithStorageProfileLocations() {
    String updatePolicy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(SELECT, UPDATE),
            Stream.of(
                    "s3://my-bucket1/path1/table1", "s3://profile-bucket2/", "s3://profile-bucket3")
                .map(NormalizedURL::from)
                .toList());

    assertThat(updatePolicy)
        .contains("s3:PutO*")
        .contains("s3:GetO*")
        .contains("s3:DeleteO*")
        .contains("arn:aws:s3:::profile-bucket2/*")
        .contains("arn:aws:s3:::profile-bucket3/*");

    String selectPolicy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(SELECT),
            Stream.of(
                    "s3://my-bucket1/path1/table1",
                    "s3://my-bucket2/path2/table2",
                    "s3://my-bucket1/path3/table3")
                .map(NormalizedURL::from)
                .toList());

    assertThat(selectPolicy)
        .doesNotContain("s3:PutO*")
        .doesNotContain("s3:DeleteO*")
        .contains("s3:GetO*");
  }

  @Test
  public void testSelectPolicyGrantsKmsDecrypt() throws Exception {
    String policy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(SELECT), List.of(NormalizedURL.from("s3://my-bucket/path1/table1")));

    JsonNode statement = findKmsStatement(JSON_MAPPER.readTree(policy));
    assertThat(statement).isNotNull();
    assertThat(statement.path("Action")).map(JsonNode::asText).containsExactly("kms:Decrypt");
    assertThat(statement.path("Resource")).map(JsonNode::asText).containsExactly("*");
    assertThat(statement.findPath("kms:ViaService").asText()).isEqualTo("s3.*.amazonaws.com");
  }

  @Test
  public void testUpdatePolicyGrantsKmsDecryptAndGenerateDataKey() throws Exception {
    String policy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(UPDATE), List.of(NormalizedURL.from("s3://my-bucket/path1/table1")));

    JsonNode statement = findKmsStatement(JSON_MAPPER.readTree(policy));
    assertThat(statement).isNotNull();
    assertThat(statement.path("Action"))
        .map(JsonNode::asText)
        .containsExactlyInAnyOrder("kms:Decrypt", "kms:GenerateDataKey*");
    assertThat(statement.path("Resource")).map(JsonNode::asText).containsExactly("*");
    assertThat(statement.findPath("kms:ViaService").asText()).isEqualTo("s3.*.amazonaws.com");
  }

  @Test
  public void testKmsStatementIsScopedToGrantedS3Arns() throws Exception {
    String policy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(SELECT), List.of(NormalizedURL.from("s3://my-bucket/path1/table1")));

    JsonNode statement = findKmsStatement(JSON_MAPPER.readTree(policy));
    assertThat(statement.findPath(KMS_ENCRYPTION_CONTEXT_KEY))
        .map(JsonNode::asText)
        .containsExactly(
            "arn:aws:s3:::my-bucket",
            "arn:aws:s3:::my-bucket/path1/table1/*",
            "arn:aws:s3:::my-bucket/path1/table1");
  }

  @Test
  public void testKmsStatementCoversEveryBucket() throws Exception {
    String policy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(UPDATE),
            Stream.of("s3://my-bucket1/path1/table1", "s3://my-bucket2")
                .map(NormalizedURL::from)
                .toList());

    JsonNode statement = findKmsStatement(JSON_MAPPER.readTree(policy));
    assertThat(statement.findPath(KMS_ENCRYPTION_CONTEXT_KEY))
        .map(JsonNode::asText)
        .containsExactlyInAnyOrder(
            "arn:aws:s3:::my-bucket1",
            "arn:aws:s3:::my-bucket1/path1/table1/*",
            "arn:aws:s3:::my-bucket1/path1/table1",
            "arn:aws:s3:::my-bucket2",
            "arn:aws:s3:::my-bucket2/*");
  }

  @Test
  public void testKmsEncryptionContextEscapesIamSpecialCharacters() throws Exception {
    String policy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(UPDATE), List.of(NormalizedURL.from("s3://victim-bucket/*")));

    JsonNode statement = findKmsStatement(JSON_MAPPER.readTree(policy));
    assertThat(statement.findPath(KMS_ENCRYPTION_CONTEXT_KEY))
        .map(JsonNode::asText)
        .containsExactly(
            "arn:aws:s3:::victim-bucket",
            "arn:aws:s3:::victim-bucket/${*}/*",
            "arn:aws:s3:::victim-bucket/${*}")
        .doesNotContain("arn:aws:s3:::victim-bucket/*", "arn:aws:s3:::victim-bucket/*/*");
  }

  @Test
  public void testKmsStatementDoesNotShiftS3Statements() throws Exception {
    String policy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(SELECT), List.of(NormalizedURL.from("s3://my-bucket/path1/table1")));

    JsonNode statements = JSON_MAPPER.readTree(policy).get("Statement");
    assertThat(statements.get(0).path("Action")).map(JsonNode::asText).containsExactly("s3:GetO*");
    assertThat(statements.get(1).path("Action"))
        .map(JsonNode::asText)
        .containsExactly("s3:ListBucket");
  }

  /**
   * Returns the statement that carries the KMS actions, or {@code null} when the policy has none.
   * Located by action prefix rather than by index so that the S3 statements the other tests assert
   * on by index stay where they are.
   */
  private static JsonNode findKmsStatement(JsonNode policyRoot) {
    for (JsonNode statement : policyRoot.path("Statement")) {
      for (JsonNode action : statement.path("Action")) {
        if (action.asText().startsWith("kms:")) {
          return statement;
        }
      }
    }
    return null;
  }
}
