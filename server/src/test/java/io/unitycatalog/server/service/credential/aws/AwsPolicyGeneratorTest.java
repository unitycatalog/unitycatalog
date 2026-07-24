package io.unitycatalog.server.service.credential.aws;

import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;
import static io.unitycatalog.server.service.credential.aws.AwsPolicyGenerator.BUCKET_STATEMENT;
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

public class AwsPolicyGeneratorTest {

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  @Test
  public void testPoliciesTemplatesYaml() {
    assertThatNoException().isThrownBy(() -> YAML_MAPPER.readTree(POLICY_STATEMENT));

    assertThatNoException().isThrownBy(() -> YAML_MAPPER.readTree(BUCKET_STATEMENT));

    assertThatNoException().isThrownBy(() -> YAML_MAPPER.readTree(OPERATION_STATEMENT));
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
  public void testWildcardsInPathAreEscapedToLiterals() {
    String policy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(SELECT, UPDATE), List.of(NormalizedURL.from("s3://victim-bucket/*")));

    // The path must match the literal key "*", not every object in the bucket.
    assertThat(policy)
        .contains("arn:aws:s3:::victim-bucket/${*}")
        .doesNotContain("arn:aws:s3:::victim-bucket/*");
  }

  @Test
  public void testSingleCharWildcardInPathIsEscapedToLiteral() {
    String policy =
        AwsPolicyGenerator.generatePolicy(
            // "%3F" is the percent-encoding of "?", which IAM treats as a single-char wildcard.
            Set.of(SELECT), List.of(NormalizedURL.from("s3://my-bucket/path%3Fx/table")));

    assertThat(policy)
        .contains("arn:aws:s3:::my-bucket/path${?}x/table")
        .doesNotContain("arn:aws:s3:::my-bucket/path?x/table");
  }

  @Test
  public void testDollarSignCannotForgeAnEscapeSequence() {
    String policy =
        AwsPolicyGenerator.generatePolicy(
            // A path that already spells out an IAM escape sequence must stay literal text.
            Set.of(SELECT), List.of(NormalizedURL.from("s3://my-bucket/$%7B*%7D")));

    assertThat(policy).contains("arn:aws:s3:::my-bucket/${$}{${*}}");
  }

  @SneakyThrows
  @Test
  public void testOrdinaryPathIsNotEscaped() {
    String policy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(SELECT), List.of(NormalizedURL.from("s3://my-bucket/path1/table1")));

    JsonNode node = JSON_MAPPER.readTree(policy);
    assertThat(node.get("Statement").get(0).get("Resource"))
        .map(JsonNode::asText)
        .containsExactly(
            "arn:aws:s3:::my-bucket/path1/table1/*", "arn:aws:s3:::my-bucket/path1/table1");
    assertThat(node.findPath("s3:prefix"))
        .map(JsonNode::asText)
        .containsExactly("path1/table1", "path1/table1/", "path1/table1/*");
  }
}
