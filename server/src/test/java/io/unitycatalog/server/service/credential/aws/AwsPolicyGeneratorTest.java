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
import java.util.List;
import java.util.Set;
import java.util.UUID;
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
    String location = "s3://%s/%s".formatted(bucket, prefix);

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
            List.of(
                "s3://my-bucket1/path1/table1", "s3://profile-bucket2/", "s3://profile-bucket3"));

    assertThat(updatePolicy)
        .contains("s3:PutO*")
        .contains("s3:GetO*")
        .contains("s3:DeleteO*")
        .contains("arn:aws:s3:::profile-bucket2/*")
        .contains("arn:aws:s3:::profile-bucket3/*");

    String selectPolicy =
        AwsPolicyGenerator.generatePolicy(
            Set.of(SELECT),
            List.of(
                "s3://my-bucket1/path1/table1",
                "s3://my-bucket2/path2/table2",
                "s3://my-bucket1/path3/table3"));

    assertThat(selectPolicy)
        .doesNotContain("s3:PutO*")
        .doesNotContain("s3:DeleteO*")
        .contains("s3:GetO*");
  }
}
