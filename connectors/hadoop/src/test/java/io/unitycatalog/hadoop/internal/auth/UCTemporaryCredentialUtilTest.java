package io.unitycatalog.hadoop.internal.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class UCTemporaryCredentialUtilTest {
  private static final long EXPIRATION = 123L;

  @ParameterizedTest(name = "{0}")
  @MethodSource("validCredentials")
  void convertsTemporaryCredentials(
      String cloud, TemporaryCredentials input, GenericCredential expected) {
    assertThat(UCTemporaryCredentialUtil.toGenericCredential(input)).isEqualTo(expected);
  }

  private static Stream<Arguments> validCredentials() {
    return Stream.of(
        Arguments.of(
            "AWS",
            new TemporaryCredentials()
                .awsTempCredentials(
                    new AwsCredentials()
                        .accessKeyId("access-key")
                        .secretAccessKey("secret-key")
                        .sessionToken("session-token"))
                .expirationTime(EXPIRATION),
            new AwsCredential("access-key", "secret-key", "session-token", EXPIRATION)),
        Arguments.of(
            "Azure",
            new TemporaryCredentials()
                .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("sas-token"))
                .expirationTime(EXPIRATION),
            new AzureCredential("sas-token", EXPIRATION)),
        Arguments.of(
            "GCS",
            new TemporaryCredentials()
                .gcpOauthToken(new GcpOauthToken().oauthToken("oauth-token"))
                .expirationTime(EXPIRATION),
            new GcsCredential("oauth-token", EXPIRATION)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidCredentials")
  void rejectsMissingRequiredField(
      String cloud, TemporaryCredentials input, String expectedMessage) {
    assertThatThrownBy(() -> UCTemporaryCredentialUtil.toGenericCredential(input))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
  }

  private static Stream<Arguments> invalidCredentials() {
    return Stream.of(
        Arguments.of(
            "AWS",
            new TemporaryCredentials()
                .awsTempCredentials(
                    new AwsCredentials()
                        .secretAccessKey("secret-key")
                        .sessionToken("session-token")),
            "AWS access key is missing"),
        Arguments.of(
            "Azure",
            new TemporaryCredentials()
                .azureUserDelegationSas(new AzureUserDelegationSAS().sasToken("")),
            "Azure SAS token is missing"),
        Arguments.of(
            "GCS",
            new TemporaryCredentials().gcpOauthToken(new GcpOauthToken()),
            "GCS OAuth token is missing"),
        Arguments.of(
            "no credential",
            new TemporaryCredentials(),
            "UC temporary credentials contained no cloud credential"));
  }
}
