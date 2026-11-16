package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.postProcessAndPrintOutput;
import static java.net.HttpURLConnection.HTTP_NOT_IMPLEMENTED;
import static java.net.HttpURLConnection.HTTP_OK;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.cli.utils.Oauth2CliExchange;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.control.model.GrantType;
import io.unitycatalog.control.model.OAuthTokenExchangeForm;
import io.unitycatalog.control.model.TokenType;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.json.JSONObject;

public class AuthCli {
  private static final ObjectMapper objectMapper = CliUtils.getObjectMapper();
  private static ObjectWriter objectWriter;

  public static void handle(CommandLine cmd, ApiClient apiClient)
      throws JsonProcessingException, ApiException {

    String[] subArgs = cmd.getArgs();
    objectWriter = CliUtils.getObjectWriter(cmd);
    String subCommand = subArgs[1];
    JSONObject json = CliUtils.createJsonFromOptions(cmd);
    String output = CliUtils.EMPTY;
    switch (subCommand) {
      case CliUtils.LOGIN:
        if (cmd.hasOption(CliParams.IDENTITY_TOKEN.val())) {
          output = exchange(apiClient, json);
        } else {
          output = login(apiClient, json);
        }
        break;
      default:
        CliUtils.printEntityHelp(CliUtils.LOGIN);
    }
    postProcessAndPrintOutput(cmd, output, subCommand);
  }

  private static String login(ApiClient apiClient, JSONObject json)
      throws JsonProcessingException, ApiException {
    try {
      Oauth2CliExchange oauth2CliExchange = new Oauth2CliExchange();
      String identityToken = oauth2CliExchange.authenticate();
      return loginInfo(
          exchangeForAccessToken(apiClient, identityToken, oauth2CliExchange.accessToken()));
    } catch (IOException e) {
      throw new ApiException(e);
    }
  }

  private static String exchange(ApiClient apiClient, JSONObject json)
      throws JsonProcessingException, ApiException {
    Map<String, String> login =
        objectMapper.readValue(json.toString(), new TypeReference<Map<String, String>>() {});
    return loginInfo(exchangeForAccessToken(apiClient, login.get("identityToken"), null));
  }

  private static String loginInfo(String accessToken) throws JsonProcessingException {
    LoginInfo loginInfo = new LoginInfo();
    loginInfo.setAccessToken(accessToken);
    return objectWriter.writeValueAsString(loginInfo);
  }

  /**
   * Exchanges the identity token for a UC access token. A server in external mode has no exchange
   * (501 UNIMPLEMENTED) and accepts the identity provider's access token directly, so that token is
   * returned instead.
   *
   * @param idpAccessToken the identity provider's own access token from the browser login, or null
   *     when there is none
   * @return the UC access token, or {@code idpAccessToken} in external mode
   * @throws ApiException if the exchange fails, or if the server is in external mode and {@code
   *     idpAccessToken} is null or not a JWT
   */
  public static String exchangeForAccessToken(
      ApiClient apiClient, String identityToken, String idpAccessToken)
      throws JsonProcessingException, ApiException {

    URI endpoint = URI.create(apiClient.getBaseUri() + "/auth/tokens");

    String body =
        new OAuthTokenExchangeForm()
            .grantType(GrantType.TOKEN_EXCHANGE)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .subjectTokenType(TokenType.ID_TOKEN)
            .subjectToken(identityToken)
            .toUrlQueryString();

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(endpoint)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

    try {
      HttpResponse<String> response =
          apiClient.getHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
      if (isExchangeDisabled(response)) {
        if (idpAccessToken == null) {
          throw new ApiException(
              "Error authenticating - token exchange is disabled on this server; use the identity"
                  + " provider's access token as --auth_token - "
                  + response.body());
        }
        if (!isJwt(idpAccessToken)) {
          throw new ApiException(
              "Error authenticating - token exchange is disabled on this server and the identity"
                  + " provider returned an opaque access token; the server needs the identity"
                  + " provider's JWT access token - "
                  + response.body());
        }
        return idpAccessToken;
      } else if (response.statusCode() != HTTP_OK) {
        throw new ApiException("Error authenticating - " + response.body());
      } else {
        Map<String, String> responseMap =
            objectMapper.readValue(response.body(), new TypeReference<Map<String, String>>() {});
        return responseMap.get("access_token");
      }
    } catch (InterruptedException | IOException e) {
      throw new ApiException(e);
    }
  }

  /** True for three dot-separated non-empty parts, the shape of a JWT. */
  private static boolean isJwt(String token) {
    String[] parts = token.split("\\.", -1);
    return parts.length == 3 && Arrays.stream(parts).noneMatch(String::isEmpty);
  }

  /**
   * True for the 501 UNIMPLEMENTED UC error a server in external mode gives the exchange. Both the
   * status and the error code must match, so a proxy's own 501 is not taken for it.
   */
  private static boolean isExchangeDisabled(HttpResponse<String> response) {
    if (response.statusCode() != HTTP_NOT_IMPLEMENTED) {
      return false;
    }
    try {
      return "UNIMPLEMENTED"
          .equals(objectMapper.readTree(response.body()).path("error_code").asText());
    } catch (JsonProcessingException e) {
      return false;
    }
  }

  static class LoginInfo {
    @JsonProperty("access_token")
    private String accessToken;

    public void setAccessToken(String accessToken) {
      this.accessToken = accessToken;
    }

    public String getAccessToken() {
      return accessToken;
    }
  }
}
