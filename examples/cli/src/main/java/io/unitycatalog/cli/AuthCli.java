package io.unitycatalog.cli;

import static io.unitycatalog.cli.utils.CliUtils.postProcessAndPrintOutput;
import static java.net.HttpURLConnection.HTTP_OK;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.unitycatalog.cli.utils.CliParams;
import io.unitycatalog.cli.utils.CliUtils;
import io.unitycatalog.cli.utils.Oauth2CliExchange;
import io.unitycatalog.cli.utils.Oauth2CliExchange.GrantTypes;
import io.unitycatalog.cli.utils.Oauth2CliExchange.TokenTypes;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
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
      Map<String, String> login = new HashMap<>();
      login.put("identityToken", identityToken);
      return doExchange(apiClient, login);
    } catch (IOException e) {
      throw new ApiException(e);
    }
  }

  private static String exchange(ApiClient apiClient, JSONObject json)
      throws JsonProcessingException, ApiException {
    Map<String, String> login =
        objectMapper.readValue(json.toString(), new TypeReference<Map<String, String>>() {});
    return doExchange(apiClient, login);
  }

  private static String doExchange(ApiClient apiClient, Map<String, String> login)
      throws JsonProcessingException, ApiException {

    URI tokensEndpoint = URI.create(apiClient.getBaseUri() + "/auth/tokens");

    StringBuilder builder = new StringBuilder();
    builder
        .append("grant_type=")
        .append(URLEncoder.encode(GrantTypes.TOKEN_EXCHANGE, StandardCharsets.UTF_8))
        .append("&requested_token_type=")
        .append(URLEncoder.encode(TokenTypes.ACCESS, StandardCharsets.UTF_8))
        .append("&subject_token_type=")
        .append(URLEncoder.encode(TokenTypes.ID, StandardCharsets.UTF_8))
        .append("&subject_token=")
        .append(URLEncoder.encode(login.get("identityToken"), StandardCharsets.UTF_8));

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(tokensEndpoint)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(builder.toString()))
            .build();

    try {
      HttpResponse<String> response =
          apiClient.getHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != HTTP_OK) {
        throw new ApiException("Error authenticating - " + response.body());
      } else {
        Map<String, String> responseMap =
            objectMapper.readValue(response.body(), new TypeReference<Map<String, String>>() {});
        LoginInfo loginInfo = new LoginInfo();
        loginInfo.setAccessToken(responseMap.get("access_token"));
        return objectWriter.writeValueAsString(loginInfo);
      }
    } catch (InterruptedException | IOException e) {
      throw new ApiException(e);
    }
  }

  static class Login {
    @JsonProperty("identity_token")
    private String identity_token;

    public void setIdentity_token(String identity_token) {
      this.identity_token = identity_token;
    }

    public String getIdentity_token() {
      return identity_token;
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
