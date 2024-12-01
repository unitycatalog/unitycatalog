package io.unitycatalog.cli.utils;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.URLDecoder.decode;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.control.model.GrantType;
import io.unitycatalog.control.model.TokenType;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Hex;

/** Simple OAuth2 authentication flow for the CLI. */
public class Oauth2CliExchange {

  // TODO: need common module for these constants, they are reused in AuthService
  public interface Fields {
    String GRANT_TYPE = "grant_type";
    String CLIENT_ID = "client_id";
    String CLIENT_SECRET = "client_secret";
    String REDIRECT_URL = "redirect_uri";
  }

  public static class RequestBody {
    private GrantType grantType;
    private TokenType requestedTokenType;
    private TokenType subjectTokenType;
    private String subjectToken;
    private TokenType actorTokenType;
    private String actorToken;

    public RequestBody grantType(GrantType grantType) {
      this.grantType = grantType;
      return this;
    }

    public RequestBody requestedTokenType(TokenType requestedTokenType) {
      this.requestedTokenType = requestedTokenType;
      return this;
    }

    public RequestBody subjectTokenType(TokenType subjectTokenType) {
      this.subjectTokenType = subjectTokenType;
      return this;
    }

    public RequestBody subjectToken(String subjectToken) {
      this.subjectToken = subjectToken;
      return this;
    }

    public RequestBody actorTokenType(TokenType actorTokenType) {
      this.actorTokenType = actorTokenType;
      return this;
    }

    public RequestBody actorToken(String actorToken) {
      this.actorToken = actorToken;
      return this;
    }

    public String encode() {
      StringBuilder builder = new StringBuilder();

      if (this.grantType == null) {
        throw new RuntimeException(
            "The 'grant_token' parameter is required and must be specified.");
      }
      builder
          .append("grant_type=")
          .append(URLEncoder.encode(this.grantType.getValue(), StandardCharsets.UTF_8));

      if (this.requestedTokenType == null) {
        throw new RuntimeException(
            "The 'requested_token_type' parameter is required and must be specified.");
      }
      builder
          .append("&requested_token_type=")
          .append(URLEncoder.encode(this.requestedTokenType.getValue(), StandardCharsets.UTF_8));

      if (this.subjectTokenType == null) {
        throw new RuntimeException(
            "The 'subject_token_type' parameter is required and must be specified.");
      }
      builder
          .append("&subject_token_type=")
          .append(URLEncoder.encode(this.subjectTokenType.getValue(), StandardCharsets.UTF_8));

      if (this.subjectToken == null) {
        throw new RuntimeException(
            "The 'subject_token' parameter is required and must be specified.");
      }
      builder
          .append("&subject_token=")
          .append(URLEncoder.encode(this.subjectToken, StandardCharsets.UTF_8));

      if (this.actorTokenType != null) {
        builder
            .append("&actor_token_type=")
            .append(URLEncoder.encode(this.actorTokenType.getValue(), StandardCharsets.UTF_8));
      }

      if (this.actorToken != null) {
        builder
            .append("&actor_token=")
            .append(URLEncoder.encode(this.actorToken, StandardCharsets.UTF_8));
      }

      return builder.toString();
    }
  }

  // Change name.
  public static class OAuthURL {}

  private static final ObjectMapper mapper = new ObjectMapper();

  Properties serverProperties = new Properties();

  public Oauth2CliExchange() throws IOException {
    // TODO: Probably should retrieve this from the server or do exchange server side
    try (InputStream input = Files.newInputStream(Paths.get("etc/conf/server.properties"))) {
      serverProperties.load(input);
    }
  }

  // TODO: Some of this should probably be done server side.
  public String authenticate() throws IOException {

    // TODO: These properties, especially client-secret should probably be server side.
    String authorizationUrl = serverProperties.getProperty("server.authorization-url");
    String tokenUrl = serverProperties.getProperty("server.token-url");
    String clientId = serverProperties.getProperty("server.client-id");
    String clientSecret = serverProperties.getProperty("server.client-secret");

    // Find a random available port
    int port = findAvailablePort();

    // Create HttpServer instance
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

    AuthCallbackHandler authCallbackHandler = new AuthCallbackHandler();
    // Create a context for the root path "/"
    server.createContext("/", authCallbackHandler);

    // Set the server executor (optional)
    server.setExecutor(null); // Use default executor

    // Start the server
    server.start();

    System.out.println("Listening on port: " + port);

    String redirectUrl = "http://localhost:" + port;

    byte[] stateBytes = new byte[16];
    new SecureRandom().nextBytes(stateBytes);

    String authUrl =
        String.format(
            "%s?client_id=%s&redirect_uri=%s&response_type=code&scope=%s&state=%s",
            authorizationUrl,
            URLEncoder.encode(clientId, StandardCharsets.UTF_8),
            URLEncoder.encode(redirectUrl, StandardCharsets.UTF_8),
            URLEncoder.encode("openid profile email", StandardCharsets.UTF_8),
            Hex.encodeHexString(stateBytes));

    System.out.println("Attempting to open the authorization page in your default browser.");
    System.out.println("If the browser does not open, you can manually open the following URL:");
    System.out.println();
    System.out.println(authUrl);
    System.out.println();

    Runtime runtime = Runtime.getRuntime();

    try {
      // TODO: Make this work on different operating-systems.
      Process exec = runtime.exec("/usr/bin/open " + authUrl);
      exec.waitFor();

      //      try (BufferedInputStream errorStream = new BufferedInputStream(exec.getErrorStream()))
      // {
      //        String errorResponse = new String(errorStream.readAllBytes());
      //        if (!errorResponse.isEmpty()) {
      //          System.out.println(errorResponse);
      //        }
      //      }
    } catch (InterruptedException e) {
      // IGNORE - we couldn't automatically open a browser tab.
    }

    String authCode;
    try {
      authCode = authCallbackHandler.value().get();
      System.out.println("Received authentication response.");
    } catch (InterruptedException | ExecutionException e) {
      // Bail if we get interrupted.
      return null;
    }

    server.stop(0);

    Map<String, String> tokenParams = new HashMap<>();
    tokenParams.put("code", authCode);
    tokenParams.put(Fields.CLIENT_ID, clientId);
    tokenParams.put(Fields.CLIENT_SECRET, clientSecret);
    tokenParams.put(Fields.GRANT_TYPE, "authorization_code");
    tokenParams.put(Fields.REDIRECT_URL, redirectUrl);

    String tokenBody = buildTokenBody(tokenParams);
    String authorization = buildAuthorization(tokenParams);

    // TODO: Replace this with a more modern web-client
    HttpURLConnection urlConnection = (HttpURLConnection) new URL(tokenUrl).openConnection();
    urlConnection.setRequestMethod("POST");
    urlConnection.setDoInput(true);
    urlConnection.setDoOutput(true);
    urlConnection.setUseCaches(false);
    urlConnection.setRequestProperty("accept", "application/json");
    urlConnection.setRequestProperty("content-type", "application/x-www-form-urlencoded");
    urlConnection.setRequestProperty("authorization", authorization);
    PrintStream printStream =
        new PrintStream(new BufferedOutputStream(urlConnection.getOutputStream()));
    printStream.print(tokenBody);
    printStream.close();
    urlConnection.connect();

    if (urlConnection.getResponseCode() == HTTP_OK) {
      String tokenResponse;
      try (InputStream inputStream = urlConnection.getInputStream()) {
        tokenResponse = new String(inputStream.readAllBytes());
      }
      System.out.println("Received token response.");
      Map<String, String> tokenResponseParams =
          mapper.readValue(tokenResponse, new TypeReference<>() {});
      urlConnection.disconnect();
      return tokenResponseParams.get("id_token");
    } else {
      String errorResponse;
      try (InputStream inputStream = urlConnection.getErrorStream()) {
        errorResponse = new String(inputStream.readAllBytes());
      }
      System.out.println(errorResponse);
      urlConnection.disconnect();
      return null;
    }
  }

  private int findAvailablePort() throws IOException {
    String port = serverProperties.getProperty("server.redirect-port");
    if (port != null && !port.isBlank()) {
      return Integer.parseInt(port);
    } else {
      try (ServerSocket serverSocket = new ServerSocket(0)) {
        return serverSocket.getLocalPort();
      }
    }
  }

  private String buildTokenBody(Map<String, String> tokenParams) throws JsonProcessingException {
    return tokenParams.entrySet().stream()
        .filter(
            p -> !p.getKey().equals(Fields.CLIENT_ID) && !p.getKey().equals(Fields.CLIENT_SECRET))
        .map(
            p ->
                URLEncoder.encode(p.getKey(), StandardCharsets.UTF_8)
                    + "="
                    + URLEncoder.encode(p.getValue(), StandardCharsets.UTF_8))
        .reduce((p1, p2) -> p1 + "&" + p2)
        .orElse("");
  }

  private String buildAuthorization(Map<String, String> tokenParams) {
    String authorizationValue =
        tokenParams.get(Fields.CLIENT_ID) + ":" + tokenParams.get(Fields.CLIENT_SECRET);
    return "Basic " + Base64.getEncoder().encodeToString(authorizationValue.getBytes());
  }

  static class AuthCallbackHandler implements HttpHandler {

    CompletableFuture<String> futureValue = new CompletableFuture<>();

    @Override
    public void handle(HttpExchange exchange) throws IOException {

      // Get request query parameters
      Map<String, List<String>> parameters =
          Pattern.compile("&")
              .splitAsStream(exchange.getRequestURI().getQuery())
              .map(s -> Arrays.copyOf(s.split("=", 2), 2))
              .collect(
                  groupingBy(
                      s -> decode(s[0], StandardCharsets.UTF_8),
                      mapping(s -> decode(s[1], StandardCharsets.UTF_8), toList())));

      // Get the authorization flow code
      String value = parameters.get("code").get(0);

      // Prepare response send to browser.
      String response = "User validated with identity provider.";
      exchange.sendResponseHeaders(HTTP_OK, response.getBytes().length);
      OutputStream os = exchange.getResponseBody();
      os.write(response.getBytes());
      os.close();

      // Set the response value
      futureValue.complete(value);
    }

    Future<String> value() {
      return futureValue;
    }
  }
}
