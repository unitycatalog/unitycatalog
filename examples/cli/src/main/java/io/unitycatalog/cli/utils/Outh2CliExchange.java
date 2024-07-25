package io.unitycatalog.cli.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.server.persist.utils.ServerPropertiesUtils;
import org.apache.commons.codec.binary.Hex;

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
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.URLDecoder.decode;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * Simple OAuth2 authentication flow for the CLI.
 */
public class Outh2CliExchange {

  interface Fields {
    String GRANT_TYPE = "grant_type";
    String CLIENT_ID = "client_id";
    String CLIENT_SECRET = "client_secret";
    String REDIRECT_URL = "redirect_uri";
  }

  ServerPropertiesUtils serverProperties = ServerPropertiesUtils.getInstance();

  private static final ObjectMapper mapper = new ObjectMapper();

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

    String authUrl = String.format("%s?client_id=%s&redirect_uri=%s&response_type=code&scope=%s&state=%s",
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

//      try (BufferedInputStream errorStream = new BufferedInputStream(exec.getErrorStream())) {
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
    PrintStream printStream = new PrintStream(new BufferedOutputStream(urlConnection.getOutputStream()));
    printStream.print(tokenBody);
    printStream.close();
    urlConnection.connect();

    if (urlConnection.getResponseCode() == HTTP_OK) {
      String tokenResponse;
      try (InputStream inputStream = urlConnection.getInputStream()) {
        tokenResponse = new String(inputStream.readAllBytes());
      }
      System.out.println("Received token response.");
      Map<String, String> tokenResponseParams = mapper.readValue(tokenResponse, new TypeReference<>() {
      });
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
            .filter(p -> !p.getKey().equals(Fields.CLIENT_ID) && !p.getKey().equals(Fields.CLIENT_SECRET))
            .map(p -> URLEncoder.encode(p.getKey(), StandardCharsets.UTF_8) +
                    "=" +
                    URLEncoder.encode(p.getValue(), StandardCharsets.UTF_8))
            .reduce((p1, p2) -> p1 + "&" + p2)
            .orElse("");
  }

  private String buildAuthorization(Map<String, String> tokenParams) {
    String authorizationValue = tokenParams.get(Fields.CLIENT_ID) + ":" + tokenParams.get(Fields.CLIENT_SECRET);
    return "Basic " + Base64.getEncoder().encodeToString(authorizationValue.getBytes());
  }

  static class AuthCallbackHandler implements HttpHandler {

    CompletableFuture<String> futureValue = new CompletableFuture<>();

    @Override
    public void handle(HttpExchange exchange) throws IOException {

      // Get request query parameters
      Map<String, List<String>> parameters = Pattern.compile("&")
              .splitAsStream(exchange.getRequestURI().getQuery())
              .map(s -> Arrays.copyOf(s.split("=", 2), 2))
              .collect(groupingBy(s ->
                              decode(s[0], StandardCharsets.UTF_8),
                      mapping(s -> decode(s[1], StandardCharsets.UTF_8),
                              toList())));

      // Get the authorization flow code
      String value = parameters.get("code").get(0);

      // Prepare response
      String response = "Authentication to UC successful.";
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