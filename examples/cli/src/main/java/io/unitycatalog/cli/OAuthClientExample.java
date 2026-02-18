package io.unitycatalog.cli;


import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.model.CatalogInfo;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class OAuthClientExample {
    public static void main(String[] args) {
        // Initialize API client
        ApiClient apiClient = new ApiClient();

        // Set the first OAuth2 token directly, if you already have it
        apiClient.setAccessToken("your-oauth-token");

        // Or retrieve a token programmatically
        String tokenEndpoint = "https://auth.example.com/oauth2/token";
        String clientId = "your-client-id";
        String clientSecret = "your-client-secret";

        try {
            String token = getOAuthToken(tokenEndpoint, clientId, clientSecret);
            apiClient.setAccessToken(token);
        } catch (Exception e) {
            System.err.println("Failed to obtain OAuth token: " + e.getMessage());
        }

        // Use the client to call your API
        CatalogsApi catalogsApi = new CatalogsApi(apiClient);
        try {
            CatalogInfo catalogInfo = catalogsApi.getCatalog("xyz");
            System.out.println(catalogInfo);
        } catch (ApiException e) {
            System.err.println("API call failed: " + e.getMessage());
        }
    }

    private static String getOAuthToken(String tokenEndpoint, String clientId, String clientSecret) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(tokenEndpoint).openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

        String payload = "grant_type=client_credentials&client_id=" + clientId + "&client_secret=" + clientSecret;
        try (OutputStream os = connection.getOutputStream()) {
            os.write(payload.getBytes());
        }

        try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String response = br.readLine();
            // Parse token from response (using your preferred JSON library)
            return parseToken(response); // Implement parseToken accordingly
        }
    }

    private static String parseToken(String response) {
        // Parse the JSON response and extract the access_token
        JSONObject json = new JSONObject(response);
        return json.getString("access_token");
    }
}
