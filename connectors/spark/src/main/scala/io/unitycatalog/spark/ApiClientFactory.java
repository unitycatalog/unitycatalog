package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;

import java.net.URI;

public class ApiClientFactory {
    private ApiClientFactory() {}

    public static ApiClient createApiClient(URI url, String token){
        ApiClient apiClient = new ApiClient()
                .setHost(url.getHost())
                .setPort(url.getPort())
                .setScheme(url.getScheme());
        if (token != null && !token.isEmpty()) {
            apiClient = apiClient.setRequestInterceptor(
                    request -> request.header("Authorization", "Bearer " + token)
            );
        }

        return apiClient;
    }
}
