package io.unitycatalog.cli;

import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.oauth.ExternalBrowserCredentialsProvider;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.model.CatalogInfo;

public class OAuthClientWithDatabricksSdkExample {

    public static void main(String[] args) {
        // Initialize API client
        ApiClient apiClient = new ApiClient();

        DatabricksConfig databricksConfig = new DatabricksConfig().setAuthType("external-browser")
                .setHost("https://your-databricks-instance.com")
                .setClientId("your-client-id")
                .setClientSecret("your-client-secret")
                .setOAuthRedirectUrl("http://localhost:8020");
        HeaderFactory headerFactory = new ExternalBrowserCredentialsProvider().configure(databricksConfig);
        headerFactory.headers().forEach(apiClient::addDefaultHeader);

        // Use the client to call your API
        CatalogsApi catalogsApi = new CatalogsApi(apiClient);
        try {
            CatalogInfo catalogInfo = catalogsApi.getCatalog("xyz");
            System.out.println(catalogInfo);
        } catch (ApiException e) {
            System.err.println("API call failed: " + e.getMessage());
        }
    }


}
