package io.unitycatalog.server.service.credential.azure;

public record AzureCredential(String sasToken, long expirationTimeInEpochMillis) {

}
