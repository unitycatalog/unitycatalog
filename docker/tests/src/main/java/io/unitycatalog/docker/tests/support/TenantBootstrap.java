package io.unitycatalog.docker.tests.support;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.GrantsApi;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.CatalogInfo;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.control.api.UsersApi;
import io.unitycatalog.control.model.Email;
import io.unitycatalog.control.model.UserResource;
import io.unitycatalog.control.model.UserResourceList;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class TenantBootstrap {

  private static final String KC_REALM = "unity-catalog";

  private TenantBootstrap() {}

  public record TenantSpec(
      String tenantId,
      String catalogName,
      String userEmail,
      String userPassword,
      String credentialName,
      String externalLocationName,
      String dataExternalLocationName,
      String bucket,
      String dataBucket,
      String storageRoleArn,
      String keycloakContainer) {

    public static Builder builder() {
      return new Builder();
    }

    public static final class Builder {
      private String tenantId;
      private String catalogName;
      private String userEmail;
      private String userPassword;
      private String credentialName;
      private String externalLocationName;
      private String dataExternalLocationName;
      private String bucket = DockerTestConfig.BUCKET;
      private String dataBucket = DockerTestConfig.DATA_BUCKET;
      private String storageRoleArn = DockerTestConfig.STORAGE_ROLE_ARN;
      private String keycloakContainer;

      public Builder tenantId(String v) {
        tenantId = v;
        return this;
      }

      public Builder catalogName(String v) {
        catalogName = v;
        return this;
      }

      public Builder userEmail(String v) {
        userEmail = v;
        return this;
      }

      public Builder userPassword(String v) {
        userPassword = v;
        return this;
      }

      public Builder credentialName(String v) {
        credentialName = v;
        return this;
      }

      public Builder externalLocationName(String v) {
        externalLocationName = v;
        return this;
      }

      public Builder dataExternalLocationName(String v) {
        dataExternalLocationName = v;
        return this;
      }

      public Builder bucket(String v) {
        bucket = v;
        return this;
      }

      public Builder dataBucket(String v) {
        dataBucket = v;
        return this;
      }

      public Builder storageRoleArn(String v) {
        storageRoleArn = v;
        return this;
      }

      public Builder keycloakContainer(String v) {
        keycloakContainer = v;
        return this;
      }

      public TenantSpec build() {
        return new TenantSpec(
            tenantId,
            catalogName,
            userEmail,
            userPassword,
            credentialName,
            externalLocationName,
            dataExternalLocationName,
            bucket,
            dataBucket,
            storageRoleArn,
            keycloakContainer);
      }
    }
  }

  public static void bootstrap(String serverUrl, String adminToken, TenantSpec spec)
      throws Exception {
    String catalogTenantPath = spec.bucket() + "/tenant/" + spec.tenantId();
    String dataTenantPath = spec.dataBucket() + "/tenant/" + spec.tenantId();
    String kcUsername = AuthSupport.deriveKeycloakUsername(spec.userEmail());
    String kcPassword =
        spec.userPassword() != null && !spec.userPassword().isBlank()
            ? spec.userPassword()
            : AuthSupport.deriveDefaultPassword(spec.userEmail());

    ApiClient adminClient = UcClientFactory.catalogClient(serverUrl, adminToken);
    CredentialsApi credentialsApi = new CredentialsApi(adminClient);
    ExternalLocationsApi externalLocationsApi = new ExternalLocationsApi(adminClient);
    CatalogsApi catalogsApi = new CatalogsApi(adminClient);
    GrantsApi grantsApi = new GrantsApi(adminClient);
    UsersApi usersApi = new UsersApi(UcClientFactory.controlClient(serverUrl, adminToken));

    UcOperations.waitForServer(serverUrl, adminToken);
    ensureKeycloakUser(spec, kcUsername, kcPassword);
    ensureStorageCredential(credentialsApi, spec);
    ensureExternalLocation(
        externalLocationsApi, spec, spec.externalLocationName(), catalogTenantPath);
    ensureExternalLocation(
        externalLocationsApi, spec, spec.dataExternalLocationName(), dataTenantPath);
    ensureCatalog(catalogsApi, spec, catalogTenantPath);
    ensureUser(usersApi, spec);

    grantPrivileges(
        grantsApi,
        SecurableType.CATALOG,
        spec.catalogName(),
        spec.userEmail(),
        List.of(
            Privilege.fromValue("USE CATALOG"),
            Privilege.fromValue("CREATE SCHEMA"),
            Privilege.fromValue("CREATE TABLE"),
            Privilege.fromValue("SELECT"),
            Privilege.fromValue("MODIFY")));

    grantExternalLocationPrivileges(grantsApi, spec.externalLocationName(), spec.userEmail());
    grantExternalLocationPrivileges(
        grantsApi, spec.dataExternalLocationName(), spec.userEmail());

    String userToken = AuthSupport.ucTokenForUser(serverUrl, spec.userEmail(), kcPassword);
    verifyCatalogVisible(serverUrl, spec.catalogName(), userToken);
  }

  private static void ensureStorageCredential(CredentialsApi credentialsApi, TenantSpec spec)
      throws ApiException {
    if (credentialExists(credentialsApi, spec.credentialName())) {
      return;
    }
    credentialsApi.createCredential(
        new CreateCredentialRequest()
            .name(spec.credentialName())
            .purpose(CredentialPurpose.STORAGE)
            .awsIamRole(new AwsIamRoleRequest().roleArn(spec.storageRoleArn())));
  }

  private static boolean credentialExists(CredentialsApi credentialsApi, String name)
      throws ApiException {
    List<CredentialInfo> credentials =
        credentialsApi.listCredentials(100, null, CredentialPurpose.STORAGE).getCredentials();
    return credentials != null
        && credentials.stream().anyMatch(credential -> name.equals(credential.getName()));
  }

  private static void ensureExternalLocation(
      ExternalLocationsApi externalLocationsApi,
      TenantSpec spec,
      String locationName,
      String url)
      throws ApiException {
    if (externalLocationExists(externalLocationsApi, locationName)) {
      return;
    }
    externalLocationsApi.createExternalLocation(
        new CreateExternalLocation()
            .name(locationName)
            .url(url)
            .credentialName(spec.credentialName()));
  }

  private static boolean externalLocationExists(
      ExternalLocationsApi externalLocationsApi, String name) throws ApiException {
    List<ExternalLocationInfo> locations =
        externalLocationsApi.listExternalLocations(100, null).getExternalLocations();
    return locations != null
        && locations.stream().anyMatch(location -> name.equals(location.getName()));
  }

  private static void ensureCatalog(CatalogsApi catalogsApi, TenantSpec spec, String tenantPath)
      throws ApiException {
    if (catalogExists(catalogsApi, spec.catalogName())) {
      return;
    }
    catalogsApi.createCatalog(
        new CreateCatalog()
            .name(spec.catalogName())
            .comment("Tenant " + spec.tenantId() + " catalog")
            .storageRoot(tenantPath));
  }

  private static boolean catalogExists(CatalogsApi catalogsApi, String name) throws ApiException {
    List<CatalogInfo> catalogs = catalogsApi.listCatalogs(null, 100).getCatalogs();
    return catalogs != null
        && catalogs.stream().anyMatch(catalog -> name.equals(catalog.getName()));
  }

  private static void ensureUser(UsersApi usersApi, TenantSpec spec)
      throws io.unitycatalog.control.ApiException {
    if (userExists(usersApi, spec.userEmail())) {
      return;
    }
    String displayName = "User " + capitalizeFirst(emailLocalPart(spec.userEmail()));
    usersApi.createUser(
        new UserResource()
            .displayName(displayName)
            .emails(List.of(new Email().value(spec.userEmail()).primary(true))));
  }

  private static boolean userExists(UsersApi usersApi, String email)
      throws io.unitycatalog.control.ApiException {
    UserResourceList users = usersApi.listUsers(null, null, 200);
    if (users.getResources() == null) {
      return false;
    }
    return users.getResources().stream()
        .flatMap(user -> Optional.ofNullable(user.getEmails()).orElse(List.of()).stream())
        .anyMatch(userEmail -> email.equals(userEmail.getValue()));
  }

  private static void grantExternalLocationPrivileges(
      GrantsApi grantsApi, String externalLocationName, String principal) throws ApiException {
    grantPrivileges(
        grantsApi,
        SecurableType.EXTERNAL_LOCATION,
        externalLocationName,
        principal,
        List.of(
            Privilege.fromValue("CREATE MANAGED STORAGE"),
            Privilege.fromValue("READ FILES"),
            Privilege.fromValue("WRITE FILES"),
            Privilege.fromValue("CREATE EXTERNAL TABLE"),
            Privilege.fromValue("CREATE EXTERNAL VOLUME")));
  }

  private static void grantPrivileges(
      GrantsApi grantsApi,
      SecurableType securableType,
      String resourceName,
      String principal,
      List<Privilege> privileges)
      throws ApiException {
    PermissionsChange change =
        new PermissionsChange().principal(principal).add(privileges).remove(List.of());
    grantsApi.update(securableType, resourceName, new UpdatePermissions().changes(List.of(change)));
  }

  private static void verifyCatalogVisible(String serverUrl, String catalog, String userToken)
      throws ApiException {
    new UcOperations(serverUrl, userToken).assertCatalogVisible(catalog);
  }

  private static void ensureKeycloakUser(TenantSpec spec, String kcUsername, String kcPassword)
      throws Exception {
    String container = resolveKeycloakContainer(spec.keycloakContainer());
    runKcadm(
        container,
        "config",
        "credentials",
        "--server",
        "http://localhost:8080",
        "--realm",
        "master",
        "--user",
        "admin",
        "--password",
        "admin");

    String userId = findKeycloakUserId(container, kcUsername);
    String emailLocalPart = emailLocalPart(spec.userEmail());
    if (userId == null) {
      runKcadm(
          container,
          "create",
          "users",
          "-r",
          KC_REALM,
          "-s",
          "username=" + kcUsername,
          "-s",
          "enabled=true",
          "-s",
          "emailVerified=true",
          "-s",
          "email=" + spec.userEmail(),
          "-s",
          "firstName=" + emailLocalPart,
          "-s",
          "lastName=" + spec.tenantId(),
          "-s",
          "requiredActions=[]");
    } else {
      runKcadm(
          container,
          "update",
          "users/" + userId,
          "-r",
          KC_REALM,
          "-s",
          "email=" + spec.userEmail(),
          "-s",
          "emailVerified=true",
          "-s",
          "enabled=true",
          "-s",
          "requiredActions=[]");
    }

    runKcadm(
        container,
        "set-password",
        "-r",
        KC_REALM,
        "--username",
        kcUsername,
        "--new-password",
        kcPassword);

    for (int attempt = 1; attempt <= 15; attempt++) {
      try {
        AuthSupport.verifyKeycloakLogin(spec.userEmail(), kcPassword);
        return;
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
          throw e;
        }
        TimeUnit.SECONDS.sleep(1);
      }
    }
    throw new IllegalStateException("Keycloak login verification failed for " + kcUsername);
  }

  private static String findKeycloakUserId(String container, String username) throws Exception {
    String output =
        runKcadmCapture(
            container,
            "get",
            "users",
            "-r",
            KC_REALM,
            "-q",
            "username=" + username,
            "--fields",
            "id",
            "--format",
            "csv",
            "--noquotes");
    for (String line : output.lines().toList()) {
      if (line.equals("id") || line.isBlank()) {
        continue;
      }
      return line.trim();
    }
    return null;
  }

  private static String resolveKeycloakContainer(String explicit) throws Exception {
    if (explicit != null && !explicit.isBlank()) {
      return explicit;
    }
    Process process =
        new ProcessBuilder("docker", "ps", "--format", "{{.Names}}")
            .redirectErrorStream(true)
            .start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    if (process.waitFor() != 0) {
      throw new IllegalStateException("docker ps failed");
    }
    List<String> names =
        output.lines().filter(name -> name.toLowerCase(Locale.ROOT).contains("keycloak")).toList();
    if (names.isEmpty()) {
      throw new IllegalStateException("Keycloak container not found");
    }
    return names.get(0);
  }

  private static void runKcadm(String container, String... args) throws Exception {
    List<String> command = new ArrayList<>();
    command.add("docker");
    command.add("exec");
    command.add(container);
    command.add("/opt/keycloak/bin/kcadm.sh");
    command.addAll(List.of(args));
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    int exit = process.waitFor();
    if (exit != 0) {
      throw new IllegalStateException("kcadm failed (" + exit + "): " + output.trim());
    }
  }

  private static String runKcadmCapture(String container, String... args) throws Exception {
    List<String> command = new ArrayList<>();
    command.add("docker");
    command.add("exec");
    command.add(container);
    command.add("/opt/keycloak/bin/kcadm.sh");
    command.addAll(List.of(args));
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    int exit = process.waitFor();
    if (exit != 0) {
      throw new IllegalStateException("kcadm failed (" + exit + "): " + output.trim());
    }
    return output;
  }

  private static String emailLocalPart(String email) {
    int at = email.indexOf('@');
    return at >= 0 ? email.substring(0, at) : email;
  }

  private static String capitalizeFirst(String value) {
    if (value == null || value.isEmpty()) {
      return value;
    }
    return Character.toUpperCase(value.charAt(0)) + value.substring(1);
  }
}
