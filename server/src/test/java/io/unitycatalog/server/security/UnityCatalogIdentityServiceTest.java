package io.unitycatalog.server.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.control.model.User;
import io.unitycatalog.server.exception.AuthorizationException;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.utils.ServerProperties;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class UnityCatalogIdentityServiceTest {

  @Test
  void keepsLenientTokenIdentityWhenAuthorizationIsDisabled() {
    UnityCatalogIdentityService service = service(false, mock(UserRepository.class));

    UnityCatalogIdentityService.Identity identity = service.authenticate("plain-token");

    assertEquals("plain-token", identity.name());
    assertEquals(
        UUID.nameUUIDFromBytes("plain-token".getBytes(StandardCharsets.UTF_8)), identity.id());
  }

  @Test
  void resolvesEnabledUsersForTrustedOnBehalfOfCalls() {
    UUID id = UUID.randomUUID();
    UserRepository users = mock(UserRepository.class);
    when(users.getUser(id.toString()))
        .thenReturn(
            new User().id(id.toString()).email("owner@example.com").state(User.StateEnum.ENABLED));
    UnityCatalogIdentityService service = service(true, users);

    UnityCatalogIdentityService.Identity identity = service.onBehalfOf(id.toString(), "ignored");

    assertEquals(id, identity.id());
    assertEquals("owner@example.com", identity.name());
  }

  @Test
  void rejectsDisabledOnBehalfOfUsers() {
    UUID id = UUID.randomUUID();
    UserRepository users = mock(UserRepository.class);
    when(users.getUser(id.toString()))
        .thenReturn(
            new User().id(id.toString()).email("owner@example.com").state(User.StateEnum.DISABLED));
    UnityCatalogIdentityService service = service(true, users);

    assertThrows(AuthorizationException.class, () -> service.onBehalfOf(id.toString(), "ignored"));
  }

  private static UnityCatalogIdentityService service(
      boolean authorizationEnabled, UserRepository users) {
    Properties values = new Properties();
    values.setProperty("server.authorization", authorizationEnabled ? "enable" : "disable");
    Repositories repositories = mock(Repositories.class);
    when(repositories.getUserRepository()).thenReturn(users);
    return new UnityCatalogIdentityService(
        mock(SecurityContext.class), repositories, new ServerProperties(values));
  }
}
