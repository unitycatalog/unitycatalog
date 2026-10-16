package io.unitycatalog.server.auth.decorator;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.control.model.User;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.MetastoreRepository;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.model.Privileges;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UnityAccessUtilTest {

  private final UserRepository userRepository = mock(UserRepository.class);
  private final MetastoreRepository metastoreRepository = mock(MetastoreRepository.class);
  private final UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
  private UnityAccessUtil unityAccessUtil;

  @BeforeEach
  public void setUp() {
    Repositories repositories = mock(Repositories.class);
    when(repositories.getUserRepository()).thenReturn(userRepository);
    when(repositories.getMetastoreRepository()).thenReturn(metastoreRepository);
    unityAccessUtil = new UnityAccessUtil(repositories);
  }

  @Test
  public void createsTheAdminWhenNoneExists() {
    UUID adminId = UUID.randomUUID();
    UUID metastoreId = UUID.randomUUID();
    when(userRepository.getUserByEmail("admin"))
        .thenThrow(new BaseException(ErrorCode.NOT_FOUND, "User not found: admin"));
    when(userRepository.createUser(any())).thenReturn(new User().id(adminId.toString()));
    when(metastoreRepository.getMetastoreId()).thenReturn(metastoreId);

    unityAccessUtil.initializeAdmin(authorizer);

    verify(authorizer).grantAuthorization(adminId, metastoreId, Privileges.OWNER);
  }

  @Test
  public void failsWhenTheAdminLookupFails() {
    BaseException lookupError = new BaseException(ErrorCode.INTERNAL, "Failed to get user");
    when(userRepository.getUserByEmail("admin")).thenThrow(lookupError);

    assertThatThrownBy(() -> unityAccessUtil.initializeAdmin(authorizer)).isSameAs(lookupError);
    verify(userRepository, never()).createUser(any());
  }
}
