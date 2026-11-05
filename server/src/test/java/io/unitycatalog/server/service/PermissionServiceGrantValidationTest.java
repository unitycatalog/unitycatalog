package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.PermissionsChange;
import io.unitycatalog.server.model.Privilege;
import io.unitycatalog.server.model.SecurableType;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class PermissionServiceGrantValidationTest {

  @ParameterizedTest
  @EnumSource(SecurableType.class)
  public void additionsRequireAValidObjectType(SecurableType type) {
    Set<Privilege> validPrivileges =
        switch (type) {
          case METASTORE ->
              Set.of(
                  Privilege.CREATE_CATALOG,
                  Privilege.CREATE_STORAGE_CREDENTIAL,
                  Privilege.CREATE_EXTERNAL_LOCATION);
          case CATALOG ->
              Set.of(
                  Privilege.BROWSE,
                  Privilege.MANAGE,
                  Privilege.USE_CATALOG,
                  Privilege.CREATE_SCHEMA,
                  Privilege.USE_SCHEMA,
                  Privilege.CREATE_TABLE,
                  Privilege.SELECT,
                  Privilege.MODIFY,
                  Privilege.CREATE_FUNCTION,
                  Privilege.EXECUTE,
                  Privilege.CREATE_VOLUME,
                  Privilege.READ_VOLUME,
                  Privilege.WRITE_VOLUME,
                  Privilege.CREATE_MODEL,
                  Privilege.CREATE_MODEL_VERSION);
          case SCHEMA ->
              Set.of(
                  Privilege.MANAGE,
                  Privilege.USE_SCHEMA,
                  Privilege.CREATE_TABLE,
                  Privilege.SELECT,
                  Privilege.MODIFY,
                  Privilege.CREATE_FUNCTION,
                  Privilege.EXECUTE,
                  Privilege.CREATE_VOLUME,
                  Privilege.READ_VOLUME,
                  Privilege.WRITE_VOLUME,
                  Privilege.CREATE_MODEL,
                  Privilege.CREATE_MODEL_VERSION);
          case TABLE -> Set.of(Privilege.SELECT, Privilege.MODIFY, Privilege.MANAGE);
          case FUNCTION -> Set.of(Privilege.EXECUTE, Privilege.MANAGE);
          case REGISTERED_MODEL ->
              Set.of(Privilege.EXECUTE, Privilege.CREATE_MODEL_VERSION, Privilege.MANAGE);
          case VOLUME -> Set.of(Privilege.READ_VOLUME, Privilege.WRITE_VOLUME, Privilege.MANAGE);
          case EXTERNAL_LOCATION ->
              Set.of(
                  Privilege.BROWSE,
                  Privilege.MANAGE,
                  Privilege.READ_FILES,
                  Privilege.WRITE_FILES,
                  Privilege.CREATE_EXTERNAL_TABLE,
                  Privilege.CREATE_EXTERNAL_VOLUME,
                  Privilege.CREATE_MANAGED_STORAGE);
          case CREDENTIAL ->
              Set.of(
                  Privilege.MANAGE,
                  Privilege.CREATE_EXTERNAL_LOCATION,
                  Privilege.READ_FILES,
                  Privilege.WRITE_FILES,
                  Privilege.CREATE_EXTERNAL_TABLE);
        };

    for (Privilege privilege : Privilege.values()) {
      List<PermissionsChange> changes =
          List.of(new PermissionsChange().principal("reader").add(List.of(privilege)));
      if (privilege == Privilege.READ_METADATA || validPrivileges.contains(privilege)) {
        assertThatCode(() -> PermissionService.validateAddedPrivileges(type, changes))
            .doesNotThrowAnyException();
      } else {
        assertThatThrownBy(() -> PermissionService.validateAddedPrivileges(type, changes))
            .isInstanceOfSatisfying(
                BaseException.class,
                exception ->
                    assertThat(exception.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT))
            .hasMessage(
                "Privilege " + privilege.getValue() + " cannot be granted on " + type.getValue());
      }

      List<PermissionsChange> removals =
          List.of(new PermissionsChange().principal("reader").remove(List.of(privilege)));
      assertThatCode(() -> PermissionService.validateAddedPrivileges(type, removals))
          .doesNotThrowAnyException();
    }
  }
}
