package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.delta.model.DeltaTableType;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.TableType;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Pins the {@link DeltaTableTypes} bijection between the Delta API table type and the persisted
 * (stored type, base_table_id) pair, so the create path and the response builder cannot drift.
 */
public class DeltaTableTypesTest {

  private static final UUID BASE_ID = UUID.fromString("11111111-2222-3333-4444-555555555555");

  @Test
  public void isShallowCloneCoversExactlyTheCloneVariants() {
    assertThat(DeltaTableTypes.isShallowClone(DeltaTableType.MANAGED_SHALLOW_CLONE)).isTrue();
    assertThat(DeltaTableTypes.isShallowClone(DeltaTableType.EXTERNAL_SHALLOW_CLONE)).isTrue();
    assertThat(DeltaTableTypes.isShallowClone(DeltaTableType.MANAGED)).isFalse();
    assertThat(DeltaTableTypes.isShallowClone(DeltaTableType.EXTERNAL)).isFalse();
  }

  @Test
  public void toStoredCollapsesCloneTypesOntoTheirBaseType() {
    assertThat(DeltaTableTypes.toStoredTableType(DeltaTableType.MANAGED))
        .isEqualTo(TableType.MANAGED);
    assertThat(DeltaTableTypes.toStoredTableType(DeltaTableType.MANAGED_SHALLOW_CLONE))
        .isEqualTo(TableType.MANAGED);
    assertThat(DeltaTableTypes.toStoredTableType(DeltaTableType.EXTERNAL))
        .isEqualTo(TableType.EXTERNAL);
  }

  @Test
  public void toStoredRejectsExternalShallowCloneAsUnimplemented() {
    assertThatThrownBy(
            () -> DeltaTableTypes.toStoredTableType(DeltaTableType.EXTERNAL_SHALLOW_CLONE))
        .isInstanceOf(BaseException.class)
        .satisfies(
            e -> assertThat(((BaseException) e).getErrorCode()).isEqualTo(ErrorCode.UNIMPLEMENTED))
        .hasMessageContaining("EXTERNAL_SHALLOW_CLONE");
  }

  @Test
  public void fromStoredDerivesCloneVariantFromBaseTableIdPresence() {
    assertThat(DeltaTableTypes.fromStored("MANAGED", null)).isEqualTo(DeltaTableType.MANAGED);
    assertThat(DeltaTableTypes.fromStored("MANAGED", BASE_ID))
        .isEqualTo(DeltaTableType.MANAGED_SHALLOW_CLONE);
    assertThat(DeltaTableTypes.fromStored("EXTERNAL", null)).isEqualTo(DeltaTableType.EXTERNAL);
    assertThat(DeltaTableTypes.fromStored("EXTERNAL", BASE_ID))
        .isEqualTo(DeltaTableType.EXTERNAL_SHALLOW_CLONE);
  }
}
