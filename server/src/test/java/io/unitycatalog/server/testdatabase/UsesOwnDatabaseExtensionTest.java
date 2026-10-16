package io.unitycatalog.server.testdatabase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class UsesOwnDatabaseExtensionTest {

  @UsesOwnDatabase(reason = "tests something on H2")
  static class WithReason {}

  @UsesOwnDatabase(reason = "  ")
  static class BlankReason {}

  @Test
  void reasonIsPrefixedWithTheClassName() {
    assertThat(UsesOwnDatabaseExtension.reasonFor(WithReason.class))
        .isEqualTo("WithReason: tests something on H2");
  }

  @Test
  void blankReasonIsRejected() {
    assertThatThrownBy(() -> UsesOwnDatabaseExtension.reasonFor(BlankReason.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("@UsesOwnDatabase on BlankReason needs a reason");
  }

  @Test
  void parallelExecutionIsRejected() {
    UsesOwnDatabaseExtension.requireSequential(Optional.empty());
    UsesOwnDatabaseExtension.requireSequential(Optional.of("false"));
    assertThatThrownBy(() -> UsesOwnDatabaseExtension.requireSequential(Optional.of("true")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("one test class at a time");
  }

  @Test
  void noMarkOutsideAnnotatedClasses() {
    assertThat(UsesOwnDatabaseExtension.currentOwnDatabase()).isEmpty();
  }
}
