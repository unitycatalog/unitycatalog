package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class LocalStorageLocationValidatorTest {

  private final ExternalLocationUtils externalLocationUtils = mock(ExternalLocationUtils.class);

  /** Validates {@code location} against the given roots ({@code null}: none configured). */
  private void validate(String roots, String location) {
    Properties properties = new Properties();
    if (roots != null) {
      properties.setProperty(Property.EXTERNAL_LOCAL_ROOTS.getKey(), roots);
    }
    LocalStorageLocationValidator.validateLocalLocation(
        NormalizedURL.from(location), new ServerProperties(properties), externalLocationUtils);
  }

  private void assertDenied(String roots, String location) {
    assertThatThrownBy(() -> validate(roots, location))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(Property.EXTERNAL_LOCAL_ROOTS.getKey())
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.PERMISSION_DENIED);
  }

  @ParameterizedTest
  @ValueSource(strings = {"/data/uc-external", "/data/uc-external/", "file:///data/uc-external/"})
  public void testLocationStrictlyUnderARootIsAllowed(String root) {
    for (String location :
        List.of(
            "file:///data/uc-external/t",
            "file:///data/uc-external/t/",
            "file:///data//uc-external//t",
            "/data/uc-external/a/b")) {
      validate(root, location);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"/data/uc-external", "/data/uc-external/", "file:///data/uc-external/"})
  public void testLocationNotStrictlyUnderARootIsDenied(String root) {
    for (String location :
        List.of(
            // The root itself, however it is spelled.
            "file:///data/uc-external",
            "file:///data/uc-external/",
            "file:///data/uc-external//",
            "/data/uc-external/",
            // A sibling that shares the root's name as a string prefix.
            "file:///data/uc-external-evil/t",
            "file:///data/uc-externalX",
            // Above or outside the root.
            "file:///data",
            "file:///home/uc/.ssh")) {
      assertDenied(root, location);
    }
  }

  @Test
  public void testNoRootsConfiguredDeniesEveryLocalLocation() {
    assertDenied(null, "file:///data/uc-external/t");
  }

  @Test
  public void testLocationUnderAnyConfiguredRootIsAllowed() {
    validate("/data/a,/mnt/b", "/mnt/b/t");
    assertDenied("/data/a,/mnt/b", "/mnt/c/t");
  }

  @Test
  public void testLocationWithAnUnneededEscapeIsRejected() {
    for (String location :
        List.of(
            "file:///data/uc-external/table%41/t",
            "file:///data/uc%2Dexternal/t",
            "file:///data/uc-external/caf%c3%a9",
            "file:///data/uc-external/café",
            "file:///data/uc-external/%5F%5Funitystorage/t")) {
      assertThatThrownBy(() -> validate("/data/uc-external", location))
          .isInstanceOf(BaseException.class)
          .hasMessageContaining("unneeded escapes")
          .extracting(e -> ((BaseException) e).getErrorCode())
          .isEqualTo(ErrorCode.INVALID_ARGUMENT);
    }
    verifyNoInteractions(externalLocationUtils);
  }

  @Test
  public void testLocationWithOnlyNeededEscapesIsAllowed() {
    validate("/data/uc-external", "file:///data/uc-external/my%20table");
    validate("/data/uc-external", "file:///data/uc-external/a%25b");
    validate("/data/uc-external", "file:///data/uc-external/caf%C3%A9");
    // A plain root path is literal: the root is a directory named "a%41".
    validate("/data/a%41", "file:///data/a%2541/t");
  }

  @Test
  public void testLocationUnderAnExternalLocationIsAllowedWithoutRoots() {
    when(externalLocationUtils.listLocalExternalLocationUrls())
        .thenReturn(List.of("file:///data/ext"));
    validate(null, "file:///data/ext");
    validate(null, "file:///data/ext/t");
    assertDenied(null, "file:///data/ext-sibling/t");
  }

  @Test
  public void testLocationUnderAnExternalLocationStoredWithAnUnneededEscapeIsDenied() {
    // Authorization matched no external location for file:///data/uc-external/el-x/t by string,
    // so it did not check the caller's privilege on el%2Dx: denied even though a root covers it.
    when(externalLocationUtils.listLocalExternalLocationUrls())
        .thenReturn(List.of("file:///data/uc-external/el%2Dx"));
    assertThatThrownBy(() -> validate("/data/uc-external", "file:///data/uc-external/el-x/t"))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("file:///data/uc-external/el%2Dx")
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.PERMISSION_DENIED);
    validate("/data/uc-external", "file:///data/uc-external/el-xy/t");

    // Stored before dot segments were rejected: /../data/uc-external/el2 is /data/uc-external/el2.
    when(externalLocationUtils.listLocalExternalLocationUrls())
        .thenReturn(List.of("file:///../data/uc-external/el2"));
    assertThatThrownBy(() -> validate("/data/uc-external", "file:///data/uc-external/el2/t"))
        .isInstanceOf(BaseException.class)
        .extracting(e -> ((BaseException) e).getErrorCode())
        .isEqualTo(ErrorCode.PERMISSION_DENIED);
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3://bucket/t", "gs://bucket/t", "abfss://c@a.dfs.core.windows.net/t"})
  public void testCloudLocationIsNotChecked(String location) {
    validate(null, location);
    verifyNoInteractions(externalLocationUtils);
  }
}
