package io.unitycatalog.hadoop.internal.fs;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FilterFileSystem;
import org.junit.jupiter.api.Test;

class ReachabilityFencedFileSystemTest {

  @Test
  void fencesEveryOverridableMethodThatFilterFileSystemForwards() {
    List<String> unfenced =
        Arrays.stream(FilterFileSystem.class.getDeclaredMethods())
            .filter(ReachabilityFencedFileSystemTest::isOverridable)
            .filter(method -> !isDeclaredByFencedFileSystem(method))
            .map(Method::toGenericString)
            .collect(Collectors.toList());

    assertThat(unfenced).isEmpty();
  }

  private static boolean isOverridable(Method method) {
    int modifiers = method.getModifiers();
    return !method.isSynthetic()
        && !Modifier.isStatic(modifiers)
        && !Modifier.isFinal(modifiers)
        && (Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers));
  }

  private static boolean isDeclaredByFencedFileSystem(Method method) {
    try {
      ReachabilityFencedFileSystem.class.getDeclaredMethod(
          method.getName(), method.getParameterTypes());
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }
}
