package io.unitycatalog.cli.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.FunctionInfo;
import io.unitycatalog.client.model.FunctionParameterInfo;
import io.unitycatalog.client.model.FunctionParameterInfos;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PythonInvokerTest {

  private static final String PYTHON_ENGINE =
      Paths.get(System.getProperty("user.dir"), "etc", "data", "function", "python_engine.py")
          .toString();

  static Stream<Arguments> routineDefinitions() {
    return Stream.of(
        // As stored by the REST API, the Python client and unitycatalog-ai.
        Arguments.of("real newlines", "b = a + 1\nreturn b", "5", "6"),
        // As written in the CLI examples: --def "b = a + 1\nreturn b".
        Arguments.of("backslash-n separators", "b = a + 1\\nreturn b", "5", "6"),
        Arguments.of(
            "backslash-n inside a string literal", "s = \"x\\ny\"\nreturn len(s) + a", "10", "13"),
        Arguments.of("indented backslash-n separators", "  c = a * 2\\n  return c", "5", "10"),
        // As a triple-quoted Python string holds it.
        Arguments.of("indented real newlines", "\n    c = a * 3\n    return c\n", "5", "15"),
        Arguments.of(
            "indented with a comment at column 0",
            "    b = a + 1\\n# explanation\\n    return b",
            "5",
            "6"),
        Arguments.of(
            "backslash-n separators and a trailing newline", "b = a + 1\\nreturn b\n", "5", "6"),
        Arguments.of(
            "indented multi-line string",
            "    s = \"\"\"x\n    y\"\"\"\n    return len(s) + a",
            "0",
            "7"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("routineDefinitions")
  public void testRoutineDefinitionLines(
      String description, String body, String argument, String expected) throws ApiException {
    assertThat(PythonInvoker.invokePython(intFunction(body), PYTHON_ENGINE, argument).strip())
        .isEqualTo(expected);
  }

  @Test
  public void testFunctionErrorFailsTheCall() {
    assertThatThrownBy(
            () ->
                PythonInvoker.invokePython(
                    intFunction("raise ValueError('a must be positive')"), PYTHON_ENGINE, "-1"))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("ValueError: a must be positive");
  }

  private static FunctionInfo intFunction(String body) {
    return new FunctionInfo()
        .name("f")
        .routineDefinition(body)
        .inputParams(
            new FunctionParameterInfos()
                .parameters(
                    List.of(
                        new FunctionParameterInfo()
                            .name("a")
                            .typeName(ColumnTypeName.INT)
                            .position(0))));
  }
}
