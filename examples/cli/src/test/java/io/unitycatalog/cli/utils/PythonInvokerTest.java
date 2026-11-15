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
import org.junit.jupiter.api.Timeout;
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

  static Stream<Arguments> failingRoutineDefinitions() {
    return Stream.of(
        Arguments.of(
            "raised error",
            "raise ValueError('a must be positive')",
            "ValueError: a must be positive"),
        Arguments.of("result that is not JSON", "return {a}", "TypeError:"),
        Arguments.of("syntax error", "return a +", "SyntaxError:"),
        Arguments.of(
            "statement at column 0 after an indented body",
            "    b = a + 1\nreturn b",
            "IndentationError: unexpected indent"),
        Arguments.of(
            "trailing statement at column 0 after an indented body",
            "    b = a + 1\n    return b\nraise ValueError('x')",
            "IndentationError: unexpected indent"),
        Arguments.of("empty body", "", "SyntaxError: the routine definition has no statements"),
        Arguments.of(
            "comment-only body",
            "# nothing to run",
            "SyntaxError: the routine definition has no statements"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("failingRoutineDefinitions")
  public void testFunctionErrorFailsTheCall(String description, String body, String error) {
    assertThatThrownBy(() -> PythonInvoker.invokePython(intFunction(body), PYTHON_ENGINE, "5"))
        .isInstanceOf(ApiException.class)
        .hasMessageStartingWith("Python function failed with exit code 1: ")
        .hasMessageContaining(error);
  }

  @Test
  public void testExitWithoutErrorOutputHasNoDanglingSeparator() {
    assertThatThrownBy(
            () ->
                PythonInvoker.invokePython(
                    intFunction("import sys\nsys.exit(3)"), PYTHON_ENGINE, "5"))
        .isInstanceOf(ApiException.class)
        .hasMessage("Python function failed with exit code 3");
  }

  @Test
  public void testTooFewArgumentsIsNotWrapped() {
    assertThatThrownBy(() -> PythonInvoker.invokePython(intFunction("return a"), PYTHON_ENGINE))
        .isInstanceOf(ApiException.class)
        .hasMessageStartingWith("Not enough parameters provided: 0, expected: 1");
  }

  @Test
  @Timeout(60)
  public void testLargeErrorOutputDoesNotBlockTheCall() throws ApiException {
    String body = "import sys\nsys.stderr.write('x' * 1000000)\nreturn a";
    assertThat(PythonInvoker.invokePython(intFunction(body), PYTHON_ENGINE, "5").strip())
        .isEqualTo("5");
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
