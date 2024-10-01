package io.unitycatalog.cli.access;

import java.util.Arrays;
import java.util.List;

class Step {
  enum Expect {
    SUCCEED,
    FAIL
  }

  Expect expectResult;
  int itemCount;

  public Expect getExpectedResult() {
    return expectResult;
  }

  public int getItemCount() {
    return itemCount;
  }

  static class CommandStep extends Step {

    List<String> args;

    public List<String> getArgs() {
      return args;
    }

    static CommandStep of(Expect expect, String... args) {
      CommandStep step = new CommandStep();
      step.expectResult = expect;
      step.itemCount = -1;
      step.args = Arrays.asList(args);
      return step;
    }

    static CommandStep of(Expect expect, int count, String... args) {
      CommandStep step = new CommandStep();
      step.expectResult = expect;
      step.itemCount = count;
      step.args = Arrays.asList(args);
      return step;
    }
  }

  static class TokenStep extends Step {
    String email;

    public String getEmail() {
      return email;
    }

    public static TokenStep of(Expect expect, String email) {
      TokenStep step = new TokenStep();
      step.expectResult = expect;
      step.email = email;
      return step;
    }
  }
}
