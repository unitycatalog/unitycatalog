package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Regression tests for {@code dev/check-migration-steps.sh}, covering both SQL and Java steps. */
class CheckMigrationStepsScriptTest {

  private static final Path SCRIPT = Path.of("dev/check-migration-steps.sh").toAbsolutePath();

  private static final String SQL_STEP = "server/src/main/resources/db/migration/h2/";
  private static final String EFFECTS = "server/src/main/resources/schema-effects/h2/";
  private static final String JAVA_DIR = "server/src/main/java/io/unitycatalog/server/migrate/";

  @TempDir Path repo;

  @Test
  void refusesEditingMergedJavaStep() throws Exception {
    initBaseRepo();
    write(JAVA_DIR + "V18__drop_old_check.java", "// edited\n");
    commit("edit java step");

    Result r = runScript("base", "main");
    assertThat(r.exitCode()).isEqualTo(1);
    assertThat(r.output()).contains("V18__drop_old_check.java");
  }

  @Test
  void refusesDeletingMergedJavaStep() throws Exception {
    initBaseRepo();
    Files.delete(repo.resolve(JAVA_DIR + "V18__drop_old_check.java"));
    commit("delete java step");

    Result r = runScript("base", "main");
    assertThat(r.exitCode()).isEqualTo(1);
    assertThat(r.output()).contains("V18__drop_old_check.java");
  }

  @Test
  void refusesEditingSharedJavaHelper() throws Exception {
    initBaseRepo();
    write(JAVA_DIR + "DropRangeCheckMigration.java", "// edited helper\n");
    commit("edit helper");

    Result r = runScript("base", "main");
    assertThat(r.exitCode()).isEqualTo(1);
    assertThat(r.output()).contains("DropRangeCheckMigration.java");
  }

  @Test
  void refusesJavaStepNumberedBelowExistingSqlStep() throws Exception {
    initBaseRepo();
    write(JAVA_DIR + "V19__out_of_order.java", "class V19__out_of_order {}\n");
    write(EFFECTS + "V19.txt", "- check uc_functions x\n");
    commit("add out-of-order java step below V20 SQL");

    Result r = runScript("base", "main");
    assertThat(r.exitCode()).isEqualTo(1);
    assertThat(r.output()).contains("not above step 20");
  }

  @Test
  void refusesSqlStepNumberedBelowExistingJavaStep() throws Exception {
    initBaseRepoWithJavaHighest();
    write(SQL_STEP + "V19__out_of_order.sql", "SELECT 1;\n");
    write(EFFECTS + "V19.txt", "+ check uc_functions y\n");
    commit("add out-of-order sql step below V21 Java");

    Result r = runScript("base", "main");
    assertThat(r.exitCode()).isEqualTo(1);
    assertThat(r.output()).contains("not above step 21");
  }

  @Test
  void refusesNewJavaStepOnReleaseBranch() throws Exception {
    initBaseRepo();
    write(JAVA_DIR + "V21__new_java.java", "class V21__new_java {}\n");
    write(EFFECTS + "V21.txt", "- check uc_functions z\n");
    commit("add java step on release branch");

    Result r = runScript("base", "branch-0.8");
    assertThat(r.exitCode()).isEqualTo(1);
    assertThat(r.output()).contains("Release branches take no new steps");
  }

  @Test
  void acceptsNewJavaStepAboveAllExistingSteps() throws Exception {
    initBaseRepo();
    write(JAVA_DIR + "V21__new_java.java", "class V21__new_java {}\n");
    write(EFFECTS + "V21.txt", "- check uc_functions z\n");
    commit("add valid java step");

    Result r = runScript("base", "main");
    assertThat(r.exitCode()).isZero();
  }

  private void initBaseRepo() throws Exception {
    git("init", "-b", "main");
    git("config", "user.email", "test@example.com");
    git("config", "user.name", "Test");
    git("config", "commit.gpgSign", "false");
    write(SQL_STEP + "V17__add_check.sql", "SELECT 1;\n");
    write(EFFECTS + "V17.txt", "+ check uc_functions a\n");
    write(JAVA_DIR + "DropRangeCheckMigration.java", "class DropRangeCheckMigration {}\n");
    write(JAVA_DIR + "V18__drop_old_check.java", "class V18__drop_old_check {}\n");
    write(EFFECTS + "V18.txt", "- check uc_functions a\n");
    write(SQL_STEP + "V20__later_sql.sql", "SELECT 1;\n");
    write(EFFECTS + "V20.txt", "+ check uc_functions b\n");
    commit("base");
    git("branch", "base");
  }

  private void initBaseRepoWithJavaHighest() throws Exception {
    initBaseRepo();
    write(JAVA_DIR + "V21__top_java.java", "class V21__top_java {}\n");
    write(EFFECTS + "V21.txt", "- check uc_functions b\n");
    commit("bump base with V21 java");
    git("branch", "-f", "base");
  }

  private void write(String relativePath, String content) throws IOException {
    Path file = repo.resolve(relativePath);
    Files.createDirectories(file.getParent());
    Files.writeString(file, content, StandardCharsets.UTF_8);
  }

  private void commit(String message) throws Exception {
    git("add", "-A");
    git("commit", "--no-verify", "-m", message);
  }

  private void git(String... args) throws Exception {
    List<String> cmd = new ArrayList<>();
    cmd.add("git");
    cmd.addAll(List.of(args));
    Result r = exec(cmd);
    assertThat(r.exitCode()).as(String.join(" ", cmd) + "\n" + r.output()).isZero();
  }

  private Result runScript(String baseRef, String baseBranch) throws Exception {
    return exec(List.of("bash", SCRIPT.toString(), baseRef, baseBranch));
  }

  private Result exec(List<String> command) throws Exception {
    Process process =
        new ProcessBuilder(command).directory(repo.toFile()).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    int exit = process.waitFor();
    return new Result(exit, output);
  }

  private record Result(int exitCode, String output) {}
}
