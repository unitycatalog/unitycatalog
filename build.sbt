import java.nio.file.Files
import sbt.util


val orgName = "io.unitycatalog"
val artifactNamePrefix = "unitycatalog"

lazy val commonSettings = Seq(
  organization := orgName,
  // Compilation configs
  initialize := {
    // Assert that the JVM is at least Java 11
    val _ = initialize.value  // ensure previous initializations are run
    assert(
      sys.props("java.specification.version").toDouble >= 11,
      "Java 11 or above is required to run this project.")
  },
  javacOptions ++= Seq(
    "-Xlint:deprecation",
    "-Xlint:unchecked",
    "-source", "1.8",
    "-target", "1.8",
    "-g:source,lines,vars"
  ),
  Compile / logLevel := util.Level.Warn,
  resolvers += Resolver.mavenLocal,
  autoScalaLibrary := false,
  crossPaths := false,  // No scala cross building
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  },

  // Test configs
  Test / testOptions  := Seq(Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-q"), Tests.Filter(name => !(name startsWith s"$orgName.server.base"))),
  Test / logLevel := util.Level.Info,
  Test / publishArtifact := false,
  fork := true,
  outputStrategy := Some(StdoutOutput),

  Compile / packageBin := {
    val packageFile = (Compile / packageBin).value
    generateClasspathFile(
      targetDir = packageFile.getParentFile,
      classpath = (Runtime / dependencyClasspath).value)
    packageFile
  }
)

def javaCheckstyleSettings(configLocation: File) = Seq(
  checkstyleConfigLocation := CheckstyleConfigLocation.File(configLocation.toString),
  checkstyleSeverityLevel := Some(CheckstyleSeverityLevel.Error),
  // (Compile / compile) := ((Compile / compile) dependsOn (Compile / checkstyle)).value,
  // (Test / test) := ((Test / test) dependsOn (Test / checkstyle)).value,
)

lazy val client = (project in file("clients/java"))
  .enablePlugins(OpenApiGeneratorPlugin)
  .settings(
    name := s"$artifactNamePrefix-client",
    commonSettings,
    javaCheckstyleSettings(file("dev") / "checkstyle-config.xml"),
    libraryDependencies ++=
      Dependencies.findBugs ++
      Dependencies.guava ++ 
      Dependencies.hadoop ++ 
      Dependencies.jackson(includeAnnotations = true) ++
      Dependencies.Jakarta.annotation ++ 
      Dependencies.junit4.map(_ % Test),
    libraryDependencies ++= Dependencies.junit5.value.map(_ % Test),
    // OpenAPI generation specs
    openApiInputSpec := (file(".") / "api" / "all.yaml").toString,
    openApiGeneratorName := "java",
    openApiOutputDir := (file("clients") / "java").toString,
    openApiApiPackage := s"$orgName.client.api",
    openApiModelPackage := s"$orgName.client.model",
    openApiAdditionalProperties := Map(
      "library" -> "native",
      "hideGenerationTimestamp" -> "true"),
    openApiGenerateApiTests := SettingDisabled,
    openApiGenerateModelTests := SettingDisabled,
    openApiGenerateApiDocumentation := SettingDisabled,
    openApiGenerateModelDocumentation := SettingDisabled,
    // Define the simple generate command to generate full client codes
    generate := {
      val _ = openApiGenerate.value
    }
  )

lazy val apiDocs = (project in file("api"))
  .enablePlugins(OpenApiGeneratorPlugin)
  .settings(
    name := s"$artifactNamePrefix-docs",
    // OpenAPI generation specs
    openApiInputSpec := (file("api") / "all.yaml").toString,
    openApiGeneratorName := "markdown",
    openApiOutputDir := (file("api")).toString,
    // Define the simple generate command to generate markdown docs
    generate := {
      val _ = openApiGenerate.value
    }
  )


lazy val server = (project in file("server"))
  .dependsOn(client % "test->test")
  .enablePlugins(OpenApiGeneratorPlugin)
  .settings (
    name := s"$artifactNamePrefix-server",
    commonSettings,
    javaCheckstyleSettings(file("dev") / "checkstyle-config.xml"),
    libraryDependencies ++=
      Dependencies.armeria ++
      Dependencies.byteBuddy ++
      Dependencies.findBugs ++
      Dependencies.iceberg ++
      Dependencies.jackson(includeAnnotations = true) ++
      Dependencies.junit4.map(_ % Test) ++
      Dependencies.Jakarta.activation ++
      Dependencies.javaxAnnotations ++
      Dependencies.log4j ++
      Dependencies.lombok.map(_ % Provided) ++
      Dependencies.persistence ++
      Dependencies.s3AccessLibraries ++
      Dependencies.vertx
    ,
    Compile / compile / javacOptions ++= Seq(
      "-processor",
      "lombok.launch.AnnotationProcessorHider$AnnotationProcessor"
    ),

    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "io" / "unitycatalog" / "server" / "utils" / "VersionUtils.java"
      IO.write(file,
        s"""package io.unitycatalog.server.utils;
           |
           |public class VersionUtils {
           |  public static String VERSION = "${version.value}";
           |}
           |""".stripMargin)
      Seq(file)
    },

    // OpenAPI generation configs for generating model codes from the spec
    openApiInputSpec := (file(".") / "api" / "all.yaml").toString,
    openApiGeneratorName := "java",
    openApiOutputDir := file("server").toString,
    openApiValidateSpec := SettingEnabled,
    openApiGenerateMetadata := SettingDisabled,
    openApiModelPackage := s"$orgName.server.model",
    openApiAdditionalProperties := Map(
      "library" -> "resteasy",  // resteasy generates the most minimal models
      "hideGenerationTimestamp" -> "true"),
    openApiGlobalProperties := Map("models" -> ""),
    openApiGenerateApiTests := SettingDisabled,
    openApiGenerateModelTests := SettingDisabled,
    openApiGenerateApiDocumentation := SettingDisabled,
    openApiGenerateModelDocumentation := SettingDisabled,

    // Define the simple generate command to generate model codes
    generate := {
      val _ = openApiGenerate.value
    }
  )

lazy val cli = (project in file("examples") / "cli")
  .dependsOn(server % "compile->compile;test->test")
  .dependsOn(client % "compile->compile;test->test")
  .settings(
    name := s"$artifactNamePrefix-cli",
    mainClass := Some(orgName + ".cli.UnityCatalogCli"),
    commonSettings,
    javaCheckstyleSettings(file("dev") / "checkstyle-config.xml"),
    Compile / logLevel := util.Level.Info,
    libraryDependencies ++=
      Dependencies.asciiTable ++
      Dependencies.commonsCLI ++
      Dependencies.delta ++
      Dependencies.jackson(includeAnnotations=false) ++
      Dependencies.junit4.map(_ % Test) ++
      Dependencies.json ++
      Dependencies.log4j ++
      Dependencies.snakeYaml
  )

def generateClasspathFile(targetDir: File, classpath: Classpath): Unit = {
  // Generate a classpath file with the entire runtime class path.
  // This is used by the launcher scripts for launching CLI directly with JAR instead of SBT.
  val classpathFile = targetDir / "classpath"
  Files.write(classpathFile.toPath, classpath.files.mkString(":").getBytes)
  println(s"Generated classpath file '$classpathFile'")
}

val generate = taskKey[Unit]("generate code from APIs")
