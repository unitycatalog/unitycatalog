import java.nio.file.Files
import java.io.File
import Tarball.createTarballSettings
import sbt.util
import sbtlicensereport.license.{LicenseInfo, LicenseCategory, DepModuleInfo}
import ReleaseSettings._

import scala.language.implicitConversions

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
  Compile / compile / javacOptions ++= Seq(
    "-Xlint:deprecation",
    "-Xlint:unchecked",
    "-source", "1.8",
    "-target", "1.8",
    "-g:source,lines,vars"
  ),
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
  },

  licenseOverrides := {
    case DepModuleInfo("io.unitycatalog", _, _) =>
      LicenseInfo(LicenseCategory.Apache, "Apache 2.0", "http://www.apache.org/licenses")
    case DepModuleInfo("org.hibernate.common", "hibernate-commons-annotations", _) =>
      // Apache 2.0: https://mvnrepository.com/artifact/org.hibernate.common/hibernate-commons-annotations
      LicenseInfo(LicenseCategory.Apache, "Apache 2.0", "http://www.apache.org/licenses")
  },
  licenseDepExclusions := {
    // LGPL 2.1: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
    // https://en.wikipedia.org/wiki/GNU_Lesser_General_Public_License
    // We can use and distribute the, but not modify the source code
    case DepModuleInfo("org.hibernate.orm", _, _) => true
    // Duo license:
    //  - Eclipse Public License 2.0
    //  - GNU General Public License, version 2 with the GNU Classpath Exception
    // I think we're good with the classpath exception in there.
    case DepModuleInfo("jakarta.transaction", "jakarta.transaction-api", _) => true
  },
  
  assembly / test := {}
)

enablePlugins(CoursierPlugin)

useCoursier := true

// Configure resolvers
resolvers ++= Seq(
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Maven Central" at "https://repo1.maven.org/maven2/"
)

def javaCheckstyleSettings(configLocation: File) = Seq(
  checkstyleConfigLocation := CheckstyleConfigLocation.File(configLocation.toString),
  checkstyleSeverityLevel := Some(CheckstyleSeverityLevel.Error),
  // (Compile / compile) := ((Compile / compile) dependsOn (Compile / checkstyle)).value,
  // (Test / test) := ((Test / test) dependsOn (Test / checkstyle)).value,
)

lazy val client = (project in file("clients/java"))
  .enablePlugins(OpenApiGeneratorPlugin)
  .disablePlugins(JavaFormatterPlugin)
  .settings(
    name := s"$artifactNamePrefix-client",
    commonSettings,
    javaOnlyReleaseSettings,
    libraryDependencies ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion,
      "org.openapitools" % "jackson-databind-nullable" % openApiToolsJacksonBindNullableVersion,
      "com.google.code.findbugs" % "jsr305" % "3.0.2",
      "jakarta.annotation" % "jakarta.annotation-api" % "3.0.0" % Provided,

      // Test dependencies
      "junit" %  "junit" % "4.13.2" % Test,
      "org.junit.jupiter" % "junit-jupiter" % "5.9.2" % Test,
      "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
      "org.assertj" % "assertj-core" % "3.25.1" % Test,
    ),

    // OpenAPI generation specs
    openApiInputSpec := (file(".") / "api" / "all.yaml").toString,
    openApiGeneratorName := "java",
    openApiOutputDir := (file("clients") / "java").toString,
    openApiApiPackage := s"$orgName.client.api",
    openApiModelPackage := s"$orgName.client.model",
    openApiAdditionalProperties := Map(
      "library" -> "native",
      "useJakartaEe" -> "true",
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

// Define the custom task key
lazy val populateTestDB = taskKey[Unit]("Run PopulateTestDatabase main class from the test folder")

lazy val server = (project in file("server"))
  .dependsOn(client % "test->test")
  .enablePlugins(OpenApiGeneratorPlugin)
  .settings (
    name := s"$artifactNamePrefix-server",
    commonSettings,
    javaOnlyReleaseSettings,
    javaCheckstyleSettings(file("dev") / "checkstyle-config.xml"),
    libraryDependencies ++= Seq(
      "com.linecorp.armeria" %  "armeria" % "1.28.4",
      // Netty dependencies
      "io.netty" % "netty-all" % "4.1.111.Final",
      "jakarta.annotation" % "jakarta.annotation-api" % "3.0.0" % Provided,
      // Jackson dependencies
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion,

      "com.google.code.findbugs" % "jsr305" % "3.0.2",
      "com.h2database" %  "h2" % "2.2.224",
      "org.hibernate.orm" % "hibernate-core" % "6.5.0.Final",
      "org.openapitools" % "jackson-databind-nullable" % openApiToolsJacksonBindNullableVersion,
      // logging
      "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,

      "jakarta.activation" % "jakarta.activation-api" % "2.1.3",
      "net.bytebuddy" % "byte-buddy" % "1.14.15",
      "org.projectlombok" % "lombok" % "1.18.32" % "provided",

      //For s3 access
      "com.amazonaws" % "aws-java-sdk-s3" % "1.12.728",
      "org.apache.httpcomponents" % "httpcore" % "4.4.16",
      "org.apache.httpcomponents" % "httpclient" % "4.5.14",

      // Iceberg REST Catalog dependencies
      "org.apache.iceberg" % "iceberg-core" % "1.5.2",
      "io.vertx" % "vertx-core" % "4.3.5",
      "io.vertx" % "vertx-web" % "4.3.5",
      "io.vertx" % "vertx-web-client" % "4.3.5",

      // Test dependencies
      "junit" %  "junit" % "4.13.2" % Test,
      "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
    ),

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
      "useJakartaEe" -> "true",
      "hideGenerationTimestamp" -> "true"),
    openApiGlobalProperties := Map("models" -> ""),
    openApiGenerateApiTests := SettingDisabled,
    openApiGenerateModelTests := SettingDisabled,
    openApiGenerateApiDocumentation := SettingDisabled,
    openApiGenerateModelDocumentation := SettingDisabled,

    // Define the simple generate command to generate model codes
    generate := {
      val _ = openApiGenerate.value
    },

    populateTestDB := {
      val log = streams.value.log
      (Test / runMain).toTask(s" io.unitycatalog.server.utils.PopulateTestDatabase").value
    },

    Test / javaOptions += s"-Duser.dir=${(ThisBuild / baseDirectory).value.getAbsolutePath}",
)

lazy val cli = (project in file("examples") / "cli")
  .dependsOn(server % "compile->compile;test->test")
  .dependsOn(client % "compile->compile;test->test")
  .settings(
    name := s"$artifactNamePrefix-cli",
    mainClass := Some(orgName + ".cli.UnityCatalogCli"),
    commonSettings,
    skipReleaseSettings,
    javaCheckstyleSettings(file("dev") / "checkstyle-config.xml"),
    libraryDependencies ++= Seq(
      "commons-cli" % "commons-cli" % "1.7.0",
      "org.json" % "json" % "20240303",
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion,
      "org.openapitools" % "jackson-databind-nullable" % openApiToolsJacksonBindNullableVersion,
      "org.yaml" % "snakeyaml" % "2.2",
      // logging
      "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,

      "io.delta" % "delta-kernel-api" % "3.2.0",
      "io.delta" % "delta-kernel-defaults" % "3.2.0",
      "io.delta" % "delta-storage" % "3.2.0",
      "org.apache.hadoop" % "hadoop-client-api" % "3.4.0",
      "org.apache.hadoop" % "hadoop-client-runtime" % "3.4.0",
      "de.vandermeer" % "asciitable" % "0.3.2",
      // for s3 access
      "org.fusesource.jansi" % "jansi" % "2.4.1",
      "com.amazonaws" % "aws-java-sdk-core" % "1.12.728",
      "org.apache.hadoop" % "hadoop-aws" % "3.4.0",
      "com.google.guava" % "guava" % "31.0.1-jre",
      // Test dependencies
      "junit" %  "junit" % "4.13.2" % Test,
      "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
    )
  )

lazy val root = (project in file("."))
  .aggregate(client, server, cli)
  .settings(
    name := s"$artifactNamePrefix",
    createTarballSettings(),
    rootReleaseSettings
  )

def generateClasspathFile(targetDir: File, classpath: Classpath): Unit = {
  // Generate a classpath file with the entire runtime class path.
  // This is used by the launcher scripts for launching CLI directly with JAR instead of SBT.
  val classpathFile = targetDir / "classpath"
  Files.write(classpathFile.toPath, classpath.files.mkString(File.pathSeparator).getBytes)
  println(s"Generated classpath file '$classpathFile'")
}

val generate = taskKey[Unit]("generate code from APIs")

// Library versions
val jacksonVersion = "2.17.0"
val openApiToolsJacksonBindNullableVersion = "0.2.6"
val log4jVersion = "2.23.1"