import java.nio.file.Files
import java.io.File
import Tarball.createTarballSettings
import sbt.{Attributed, util}
import sbt.Keys.*
import sbtlicensereport.license.{DepModuleInfo, LicenseCategory, LicenseInfo}
import ReleaseSettings.*

import scala.language.implicitConversions

val orgName = "io.unitycatalog"
val artifactNamePrefix = "unitycatalog"

// Use Java 11 for two modules: clients and spark
// for better Spark compatibility
// until Spark 4 comes out with newer Java compatibility
lazy val javacRelease11 = Seq("--release", "11")
lazy val javacRelease17 = Seq("--release", "17")

lazy val scala212 = "2.12.15"
lazy val scala213 = "2.13.14"

lazy val deltaVersion = "3.2.1"
lazy val sparkVersion = "3.5.3"

lazy val commonSettings = Seq(
  organization := orgName,
  // Compilation configs
  initialize := {
    // Assert that the JVM is at least Java 17
    val _ = initialize.value  // ensure previous initializations are run
    assert(
      sys.props("java.specification.version").toDouble >= 17,
      "Java 17 or above is required to run this project.")
  },
  Compile / compile / javacOptions ++= Seq(
    "-Xlint:deprecation",
    "-Xlint:unchecked",
    "-g:source,lines,vars",
  ),
  Test / javaOptions ++= Seq (
    "-ea",
  ),
  libraryDependencies ++= Seq(
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion % Test,
    "org.apache.logging.log4j" % "log4j-slf4j2-impl" % log4jVersion,
    "org.apache.logging.log4j" % "log4j-api" % log4jVersion
  ),
  excludeDependencies ++= Seq(
    ExclusionRule("org.slf4j", "slf4j-reload4j")
  ),
  resolvers += Resolver.mavenLocal,
  autoScalaLibrary := false,
  crossPaths := false,  // No scala cross building
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
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
      // Also include the jar being built (packageFile) in the classpath
      // This is specifically required by the server project since the server and control models are provided dependencies
      classpath = (Runtime / dependencyClasspath).value :+ Attributed.blank(packageFile)
    )
    packageFile
  },

  licenseConfigurations := Set("compile"),
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
    case DepModuleInfo("com.unboundid.scim2", _, _) => true
    case DepModuleInfo("com.unboundid.product.scim2", _, _) => true
    case DepModuleInfo("com.googlecode.aviator", _, _) => true
    // Duo license:
    //  - Eclipse Public License 2.0
    //  - GNU General Public License, version 2 with the GNU Classpath Exception
    // I think we're good with the classpath exception in there.
    case DepModuleInfo("jakarta.transaction", "jakarta.transaction-api", _) => true
    case DepModuleInfo("javax.annotation", "javax.annotation-api", _) => true
  },
  
  assembly / test := {}
)

enablePlugins(CoursierPlugin)

useCoursier := true

// Configure resolvers
resolvers ++= Seq(
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Maven Central" at "https://repo1.maven.org/maven2/",
)

def javaCheckstyleSettings(configLocation: File) = Seq(
  checkstyleConfigLocation := CheckstyleConfigLocation.File(configLocation.toString),
  checkstyleSeverityLevel := Some(CheckstyleSeverityLevel.Error),
  // (Compile / compile) := ((Compile / compile) dependsOn (Compile / checkstyle)).value,
  // (Test / test) := ((Test / test) dependsOn (Test / checkstyle)).value,
)

// enforce java code style
def javafmtCheckSettings() = Seq(
  (Compile / compile) := ((Compile / compile) dependsOn (Compile / javafmtAll)).value
)

lazy val controlApi = (project in file("target/control/java"))
  .enablePlugins(OpenApiGeneratorPlugin)
  .disablePlugins(JavaFormatterPlugin)
  .settings(
    name := s"$artifactNamePrefix-controlapi",
    commonSettings,
    skipReleaseSettings,
    libraryDependencies ++= Seq(
      "jakarta.annotation" % "jakarta.annotation-api" % jakartaAnnotationVersion % Provided,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion,
    ),
    (Compile / compile) := ((Compile / compile) dependsOn generate).value,

    // OpenAPI generation specs
    openApiInputSpec := (file(".") / "api" / "control.yaml").toString,
    openApiGeneratorName := "java",
    openApiOutputDir := (file("target") / "control" / "java").toString,
    openApiApiPackage := s"$orgName.control.api",
    openApiModelPackage := s"$orgName.control.model",
    openApiAdditionalProperties := Map(
      "library" -> "native",
      "useJakartaEe" -> "true",
      "hideGenerationTimestamp" -> "true",
      "openApiNullable" -> "false"),
    openApiGenerateApiTests := SettingDisabled,
    openApiGenerateModelTests := SettingDisabled,
    openApiGenerateApiDocumentation := SettingDisabled,
    openApiGenerateModelDocumentation := SettingDisabled,
    // Define the simple generate command to generate full client codes
    generate := {
      val _ = openApiGenerate.value

      // Delete the generated build.sbt file so that it is not used for our sbt config
      val buildSbtFile = file(openApiOutputDir.value) / "build.sbt"
      if (buildSbtFile.exists()) {
        buildSbtFile.delete()
      }
    }
  )

lazy val client = (project in file("target/clients/java"))
  .enablePlugins(OpenApiGeneratorPlugin)
  .disablePlugins(JavaFormatterPlugin)
  .settings(
    name := s"$artifactNamePrefix-client",
    commonSettings,
    javaOnlyReleaseSettings,
    Compile / compile / javacOptions ++= javacRelease11,
    libraryDependencies ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion,
      "org.openapitools" % "jackson-databind-nullable" % openApiToolsJacksonBindNullableVersion,
      "com.google.code.findbugs" % "jsr305" % findbugsVersion,
      "jakarta.annotation" % "jakarta.annotation-api" % jakartaAnnotationVersion % Provided,

      // Test dependencies
      "org.junit.jupiter" % "junit-jupiter" % junitVersion % Test,
      "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
      "org.assertj" % "assertj-core" % assertJVersion % Test,
    ),
    (Compile / compile) := ((Compile / compile) dependsOn generate).value,

    // OpenAPI generation specs
    openApiInputSpec := (file(".") / "api" / "all.yaml").toString,
    openApiGeneratorName := "java",
    openApiOutputDir := (file("target") / "clients" / "java").toString,
    openApiApiPackage := s"$orgName.client.api",
    openApiModelPackage := s"$orgName.client.model",
    openApiAdditionalProperties := Map(
      "library" -> "native",
      "useJakartaEe" -> "true",
      "hideGenerationTimestamp" -> "true",
      "openApiNullable" -> "false"),
    openApiGenerateApiTests := SettingDisabled,
    openApiGenerateModelTests := SettingDisabled,
    openApiGenerateApiDocumentation := SettingDisabled,
    openApiGenerateModelDocumentation := SettingDisabled,
    // Define the simple generate command to generate full client codes
    generate := {
      val _ = openApiGenerate.value

      // Delete the generated build.sbt file so that it is not used for our sbt config
      val buildSbtFile = file(openApiOutputDir.value) / "build.sbt"
      if (buildSbtFile.exists()) {
        buildSbtFile.delete()
      }
    },
    // Add VersionInfo in the same way like in server
    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "io" / "unitycatalog" / "cli" / "utils" / "VersionUtils.java"
      IO.write(file,
        s"""package io.unitycatalog.cli.utils;
          |
          |public class VersionUtils {
          |  public static String VERSION = "${version.value}";
          |}
          |""".stripMargin)
      Seq(file)
    }
  )

lazy val prepareGeneration = taskKey[Unit]("Prepare the environment for OpenAPI code generation")

lazy val pythonClient = (project in file("clients/python"))
  .enablePlugins(OpenApiGeneratorPlugin)
  .settings(
    name := s"$artifactNamePrefix-python-client",
    commonSettings,
    skipReleaseSettings,
    Compile / compile := (Compile / compile).dependsOn(generate).value,
    openApiInputSpec := (baseDirectory.value.getParentFile.getParentFile / "api" / "all.yaml").getAbsolutePath,
    openApiGeneratorName := "python",
    openApiOutputDir := (baseDirectory.value / "target").getAbsolutePath,
    openApiPackageName := s"$artifactNamePrefix.client",
    openApiAdditionalProperties := Map(
      "packageVersion" -> s"${version.value.replace("-SNAPSHOT", ".dev0")}",
      "library"        -> "asyncio"
    ),
    openApiGenerateApiTests := SettingDisabled,
    openApiGenerateModelTests := SettingDisabled,
    openApiGenerateApiDocumentation := SettingDisabled,
    openApiGenerateModelDocumentation := SettingDisabled,

    prepareGeneration := PythonClientPostBuild.prepareGeneration(streams.value.log, baseDirectory.value, openApiOutputDir.value),

    generate := Def.sequential(
      prepareGeneration,
      openApiGenerate,
      Def.task {
        val log = streams.value.log

        PythonClientPostBuild.processGeneratedFiles(
          log,
          openApiOutputDir.value,
          baseDirectory.value,
        )
        log.info("OpenAPI Python client generation completed.")
      }
    ).value
  )

lazy val apiDocs = (project in file("api"))
  .enablePlugins(OpenApiGeneratorPlugin)
  .settings(
    name := s"$artifactNamePrefix-docs",
    skipReleaseSettings,
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
  // Server and control models are added as provided to avoid them being added as maven dependencies
  // This is because the server and control models are included in the server jar
  .dependsOn(serverModels % "provided", controlModels % "provided")
  .settings (
    name := s"$artifactNamePrefix-server",
    mainClass := Some(orgName + ".server.UnityCatalogServer"),
    commonSettings,
    javaOnlyReleaseSettings,
    javafmtCheckSettings,
    javaCheckstyleSettings(file("dev") / "checkstyle-config.xml"),
    Compile / compile / javacOptions ++= Seq(
      "-processor",
      "lombok.launch.AnnotationProcessorHider$AnnotationProcessor"
    ) ++ javacRelease17,
    libraryDependencies ++= Seq(
      "com.linecorp.armeria" %  "armeria" % armeriaVersion,
      // Netty dependencies
      "io.netty" % "netty-all" % nettyVersion,
      "jakarta.annotation" % "jakarta.annotation-api" % jakartaAnnotationVersion % Provided,
      // Jackson dependencies
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion,

      "com.google.code.findbugs" % "jsr305" % findbugsVersion,
      "com.h2database" %  "h2" % h2Version,

      "org.hibernate.orm" % "hibernate-core" % hibernateVersion,

      "jakarta.activation" % "jakarta.activation-api" % jakartaVersion,
      "net.bytebuddy" % "byte-buddy" % bytebuddyVersion,
      "org.projectlombok" % "lombok" % lombokVersion % Provided,

      // For ALDS access
      "com.azure" % "azure-identity" % azureIdentityVersion,
      "com.azure" % "azure-storage-file-datalake" % azureStorageFileDatalakeVersion,

      // For GCS Access
      "com.google.cloud" % "google-cloud-storage" % googleCloudStorage,
      "com.google.auth" % "google-auth-library-oauth2-http" % googleAuthLibraryOauth2HttpVersion,

      //For s3 access
      "com.amazonaws" % "aws-java-sdk-s3" % awsJavaSdkVersion,
      "software.amazon.awssdk" % "sso" % awsSdkSsoVersion,
      "software.amazon.awssdk" % "ssooidc" % awsSdkSsoVersion,

      "org.apache.httpcomponents" % "httpcore" % httpcoreVersion,
      "org.apache.httpcomponents" % "httpclient" % httpclientVersion,

      // Iceberg REST Catalog dependencies
      "org.apache.iceberg" % "iceberg-core" % icebergVersion,
      "org.apache.iceberg" % "iceberg-aws" % icebergVersion,
      "org.apache.iceberg" % "iceberg-azure" % icebergVersion,
      "org.apache.iceberg" % "iceberg-gcp" % icebergVersion,
      "software.amazon.awssdk" % "s3" % awssdkS3Version,
      "software.amazon.awssdk" % "sts" % awssdkStsVersion,
      "io.vertx" % "vertx-core" % vertxVersion,
      "io.vertx" % "vertx-web" % vertxVersion,
      "io.vertx" % "vertx-web-client" % vertxVersion,

      // Auth dependencies
      "com.unboundid.product.scim2" % "scim2-sdk-common" % scim2SdkCommonVersion,
      "org.casbin" % "jcasbin" % jcasbinVersion,
      "org.casbin" % "jdbc-adapter" % jdbcAdapterVersion
        exclude("com.microsoft.sqlserver", "mssql-jdbc")
        exclude("com.oracle.database.jdbc", "ojdbc6"),
      "org.springframework" % "spring-expression" % springExpressionVersion,
      "com.auth0" % "java-jwt" % javaJwtVersion,
      "com.auth0" % "jwks-rsa" % jwksRsaVersion,

      // Test dependencies
      "org.junit.jupiter" %  "junit-jupiter" % junitVersion % Test,
      "org.mockito" % "mockito-core" % mockitoCoreVersion % Test,
      "org.mockito" % "mockito-inline" % mockitoInlineVersion % Test,
      "org.mockito" % "mockito-junit-jupiter" % mockitoJunitJupiterVersion % Test,
      "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
      "com.adobe.testing" % "s3mock-junit5" % s3mockJunit5Version % Test
        exclude("ch.qos.logback", "logback-classic")
        exclude("org.apache.logging.log4j", "log4j-to-slf4j"),
      "javax.xml.bind" % "jaxb-api" % jaxbApiVersion % Test,

      // CLI dependencies
      "commons-cli" % "commons-cli" % commonsCliVersion
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
    populateTestDB := {
      val log = streams.value.log
      (Test / runMain).toTask(s" io.unitycatalog.server.utils.PopulateTestDatabase").value
    },
    Test / javaOptions += s"-Duser.dir=${(ThisBuild / baseDirectory).value.getAbsolutePath}",
    // Include server and control models in the bin package for server
    // This will allow us to have a single maven artifact and not 3 (server, server models, control models)
    Compile / packageBin / mappings ++= (Compile / packageBin / mappings).value ++
      (serverModels / Compile / packageBin / mappings).value ++
      (controlModels / Compile / packageBin / mappings).value
  )

lazy val serverModels = (project in file("server") / "target" / "models")
  .enablePlugins(OpenApiGeneratorPlugin)
  .disablePlugins(JavaFormatterPlugin)
  .settings(
    name := s"$artifactNamePrefix-servermodels",
    commonSettings,
    skipReleaseSettings,
    (Compile / compile) := ((Compile / compile) dependsOn generate).value,
    Compile / compile / javacOptions ++= javacRelease17,
    libraryDependencies ++= Seq(
      "jakarta.annotation" % "jakarta.annotation-api" % jakartaAnnotationVersion % Provided,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
    ),
    // OpenAPI generation configs for generating model codes from the spec
    openApiInputSpec := (file(".") / "api" / "all.yaml").toString,
    openApiGeneratorName := "java",
    openApiOutputDir := (file("server") / "target" / "models").toString,
    openApiValidateSpec := SettingEnabled,
    openApiGenerateMetadata := SettingDisabled,
    openApiModelPackage := s"$orgName.server.model",
    openApiAdditionalProperties := Map(
      "library" -> "resteasy", // resteasy generates the most minimal models
      "useJakartaEe" -> "true",
      "hideGenerationTimestamp" -> "true"
    ),
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

lazy val controlModels = (project in file("server") / "target" / "controlmodels")
  .enablePlugins(OpenApiGeneratorPlugin)
  .disablePlugins(JavaFormatterPlugin)
  .settings(
    name := s"$artifactNamePrefix-controlmodels",
    commonSettings,
    skipReleaseSettings,
    (Compile / compile) := ((Compile / compile) dependsOn generate).value,
    Compile / compile / javacOptions ++= javacRelease17,
    libraryDependencies ++= Seq(
      "jakarta.annotation" % "jakarta.annotation-api" % jakartaAnnotationVersion % Provided,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
    ),
    // OpenAPI generation configs for generating model codes from the spec
    openApiInputSpec := (file(".") / "api" / "control.yaml").toString,
    openApiGeneratorName := "java",
    openApiOutputDir := (file("server") / "target" / "controlmodels").toString,
    openApiValidateSpec := SettingEnabled,
    openApiGenerateMetadata := SettingDisabled,
    openApiModelPackage := s"$orgName.control.model",
    openApiAdditionalProperties := Map(
      "library" -> "resteasy", // resteasy generates the most minimal models
      "useJakartaEe" -> "true",
      "hideGenerationTimestamp" -> "true"
    ),
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
  .dependsOn(server % "test->test")
  .dependsOn(serverModels, controlModels)
  .dependsOn(client % "compile->compile;test->test")
  .dependsOn(controlApi % "compile->compile")
  .settings(
    name := s"$artifactNamePrefix-cli",
    mainClass := Some(orgName + ".cli.UnityCatalogCli"),
    commonSettings,
    skipReleaseSettings,
    javafmtCheckSettings,
    javaCheckstyleSettings(file("dev") / "checkstyle-config.xml"),
    Compile / compile / javacOptions ++= javacRelease17,
    libraryDependencies ++= Seq(
      "commons-cli" % "commons-cli" % commonsCliVersion,
      "org.json" % "json" % jsonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion,
      "org.openapitools" % "jackson-databind-nullable" % openApiToolsJacksonBindNullableVersion,
      "org.yaml" % "snakeyaml" % snakeyamlVersion,
      "io.delta" % "delta-kernel-api" % deltaVersion,
      "io.delta" % "delta-kernel-defaults" % deltaVersion,
      "io.delta" % "delta-storage" % deltaVersion,
      "org.apache.hadoop" % "hadoop-client-api" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion,
      "de.vandermeer" % "asciitable" % asciitableVersion,
      // for s3 access
      "org.fusesource.jansi" % "jansi" % jansiVersion,
      "com.amazonaws" % "aws-java-sdk-core" % awsJavaSdkVersion,
      "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-azure" % hadoopVersion,
      "com.google.guava" % "guava" % guavaVersion,
      // Test dependencies
      "org.junit.jupiter" % "junit-jupiter" % junitVersion % Test,
      "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
    ),
    Test / javaOptions += s"-Duser.dir=${(ThisBuild / baseDirectory).value.getAbsolutePath}",
  )

/*
  * This project is a combination of the server and client projects, shaded into a single JAR.
  * It also includes the test classes from the server project.
  * It is used for the Spark connector project(the client is required as a compile dependency,
  * and the server(with tests) is required as a test dependency)
  * This was necessary because Spark 3.5 has a dependency on Jackson 2.15, which conflicts with the Jackson 2.17
 */
lazy val serverShaded = (project in file("server-shaded"))
  .dependsOn(server % "compile->compile, test->compile")
  .settings(
    name := s"$artifactNamePrefix-server-shaded",
    commonSettings,
    skipReleaseSettings,
    Compile / packageBin := assembly.value,
    assembly / mainClass := Some("io.unitycatalog.server.UnityCatalogServer"),
    assembly / logLevel := Level.Warn,
    assembly / test := {},
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("com.fasterxml.**" -> "shaded.@0").inAll,
      ShadeRule.rename("org.antlr.**" -> "shaded.@0").inAll,
    ),
    assemblyPackageScala / assembleArtifact := false,
    assembly / fullClasspath := {
      val compileClasspath = (server / Compile / fullClasspath).value
      val testClasses = (server / Test / products).value
      compileClasspath ++ testClasses.map(Attributed.blank)
    }
  )

lazy val spark = (project in file("connectors/spark"))
  .dependsOn(client)
  .settings(
    name := s"$artifactNamePrefix-spark",
    scalaVersion := scala212,
    crossScalaVersions := Seq(scala212, scala213),
    commonSettings,
    scalaReleaseSettings,
    javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    ),
    javaCheckstyleSettings(file("dev/checkstyle-config.xml")),
    Compile / compile / javacOptions ++= javacRelease11,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % jacksonVersion,
      "org.antlr" % "antlr4-runtime" % antlr4Version,
      "org.antlr" % "antlr4" % antlr4Version,
      "com.google.cloud.bigdataoss" % "util-hadoop" % utilHadoopVersion % Provided,
      "org.apache.hadoop" % "hadoop-azure" % hadoopVersion % Provided,
    ),
    libraryDependencies ++= Seq(
      // Test dependencies
      "org.junit.jupiter" % "junit-jupiter" % junitVersion % Test,
      "org.assertj" % "assertj-core" % assertJVersion % Test,
      "org.mockito" % "mockito-core" % mockitoCoreVersion % Test,
      "org.mockito" % "mockito-inline" % mockitoInlineVersion % Test,
      "org.mockito" % "mockito-junit-jupiter" % mockitoJunitJupiterVersion % Test,
      "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
      "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion,
      "io.delta" %% "delta-spark" % deltaVersion % Test,
    ),
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % jacksonVersion,
      "org.antlr" % "antlr4-runtime" % antlr4Version,
      "org.antlr" % "antlr4" % antlr4Version,
    ),
    Test / unmanagedJars += (serverShaded / assembly).value,
    licenseDepExclusions := {
      case DepModuleInfo("org.hibernate.orm", _, _) => true
      case DepModuleInfo("jakarta.annotation", "jakarta.annotation-api", _) => true
      case DepModuleInfo("jakarta.servlet", "jakarta.servlet-api", _) => true
      case DepModuleInfo("jakarta.transaction", "jakarta.transaction-api", _) => true
      case DepModuleInfo("jakarta.ws.rs", "jakarta.ws.rs-api", _) => true
      case DepModuleInfo("javax.activation", "activation", _) => true
      case DepModuleInfo("javax.servlet", "javax.servlet-api", _) => true
      case DepModuleInfo("org.glassfish.hk2", "hk2-api", _) => true
      case DepModuleInfo("org.glassfish.hk2", "hk2-locator", _) => true
      case DepModuleInfo("org.glassfish.hk2", "hk2-utils", _) => true
      case DepModuleInfo("org.glassfish.hk2", "osgi-resource-locator", _) => true
      case DepModuleInfo("org.glassfish.hk2.external", "aopalliance-repackaged", _) => true
      case DepModuleInfo("ch.qos.logback", "logback-classic", _) => true
      case DepModuleInfo("ch.qos.logback", "logback-core", _) => true
      case DepModuleInfo("org.apache.xbean", "xbean-asm9-shaded", _) => true
      case DepModuleInfo("oro", "oro", _) => true
      case DepModuleInfo("org.glassfish", "javax.json", _) => true
      case DepModuleInfo("org.glassfish.hk2.external", "jakarta.inject", _) => true
      case DepModuleInfo("org.antlr", "ST4", _) => true
    }
  )

lazy val integrationTests = (project in file("integration-tests"))
  .settings(
    name := s"$artifactNamePrefix-integration-tests",
    commonSettings,
    javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
    ),
    skipReleaseSettings,
    libraryDependencies ++= Seq(
      "org.junit.jupiter" % "junit-jupiter" % junitVersion % Test,
      "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
      "org.assertj" % "assertj-core" % assertJVersion % Test,
      "org.projectlombok" % "lombok" % lombokVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
      "io.delta" %% "delta-spark" % deltaVersion % Test,
      "org.apache.hadoop" % "hadoop-aws" % hadoopVersion % Test,
      "org.apache.hadoop" % "hadoop-azure" % hadoopVersion % Test,
      "com.google.cloud.bigdataoss" % "gcs-connector" % gcsConnectorVersion % Test classifier "shaded",
      "io.unitycatalog" %% "unitycatalog-spark" % unitycatalogSparkVersion % Test,
    ),
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % jacksonVersion,
      "org.antlr" % "antlr4-runtime" % antlr4Version,
      "org.antlr" % "antlr4" % antlr4Version,
      "org.apache.hadoop" % "hadoop-client-api" % hadoopVersion,
    ),
    Test / javaOptions += s"-Duser.dir=${((ThisBuild / baseDirectory).value / "integration-tests").getAbsolutePath}",
  )

lazy val root = (project in file("."))
  .aggregate(serverModels, client, pythonClient, server, cli, spark, controlApi, controlModels, apiDocs)
  .settings(
    name := s"$artifactNamePrefix",
    createTarballSettings(),
    commonSettings,
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
val antlr4Version = "4.9.3"
val armeriaVersion = "1.28.4"
val asciitableVersion = "0.3.2"
val assertJVersion = "3.26.3"
val awsJavaSdkVersion = "1.12.728"
val awsSdkSsoVersion = "2.27.12"
val awssdkS3Version = "2.24.0"
val awssdkStsVersion = "2.24.0"
val azureIdentityVersion = "1.13.2"
val azureStorageFileDatalakeVersion = "12.20.0"
val bytebuddyVersion = "1.14.15"
val commonsCliVersion = "1.7.0"
val findbugsVersion = "3.0.2"
val gcsConnectorVersion = "3.0.2"
val googleAuthLibraryOauth2HttpVersion = "1.20.0"
val googleCloudStorage = "2.30.1"
val guavaVersion = "31.0.1-jre"
val h2Version = "2.2.224"
val hibernateVersion = "6.5.0.Final"
val hadoopVersion = "3.4.0"
val httpclientVersion = "4.5.14"
val httpcoreVersion = "4.4.16"
val icebergVersion = "1.5.2"
val jacksonVersion = "2.17.0"
val jakartaVersion = "2.1.3"
val jakartaAnnotationVersion = "3.0.0"
val jansiVersion = "2.4.1"
val javaJwtVersion = "4.4.0"
val jaxbApiVersion = "2.3.1"
val jcasbinVersion = "1.55.0"
val jdbcAdapterVersion = "2.7.0"
val jsonVersion = "20240303"
val junitVersion = "5.10.3"
val jwksRsaVersion = "0.22.1"
val log4jVersion = "2.23.1"
val lombokVersion = "1.18.32"
val mockitoCoreVersion = "5.11.0"
val mockitoInlineVersion = "5.2.0"
val mockitoJunitJupiterVersion = "5.12.0"
val nettyVersion = "4.1.111.Final"
val openApiToolsJacksonBindNullableVersion = "0.2.6"
val s3mockJunit5Version = "3.9.1"
val scim2SdkCommonVersion = "3.1.0"
val slf4jVersion = "2.0.13"
val snakeyamlVersion = "2.2"
val springExpressionVersion = "6.1.11"
val unitycatalogSparkVersion = "0.2.0"
val utilHadoopVersion = "3.0.2"
val vertxVersion = "4.3.5"
