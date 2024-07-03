import java.nio.file.Files
import java.io.File
import Tarball.createTarballSettings
import sbt.util

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
  .settings(
    name := s"$artifactNamePrefix-client",
    commonSettings,
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


lazy val server = (project in file("server"))
  .dependsOn(client % "test->test")
  .enablePlugins(OpenApiGeneratorPlugin)
  .settings (
    name := s"$artifactNamePrefix-server",
    commonSettings,
    javaCheckstyleSettings(file("dev") / "checkstyle-config.xml"),
    libraryDependencies ++= Seq(
      "com.linecorp.armeria" %  "armeria" % "1.28.4",
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
      "com.amazonaws" % "aws-java-sdk-core" % "1.12.728",
      "org.apache.hadoop" % "hadoop-aws" % "3.4.0",
      "com.google.guava" % "guava" % "31.0.1-jre",
      // Test dependencies
      "junit" %  "junit" % "4.13.2" % Test,
      "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
    ),
  )

lazy val root = (project in file("."))
  .aggregate(client, server, cli)
  .settings(
    name := s"$artifactNamePrefix",
    createTarballSettings()
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

/*
 ********************
 * Release settings *
 ********************
 */
import ReleaseTransformations._

lazy val skipReleaseSettings = Seq(
  publishArtifact := false,
  publish / skip := true
)

// define getMajorMinorPatch function
/**
 * @return tuple of (major, minor, patch) versions extracted from a version string.
 *         e.g. "1.2.3" would return (1, 2, 3)
 */
def getMajorMinorPatch(versionStr: String): (Int, Int, Int) = {
  implicit def extractInt(str: String): Int = {
    """\d+""".r.findFirstIn(str).map(java.lang.Integer.parseInt).getOrElse {
      throw new Exception(s"Could not extract version number from $str in $version")
    }
  }

  versionStr.split("\\.").toList match {
    case majorStr :: minorStr :: patchStr :: _ =>
      (majorStr, minorStr, patchStr)
    case _ => throw new Exception(s"Could not parse version for $version.")
  }
}


// Release settings for artifact that contains only Java source code
lazy val javaOnlyReleaseSettings = releaseSettings ++ Seq(
  // drop off Scala suffix from artifact names
  crossPaths := false,
  // we publish jars for each scalaVersion in crossScalaVersions. however, we only need to publish
  // one java jar. thus, only do so when the current scala version == default scala version
  publishArtifact := {
    val (expMaj, expMin, _) = getMajorMinorPatch(scalaVersion.value)
    s"$expMaj.$expMin" == scalaBinaryVersion.value
  },

  // exclude scala-library from dependencies in generated pom.xml
  autoScalaLibrary := false,
)

lazy val releaseSettings = Seq(
  publishMavenStyle := true,
  publishArtifact := true,
  Test / publishArtifact := false,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  releaseCrossBuild := true,
  pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toArray),

  // TODO: This isn't working yet ...
  sonatypeProfileName := "io.unitycatalog", // sonatype account domain name prefix / group ID
  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    sys.env.getOrElse("SONATYPE_USERNAME", ""),
    sys.env.getOrElse("SONATYPE_PASSWORD", "")
  ),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) {
      Some("snapshots" at nexus + "content/repositories/snapshots")
    } else {
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  },
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
  pomExtra :=
    <url>https://www.unitycatalog.io/</url>
      <scm>
        <url>git@github.com:unitycatalog/unitycatalog.git</url>
        <connection>scm:git:git@github.com:unitycatalog/unitycatalog.git</connection>
      </scm>
      <developers>
        <developer>
          <id>tdas</id>
          <name>Tathagata Das</name>
          <url>https://github.com/tdas</url>
        </developer>
      </developers>
)

// Looks like some of release settings should be set for the root project as well.
publishArtifact := false  // Don't release the root project
publish / skip := true
publishTo := Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
releaseCrossBuild := false  // Don't use sbt-release's cross facility
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
  // Do NOT use `sonatypeBundleRelease` - it will actually release to Maven! We want to do that
  // manually.
  //
  // Do NOT use `sonatypePromote` - it will promote the closed staging repository (i.e. sync to
  //                                Maven central)
  //
  // See https://github.com/xerial/sbt-sonatype#publishing-your-artifact.
  //
  // - sonatypePrepare: Drop the existing staging repositories (if exist) and create a new staging
  //                    repository using sonatypeSessionName as a unique key
  // - sonatypeBundleUpload: Upload your local staging folder contents to a remote Sonatype
  //                         repository
  // - sonatypeClose: closes your staging repository at Sonatype. This step verifies Maven central
  //                  sync requirement, GPG-signature, javadoc and source code presence, pom.xml
  //                  settings, etc
  // TODO: this isn't working yet
  // releaseStepCommand("sonatypePrepare; sonatypeBundleUpload; sonatypeClose"),
  setNextVersion,
  commitNextVersion
)