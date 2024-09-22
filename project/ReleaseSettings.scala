import sbt._
import sbt.Keys._
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleaseStateTransformations._
import xerial.sbt.Sonatype.autoImport._
import com.jsuereth.sbtpgp.SbtPgp.autoImport._

import scala.language.implicitConversions


object ReleaseSettings {

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

  lazy val skipReleaseSettings = Seq(
    publishArtifact := false,
    publish / skip := true
  )


  // Define your release settings
  lazy val javaOnlyReleaseSettings = releaseSettings ++ Seq(
    crossPaths := false,
    publishArtifact := {
      val (expMaj, expMin, _) = getMajorMinorPatch(scalaVersion.value)
      s"$expMaj.$expMin" == scalaBinaryVersion.value
    },
    autoScalaLibrary := false,
  )

  lazy val scalaReleaseSettings = releaseSettings ++ Seq(
    crossPaths := true,
    releaseCrossBuild := true,
  )

  private lazy val releaseSettings = Seq(
    publishMavenStyle := true,
    publishArtifact := true,
    Test / publishArtifact := false,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toArray),
    sonatypeProfileName := "io.unitycatalog",
    credentials += Credentials(
      "Sonatype Nexus Repository Manager",
      "s01.oss.sonatype.org",
      sys.env.getOrElse("SONATYPE_USERNAME", ""),
      sys.env.getOrElse("SONATYPE_PASSWORD", "")
    ),
    publishTo := {
      val nexus = "https://s01.oss.sonatype.org/"
      if (isSnapshot.value) {
        Some("snapshots" at nexus + "content/repositories/snapshots")
      } else {
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
      }
    },
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    pomExtra :=
      <url>https://unitycatalog.io/</url>
        <scm>
          <url>git@github.com:unitycatalog/unitycatalog.git</url>
          <connection>scm:git:git@github.com:unitycatalog/unitycatalog.git</connection>
        </scm>
        <developers>
          <developer>
            <id>rameshchandra</id>
            <name>Ramesh Chandra</name>
            <url>https://github.com/rameshchandra</url>
          </developer>
          <developer>
            <id>tdas</id>
            <name>Tathagata Das</name>
            <url>https://github.com/tdas</url>
          </developer>
          <developer>
            <id>ravivj-db</id>
            <name>Ravi Vijay</name>
            <url>https://github.com/ravivj-db/</url>
          </developer>
          <developer>
            <id>vikrantpuppala</id>
            <name>Vikrant Puppala</name>
            <url>https://github.com/vikrantpuppala</url>
          </developer>
        </developers>
  )

  lazy val rootReleaseSettings = Seq(
    publishArtifact := false,
    publish / skip := true,
    publishTo := Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"),
    releaseCrossBuild := false,
    crossScalaVersions := Nil,
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runTest,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      releaseStepCommandAndRemaining("+publishSigned"),
      setNextVersion,
      commitNextVersion
    )
  )
}