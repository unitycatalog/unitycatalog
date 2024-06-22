import net.aichler.jupiter.sbt.Import.JupiterKeys
import sbt.*

object Dependencies {

  val armeria:Seq[ModuleID] = Seq(
    "com.linecorp.armeria" %  "armeria" % Versions.armeria
  )

  val asciiTable:Seq[ModuleID] = Seq(
    "de.vandermeer" % "asciitable" % Versions.asciiTable
  )

  val byteBuddy:Seq[ModuleID] = Seq(
    "net.bytebuddy" % "byte-buddy" % Versions.byteBuddy
  )

  val commonsCLI:Seq[ModuleID] = Seq(
    "commons-cli" % "commons-cli" % Versions.commonsCLI
  )

  val delta: Seq[ModuleID] = Seq(
    "io.delta" % "delta-kernel-api" % Versions.delta,
    "io.delta" % "delta-kernel-defaults" % Versions.delta,
    "io.delta" % "delta-storage" % Versions.delta
  )

  val findBugs: Seq[ModuleID] = Seq(
    "com.google.code.findbugs" % "jsr305" % Versions.findBugs
  )

  val guava: Seq[ModuleID] = Seq(
    "com.google.guava" % "guava" % Versions.guava,
  )

  val hadoop: Seq[ModuleID] = Seq(
    "org.apache.hadoop" % "hadoop-client-api" % Versions.hadoop,
    "org.apache.hadoop" % "hadoop-client-runtime" % Versions.hadoop,
    "org.apache.hadoop" % "hadoop-aws" % Versions.hadoop
  )

  val iceberg: Seq[ModuleID] = Seq(
    "org.apache.iceberg" % "iceberg-core" % Versions.iceberg
  )

  val log4j: Seq[ModuleID] = Seq(
    "org.apache.logging.log4j" % "log4j-api" % Versions.log4j,
    "org.apache.logging.log4j" % "log4j-core" % Versions.log4j,
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % Versions.log4j
  )

  val lombok:Seq[ModuleID] = Seq(
    "org.projectlombok" % "lombok" % Versions.lombok
  )

  def jackson(includeAnnotations:Boolean): Seq[ModuleID] = {
    val common = Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson,
      "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jackson,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % Versions.jackson,
      "org.openapitools" % "jackson-databind-nullable" % Versions.openApiToolsJacksonBindNullableVersion,
    )
    val extra = if(includeAnnotations)
      Seq("com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jackson)
    else
      Seq.empty
    common ++ extra
  }

  object Jakarta {

    val annotation:Seq[ModuleID] = Seq(
      "jakarta.annotation" % "jakarta.annotation-api" % Versions.Jakarta.annotation % Provided
    )

    val activation:Seq[ModuleID] = Seq(
      "jakarta.activation" % "jakarta.activation-api" % Versions.Jakarta.activation
    )
  }

  val javaxAnnotations = Seq(
    "javax.annotation" %  "javax.annotation-api" % Versions.javaxAnnotations
  )

  val json: Seq[ModuleID] = Seq(
    "org.json" % "json" % Versions.json
  )

  val junit4:Seq[ModuleID] = Seq(
    "junit" %  "junit" % Versions.junit,
    "com.github.sbt" % "junit-interface" % Versions.sbtJunitInterface
  )

  val junit5:Def.Initialize[Seq[ModuleID]] = Def.setting(Seq(
    "org.junit.jupiter" % "junit-jupiter" % Versions.junitJupiter,
    "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value,
  ))

  val persistence:Seq[ModuleID] = Seq(
    "com.h2database" %  "h2" % Versions.h2,
    "org.hibernate.orm" % "hibernate-core" % Versions.hibernate
  )


  val s3AccessLibraries: Seq[ModuleID] = Seq(
   "httpcore", "httpclient"
  ).map(
    artifact =>  "org.apache.httpcomponents" % artifact % Versions.apacheHTTPComponents(artifact)
  ) :+ "com.amazonaws" % "aws-java-sdk-s3" % Versions.awsSDK

  val snakeYaml:Seq[ModuleID] = Seq(
    "org.yaml" % "snakeyaml" % Versions.snakeYaml
  )

  val vertx: Seq[ModuleID] = Seq(
    "io.vertx" % "vertx-core" % Versions.vertx,
    "io.vertx" % "vertx-web" % Versions.vertx,
    "io.vertx" % "vertx-web-client" % Versions.vertx,
  )

}
