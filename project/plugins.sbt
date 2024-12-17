/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.3")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.3")

addSbtPlugin("net.aichler" % "sbt-jupiter-interface" % "0.11.1")

addSbtPlugin("org.openapitools" % "sbt-openapi-generator" % "7.9.0")

addSbtPlugin("com.github.sbt" % "sbt-license-report" % "1.6.1")

addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.8.0")

addSbtPlugin("com.etsy" % "sbt-checkstyle-plugin" % "3.1.1")
// By default, sbt-checkstyle-plugin uses checkstyle version 6.15, but we should set it to use the
// same version as Spark
dependencyOverrides += "com.puppycrawl.tools" % "checkstyle" % "8.43"

addSbtPlugin("com.github.sbt" % "sbt-jacoco" % "3.4.0")
