/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

val circeForScala211Version = "0.11.1" // Only for Scala v2.11
val circeLatestVersion      = "0.12.1" // for Scala v2.12+
val mdedetrichVersion       = "0.4.0"
val scalacticVersion        = "3.0.8"
val scalatestVersion        = "3.0.8"
val typesafeConfigVersion   = "1.3.3"
val akkaHttpVersion         = "10.1.9"
val sealerateVersion        = "0.0.6"
val logbackVersion          = "1.2.3"
val collectionCompatVersion = "2.1.2"

def scalaVersionSpecificDependencies(scalaVer: String): Seq[ModuleID] = {

  val circeScalaSpecificVersion =
    if (scalaVer.startsWith("2.11")) circeForScala211Version
    else circeLatestVersion

  val circeCommonArtifacts = Seq(
    "io.circe" %% "circe-core"    % circeScalaSpecificVersion,
    "io.circe" %% "circe-parser"  % circeScalaSpecificVersion,
    "io.circe" %% "circe-generic" % circeScalaSpecificVersion
  )

  val circeScalaSpecificArtifacts =
    CrossVersion.partialVersion(scalaVer) match {
      case Some((2, 11)) => Seq("io.circe" %% "circe-java8" % circeScalaSpecificVersion)
      case _             => Seq.empty
    }

  circeCommonArtifacts ++ circeScalaSpecificArtifacts
}

def unusedWarnings(scalaVer: String): Seq[String] = {
  val commonWarnings = Seq("-Ywarn-unused")

  val scalacSpecificWarnings =
    CrossVersion.partialVersion(scalaVer) match {
      case Some((2, v)) if v >= 13 => Seq("-Ywarn-unused:imports")
      case _                       => Seq("-Ywarn-unused-import")
    }

  commonWarnings ++ scalacSpecificWarnings
}

lazy val commonSettings: Seq[Setting[_]] = Seq(
  bintrayOrganization := Some("ing-bank"),
  bintrayRepository := "maven-releases",
  bintrayPackage := "scruid",
  organization in ThisBuild := "ing.wbaa.druid",
  homepage in ThisBuild := Some(
    url(s"https://github.com/${bintrayOrganization.value.get}/${name.value}/#readme")
  ),
  licenses in ThisBuild := Seq(
    ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))
  ),
  description in ThisBuild := "Scala library for composing Druid queries",
  bintrayReleaseOnPublish in ThisBuild := false,
  scalafmtOnCompile in ThisBuild := true,
  developers in ThisBuild := List(
    Developer("fokko", "Fokko Driesprong", "@fokkodriesprong", url("https://github.com/fokko")),
    Developer("bjgbeelen", "Bas Beelen", "", url("https://github.com/bjgbeelen")),
    Developer("krisgeus", "Kris Geusebroek", "", url("https://github.com/krisgeus"))
  ),
  scmInfo in ThisBuild := Some(
    ScmInfo(
      url(s"https://github.com/${bintrayOrganization.value.get}/${name.value}"),
      s"git@github.com:${bintrayOrganization.value.get}/${name.value}.git"
    )
  ),
  crossScalaVersions in ThisBuild := Seq("2.11.12", "2.12.10", "2.13.1"),
  scalaVersion in ThisBuild := "2.12.9",
  scalacOptions ++= Seq(Opts.compile.deprecation, "-Xlint", "-feature"),
  scalacOptions ++= unusedWarnings(scalaVersion.value),
  publishArtifact in Test := false,
  Test / parallelExecution := false
) ++ Seq(Compile, Test).flatMap(
  c => scalacOptions in (c, console) --= unusedWarnings(scalaVersion.value)
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "scruid",
    version := "2.3.1-SNAPSHOT",
    resolvers += Resolver.sonatypeRepo("releases"),
    libraryDependencies ++= Seq(
      "com.typesafe"           % "config"                   % typesafeConfigVersion,
      "org.mdedetrich"         %% "akka-stream-json"        % mdedetrichVersion,
      "org.mdedetrich"         %% "akka-http-json"          % mdedetrichVersion,
      "org.mdedetrich"         %% "akka-stream-circe"       % mdedetrichVersion,
      "org.mdedetrich"         %% "akka-http-circe"         % mdedetrichVersion,
      "com.typesafe.akka"      %% "akka-http"               % akkaHttpVersion,
      "ca.mrvisser"            %% "sealerate"               % sealerateVersion,
      "org.scala-lang.modules" %% "scala-collection-compat" % collectionCompatVersion,
      "ch.qos.logback"         % "logback-classic"          % logbackVersion % Provided,
      "org.scalactic"          %% "scalactic"               % scalacticVersion,
      "org.scalatest"          %% "scalatest"               % scalatestVersion % Test
    )
  )
  .settings(libraryDependencies ++= scalaVersionSpecificDependencies(scalaVersion.value))
