
name := "druid-library"

organization := "ing.wbaa.druid"

organizationName := "ING Wholesale Banking Advanced Analytics"

scalaVersion in ThisBuild := "2.12.2"

version := "0.1-SNAPSHOT"

publishTo := Some(Resolver.file("file", new File(Path.userHome.absolutePath + "/.m2/repository")))

/*
 * Scala Options
 */
scalacOptions ++= Seq(
  /** character encoding of source files */
  "-encoding", "UTF-8",

  /** Enable language features */
  "-Xfuture",

  /** DEBUG INFO: none, source, line, vars, */
  "-g:vars",

  /** Do not adapt matching receiver arg list */
  "-Yno-adapted-args",

  /** WARN for unused imports */
  "-Ywarn-unused-import",

  /** WARN for non-Unit expression results */
  "-Ywarn-value-discard",

  /** WARN upon usages of deprecated APIs */
  "-deprecation",

  /** WARN about explicitly imported features */
  "-feature",

  /** WARN when output depends on assumptions */
  "-unchecked"
)

/*
 * Dependencies
 */
libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.1",

  "org.json4s" %% "json4s-jackson" % "3.5.2",
  "org.json4s" %% "json4s-ext" % "3.5.2",

  "org.scalaj" %% "scalaj-http" % "2.3.0",

  "ch.qos.logback" % "logback-classic" % "1.1.3",

  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

/*
 * Scalastyle
 */
scalastyleFailOnError := true
