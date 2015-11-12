

name := """btrdb"""

organization := "io.btrdb"

version := "0.1.1"

scalaVersion := "2.10.4"

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

bintrayVcsUrl := Some("git@github.com:immesys/btrdb-scala.git")

licenses += ("GPL-3.0", url("http://opensource.org/licenses/GPL-3.0"))

libraryDependencies += "org.capnproto" % "runtime" % "0.1.0-SNAPSHOT"

libraryDependencies += "io.jvm.uuid" %% "scala-uuid" % "0.2.1"

mainClass in (Compile, run) := Some("io.btrdb.Main")

scalacOptions += "-feature"
scalacOptions += "-deprecation"

scalaSource in Compile <<= baseDirectory(_ / "src" / "scala")
javaSource in Compile <<= baseDirectory(_ / "src" / "java")
