import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

  val appName         = "couch"
  val appVersion      = "1.0-SNAPSHOT"

  val appDependencies = Seq(
    // Add your project dependencies here,
    jdbc,
    anorm,
    "com.google.code.gson" % "gson" % "2.2.3",
    "couchbase" % "couchbase-client" % "1.1.6"

  )

  val main = play.Project(appName, appVersion, appDependencies).settings(
    resolvers += "couch" at "http://files.couchbase.com/maven2"
  )

}