import Dependencies._

resolvers += Resolver.sonatypeRepo("releases")

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.8")

lazy val inBuild = Seq(
  organization := "elgca",
  scalaVersion := "2.12.6",
  version := "0.1.0-SNAPSHOT"
)

lazy val root = (project in file(".")).
  settings(
    inThisBuild(inBuild),
    name := "avro magnolia",
    libraryDependencies ++= {
      Seq(
        scalaTest % Test,
        "com.propensive" %% "magnolia" % "0.9.1",
        "org.reactivemongo" %% "reactivemongo" % "0.12.6",
        "org.apache.avro" % "avro" % "1.8.2"
      )
    }
  )

//lazy val macroBase = (project in file("macro-bases")).
//  settings(
//    inThisBuild(inBuild),
//    name := "macro bases",
//    libraryDependencies ++= {
//      Seq(
//        scalaTest % Test,
//        "com.propensive" %% "magnolia" % "0.9.1",
//        "com.propensive" %% "mercator" % "0.1.1"
//      )
//    }
//  )