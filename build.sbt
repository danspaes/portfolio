name := "nifi"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= {
  val scalaTestV = "2.2.6"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % "2.4.17",
    "com.typesafe.akka" %% "akka-http" % "10.0.3",
    "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.0",
    "io.spray" %%  "spray-json" % "1.3.2"
  )
}
