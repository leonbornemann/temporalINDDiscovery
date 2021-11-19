
name := "INDInTemporalData"

version := "0.1"

scalaVersion := "2.13.7"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

// https://mvnrepository.com/artifact/org.json4s/json4s-jackson
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.7.0-M4"
// https://mvnrepository.com/artifact/org.jsoup/jsoup
libraryDependencies += "org.jsoup" % "jsoup" % "1.14.3"

// https://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"