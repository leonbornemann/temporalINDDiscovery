
name := "INDInTemporalData"

version := "0.1"

scalaVersion := "2.13.7"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

// https://mvnrepository.com/artifact/org.json4s/json4s-jackson
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.7.0-M4"
// https://mvnrepository.com/artifact/org.jsoup/jsoup
libraryDependencies += "org.jsoup" % "jsoup" % "1.14.3"

// https://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.10"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"

// https://mvnrepository.com/artifact/com.google.zetasketch/zetasketch
//libraryDependencies += "com.google.zetasketch" % "zetas-ketch" % "0.1.0"


libraryDependencies += "org.scala-lang.modules" %% "scala-collection-contrib" % "0.3.0"


//// https://mvnrepository.com/artifact/de.ruedigermoeller/fst
//libraryDependencies += "de.ruedigermoeller" % "fst" % "3.0.0"

//// https://mvnrepository.com/artifact/de.ruedigermoeller/fst
//libraryDependencies += "de.ruedigermoeller" % "fst" % "3.0.3"
//// https://mvnrepository.com/artifact/de.ruedigermoeller/fst
//libraryDependencies += "de.ruedigermoeller" % "fst" % "2.57"

//// https://mvnrepository.com/artifact/com.esotericsoftware/kryo
//libraryDependencies += "com.esotericsoftware" % "kryo" % "4.0.2"

// https://mvnrepository.com/artifact/com.esotericsoftware/kryo
libraryDependencies += "com.esotericsoftware" % "kryo" % "5.1.1"

//// https://mvnrepository.com/artifact/com.twitter/chill
//libraryDependencies += "com.twitter" %% "chill" % "0.10.0"

