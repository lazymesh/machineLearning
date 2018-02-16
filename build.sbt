name := "machineLearning"

version := "1.0"

scalaVersion := "2.11.8"

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.1.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.1.0",
  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.8.0" % "test",
  "org.scalatest" % "scalatest_2.11" % "2.1.0" % "test",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.8.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.8.0" classifier "models"

)
    
