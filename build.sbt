import scalapb.compiler.Version.scalapbVersion
name := "spark-kafka-stream"

version := "0.1"

scalaVersion := "2.12.8"
val sparkVersion = "2.4.4"

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

libraryDependencies ++= Seq(
  "com.typesafe"          % "config"          % "1.4.0",
  "org.apache.kafka"      %% "kafka"          % "2.3.1",
  "net.liftweb"           %% "lift-json"      % "3.4.0",
  "com.thesamet.scalapb"  %% "scalapb-runtime" % scalapbVersion % "protobuf",
  "org.apache.spark"      %% "spark-core"     % sparkVersion,
  "org.apache.spark"      %% "spark-sql"      % sparkVersion,
  "org.apache.spark"      %% "spark-sql-kafka-0-10"             % sparkVersion      % "provided",
  "org.apache.spark"      %% "spark-streaming"                  % sparkVersion      % "provided",
  "org.apache.spark"      %% "spark-streaming-kafka-0-10"       % sparkVersion,
  "org.apache.flink"      %% "flink-streaming-scala"            % "1.9.1",
  "org.apache.flink"      %% "flink-connector-kafka-0.10"       % "1.9.1",
  "org.apache.flink"      %% "flink-table-api-scala-bridge"     % "1.9.1",
  "org.apache.flink"      %% "flink-table-api-scala"            % "1.9.1",
  "org.apache.flink"      % "flink-table-common"                % "1.9.1"           % "provided",
  "org.apache.flink"      %% "flink-sql-connector-kafka-0.10"   % "1.9.1",
  "org.apache.flink"      %% "flink-table-planner"              % "1.9.1"           % "provided",
  "org.apache.flink"      %% "flink-table-planner-blink"        % "1.9.1"
)




