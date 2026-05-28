// ============================================================================
// 实验二 Scala 项目构建定义
// 与课程要求对齐：Spark 3.0.0 + Scala 2.12.10 + JDK 1.8
// 仅依赖 spark-core，严禁 Spark SQL/MLlib/GraphX
// ============================================================================

ThisBuild / organization := "com.njuse"
ThisBuild / version      := "1.0.0"
ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "exp2-task1-scala",

    // 编译目标 1.8
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation"),

    // 依赖 spark-core（provided：集群/spark-submit 时由 Spark 提供，jar 不打入）
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.0.0" % Provided
    )
  )
