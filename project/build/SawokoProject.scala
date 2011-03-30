import sbt._
import java.io.File

trait PublishLocalMavenProject extends BasicManagedProject
                                  with MavenStyleScalaPaths {
  override def managedStyle = ManagedStyle.Maven
  val localMavenRepo = Resolver.file("Local Maven Repository",
                                     new File(Resolver.userMavenRoot))

  protected def publishLocalMavenConfiguration =
    new DefaultPublishConfiguration(localMavenRepo, "release", false)
  protected def publishLocalMavenAction =
    publishTask(publishIvyModule, publishLocalMavenConfiguration).
      dependsOn(deliverLocal, makePom)
  lazy val publishLocalMaven: Task = publishLocalMavenAction
}

trait CommonProject extends BasicScalaProject with PublishLocalMavenProject {
  override def mainScalaSourcePath: Path = "src"
  override def testScalaSourcePath: Path = "tests"
  override def compileOptions = super.compileOptions ++
                                Seq(Deprecation, Unchecked)
}

class SawokoProject(info: ProjectInfo) extends DefaultProject(info)
                                          with posterous.Publish
                                          with CommonProject { sawoko =>
  trait SubProject extends BasicScalaProject with CommonProject {
    override def dependencies = super.dependencies ++ Seq(sawoko)
  }
  class ExamplesProject(info: ProjectInfo) extends ParentProject(info) {
    trait ExampleProject extends BasicScalaProject with SubProject
    class PongHttpServerProject(info: ProjectInfo)
          extends DefaultProject(info)
             with ExampleProject {
      val argotDep = "org.clapper" %% "argot" % "0.2"
    }

    lazy val pongHttpServer =
      project("pong-http-server", "sawoko-examples-pong-http-server",
              new PongHttpServerProject(_))
  }

  lazy val examples = project("examples", "sawoko-examples",
                              new ExamplesProject(_))

  override def dependencies = info.dependencies ++ Nil

  val scalaToolsSnapshotsRepo =
    "Scala-Tools Maven2 Snapshots Repository" at
    "http://nexus.scala-tools.org/content/repositories/snapshots"

  val specs = "org.specs2" %% "specs2" % "[1.0.1,2)" % "test"
  override def testFrameworks =
    super.testFrameworks ++
    Seq(new TestFramework("org.specs2.runner.SpecsFramework"))
}
