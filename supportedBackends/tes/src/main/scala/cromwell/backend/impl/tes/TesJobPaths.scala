package cromwell.backend.impl.tes

import com.typesafe.config.Config
import cromwell.backend.BackendJobDescriptor
import cromwell.backend.io.{JobPaths, WorkflowPaths}
import cromwell.core.path._

object TesJobPaths {
  def apply(jobDescriptor: BackendJobDescriptor,
  config: Config,
  pathBuilders: List[PathBuilder] = WorkflowPaths.DefaultPathBuilders) = {
    val workflowPaths = TesWorkflowPaths(jobDescriptor.workflowDescriptor, config, pathBuilders)
    new TesJobPaths(workflowPaths, jobDescriptor)
  }
}

case class TesJobPaths private[tes] (override val workflowPaths: TesWorkflowPaths,
                  jobDescriptor: BackendJobDescriptor) extends JobPaths {

  import JobPaths._

  override lazy val callExecutionRoot = {
    callRoot.resolve("execution")
  }
  val callDockerRoot = callPathBuilder(workflowPaths.dockerWorkflowRoot, jobDescriptor.key)
  val callExecutionDockerRoot = callDockerRoot.resolve("execution")
  val callInputsDockerRoot = callDockerRoot.resolve("inputs")
  val callInputsRoot = callRoot.resolve("inputs")

  // Given an output path, return a path localized to the storage file system
  def storageOutput(path: String): String = {
    callExecutionRoot.resolve(path).toString
  }

  def containerInput(path: String): String = {
    cleanContainerInputPath(callInputsDockerRoot, path)
  }

  // Given an output path, return a path localized to the container file system
  def containerOutput(cwd: Path, path: String): String = containerExec(cwd, path)

  // TODO this could be used to create a separate directory for outputs e.g.
  // callDockerRoot.resolve("outputs").resolve(name).toString

  // Given an file name, return a path localized to the container's execution directory
  def containerExec(cwd: Path, path: String): String = {
    cwd.resolve(path).toString
  }

  private def cleanContainerInputPath(inputDir: Path, path: String): String = {
    path match {
      case p if p.startsWith("gs:") =>
        inputDir.resolve(p.replaceFirst("gs:/?/?", "")).pathAsString
      case p if p.startsWith(callExecutionRoot.pathAsString) =>
        val f = DefaultPathBuilder.get(p)
        callExecutionDockerRoot.resolve(f.name).pathAsString
      case p =>
        inputDir.resolve(p).pathAsString
    }
  }
}
