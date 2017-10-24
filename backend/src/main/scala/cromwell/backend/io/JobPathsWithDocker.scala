package cromwell.backend.io

import com.typesafe.config.Config
import cromwell.backend.BackendJobDescriptor
import cromwell.core.path.{Path, PathBuilder}

object JobPathsWithDocker {
  def apply(jobDescriptor: BackendJobDescriptor,
            config: Config,
            pathBuilders: List[PathBuilder] = WorkflowPaths.DefaultPathBuilders) = {
    val workflowPaths = new WorkflowPathsWithDocker(jobDescriptor.workflowDescriptor, config, pathBuilders)
    new JobPathsWithDocker(workflowPaths, jobDescriptor)
  }
}

case class JobPathsWithDocker private[io] (override val workflowPaths: WorkflowPathsWithDocker, override val jobDescriptor: BackendJobDescriptor) extends JobPaths {
  import JobPaths._

  override lazy val callExecutionRoot = { callRoot.resolve("execution") }
  val callDockerRoot = callPathBuilder(workflowPaths.dockerWorkflowRoot, jobDescriptor.key)
  val callExecutionDockerRoot = callDockerRoot.resolve("execution")
  val callInputsRoot = callRoot.resolve("inputs")

  def toDockerPath(path: Path): Path = {
    path.toAbsolutePath match {
      case p if p.startsWith(WorkflowPathsWithDocker.DockerRoot) => p
      case p =>
        /* For example:
          *
          * p = /abs/path/to/cromwell-executions/three-step/f00ba4/call-ps/stdout.txt
          * localExecutionRoot = /abs/path/to/cromwell-executions
          * subpath = three-step/f00ba4/call-ps/stdout.txt
          *
          * return value = /root/three-step/f00ba4/call-ps/stdout.txt
          *
          * TODO: this assumes that p.startsWith(localExecutionRoot)
          */
        val subpath = p.subpath(workflowPaths.executionRoot.getNameCount, p.getNameCount)
        WorkflowPathsWithDocker.DockerRoot.resolve(subpath)
    }
  }
}
