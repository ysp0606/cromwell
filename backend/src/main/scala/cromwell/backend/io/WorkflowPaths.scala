package cromwell.backend.io

import com.typesafe.config.Config
import cromwell.backend.{BackendJobDescriptor, BackendWorkflowDescriptor}
import cromwell.core.WorkflowOptions.FinalCallLogsDir
import cromwell.core.path.{DefaultPathBuilder, Path, PathFactory}
import net.ceedubs.ficus.Ficus._

import scala.util.Try

object WorkflowPaths {
  val DefaultPathBuilders = List(DefaultPathBuilder)
}

trait WorkflowPaths extends PathFactory {
  def workflowDescriptor: BackendWorkflowDescriptor
  def config: Config

  protected lazy val executionRootString: String = config.as[Option[String]]("root").getOrElse("cromwell-executions")

  def getPath(url: String): Try[Path] = Try(PathFactory.buildPath(url, pathBuilders))

  // Rebuild potential intermediate call directories in case of a sub workflow
  protected def workflowPathBuilder(root: Path): Path = {
    workflowDescriptor.breadCrumbs.foldLeft(root)((acc, breadCrumb) => {
      breadCrumb.toPath(acc)
    }).resolve(workflowDescriptor.workflow.name).resolve(workflowDescriptor.id.toString + "/")
  }

  lazy val executionRoot: Path = PathFactory.buildPath(executionRootString, pathBuilders).toAbsolutePath
  lazy val workflowRoot: Path = workflowPathBuilder(executionRoot)
  lazy val finalCallLogsPath: Option[Path] =
    workflowDescriptor.getWorkflowOption(FinalCallLogsDir) map getPath map { _.get }

  /**
    * Creates job paths using the key and workflow descriptor.
    *
    * NOTE: For sub workflows, the jobWorkflowDescriptor will be different than the WorkflowPaths.workflowDescriptor.
    *
    * @param jobDescriptor         The job descriptor.
    * @return The paths for the job.
    */
  def toJobPaths(jobDescriptor: BackendJobDescriptor): JobPaths = {
    // If the descriptors are the same, no need to create a new WorkflowPaths
    if (workflowDescriptor == jobDescriptor.workflowDescriptor) toJobPaths(this, jobDescriptor)
    else toJobPaths(withDescriptor(jobDescriptor.workflowDescriptor), jobDescriptor)
  }
  
  protected def toJobPaths(workflowPaths: WorkflowPaths, jobDescriptor: BackendJobDescriptor): JobPaths
  
  protected def withDescriptor(workflowDescriptor: BackendWorkflowDescriptor): WorkflowPaths
}
