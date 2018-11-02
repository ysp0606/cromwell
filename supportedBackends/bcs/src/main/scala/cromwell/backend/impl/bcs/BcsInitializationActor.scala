package cromwell.backend.impl.bcs

import akka.actor.ActorRef
import cromwell.backend.standard.{StandardInitializationActor, StandardInitializationActorParams, StandardValidatedRuntimeAttributesBuilder}
import cromwell.backend.{BackendConfigurationDescriptor, BackendInitializationData, BackendWorkflowDescriptor}
import cromwell.core.path.{DefaultPathBuilder, PathBuilder}
import cromwell.filesystems.oss.OssPathBuilderFactory
import cats.instances.future._
import cats.instances.list._
import cats.syntax.traverse._

import scala.concurrent.Future
import wom.graph.TaskCallNode


final case class BcsInitializationActorParams
(
  workflowDescriptor: BackendWorkflowDescriptor,
  calls: Set[TaskCallNode],
  bcsConfiguration: BcsConfiguration,
  serviceRegistryActor: ActorRef
) extends StandardInitializationActorParams {
  override val configurationDescriptor: BackendConfigurationDescriptor = bcsConfiguration.configurationDescriptor
}

final class BcsInitializationActor(params: BcsInitializationActorParams)
  extends StandardInitializationActor(params) {

  private val bcsConfiguration = params.bcsConfiguration
  private implicit val system = context.system

  lazy val ossPathBuilderFactory: Option[OssPathBuilderFactory] = {
    Some(OssPathBuilderFactory(new BackendTTLOssStorageConfiguration))
  }

  override lazy val pathBuilders: Future[List[PathBuilder]] = ossPathBuilderFactory.toList.traverse(_.withOptions(workflowDescriptor.workflowOptions)).map(_ ++ Option(DefaultPathBuilder))

  override lazy val workflowPaths: Future[BcsWorkflowPaths] = pathBuilders map {
    BcsWorkflowPaths(workflowDescriptor, bcsConfiguration.configurationDescriptor.backendConfig, _)
  }

  override lazy val runtimeAttributesBuilder: StandardValidatedRuntimeAttributesBuilder = BcsRuntimeAttributes.runtimeAttributesBuilder(bcsConfiguration.runtimeConfig)

  override def beforeAll(): Future[Option[BackendInitializationData]] = {
    pathBuilders map { builders => BcsMount.pathBuilders = builders}

    for {
      paths <- workflowPaths
      builders <- pathBuilders
    } yield Option(BcsBackendInitializationData(paths, runtimeAttributesBuilder, bcsConfiguration, builders))

  }

}
