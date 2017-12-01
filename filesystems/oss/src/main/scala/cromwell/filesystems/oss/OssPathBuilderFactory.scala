package cromwell.filesystems.oss

import akka.actor.ActorSystem
import cromwell.core.WorkflowOptions
import cromwell.core.path.PathBuilderFactory

import scala.concurrent.{ExecutionContext, Future}

object OssPathBuilderFactory {
}

case class OssPathBuilderFactory(endpoint: String,
                                 accessId: String,
                                 accessKey: String,
                                 securityToken: Option[String]
                                ) extends PathBuilderFactory {
  def withOptions(options: WorkflowOptions)(implicit as: ActorSystem, ec: ExecutionContext) = {
    Future.successful(OssPathBuilder.fromCredentials(endpoint, accessId, accessKey, securityToken, options))
  }
}
