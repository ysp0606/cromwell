package cromwell.backend.impl.bcs

import scala.util.{Failure, Success, Try}

trait BcsDocker {
  val image: String
}

final case class BcsDockerWithoutPath(image: String) extends BcsDocker
final case class BcsDockerWithPath(image: String, path: String) extends BcsDocker


object BcsDocker{
  val dockerWithPathPattern = s"""(\\S+)\\s+(\\S+)""".r
  val dockerWithoutPathPatter = s"""(\\S+)""".r
  val ossRegistryImageIdPattern = s"""^localhost:5000/(\\S+)""".r

  def parse(s: String): Try[BcsDocker] = {
    s match {
      case dockerWithoutPathPatter(dockerImage) => Success(BcsDockerWithoutPath(dockerImage))
      case dockerWithPathPattern(dockerImage, dockerPath) =>  dockerImage match {
        case ossRegistryImageIdPattern(_) => Success (BcsDockerWithPath (dockerImage, dockerPath) )
        case _ => Failure(new IllegalArgumentException("must be 'localhost:5000/ubuntu:14.04 oss://docker-reg/ubuntu/' like"))
      }
      case _ => Failure(new IllegalArgumentException("must be 'ubuntu:14.04' or 'localhost:5000/ubuntu:14.04 oss://docker-reg/ubuntu/' like"))
    }
  }
}
