package cromwell.core.callcaching

sealed trait CallCachingEligibility {
  def isEligible: Boolean
  def dockerHash: Option[String]
}

sealed trait CallCachingEligible extends CallCachingEligibility {
  def isEligible = true
}
sealed trait CallCachingIneligible extends CallCachingEligibility {
  def isEligible = false
}

case object NoDocker extends CallCachingEligible {
  override def dockerHash: Option[String] = None
}
case class DockerWithHash(dockerAttribute: String) extends CallCachingEligible {
  override def dockerHash: Option[String] = Option(dockerAttribute)
}

// Currently unused
case class FloatingDockerTagWithHash(dockerAttributeWithTag: String, dockerAttributeWithHash: String) extends CallCachingEligible {
  override def dockerHash: Option[String] = Option(dockerAttributeWithHash)
}

case class FloatingDockerTagWithoutHash(dockerTag: String) extends CallCachingIneligible {
  override def dockerHash: Option[String] = None
}
