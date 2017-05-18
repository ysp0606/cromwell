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

// Commenting this out because it's not used and I'm not sure that it would be desirable to use the dockerAttributeWithHash
// as the String to hash if that might differ from that String in an equivalent DockerWithHash.
//
//case class FloatingDockerTagWithHash(dockerAttributeWithTag: String, dockerAttributeWithHash: String) extends CallCachingEligible {
//  override def dockerHash: Option[String] = Option(dockerAttributeWithHash)
//}

case class FloatingDockerTagWithoutHash(dockerTag: String) extends CallCachingIneligible {
  override def dockerHash: Option[String] = None
}
