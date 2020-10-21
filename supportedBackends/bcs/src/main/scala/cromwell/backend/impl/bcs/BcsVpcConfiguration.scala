package cromwell.backend.impl.bcs


import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}


final case class BcsVpcConfiguration(vpcId: Option[String] = None, vSwitchId: Option[List[String]])


object BcsVpcConfiguration {
//  val cidrBlockPattern: Regex = """(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])(\/([0-9]|[1-2][0-9]|3[0-2]){1,2})""".r
  val vpcIdPattern: Regex = """(vpc-[^\s]+)""".r
  val vSwitchIdPattern: Regex = """(vsw-[^\s]+)""".r

  def parse(s: String): Try[BcsVpcConfiguration] = {
    val vpcId = vpcIdPattern findFirstIn s
    val vSwitchId = vSwitchIdPattern findAllIn s

    if (vSwitchId.isEmpty || vpcId.isEmpty) {
      Failure(new IllegalArgumentException("vpc configuration must be a string like 'vpc-xxxx vsw-xxxx vsw-yyyy' "))
    } else {
      Success(BcsVpcConfiguration(vpcId, Option(vSwitchId.toList)))
    }
  }
}