package cromwell.backend.impl.bcs

import cats.data.Validated.{Invalid, Valid}
import cats.syntax.validated._
import common.exception.MessageAggregation
import common.validation.ErrorOr.ErrorOr
import cromwell.backend.DiskPatterns._

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

trait BcsDisk {
  val name:String
  val diskType: String
  val sizeInGB: Int
}

object BcsSystemDisk {
  val Name = "local-disk"
  val Default = BcsSystemDisk("cloud_efficiency", 40)
}

case class BcsSystemDisk(diskType: String, sizeInGB: Int) extends BcsDisk {
  val name: String = BcsSystemDisk.Name
}

object BcsDataDisk {
  val Name = "data-disk"
  val Default: None.type = None
}
final case class BcsDataDisk(diskType: String, sizeInGB: Int, mountPoint: String) extends BcsDisk {
  val name: String = BcsDataDisk.Name
}

object BcsDisk{
  val systemDiskPattern: Regex = s"""(\\S+)\\s+(\\d+)""".r
  val dataDiskPattern: Regex = s"""(\\S+)\\s+(\\d+)\\s+(\\S+)""".r

  def parse(s: String): Try[BcsDisk] = {
    s match {
      case systemDiskPattern(diskType, sizeInGB) => Success(BcsSystemDisk(diskType, sizeInGB.toInt))
      case dataDiskPattern(diskType, sizeInGB, mountPoint) => Success(BcsDataDisk(diskType, sizeInGB.toInt, mountPoint))
      case _ => Failure(new IllegalArgumentException("disk must be 'cloud 40' or 'cloud 200 /home/input/'"))
    }
  }

  def parseStandard(s: String): Try[BcsDisk] = {
    val validation: ErrorOr[BcsDisk] = s match {
      case WorkingDiskPattern(sizeGb, diskType) =>   Valid(BcsSystemDisk(diskType, sizeGb.toInt))
      case MountedDiskPattern(mountPoint, sizeGb, diskType) => Valid(BcsDataDisk(diskType, sizeGb.toInt, mountPoint))
      case _ => s"Disk strings should be of the format 'local-disk SIZE TYPE' or '/mount/point SIZE TYPE' but got: '$s'".invalidNel
    }

    Try(validation match {
      case Valid(localDisk) => localDisk
      case Invalid(nels) =>
        throw new UnsupportedOperationException with MessageAggregation {
          val exceptionContext = ""
          val errorMessages: List[String] = nels.toList
        }
    })
  }

}