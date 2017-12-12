package wom.values

import wom.types._

import scala.util.{Success, Try}

sealed trait WomFile extends WomPrimitive {
  val value: String

  override def valueString = value.toString

  // TODO: WOM: WOMFILE: When WDL supports directories this check may need more work (refactoring to subclasses?)
  override def equals(rhs: WomValue): Try[WomBoolean] = rhs match {
    case r: WomFile => Success(WomBoolean(value.equals(r.value) && womType.equals(r.womType)))
    case r: WomString => Success(WomBoolean(value.toString.equals(r.value.toString) && womType == WomSingleFileType))
    case r: WomOptionalValue => evaluateIfDefined("==", r, equals)
    case _ => invalid(s"$value == $rhs")
  }
}

object WomFile {
  def apply(fileType: WomFileType, value: String) = {
    fileType match {
      case WomSingleDirectoryType => WomSingleDirectory(value)
      case WomSingleFileType => WomSingleFile(value)
      case WomGlobFileType => WomGlobFile(value)
    }
  }
}

sealed trait WomSingleDirectoryOrFile extends WomFile {
  def pathOption: Option[String]

  // TODO: WOM: WOMFILE: When WDL supports the other parts beyond `value`, toWomString should return the _WDL_ string.
  override def toWomString = "\"" + value.toString + "\""

  def basenameOption: Option[String] = Some("TODO") //pathOption.map(path => path.substring(path.lastIndexOf("/") + 1))

  override def add(rhs: WomValue): Try[WomValue] = rhs match {
    case r: WomString => Success(WomSingleDirectory(value + r.value))
    case r: WomOptionalValue => evaluateIfDefined("+", r, add)
    case _ => invalid(s"$value + $rhs")
  }
}

case class WomSingleDirectory(value: String,
                              pathOption: Option[String] = None,
                              listing: Seq[WomSingleDirectoryOrFile] = Vector.empty
                             ) extends WomSingleDirectoryOrFile {
  override val womType: WomType = WomSingleDirectoryType
}

case class WomSingleFile(value: String,
                         pathOption: Option[String] = None,
                         checksumOption: Option[String] = None,
                         sizeOption: Option[Long] = None,
                         secondaryFiles: Seq[WomSingleDirectoryOrFile] = Vector.empty,
                         format: Option[String] = None,
                         contents: Option[String] = None
                        ) extends WomSingleDirectoryOrFile {

  override val womType: WomType = WomSingleFileType

  def dirnameOption: Option[String] = Some("TODO") //pathOption.map(path => path.substring(0, path.lastIndexOf("/")))

  def namerootOption: Option[String] = Some("TODO") //pathOption.map(path => path.substring(0, path.lastIndexOf("/")))

  def nameextOption: Option[String] = Some("TODO") //pathOption.map(path => path.substring(0, path.lastIndexOf("/")))

  override def add(rhs: WomValue): Try[WomValue] = rhs match {
    case r: WomString => Success(WomSingleFile(value + r.value))
    case r: WomOptionalValue => evaluateIfDefined("+", r, add)
    case _ => invalid(s"$value + $rhs")
  }
}

case class WomGlobFile(value: String) extends WomFile {
  override val womType: WomType = WomGlobFileType

  override def toWomString = "glob(\"" + value.toString + "\")"

  override def add(rhs: WomValue): Try[WomValue] = rhs match {
    case r: WomString => Success(WomGlobFile(value + r.value))
    case r: WomOptionalValue => evaluateIfDefined("+", r, add)
    case _ => invalid(s"$value + $rhs")
  }
}
