package wom.values

import wom.types.{WomFileType, WomType}

import scala.util.{Success, Try}

sealed trait WomFile extends WomPrimitive {
  val value: String
  override val womType: WomType = WomFileType

  override def valueString = value.toString
}

sealed trait WomSingleDirectoryOrFile extends WomFile {
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
                             ) extends WomSingleDirectoryOrFile

case class WomSingleFile(value: String,
                         pathOption: Option[String] = None,
                         checksumOption: Option[String] = None,
                         sizeOption: Option[Long] = None,
                         secondaryFiles: Seq[WomSingleDirectoryOrFile] = Vector.empty,
                         format: Option[String] = None,
                         contents: Option[String] = None
                        ) extends WomSingleDirectoryOrFile {
  override def toWomString = "\"" + value.toString + "\""

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
  override def toWomString = "glob(\"" + value.toString + "\")"
}
