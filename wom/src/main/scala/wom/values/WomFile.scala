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

  // TODO: WOM: WOMFILE: Should only map the path _or_ location? Come up with / Look up documented definitions of what the difference between path and location are stick to them.

  // TODO: WOM: WOMFILE: A typeclass would make these less redundant. Can WOM use typeclasses? (even if WDL perhaps cannot?)

  def mapFile(womFile: WomFile)(f: String => String): WomFile = {
    womFile match {
      case womSingleDirectory: WomSingleDirectory => mapSingleDirectory(womSingleDirectory)(f)
      case womSingleFile: WomSingleFile => mapSingleFile(womSingleFile)(f)
      case womGlobFile: WomGlobFile => mapGlobFile(womGlobFile)(f)
    }
  }

  def mapSingleDirectoryOrFile(womFile: WomSingleDirectoryOrFile)(f: String => String): WomSingleDirectoryOrFile = {
    womFile match {
      case womSingleDirectory: WomSingleDirectory => mapSingleDirectory(womSingleDirectory)(f)
      case womSingleFile: WomSingleFile => mapSingleFile(womSingleFile)(f)
    }
  }

  def mapSingleFile(womFile: WomSingleFile)(f: String => String): WomSingleFile = {
    WomSingleFile(
      f(womFile.value),
      womFile.pathOption.map(f),
      womFile.checksumOption,
      womFile.sizeOption,
      womFile.secondaryFiles.map(mapSingleDirectoryOrFile(_)(f)),
      womFile.formatOption,
      womFile.contentsOption
    )
  }

  def mapSingleDirectory(womFile: WomSingleDirectory)(f: String => String): WomSingleDirectory = {
    WomSingleDirectory(
      f(womFile.value),
      womFile.pathOption.map(f),
      womFile.listing.map(mapSingleDirectoryOrFile(_)(f))
    )
  }

  def mapGlobFile(womFile: WomGlobFile)(f: String => String): WomGlobFile = {
    womFile match {
      case WomGlobFile(value) => WomGlobFile(f(value))
    }
  }

  def flattenFile(womFile: WomFile): Seq[WomFile] = {
    womFile match {
      case womSingleDirectory: WomSingleDirectory =>
        womSingleDirectory.listing.toList match {
          case Nil => List(womFile)
          case list => list flatMap flattenFile
        }
      case womSingleFile: WomSingleFile =>
        womSingleFile.secondaryFiles.foldLeft(List(womFile)) {
          (womFiles, womSingleDirectoryOrFile) =>
            womFiles ++ flattenFile(womSingleDirectoryOrFile)
        }
      case womGlobFile: WomGlobFile => List(womGlobFile)
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
                         formatOption: Option[String] = None,
                         contentsOption: Option[String] = None
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
