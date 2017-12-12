package cwl

import cats.implicits._
import common.validation.ErrorOr.ErrorOr
import eu.timepit.refined._
import shapeless.syntax.singleton._
import shapeless.{:+:, CNil, Poly1}
import wom.values.{WomSingleDirectory, WomSingleDirectoryOrFile, WomSingleFile}

object CwlType extends Enumeration {
  type CwlType = Value

  val Null = Value("null")
  val Boolean = Value("boolean")
  val Int = Value("int")
  val Long = Value("long")
  val Float = Value("float")
  val Double = Value("double")
  val String = Value("string")
  val File = Value("File")
  val Directory = Value("Directory")
}

case class File private
(
  `class`: W.`"File"`.T,
  location: Option[String], //TODO refine w/ regex  of IRI
  path: Option[String],
  checksum: Option[String],
  size: Option[Long],
  secondaryFiles: Option[Array[File :+: Directory :+: CNil]],
  format: Option[String],
  contents: Option[String]
) {

  lazy val asWomValue: ErrorOr[WomSingleFile] = {
    // TODO WOM: needs to handle basename and maybe other fields. We might need to augment WdlFile, or have a smarter WomFile
    path.orElse(location) match {
      case Some(value) =>
        val dirsOrFiles: List[File :+: Directory :+: CNil] = secondaryFiles.getOrElse(Array.empty).toList
        val errorOrList: ErrorOr[List[WomSingleDirectoryOrFile]] =
          dirsOrFiles.traverse[ErrorOr, WomSingleDirectoryOrFile] {
            _.fold(SecondaryFileAsWomSingleDirectoryOrFile)
          }
        errorOrList map {
          WomSingleFile(value, location, checksum, size, _, format, contents)
        }
      case None => "Cannot convert CWL File to WomValue without either a location or a path".invalidNel
    }
  }
}

object File {
  def apply(
             location: Option[String] = None, //TODO refine w/ regex  of IRI
             path: Option[String] = None,
             checksum: Option[String] = None,
             size: Option[Long] = None,
             secondaryFiles: Option[Array[File :+: Directory :+: CNil]] = None,
             format: Option[String] = None,
             contents: Option[String] = None): File =
    new cwl.File(
      "File".narrow,
      location,
      path,
      checksum,
      size,
      secondaryFiles,
      format,
      contents
    )
}

case class Directory
(
  `class`: W.`"Directory"`.T,
  location: Option[String],
  path: Option[String],
  basename: Option[String],
  listing: Option[Array[File :+: Directory :+: CNil]]
) {

  lazy val asWomValue: ErrorOr[WomSingleDirectory] = {
    path.orElse(location) match {
      case Some(value) =>
        val dirsOrFiles: List[File :+: Directory :+: CNil] = listing.getOrElse(Array.empty).toList
        val errorOrList: ErrorOr[List[WomSingleDirectoryOrFile]] =
          dirsOrFiles.traverse[ErrorOr, WomSingleDirectoryOrFile] {
            _.fold(SecondaryFileAsWomSingleDirectoryOrFile)
          }
        errorOrList map {
          WomSingleDirectory(value, location, _)
        }
      case None => "Cannot convert CWL File to WomValue without either a location or a path".invalidNel
    }
  }
}


private[cwl] object SecondaryFileAsWomSingleDirectoryOrFile extends Poly1 {
  implicit def cwlFileToWdlValue: Case.Aux[File, ErrorOr[WomSingleFile]] = at[File] {
    _.asWomValue
  }

  implicit def cwlDirectoryToWdlValue: Case.Aux[Directory, ErrorOr[WomSingleDirectory]] = at[Directory] {
    _.asWomValue
  }
}
