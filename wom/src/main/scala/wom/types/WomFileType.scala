package wom.types

import spray.json.JsString
import wom.values.{WomFile, WomGlobFile, WomSingleDirectory, WomSingleFile, WomString}

import scala.util.{Success, Try}

sealed abstract class WomFileType extends WomPrimitiveType

sealed trait WomSingleDirectoryOrFileType

case object WomSingleDirectoryType extends WomFileType with WomSingleDirectoryOrFileType {
  val toDisplayString: String = "Dir"

  override protected def coercion = {
    case s: String => WomSingleDirectory(s)
    case s: JsString => WomSingleDirectory(s.value)
    case s: WomString => WomSingleDirectory(s.valueString)
    case f: WomFile => f
  }

  override def add(rhs: WomType): Try[WomType] = rhs match {
    case WomStringType => Success(WomSingleDirectoryType)
    case WomOptionalType(memberType) => add(memberType)
    case _ => invalid(s"$this + $rhs")
  }

  override def equals(rhs: WomType): Try[WomType] = rhs match {
    case WomSingleDirectoryType => Success(WomBooleanType)
    case WomStringType => Success(WomBooleanType)
    case WomOptionalType(memberType) => equals(memberType)
    case _ => invalid(s"$this == $rhs")
  }
}

case object WomSingleFileType extends WomFileType with WomSingleDirectoryOrFileType {
  val toDisplayString: String = "File"

  override protected def coercion = {
    case s: String => WomSingleFile(s)
    case s: JsString => WomSingleFile(s.value)
    case s: WomString => WomSingleFile(s.valueString)
    case f: WomFile => f
  }

  override def add(rhs: WomType): Try[WomType] = rhs match {
    case WomStringType => Success(WomSingleFileType)
    case WomOptionalType(memberType) => add(memberType)
    case _ => invalid(s"$this + $rhs")
  }

  override def equals(rhs: WomType): Try[WomType] = rhs match {
    case WomSingleFileType => Success(WomBooleanType)
    case WomStringType => Success(WomBooleanType)
    case WomOptionalType(memberType) => equals(memberType)
    case _ => invalid(s"$this == $rhs")
  }
}

case object WomGlobFileType extends WomFileType {
  val toDisplayString: String = "Glob"

  override protected def coercion = {
    case s: String => WomGlobFile(s)
    case s: JsString => WomGlobFile(s.value)
    case s: WomString => WomGlobFile(s.valueString)
    case f: WomFile => f
  }

  override def add(rhs: WomType): Try[WomType] = rhs match {
    case WomStringType => Success(WomGlobFileType)
    case WomOptionalType(memberType) => add(memberType)
    case _ => invalid(s"$this + $rhs")
  }

  override def equals(rhs: WomType): Try[WomType] = rhs match {
    case WomGlobFileType => Success(WomBooleanType)
    case WomStringType => Success(WomBooleanType)
    case WomOptionalType(memberType) => equals(memberType)
    case _ => invalid(s"$this == $rhs")
  }
}
