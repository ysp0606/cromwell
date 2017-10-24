package cromwell.backend.validation

import cats.syntax.validated._
import common.validation.ErrorOr.ErrorOr
import wom.values._

private[validation] abstract class StdOutErrRedirectValidation(key: String) extends StringRuntimeAttributesValidation(key) {
  override protected def usedInCallCaching: Boolean = true

  override protected def missingValueMessage: String = s"Can't find an attribute value for key $key"

  override protected def invalidValueMessage(value: WomValue): String = super.missingValueMessage

  // NOTE: Docker's current test specs don't like WdlInteger, etc. auto converted to WdlString.
  override protected def validateValue: PartialFunction[WomValue, ErrorOr[String]] = {
    case WomString(value) => value.validNel
  }
}

case object StdoutRedirectValidation extends StdOutErrRedirectValidation(RuntimeAttributesKeys.StdoutRedirect)  {
  lazy val instance: RuntimeAttributesValidation[String] = this
}

case object StderrRedirectValidation extends StdOutErrRedirectValidation(RuntimeAttributesKeys.StderrRedirect) {
  lazy val instance: RuntimeAttributesValidation[String] = this
}
