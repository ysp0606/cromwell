package cwl

import cats.syntax.validated._
import common.validation.ErrorOr.ErrorOr
import wom.expression.WomExpression
import wom.types._

trait CwlWomExpression extends WomExpression {

  def cwlExpressionType: WomType

  override def evaluateType(inputTypes: Map[String, WomType]): ErrorOr[WomType] = cwlExpressionType.validNel
}

