package cwl

import cats.syntax.validated._
import common.validation.ErrorOr.ErrorOr
import wom.expression.WomExpression
import wom.types._
import wom.values._
import wom.expression.{IoFunctionSet, WomExpression}
import cats.syntax.validated._
import cwl.WorkflowStepInput.InputSource
import cwl.command.ParentName

import scala.language.postfixOps

trait CwlWomExpression extends WomExpression {

  def cwlExpressionType: WomType

  override def evaluateType(inputTypes: Map[String, WomType]): ErrorOr[WomType] = cwlExpressionType.validNel
}

