package cwl

import cats.syntax.option._
import cats.syntax.validated._
import common.validation.ErrorOr.ErrorOr
import cwl.WorkflowStepInput.InputSource
import cwl.command.ParentName
import wom.expression.{IoFunctionSet, WomExpression}
import wom.types._
import wom.values._

sealed trait CwlWomExpression extends WomExpression {

  def cwlExpressionType: WomType

  override def evaluateType(inputTypes: Map[String, WomType]): ErrorOr[WomType] = cwlExpressionType.validNel
}

case class CommandOutputExpression
(
  outputBinding: CommandOutputBinding,
  override val cwlExpressionType: WomType,
  override val inputs: Set[String],
  secondaryFilesCoproduct: Option[SecondaryFiles] = None,
  formatCoproduct: Option[StringOrExpression] = None //only valid when type: File
) extends CwlWomExpression {

  // TODO WOM: outputBinding.toString is probably not be the best representation of the outputBinding
  override def sourceString = outputBinding.toString

  // TODO: WOM: Can these also be wrapped in a WomOptional if the cwlExpressionType is '[null, File]'? Write a test and see what cromwell/salad produces
  override def evaluateValue(inputValues: Map[String, WomValue], ioFunctionSet: IoFunctionSet): ErrorOr[WomValue] = {
    outputBinding.generateOutputWomValue(
      inputValues,
      ioFunctionSet,
      cwlExpressionType,
      secondaryFilesCoproduct,
      formatCoproduct)
  }

  /**
    * Returns the list of files that _will be_ output after the command is run.
    *
    * In CWL, a list of outputs is specified as glob, say `*.bam`, plus a list of secondary files that may be in the
    * form of paths or specified using carets such as `^.bai`.
    *
    * The coerceTo may be one of four different values:
    * - WomSingleFileType
    * - WomArrayType(WomSingleFileType)
    * - WomSingleDirectoryType
    * - WomArrayType(WomSingleDirectoryType) (Possible according to the way the spec is written, but not likely?)
    */
  override def evaluateFiles(inputValues: Map[String, WomValue],
                             ioFunctionSet: IoFunctionSet,
                             coerceTo: WomType): ErrorOr[Set[WomFile]] = {
    outputBinding.primaryAndSecondaryFiles(inputValues, ioFunctionSet, coerceTo, secondaryFilesCoproduct).map(_.toSet)
  }
}

final case class WorkflowStepInputExpression(input: WorkflowStepInput, override val cwlExpressionType: WomType, graphInputs: Set[String])(implicit parentName: ParentName) extends CwlWomExpression {

  override def sourceString = input.toString

  override def evaluateValue(inputValues: Map[String, WomValue], ioFunctionSet: IoFunctionSet) = {
    (input.valueFrom, input.source) match {
      case (None, Some(WorkflowStepInputSource.String(id))) =>
        inputValues.
          get(FullyQualifiedName(id).id).
          toValidNel(s"could not find id $id in typeMap\n${inputValues.mkString("\n")}\nwhen evaluating $input.  Graph Inputs were ${graphInputs.mkString("\n")}")
      case _ => s"Could not do evaluateValue(${input.valueFrom}, ${input.source}), most likely it has not been implemented yet".invalidNel
    }
  }

  override def evaluateFiles(inputTypes: Map[String, WomValue], ioFunctionSet: IoFunctionSet, coerceTo: WomType) = ???

  override def inputs = graphInputs ++ input.source.toSet.flatMap{ inputSource: InputSource => inputSource match {
    case WorkflowStepInputSource.String(s) => Set(FullyQualifiedName(s).id)
    case WorkflowStepInputSource.StringArray(sa) => sa.map(FullyQualifiedName(_).id).toSet
  }}
}
