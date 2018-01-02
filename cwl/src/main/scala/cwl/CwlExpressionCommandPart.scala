package cwl

import common.validation.ErrorOr.ErrorOr
import common.validation.Validation._
import cwl.CommandLineTool.CommandInputParameter
import cwl.command.ParentName
import mouse.all._
import wom.callable.RuntimeEnvironment
import wom.expression.IoFunctionSet
import wom.graph.LocalName
import wom.values._
import wom.{CommandPart, InstantiatedCommand}

import scala.util.Try
import JSArguments.ops._
import JsCompatible.ops._
import cwl.ParameterContext.{ECMAScriptSupportedPrimitives, JSMap}
import shapeless.Coproduct

import scala.language.postfixOps

case class CwlExpressionCommandPart(expr: Expression) extends CommandPart {
  override def instantiate(inputsMap: Map[LocalName, WomValue],
                           functions: IoFunctionSet,
                           valueMapper: (WomValue) => WomValue,
                           runtimeEnvironment: RuntimeEnvironment): ErrorOr[InstantiatedCommand] =
    Try {
      val stringKeyMap: Map[String, WomValue] = inputsMap.map { case (LocalName(localName), value) => localName -> value }

      val pc =
        ParameterContext(
          inputs = stringKeyMap.mapValues(v => Coproduct[ECMAScriptSupportedPrimitives](v.convert)),
          runtime = runtimeEnvironment.convert
        )

      val womValue: WomValue = expr.fold(EvaluateExpression).apply(pc)

      InstantiatedCommand(womValue.valueString)
    } toErrorOr
}

// TODO: Dan to revisit making this an Either (and perhaps adding some other cases)
case class CommandLineBindingCommandPart(argument: CommandLineBinding) extends CommandPart {
  override def instantiate(inputsMap: Map[LocalName, WomValue],
                           functions: IoFunctionSet,
                           valueMapper: (WomValue) => WomValue,
                           runtimeEnvironment: RuntimeEnvironment): ErrorOr[InstantiatedCommand] =
    Try {
      val inputs: JSMap = inputsMap.map({
        case (LocalName(localName), WomSingleFile(path)) => localName -> Coproduct[ECMAScriptSupportedPrimitives](path.convert)
        case (LocalName(localName), value) => localName -> Coproduct[ECMAScriptSupportedPrimitives](value.convert)
      })

      val pc = ParameterContext(inputs)

      val womValue: WomValue = argument match {
        case CommandLineBinding(_, _, _, _, _, Some(StringOrExpression.Expression(expression)), Some(false)) =>
          expression.fold(EvaluateExpression).apply(pc) |> valueMapper
        case CommandLineBinding(_, _, _, _, _, Some(StringOrExpression.String(string)), Some(false)) =>
          WomString(string)
        // There's a fair few other cases to add, but until then...
        case other => throw new NotImplementedError(s"As-yet-unsupported command line binding: $other")
      }
      InstantiatedCommand(womValue.valueString)
    } toErrorOr
}

case class InputParameterCommandPart(commandInputParameter: CommandInputParameter)(implicit parentName: ParentName) extends CommandPart {

  override def instantiate(inputsMap: Map[LocalName, WomValue],
                           functions: IoFunctionSet,
                           valueMapper: (WomValue) => WomValue,
                           runtimeEnvironment: RuntimeEnvironment) =
    Try {
      val womValue: WomValue = commandInputParameter match {
        case cip: CommandInputParameter =>
          val localizedId = LocalName(FullyQualifiedName(cip.id).id)
          inputsMap.get(localizedId).cata(
            valueMapper, throw new RuntimeException(s"could not find $localizedId in map $inputsMap"))

        // There's a fair few other cases to add, but until then...
        case other => throw new NotImplementedError(s"As-yet-unsupported commandPart from command input parameters: $other")
      }
      InstantiatedCommand(womValue.valueString)
    } toErrorOr
}
