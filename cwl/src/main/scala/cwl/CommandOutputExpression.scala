package cwl

import common.validation.ErrorOr.ErrorOr
import cwl.ParameterContext.{ECMAScriptSupportedPrimitives, JSMap}
import shapeless.Coproduct
import wom.expression.IoFunctionSet
import wom.types._
import wom.values.{WomArray, WomFile, WomGlobFile, WomMap, WomString, WomValue}
import JsCompatible.ops._
import mouse.all._

import scala.concurrent.Await
import common.validation.Validation._
import cats.data.Validated._
import ParameterContext.liftPrimitive

import scala.language.postfixOps
import scala.concurrent.Await
import scala.concurrent.duration._

case class CommandOutputExpression(outputBinding: CommandOutputBinding,
                                   override val cwlExpressionType: WomType,
                                   override val inputs: Set[String]) extends CwlWomExpression {

  // TODO WOM: outputBinding.toString is probably not be the best representation of the outputBinding
  override def sourceString = outputBinding.toString

  override def evaluateValue(inputValues: Map[String, WomValue], ioFunctionSet: IoFunctionSet): ErrorOr[WomValue] = {

    val parameterContext = ParameterContext(inputs = inputValues.mapValues(_.convert).mapValues(liftPrimitive))

    /*
    CommandOutputBinding.glob:
    Find files relative to the output directory, using POSIX glob(3) pathname matching. If an array is provided, find
    files that match any pattern in the array. If an expression is provided, the expression must return a string or an
    array of strings, which will then be evaluated as one or more glob patterns. Must only match and return files which
    actually exist.

    http://www.commonwl.org/v1.0/CommandLineTool.html#CommandOutputBinding
     */
    def commandOutputBindingToWomValue: WomValue = {
      import StringOrExpression._
      outputBinding match {
        case CommandOutputBinding(_, _, Some(String(value))) => WomString(value)
        case CommandOutputBinding(Some(glob), _, None) => WomArray(WomArrayType(WomStringType), GlobEvaluator.globPaths(glob, parameterContext, ioFunctionSet).map(WomString.apply))

        case CommandOutputBinding(glob, loadContents, Some(Expression(expression))) =>

          val paths: Seq[String] = glob map { globValue =>
            GlobEvaluator.globPaths(globValue, parameterContext, ioFunctionSet)
          } getOrElse {
            Vector.empty
          }

          val _loadContents: Boolean = loadContents getOrElse false

          val womMaps: Array[JSMap] =
            paths.map({
              (path:String) =>
                // TODO: WOM: basename/dirname/size/checksum/etc.

                val contents: JSMap =
                  if (_loadContents)
                    Map("contents" -> Coproduct[ECMAScriptSupportedPrimitives](load64KiB(path, ioFunctionSet).convert))
                  else
                    Map.empty

                Map(
                  "location" -> Coproduct[ECMAScriptSupportedPrimitives](path.convert)
                ) ++ contents
            }).toArray

          val outputEvalParameterContext = parameterContext.copy(self = womMaps)

          expression.fold(EvaluateExpression).apply(outputEvalParameterContext)
      }
    }
    //To facilitate ECMAScript evaluation, filenames are stored in a map under the key "location"
    val womValue =
      commandOutputBindingToWomValue match {
        case WomArray(_, Seq(WomMap(WomMapType(WomStringType, WomStringType), map))) => map(WomString("location"))
        case other => other
      }

    //If the value is a string but the output is expecting a file, we consider that string a POSIX "glob" and apply
    //it accordingly to retrieve the file list to which it expands.
    val globbedIfFile =
      (womValue, cwlExpressionType) match {

        //In the case of a single file being expected, we must enforce that the glob only represents a single file
        case (WomString(glob), WomSingleFileType) =>
          ioFunctionSet.glob(glob) match {
            case head :: Nil => WomString(head)
            case list => throw new RuntimeException(s"expecting a single File glob but instead got $list")
          }

        case _  => womValue
      }

    //CWL tells us the type this output is expected to be.  Attempt to coerce the actual output into this type.
    cwlExpressionType.coerceRawValue(globbedIfFile).toErrorOr
  }

  /*
  TODO:
   DB: It doesn't make sense to me that this function returns type WomFile but accepts a type to which it coerces.
   Wouldn't coerceTo always == WomFileType, and if not then what?
   */
  override def evaluateFiles(inputs: Map[String, WomValue], ioFunctionSet: IoFunctionSet, coerceTo: WomType): ErrorOr[Set[WomFile]] ={

    val pc = ParameterContext(inputs.mapValues(_.convert).mapValues(liftPrimitive))

    val files = for {
      globValue <- outputBinding.glob.toList
      path <- GlobEvaluator.globPaths(globValue, pc, ioFunctionSet).toList
    } yield WomGlobFile(path): WomFile

    files.toSet |> validNel
  }

  private def load64KiB(path: String, ioFunctionSet: IoFunctionSet): String = {
    // This suggests the IoFunctionSet should have a length-limited read API as both CWL and WDL support this concept.
    // ChrisL: But remember that they are different (WDL => failure, CWL => truncate)
    val content = ioFunctionSet.readFile(path)

    // TODO: propagate IO, Try, or Future or something all the way out via "commandOutputBindingtoWomValue" signature
    // TODO: Stream only the first 64 KiB, this "read everything then ignore most of it" method is terrible
    val initialResult = Await.result(content, 5 seconds)
    initialResult.substring(0, Math.min(initialResult.length, 64 * 1024))
  }
}
