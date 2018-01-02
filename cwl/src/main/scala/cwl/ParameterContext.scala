package cwl

import cats.data.NonEmptyList
import shapeless.{:+:, CNil, Coproduct}
import simulacrum.typeclass
import wom.callable.RuntimeEnvironment
import wom.types.WomNothingType
import wom.values.{WomArray, WomBoolean, WomFloat, WomInteger, WomMap, WomOptionalValue, WomSingleFile, WomString, WomValue}
import cats.syntax.traverse._
import cats.syntax.apply._
import cats.syntax.validated._
import cats.instances.map._
import common.validation.ErrorOr.ErrorOr

import scala.collection.JavaConverters._
import scala.language.implicitConversions

object ParameterContext {
  val Empty = ParameterContext()
}

case class ParameterContext(private val inputs: Map[String, AnyRef] = Map.empty,
                       private val self: Array[Map[String, String]] = Array.empty,
                       private val runtime: Map[String, AnyRef] = Map.empty) {

  def addInputs(womMap: Map[String, WomValue]): Either[NonEmptyList[String], ParameterContext] = {
    val x: ErrorOr[Map[String, AnyRef]] = womMap.traverse({
      case (key, womValue) => (key.validNel[String], toJavascript(womValue)).tupled
    })
  }

  def setSelf(newSelf: Array[Map[String, String]]): ParameterContext = this.copy(self = newSelf)

  def ecmaScriptValues:java.util.Map[String, AnyRef] =
    Map(
      "inputs" -> inputs.asInstanceOf[AnyRef],
      "runtime" -> runtime.asInstanceOf[AnyRef],
      "self" -> self.asInstanceOf[AnyRef]
    ).asJava

  private def toJavascript(value: WomValue): ErrorOr[AnyRef] = {
    value match {
      case WomOptionalValue(WomNothingType, None) => null
      case WomString(string) => string.validNel
      case WomInteger(int) => int.asInstanceOf[java.lang.Integer].validNel
      case WomFloat(double) => double.asInstanceOf[java.lang.Double].validNel
      case WomBoolean(boolean) => boolean.asInstanceOf[java.lang.Boolean].validNel
      case WomArray(_, array) => array.map(toJavascript).toArray.validNel
      case WomSingleFile(path) => path.validNel
      case WomMap(_, map) =>
        map.traverse({
          case (mapKey, mapValue) => (toJavascript(mapKey), toJavascript(mapValue)).tupled
        }).asJava.validNel
      case _ => (s"Value is unsupported in JavaScript: $value").invalidNel
    }
  }
}
