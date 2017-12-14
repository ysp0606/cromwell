package cwl

import cwl.ParameterContext.{ECMAScriptSupportedPrimitive, ECMAScriptSupportedPrimitives, JSMap}
import shapeless.{:+:, CNil, Coproduct}
import simulacrum.typeclass
import wom.callable.RuntimeEnvironment
import wom.types.WomNothingType
import wom.values.{WomInteger, WomOptionalValue, WomString, WomValue}

import scala.language.implicitConversions

object ParameterContext {
  val Empty = ParameterContext()

  type ECMAScriptSupportedPrimitive =
    Int :+:
    Double :+:
      Boolean :+:
      String :+:
      CNil

  type ECMAScriptSupportedPrimitives =
    ECMAScriptSupportedPrimitive :+:
      Array[ECMAScriptSupportedPrimitive] :+:
      CNil

  def liftPrimitive: ECMAScriptSupportedPrimitive => ECMAScriptSupportedPrimitives = Coproduct[ECMAScriptSupportedPrimitives].apply

  def liftArrayOfPrimitives: Array[ECMAScriptSupportedPrimitive] => ECMAScriptSupportedPrimitives = Coproduct[ECMAScriptSupportedPrimitives].apply

  type JSMap = Map[String, ECMAScriptSupportedPrimitives]
}

/**
  * Used to convert case classes into sets of JS arguments
  */
@typeclass trait JSArguments[A] {
  def convert(a: A): JSMap
}


object JSArguments {
  def apply[A](f: A => JSMap): JSArguments[A] = new JSArguments[A] {
    override def convert(a: A): JSMap = f(a)
  }

  implicit final val rc: JSArguments[RuntimeEnvironment] =

    JSArguments{ (rc:RuntimeEnvironment) =>
      import rc._
      Map(
        "outdir" -> outputPath,
        "tmpdir" -> tempPath,
        "cores" -> cores.toString,
        "ram" -> ram.toString,
        "outdirSize" -> outputPathSize.toString,
        "tmpdirSize" -> tempPathSize.toString
      ).mapValues(_.convert).mapValues(Coproduct[ECMAScriptSupportedPrimitives].apply _)
    }
}

/**
  * Used to convert low level types to low-level JS-compatible types.
  */
@typeclass trait JsCompatible[A] {
  def convert(a: A): ECMAScriptSupportedPrimitive
}

object JsCompatible {
  def apply[A](f: A => ECMAScriptSupportedPrimitive): JsCompatible[A] = new JsCompatible[A] {
    override def convert(a: A): ECMAScriptSupportedPrimitive = f(a)
  }

  //use kittens or somehow derive typeclass instances for WOM?
  implicit final val string = new JsCompatible[String] {
    override def convert(a: String): ECMAScriptSupportedPrimitive = Coproduct[ECMAScriptSupportedPrimitive](a)
  }

  implicit final val integer = new JsCompatible[Int] {
    override def convert(a: Int): ECMAScriptSupportedPrimitive = Coproduct[ECMAScriptSupportedPrimitive](a)
  }

  implicit final val womInteger = new JsCompatible[WomInteger] {
    override def convert(a: WomInteger): ECMAScriptSupportedPrimitive = Coproduct[ECMAScriptSupportedPrimitive](a.value)
  }

  implicit final val womString = new JsCompatible[WomString] {
    override def convert(a: WomString): ECMAScriptSupportedPrimitive = Coproduct[ECMAScriptSupportedPrimitive](a.value)
  }

  implicit final val womValue =
    new JsCompatible[WomValue] {
      override def convert(wv: WomValue): ECMAScriptSupportedPrimitive =
        wv match {
          case WomOptionalValue(WomNothingType, None) => null
          case ws: WomString => womString.convert(ws)
          case wi: WomInteger => womInteger.convert(wi)
          /*
        case WomFloat(double) => double.asInstanceOf[java.lang.Double]
        case WomBoolean(boolean) => boolean.asInstanceOf[java.lang.Boolean]
        case WomArray(_, array) => array.map(toJavascript).toArray
        case WomSingleFile(path) => path
        case WomMap(_, map) =>
          map.map({
            case (mapKey, mapValue) => toJavascript(mapKey) -> toJavascript(mapValue)
          }).asJava
          */
          case _ => throw new IllegalArgumentException(s"Unexpected value: $wv")
        }
    }
}



case class ParameterContext(inputs: JSMap = Map.empty,
                            self: Array[JSMap] = Array.empty,
                            runtime: JSMap = Map.empty)
