package cwl

import cwl.ParameterContext.{ECMAScriptSupportedPrimitive, ECMAScriptSupportedPrimitives}
import shapeless.Poly1

object ECMAScriptPrimitivesToAnyRef extends Poly1{
  implicit def esps: Case.Aux[ECMAScriptSupportedPrimitive, AnyRef] = at[ECMAScriptSupportedPrimitive] {
    _.fold(ECMAScriptPrimitiveToAnyRef)
  }

  implicit def aesps:Case.Aux[Array[ECMAScriptSupportedPrimitives], AnyRef] = at[Array[ECMAScriptSupportedPrimitives]] {
    _.map(_.fold(ECMAScriptPrimitivesToAnyRef))
  }

  implicit def mesps:Case.Aux[Map[ECMAScriptSupportedPrimitives, ECMAScriptSupportedPrimitives], AnyRef] = at[Map[ECMAScriptSupportedPrimitives, ECMAScriptSupportedPrimitives]] {
    _.map(_.fold(ECMAScriptPrimitivesToAnyRef))
  }
}

object ECMAScriptPrimitiveToAnyRef extends Poly1 {

  implicit def i:Case.Aux[Int, AnyRef] = at[Int] {_.asInstanceOf[java.lang.Integer]}
  implicit def d:Case.Aux[Double, AnyRef] = at[Double] {_.asInstanceOf[java.lang.Double]}
  implicit def b:Case.Aux[Boolean, AnyRef] = at[Boolean] {_.asInstanceOf[java.lang.Boolean] }
  implicit def s:Case.Aux[String, AnyRef] = at[String] {identity}
}
