package wom.callable

import lenthall.util.TryUtil
import wom.core._
import common.validation.ErrorOr.ErrorOr
import wdl.util.StringUtil
import wom.expression.IoFunctionSet
import wom.graph.{Graph, TaskCall}
import wom.values.{WomEvaluatedCallInputs, WomValue}
import wom.{CommandPart, RuntimeAttributes}

import scala.util.{Success, Try}

final case class TaskDefinition(name: String,
                                commandTemplate: Seq[CommandPart],
                                runtimeAttributes: RuntimeAttributes,
                                meta: Map[String, String],
                                parameterMeta: Map[String, String],
                                outputs: List[Callable.OutputDefinition],
                                inputs: List[_ <: Callable.InputDefinition],
                                prefixSeparator: String = ".",
                                commandPartSeparator: String = "",
                                stdoutRedirect: Option[CommandPart] = None,
                                stderrRedirect: Option[CommandPart] = None) extends Callable {

  val unqualifiedName: LocallyQualifiedName = name

  override lazy val graph: ErrorOr[Graph] = TaskCall.graphFromDefinition(this)

  def instantiateCommand(taskInputs: WomEvaluatedCallInputs,
                         functions: IoFunctionSet,
                         valueMapper: WomValue => WomValue = identity[WomValue],
                         separate: Boolean = false): Try[String] =  for {
    instantiatedCommandTemplate <- TryUtil.sequence(commandTemplate.map(instantiateCommandPart(_, taskInputs, functions, valueMapper)))
    command = StringUtil.normalize(instantiatedCommandTemplate.mkString(commandPartSeparator))
  } yield command

  def commandTemplateString: String = StringUtil.normalize(commandTemplate.map(_.toString).mkString)

  def instantiateStdoutFilename(taskInputs: WomEvaluatedCallInputs,
                                functions: IoFunctionSet,
                                valueMapper: WomValue => WomValue = identity[WomValue]): Try[String] =
    stdoutRedirect map { instantiateCommandPart(_, taskInputs, functions, valueMapper) } getOrElse Success("stdout")

  def instantiateStderrFilename(taskInputs: WomEvaluatedCallInputs,
                                functions: IoFunctionSet,
                                valueMapper: WomValue => WomValue = identity[WomValue]): Try[String] =
    stdoutRedirect map { instantiateCommandPart(_, taskInputs, functions, valueMapper) } getOrElse Success("stderr")

  private def instantiateCommandPart(commandPart: CommandPart,
                                     taskInputs: WomEvaluatedCallInputs,
                                     functions: IoFunctionSet,
                                     valueMapper: WomValue => WomValue): Try[String] = {
    val mappedInputs = taskInputs.map({case (k, v) => k.localName -> v})
    Try(commandPart.instantiate(mappedInputs, functions, valueMapper))
  }

  override def toString: String = s"[Task name=$name commandTemplate=$commandTemplate}]"
}
