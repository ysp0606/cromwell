package cromwell.backend.impl.tes

import cats.syntax.validated._
import com.typesafe.config.Config
import cromwell.backend.MemorySize
import cromwell.backend.standard.StandardValidatedRuntimeAttributesBuilder
import cromwell.backend.validation._
import lenthall.validation.ErrorOr.ErrorOr
import wdl4s.values.{WdlString, WdlValue}

case class TesRuntimeAttributes(continueOnReturnCode: ContinueOnReturnCode,
                                dockerImage: String,
                                dockerWorkingDir: Option[String],
                                failOnStderr: Boolean,
                                cpu: Option[Int],
                                memory: Option[MemorySize],
                                disk: Option[MemorySize])

object TesRuntimeAttributes {

  val DockerWorkingDirKey = "dockerWorkingDir"
  val DiskSizeKey = "disk"

  private def cpuValidation: OptionalRuntimeAttributesValidation[Int] = CpuValidation.optional

  private def failOnStderrValidation(runtimeConfig: Option[Config]) = FailOnStderrValidation.default(runtimeConfig)

  private def continueOnReturnCodeValidation(runtimeConfig: Option[Config]) = ContinueOnReturnCodeValidation.default(runtimeConfig)

  private def diskSizeValidation: OptionalRuntimeAttributesValidation[MemorySize] = MemoryValidation.optional(DiskSizeKey)

  private def memoryValidation: OptionalRuntimeAttributesValidation[MemorySize] = MemoryValidation.optional(RuntimeAttributesKeys.MemoryKey)

  private val dockerValidation: RuntimeAttributesValidation[String] = DockerValidation.instance

  private val dockerWorkingDirValidation: OptionalRuntimeAttributesValidation[String] = DockerWorkingDirValidation.optional

  def runtimeAttributesBuilder(backendRuntimeConfig: Option[Config]): StandardValidatedRuntimeAttributesBuilder =
    StandardValidatedRuntimeAttributesBuilder.default(backendRuntimeConfig).withValidation(
      cpuValidation,
      memoryValidation,
      diskSizeValidation,
      dockerValidation,
      dockerWorkingDirValidation
    )

  def apply(validatedRuntimeAttributes: ValidatedRuntimeAttributes, backendRuntimeConfig: Option[Config]): TesRuntimeAttributes = {
    val docker: String = RuntimeAttributesValidation.extract(dockerValidation, validatedRuntimeAttributes)
    val dockerWorkingDir: Option[String] = RuntimeAttributesValidation.extractOption(dockerWorkingDirValidation.key, validatedRuntimeAttributes)
    val cpu: Option[Int] = RuntimeAttributesValidation.extractOption(cpuValidation.key, validatedRuntimeAttributes)
    val memory: Option[MemorySize] = RuntimeAttributesValidation.extractOption(memoryValidation.key, validatedRuntimeAttributes)
    val disk: Option[MemorySize] = RuntimeAttributesValidation.extractOption(diskSizeValidation.key, validatedRuntimeAttributes)
    val failOnStderr: Boolean =
      RuntimeAttributesValidation.extract(failOnStderrValidation(backendRuntimeConfig), validatedRuntimeAttributes)
    val continueOnReturnCode: ContinueOnReturnCode =
      RuntimeAttributesValidation.extract(continueOnReturnCodeValidation(backendRuntimeConfig), validatedRuntimeAttributes)

    new TesRuntimeAttributes(
      continueOnReturnCode,
      docker,
      dockerWorkingDir,
      failOnStderr,
      cpu,
      memory,
      disk
    )
  }
}

object DockerWorkingDirValidation {
  lazy val instance: RuntimeAttributesValidation[String] = new DockerWorkingDirValidation
  lazy val optional: OptionalRuntimeAttributesValidation[String] = instance.optional
}

class DockerWorkingDirValidation extends StringRuntimeAttributesValidation(TesRuntimeAttributes.DockerWorkingDirKey) {
  // NOTE: Docker's current test specs don't like WdlInteger, etc. auto converted to WdlString.
  override protected def validateValue: PartialFunction[WdlValue, ErrorOr[String]] = {
    case WdlString(value) => value.validNel
  }
}

