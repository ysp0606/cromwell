package cromwell.backend.validation

object RuntimeAttributesKeys {
  val DockerKey = "docker"
  val FailOnStderrKey = "failOnStderr"
  val ContinueOnReturnCodeKey = "continueOnReturnCode"
  val CpuKey = "cpu"
  val MemoryKey = "memory"
  val StdoutRedirect = "stdoutRedirect"
  val StderrRedirect = "stderrRedirect"
}
