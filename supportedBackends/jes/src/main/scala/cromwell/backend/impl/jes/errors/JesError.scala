package cromwell.backend.impl.jes.errors

import java.nio.file.Path

import cromwell.backend.impl.jes.RunStatus
import JesError._
import cromwell.backend.async.{AbortedExecutionHandle, ExecutionHandle, FailedNonRetryableExecutionHandle, FailedRetryableExecutionHandle}

import scala.util.Try
import scala.language.postfixOps

object JesError {
  def apply(errorCode: Int,
            errorMessage: Option[String],
            jobReturnCode: Option[Int],
            stderr: Option[Path]): JesError = {
    val jesCode: Option[Int] = errorMessage flatMap getJesErrorCode

    (errorCode, jesCode) match {
      case (1, None) => Aborted(errorMessage, jobReturnCode)
      case (5, Some(10)) => FailedToDelocalize(errorMessage, jobReturnCode, stderr)
        // FIXME: What about including the attempt/max here and have e.g. RetryableUnexpectedTermination
      case (10, Some(13)) => UnexpectedTermination(errorCode, errorMessage, jobReturnCode)
      case (10, Some(14)) => Preemption(errorCode, errorMessage, jobReturnCode)
      case _ => UnknownJesError(errorCode, errorMessage, jobReturnCode)
    }
  }

  /**
    * JES sometimes embeds a JES specific error code in the message beyond the standard Google RPC error code,
    * so the error message might look like "10: Something bad happened". If this numeric code exists, parse it out.
    */
  def getJesErrorCode(errorMessage: String): Option[Int] = {
    Try { errorMessage.substring(0, errorMessage.indexOf(':')).toInt } toOption
  }

  def StandardException(errorCode: Int, message: String, jobTag: String) = {
    new RuntimeException(s"Task $jobTag failed: error code $errorCode.$message")
  }
}

sealed abstract class JesError {
  val errorMessage: Option[String]
  val jobReturnCode: Option[Int]

  def toExecutionHandle(jobTag: String, currentAttempt: Int = 0, maxAttempts: Int = 0): ExecutionHandle = {
    case a: Aborted => AbortedExecutionHandle
    case f: FailedToDelocalize => FailedNonRetryableExecutionHandle(FailedToDelocalizeFailure(prettyPrintedError, jobTag, f.stderr))
    case u: UnexpectedTermination => FailedRetryableExecutionHandle(StandardException(u.errorCode, prettyPrintedError, jobTag), jobReturnCode)
    case p: Preemption =>
      import lenthall.numeric.IntegerUtil._
      val preemptedMsg = s"Task $taskName was preempted for the ${currentAttempt.toOrdinal} time."
      if (currentAttempt < maxAttempts) {
          val errorMsg = s"""$preemptedMsg The call will be restarted with another preemptible VM (max preemptible attempts number is $maxAttempts).
                 |Error code ${p.errorCode}. Message: $errorMessage""".stripMargin
        FailedRetryableExecutionHandle(StandardException(p.errorCode, errorMsg, jobTag), jobReturnCode)

      } else {
        val errorMsg =  s"""$preemptedMsg The maximum number of preemptible attempts ($maxAttempts) has been reached. The call will be restarted with a non-preemptible VM.
                            |Error code ${p.errorCode}. Message: $errorMessage)""".stripMargin
        FailedNonRetryableExecutionHandle(StandardException(p.errorCode, errorMsg, jobTag), jobReturnCode)
      }
    case uje: UnknownJesError => FailedNonRetryableExecutionHandle(StandardException(uje.errorCode, prettyPrintedError, jobTag), jobReturnCode)
  }

  protected val prettyPrintedError: String = errorMessage map { e => s" Message: $e" } getOrElse ""
}

/**
 * "Unknown" JES errors in the sense that Cromwell isn't aware of them, there are several that we the humans
 * know exist but we're not handling them in any special way at the moment.
 */
final case class UnknownJesError(errorCode: Int, errorMessage: Option[String], jobReturnCode: Option[Int]) extends JesError

final case class FailedToDelocalize(errorMessage: Option[String], jobReturnCode: Option[Int], stderr: Option[Path]) extends JesError
final case class Aborted(errorMessage: Option[String], jobReturnCode: Option[Int]) extends JesError
final case class UnexpectedTermination(errorCode: Int, errorMessage: Option[String], jobReturnCode: Option[Int]) extends JesError
final case class Preemption(errorCode: Int, errorMessage: Option[String], jobReturnCode: Option[Int]) extends JesError
