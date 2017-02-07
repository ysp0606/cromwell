package cromwell.backend.impl.jes.errors

import java.nio.file.Path

import cromwell.backend.impl.jes.RunStatus
import JesError._
import cromwell.backend.async.{AbortedExecutionHandle, ExecutionHandle, FailedNonRetryableExecutionHandle, FailedRetryableExecutionHandle}

import scala.util.Try
import scala.language.postfixOps

object JesError {
  private val KnownJesErrors = List(FailedToDelocalize, Preemption, UnexpectedTermination)

  def fromFailedStatus(failedStatus: RunStatus.Failed): JesError = {
    failedStatus.errorMessage flatMap { e => KnownJesErrors.toStream.find(_.isMatch(failedStatus.errorCode, e)) } getOrElse UnknownJesError(failedStatus.errorCode)
  }

  /**
    * JES sometimes embeds a JES specific error code in the message beyond the standard Google RPC error code,
    * so the error message might look like "10: Something bad happened". If this numeric code exists, parse it out.
    */
  def getJesErrorCode(errorMessage: String): Option[Int] = {
    Try { errorMessage.substring(0, errorMessage.indexOf(':')).toInt } toOption
  }

  def StandardException(errorCode: Int, message: String, jobTag: String) = {
    new RuntimeException(s"Task $jobTag failed: error code $errorCode. Message: $message")
  }
}

/**
  * FIXME:
  *
  * errorMessage is an option, but all known errors have one
  * jes code is an option
  * return code is an option
  *
  * error message needs to be able to be changed, even when Some
  *
  * do we care about stripping out the numeric code for the known errors which have one? i don't think so, so far
  * only the delocalizer is doing that. who cares.
  *
  * can have top level function to do the prettifying for "Message: ..." in the Some cases for error message
  *
  */

sealed abstract class JesError {
  def errorCode: Int
  def toExecutionHandle(message: String, jobReturnCode: Option[Int], jobTag: String, stderrPath: Option[Path]): ExecutionHandle
}

/**
  * "Unknown" JES errors in the sense that Cromwell isn't aware of them, there are several that we the humans
  * know exist but we're not handling them in any special way at the moment.
  */
private final case class UnknownJesError(errorCode: Int) extends JesError {
  override def toExecutionHandle(message: String, jobReturnCode: Option[Int], jobTag: String, stderrPath: Option[Path]): ExecutionHandle = {
    FailedNonRetryableExecutionHandle(StandardException(errorCode, message, jobTag), jobReturnCode)
  }
}

// FIXME ADTize?
sealed abstract class KnownJesError(val errorCode: Int, val jesCode: Option[Int]) extends JesError {
  def isMatch(otherErrorCode: Int, otherErrorMessage: String): Boolean = {
    val otherJesCode = getJesErrorCode(otherErrorMessage)
    (errorCode == otherErrorCode) && (jesCode == otherJesCode)
  }

  def toExecutionHandle(message: String, jobReturnCode: Option[Int], jobTag: String, stderrPath: Option[Path]): ExecutionHandle = {
    this match {
      case FailedToDelocalize => FailedNonRetryableExecutionHandle(FailedToDelocalizeFailure(message, jobTag, stderrPath), jobReturnCode)
      case Aborted => AbortedExecutionHandle
      case UnexpectedTermination | Preemption => FailedRetryableExecutionHandle(StandardException(errorCode, message, jobTag), jobReturnCode)
    }
  }
}

private case object Aborted extends KnownJesError(5, None)
private case object FailedToDelocalize extends KnownJesError(5, Option(10))
private case object UnexpectedTermination extends KnownJesError(10, Option(13))
private case object Preemption extends KnownJesError(10, Option(14))



