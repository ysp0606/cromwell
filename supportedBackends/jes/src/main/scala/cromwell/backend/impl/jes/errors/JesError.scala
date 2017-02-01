package cromwell.backend.impl.jes.errors

import java.nio.file.Path

import cromwell.backend.impl.jes.RunStatus
import JesError._

import scala.util.{Failure, Success, Try}

object JesError {
  // List of known JES errors
  private val KnownErrors = List(FailedToDelocalize, Preemption, UnexpectedTermination)

  /**
    * The error message looks like "10: Something bad happened"
    * Extract 10 as an inner error code and then the rest of the message
    */
  def splitInnerCodeAndMessage(errorMessage: String): (Option[Int], String) = {
    Try {
      val sep = errorMessage.indexOf(':')
      val innerCode = errorMessage.substring(0, sep).toInt
      val message = errorMessage.substring(sep + 1, errorMessage.length - 1).trim
      (innerCode, message)
    } match {
      case Success((code, message)) => (Option(code), message)
      case Failure(_) => (None, errorMessage)
    }
  }

  def fromFailedStatus(failedStatus: RunStatus.Failed): JesError = {
    def lookupError(innerCode: Option[Int], message: String) = KnownErrors.toStream find { _.isMatch(failedStatus.errorCode, innerCode) }

    // See if there was a KnownJesError which matches this, return the match or an UnknownJesError if none were found
    val knownJesError = failedStatus.errorMessage flatMap splitInnerCodeAndMessage flatMap Function.tupled(lookupError)
    knownJesError.getOrElse(UnknownJesError(failedStatus.errorCode))
  }

  def StandardException(errorCode: Int, message: String, jobTag: String) = {
    new RuntimeException(s"Task $jobTag failed: error code $errorCode. Message: $message")
  }
}

sealed abstract class JesError {
  def errorCode: Int
  def toException(message: String, jobTag: String, stderrPath: Option[Path]): Exception

  // FIXME: make the ismatch just take a string and start here
}

final case class UnknownJesError(errorCode: Int) extends JesError {
  override def toException(message: String, jobTag: String, stderrPath: Option[Path]): Exception = {
    StandardException(errorCode, message, jobTag)
  }
}

// FIXME - I made the inner an option - fix isMatch & ADTize?
sealed abstract class KnownJesError(val errorCode: Int, val JesCode: Option[Int]) extends JesError {
  def isMatch(otherErrorCode: Int, otherJesCode: Int): Boolean = (errorCode == otherErrorCode) && (JesCode == otherJesCode)

  def toException(message: String, jobTag: String, stderrPath: Option[Path]): Exception = {
    this match {
      case FailedToDelocalize => FailedToDelocalizeFailure(message, jobTag, stderrPath)
      case Aborted => ???
      case _ => StandardException(errorCode, message, jobTag)
    }
  }
}

private case object InvalidValue extends KnownJesError(3, None)
private case object Aborted extends KnownJesError(5, None)
private case object NoMachinesAvailable extends KnownJesError(5, None)
private case object FailedToPullImage extends KnownJesError(5, Option(8))
private case object FailedToLocalize extends KnownJesError(5, Option(9))
private case object FailedToDelocalize extends KnownJesError(5, Option(10))
private case object Forbidden extends KnownJesError(7, None)
private case object QuotaExceeded extends KnownJesError(7, None)
private case object DockerRunFailed extends KnownJesError(10, Option(11))
private case object UnexpectedTermination extends KnownJesError(10, Option(13))
private case object Preemption extends KnownJesError(10, Option(14))
private case object GsutilFailed extends KnownJesError(10, Option(15))
private case object InvalidCredentials extends KnownJesError(16, None)
