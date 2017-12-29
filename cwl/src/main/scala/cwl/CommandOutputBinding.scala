package cwl

import cats.implicits._
import common.validation.ErrorOr._
import common.validation.Validation._
import wom.expression.IoFunctionSet
import wom.types._
import wom.values._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

/** @see <a href="http://www.commonwl.org/v1.0/Workflow.html#CommandOutputBinding">CommandOutputBinding</a> */
case class CommandOutputBinding(
                                 glob: Option[Glob] = None,
                                 loadContents: Option[Boolean] = None,
                                 outputEval: Option[StringOrExpression] = None) {

  /**
    * Returns the list of globs for this command output binding.
    */
  def primaryCwlGlobPaths(parameterContext: ParameterContext): ErrorOr[List[String]] = {
    glob map { globValue =>
      globValue.fold(GlobEvaluator.GlobEvaluatorPoly).apply(parameterContext)
    } getOrElse {
      Nil.valid
    }
  }

  /**
    * Returns the list of secondary files for the primary file.
    */
  def secondaryFromPrimaryFile(parameterContext: ParameterContext,
                               primaryWomFile: WomFile,
                               secondaryFilesOption: Option[SecondaryFiles]): ErrorOr[List[WomFile]] = {
    secondaryFilesOption.map(
      _
        .fold(CommandLineTool.CommandOutputParameter.SecondaryFilesPoly)
        .apply(primaryWomFile, parameterContext)
    ).getOrElse(Nil.valid)
  }

  /**
    * Returns all the primary and secondary files that _will be_ created by this command output binding.
    */
  def primaryAndSecondaryFiles(inputValues: Map[String, WomValue],
                               ioFunctionSet: IoFunctionSet,
                               outputWomType: WomType,
                               secondaryFilesOption: Option[SecondaryFiles]): ErrorOr[List[WomFile]] = {
    val parameterContext = ParameterContext(inputs = inputValues)
    val womSingleDirectoryOrFileType = outputWomType match {
      case WomSingleDirectoryType => WomSingleDirectoryType
      case WomArrayType(WomSingleDirectoryType) => WomSingleDirectoryType
      case _ => WomSingleFileType
    }

    val primaryPathsErrorOr = primaryCwlGlobPaths(parameterContext)
    primaryPathsErrorOr flatMap { primaryPaths =>
      val primary: List[WomFile] = womSingleDirectoryOrFileType match {
        case WomSingleFileType => primaryPaths.map(WomGlobFile)
        case WomSingleDirectoryType => primaryPaths.map(WomSingleDirectory(_))
      }
      val secondaryErrorOr: ErrorOr[List[WomFile]] =
        primary.flatTraverse(secondaryFromPrimaryFile(parameterContext, _, secondaryFilesOption))

      secondaryErrorOr.map(primary ++ _)
    }
  }

  /**
    * Loads a directory with the files listed, each file with contents populated.
    */
  def loadDirectoryWithListing(path: String, ioFunctionSet: IoFunctionSet): ErrorOr[WomSingleDirectory] = {
    ioFunctionSet.listAllFilesUnderDirectory(path).toList traverse loadFileWithContents(ioFunctionSet) map {
      listing => WomSingleDirectory(path, Option(listing))
    }
  }

  /**
    * Loads a file at path reading 64KiB of data into the contents.
    */
  def loadFileWithContents(ioFunctionSet: IoFunctionSet)(path: String): ErrorOr[WomSingleFile] = {
    val contentsOptionErrorOr = {
      if (loadContents getOrElse false) {
        load64KiB(path, ioFunctionSet).map(Option(_))
      } else {
        None.valid
      }
    }
    contentsOptionErrorOr.map(contentsOption => WomSingleFile(value = path, contentsOption = contentsOption))
  }

  /**
    * Given a cwl glob path and an output type, gets the listing using the ioFunctionSet, and optionally loads the
    * contents of the file(s).
    */
  def getFilesWithContents(ioFunctionSet: IoFunctionSet,
                           outputWomType: WomType)(cwlPath: String): ErrorOr[List[WomSingleDirectoryOrFile]] = {
    /*
    For each file matched in glob, read up to the first 64 KiB of text from the file and place it in the contents field
    of the file object for manipulation by outputEval.
     */
    val womSingleDirectoryOrFileType = outputWomType match {
      case WomSingleDirectoryType => WomSingleDirectoryType
      case WomArrayType(WomSingleDirectoryType) => WomSingleDirectoryType
      case _ => WomSingleFileType
    }
    womSingleDirectoryOrFileType match {
      case WomSingleDirectoryType =>
        // Even if multiple directories are _somehow_ requested, a single flattened directory is returned.
        loadDirectoryWithListing(cwlPath, ioFunctionSet).map(List(_))
      case WomSingleFileType =>
        ioFunctionSet.glob(cwlPath).toList traverse loadFileWithContents(ioFunctionSet)
    }
  }

  /**
    * Creates a wom array with the appropriate concrete member type based on the list of directory or files.
    *
    * If the array is empty, creates an array of WomSingleFile.
    */
  def toWomFilesArray(womDirectoryOrFiles: List[WomSingleDirectoryOrFile]): WomArray = {
    val womArrayType = WomArrayType(womDirectoryOrFiles.headOption.map(_.womType).getOrElse(WomSingleFileType))
    WomArray(womArrayType, womDirectoryOrFiles)
  }

  /**
    * Generates an output wom value based on the specification in command output binding.
    *
    * Depending on the outputWomType, the following steps will be applied as specified in the CWL spec:
    * 1. glob: get a list the globbed files as our primary files
    * 2. loadContents: load the contents of the primary files
    * 3. outputEval: pass in the primary files to an expression to generate our return value
    * 4. secondaryFiles: just before returning the value, fill in the secondary files on the return value
    *
    * The result type will be coerced to the output type.
    */
  def generateOutputWomValue(inputValues: Map[String, WomValue],
                             ioFunctionSet: IoFunctionSet,
                             outputWomType: WomType,
                             secondaryFilesCoproduct: Option[SecondaryFiles],
                             formatCoproduct: Option[StringOrExpression]): ErrorOr[WomValue] = {
    val parameterContext = ParameterContext(inputs = inputValues)

    def evaluateWomValue(womFilesArray: WomArray): ErrorOr[WomValue] = {
      outputEval match {
        case Some(StringOrExpression.String(string)) => WomString(string).valid
        case Some(StringOrExpression.Expression(expression)) =>
          val outputEvalParameterContext = parameterContext.copy(self = womFilesArray)
          expression.fold(EvaluateExpression).apply(outputEvalParameterContext)
        case None =>
          womFilesArray.valid
      }
    }

    def populateWomValue(evaluatedWomValue: WomValue): ErrorOr[WomValue] = {
      evaluatedWomValue match {
        case womSingleFile: WomSingleFile =>
          val secondaryFilesErrorOr = secondaryFromPrimaryFile(parameterContext, womSingleFile, secondaryFilesCoproduct)
          val formatOptionErrorOr = formatCoproduct.traverse[ErrorOr, String](
            _.fold(CommandLineTool.CommandOutputParameter.FormatPoly).apply(parameterContext)
          )

          val secondaryDirectoryOrFilesErrorOr = secondaryFilesErrorOr flatMap {
            _.traverse[ErrorOr, WomSingleDirectoryOrFile] { womFile =>
              validate {
                womFile.asInstanceOf[WomSingleDirectoryOrFile]
              }
            }
          }

          (secondaryDirectoryOrFilesErrorOr, formatOptionErrorOr) mapN { (secondaryFiles, formatOption) =>
            womSingleFile.copy(secondaryFiles = secondaryFiles, formatOption = formatOption)
          }

        case womArray: WomArray if womArray.womType.memberType == WomSingleFileType =>
          val formatOptionErrorOr = formatCoproduct.traverse[ErrorOr, String](
            _.fold(CommandLineTool.CommandOutputParameter.FormatPoly).apply(parameterContext)
          )
          formatOptionErrorOr map { formatOption =>
            womArray.map(_.asInstanceOf[WomSingleFile].copy(formatOption = formatOption))
          }

        case womValue: WomValue => womValue.valid
      }
    }

    def coerceWomValue(populatedWomValue: WomValue): ErrorOr[WomValue] = {
      (outputWomType, populatedWomValue) match {
        case (womType: WomArrayType, womValue: WomArray) => womType.coerceRawValue(womValue).toErrorOr
        case (womType: WomArrayType, womValue) =>
          // Coerce a single value to an array
          womType.coerceRawValue(womValue).toErrorOr.map(value => WomArray(womType, List(value)))
        case (womType, womValue: WomArray) if womValue.value.lengthCompare(1) == 0 =>
          // Coerce an array to a single value
          womType.coerceRawValue(womValue.value.head).toErrorOr
        case (womType, womValue) => womType.coerceRawValue(womValue).toErrorOr
      }
    }

    for {
      // 1. glob: get a list the globbed files as our primary files
      primaryPaths <- primaryCwlGlobPaths(parameterContext)

      // 2. loadContents: load the contents of the primary files
      primaryAsDirectoryOrFiles <- primaryPaths.flatTraverse(getFilesWithContents(ioFunctionSet, outputWomType))
      womFilesArray = toWomFilesArray(primaryAsDirectoryOrFiles)

      // 3. outputEval: pass in the primary files to an expression to generate our return value
      evaluatedWomValue <- evaluateWomValue(womFilesArray)

      // 4. secondaryFiles: just before returning the value, fill in the secondary files on the return value
      populatedWomValue <- populateWomValue(evaluatedWomValue)

      // CWL tells us the type this output is expected to be. Attempt to coerce the actual output into this type.
      coercedWomValue <- coerceWomValue(populatedWomValue)
    } yield coercedWomValue
  }

  private def load64KiB(path: String, ioFunctionSet: IoFunctionSet): ErrorOr[String] = {
    // This suggests the IoFunctionSet should have a length-limited read API as both CWL and WDL support this concept.
    // ChrisL: But remember that they are different (WDL => failure, CWL => truncate)
    val content = ioFunctionSet.readFile(path)

    // TODO: WOM: propagate Future (or IO?) signature
    // TODO: WOM: Stream only the first 64 KiB, this "read everything then ignore most of it" method is terrible
    validate {
      val initialResult = Await.result(content, 5 seconds)
      initialResult.substring(0, Math.min(initialResult.length, 64 * 1024))
    }
  }
}
