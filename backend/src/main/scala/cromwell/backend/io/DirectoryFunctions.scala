package cromwell.backend.io

import cats.instances.list._
import cats.syntax.traverse._
import common.validation.ErrorOr.ErrorOr
import cromwell.backend.BackendJobDescriptor
import wom.expression.IoFunctionSet
import wom.graph.TaskCallNode
import wom.values.WomSingleDirectory

trait DirectoryFunctions extends IoFunctionSet {
  def findDirectoryOutputs(call: TaskCallNode, jobDescriptor: BackendJobDescriptor): ErrorOr[List[WomSingleDirectory]] =
    call.callable.outputs.flatTraverse[ErrorOr, WomSingleDirectory] { outputDefinition =>
      outputDefinition.expression.evaluateFiles(jobDescriptor.localInputs, this, outputDefinition.womType) map {
        _.toList collect { case directory: WomSingleDirectory => directory }
      }
    }
}

object DirectoryFunctions {
  def directoryName(directory: String) = FileFunctions.collectionName(directory, "dir")
}
