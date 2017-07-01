package cromwell.core.path

/**
  * Implements bridges for code that hasn't been updated to use cromwell.core.path.Path.
  *
  * Before adding methods here, recommend checking out and trying [[cromwell.core.path.Path]].
  */
object Obsolete {
  def rm(path: Path) = path.delete(true)

  implicit class PathMethodAliases(val originalPath: Path) extends AnyVal {
    // Instead of `myPath.path`, just use `myPath`.
    def path: Path = originalPath
  }

  implicit class StringToPath(val path: String) extends AnyVal {
    def toFile: Path = DefaultPathBuilder.get(path)
  }

  type File = Path
  val File = ObsoleteFile

  object ObsoleteFile {
    def newTemporaryDirectory(prefix: String = ""): DefaultPath = {
      DefaultPath(better.files.File.newTemporaryDirectory(prefix).path)
    }
  }
}
