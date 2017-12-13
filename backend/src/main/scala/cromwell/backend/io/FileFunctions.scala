package cromwell.backend.io

import wom.values.HashableString

object FileFunctions {
  def collectionName(name: String, collectionType: String) = s"$collectionType-${name.md5Sum}"
}
