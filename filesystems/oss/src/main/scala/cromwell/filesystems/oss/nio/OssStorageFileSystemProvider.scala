package cromwell.filesystems.oss.nio

import java.io.{BufferedOutputStream, OutputStream}
import java.net.URI
import java.nio.channels.SeekableByteChannel
import java.nio.file._
import java.nio.file.attribute.{BasicFileAttributeView, BasicFileAttributes, FileAttribute, FileAttributeView}
import java.nio.file.spi.FileSystemProvider
import java.util

import com.aliyun.oss.OSSClient
import com.aliyun.oss.model.{CompleteMultipartUploadRequest, GenericRequest, InitiateMultipartUploadRequest}
import com.aliyun.oss.model.{InitiateMultipartUploadResult, ListObjectsRequest, ObjectMetadata, PartETag}
import com.aliyun.oss.model.{UploadPartCopyRequest, UploadPartCopyResult}
import com.google.common.collect.AbstractIterator

import scala.collection.JavaConverters._
import scala.collection.immutable.Set
import collection.mutable.ArrayBuffer

object OssStorageFileSystemProvider{
  val maxCopySize: Long = 1 * 1024 * 1024 * 1024//1GB
  val partSize: Long = 10 * 1024 * 1024//10MB
  var providers: Map[String, OssStorageFileSystemProvider] = Map()
  var filesystems: Map[String, OssStorageFileSystem] = Map()

  def formConfig(config: OssStorageConfiguration): OssStorageFileSystemProvider = {
    val key = getProviderKey(config)
    if (providers.contains(key)) {
      providers.apply(key)
    } else {
      val provider = new OssStorageFileSystemProvider(config)
      providers += (key -> provider)
      provider
    }
  }

  def getProviderKey(config: OssStorageConfiguration): String = {
    val accessKey = config.accessId
    if (accessKey == null) {
      throw new IllegalArgumentException("AccessKey is null")
    }
    val endPoint = config.endpoint
    if (endPoint == null) {
      throw new IllegalArgumentException("AccessKey is null")
    }

    (accessKey + "@" + endPoint)
  }
}

final case class OssStorageFileSystemProvider(config: OssStorageConfiguration) extends FileSystemProvider {
  def ossClient: OSSClient = config.newOssClient()
  class PathIterator(ossClient: OSSClient, prefix: OssStoragePath, filter: DirectoryStream.Filter[_ >: Path]) extends AbstractIterator[Path] {
    var nextMarker: Option[String] = None

    var iterator: Iterator[String] = Iterator()

    override def computeNext(): Path = {
      if (!iterator.hasNext) {
        nextMarker match {
          case None => iterator = listNext("")
          case Some(marker: String) if !marker.isEmpty => iterator = listNext(marker)
          case Some(marker: String) if marker.isEmpty  => iterator = Iterator()
          case Some(null) => iterator = Iterator()
        }
      }


      if (iterator.hasNext) {
        val path = OssStoragePath.getPath(prefix.getFileSystem, iterator.next())
        if (filter.accept(path)) {
          path
        } else {
          computeNext()
        }
      } else {
        endOfData()
      }
    }

    private[this] def listNext(marker: String): Iterator[String] = {
      val objectListing = OssStorageRetry.from(
        () => {
          val listObjectRequest = new ListObjectsRequest(prefix.bucket)
          listObjectRequest.setDelimiter(UnixPath.SEPARATOR.toString)
          listObjectRequest.setPrefix(prefix.key)
          listObjectRequest.setMarker(marker)

          ossClient.listObjects(listObjectRequest)
        }
      )

      val result = ArrayBuffer.empty[String]

      objectListing.getObjectSummaries.asScala.filterNot(_.equals(prefix.key)).foreach(obj => {result append obj.getKey.stripPrefix(prefix.key)})
      objectListing.getCommonPrefixes.asScala.filterNot(_.equals(prefix.key)).foreach(obj => {result append obj.stripPrefix(prefix.key)})

      nextMarker = Some(objectListing.getNextMarker)
      result.iterator
    }
  }

  class OssStorageDirectoryStream(ossClient: OSSClient, prefix: OssStoragePath, filter: DirectoryStream.Filter[_ >: Path]) extends DirectoryStream[Path] {

    override def iterator(): util.Iterator[Path] = new PathIterator(ossClient, prefix, filter)

    override def close(): Unit = {}

  }

  override def getScheme: String = OssStorageFileSystem.URI_SCHEMA

  override def newFileSystem(uri: URI, env: util.Map[String, _]): OssStorageFileSystem = {
    if (uri.getScheme != getScheme) {
      throw new IllegalArgumentException(s"Schema ${uri.getScheme} not match")
    }

    val bucket = uri.getHost
    if (bucket.isEmpty) {
      throw new IllegalArgumentException(s"Bucket is empty")
    }

    if (uri.getPort != -1) {
      throw new IllegalArgumentException(s"Port is not permitted")
    }
    val key = getFileSystemKey(bucket, config)
    if (OssStorageFileSystemProvider.filesystems.contains(key)) {
      throw new FileSystemAlreadyExistsException("File system " + uri.getScheme + ':' + key + " already exists")
    }
    // create the filesystem
    val fileSystem = OssStorageFileSystem(this, bucket, config)
    OssStorageFileSystemProvider.filesystems += (key -> fileSystem)
    return fileSystem
  }

  def getFileSystem(uri: URI, env: java.util.Map[String, _]): OssStorageFileSystem = {
    if (uri.getScheme != getScheme) {
      throw new IllegalArgumentException(s"Schema ${uri.getScheme} not match")
    }

    val bucket = uri.getHost
    if (bucket.isEmpty) {
      throw new IllegalArgumentException(s"Bucket is empty")
    }

    val key = getFileSystemKey(bucket)
    if (OssStorageFileSystemProvider.filesystems.contains(key)) {
      OssStorageFileSystemProvider.filesystems.apply(key)
    } else {
      newFileSystem(uri, env)
    }
  }

  override def getFileSystem(uri: URI): OssStorageFileSystem = {
    if (uri.getScheme != getScheme) {
      throw new IllegalArgumentException(s"Schema ${uri.getScheme} not match")
    }

    val bucket = uri.getHost
    if (bucket.isEmpty) {
      throw new IllegalArgumentException(s"Bucket is empty")
    }

    val key = getFileSystemKey(bucket)
    if (OssStorageFileSystemProvider.filesystems.contains(key)) {
      return OssStorageFileSystemProvider.filesystems.apply(key)
    }

    throw new FileSystemNotFoundException("OSS filesystem not yet created. Use newFileSystem() instead")
  }

  protected def getFileSystemKey(bucket: String, config: OssStorageConfiguration): String = {

    val accessKey = config.accessId
    if (accessKey == null) {
      throw new IllegalArgumentException("AccessKey is null")
    }
    val endPoint = config.endpoint
    if (endPoint == null) {
      throw new IllegalArgumentException("AccessKey is null")
    }

    (accessKey + "@" + bucket + "@" + endPoint)
  }

  private def getFileSystemKey(bucket: String):String = getFileSystemKey(bucket, config)

  override def getPath(uri: URI): OssStoragePath = {
    OssStoragePath.getPath(getFileSystem(uri), uri.getPath)
  }

  override def newOutputStream(path: Path, options: OpenOption*): OutputStream = {
    if (!path.isInstanceOf[OssStoragePath]) {
      throw new ProviderMismatchException(s"Not a oss storage path $path")
    }

    val len = options.length

    var opts = Set[OpenOption]()
    if (len == 0) {
      opts = Set(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    } else {
      for (opt <- options) {
        if (opt == StandardOpenOption.READ) {
          throw new IllegalArgumentException("READ not allowed")
        }

        opts += opt
      }
    }

    opts += StandardOpenOption.WRITE
    val ossStream = OssAppendOutputStream(ossClient, path.asInstanceOf[OssStoragePath], true)

    new BufferedOutputStream(ossStream, 256*1024)
  }

  override def newByteChannel(path: Path, options: util.Set[_ <: OpenOption], attrs: FileAttribute[_]*): SeekableByteChannel = {
    if (!path.isInstanceOf[OssStoragePath]) {
      throw new ProviderMismatchException(s"Not a oss storage path $path")
    }

    for (opt <- options.asScala) {
      opt match {
        case StandardOpenOption.READ =>
        case StandardOpenOption.WRITE => throw new IllegalArgumentException(s"WRITE byte channel not allowed currently, $path")
        case StandardOpenOption.SPARSE | StandardOpenOption.TRUNCATE_EXISTING =>
        case StandardOpenOption.APPEND | StandardOpenOption.CREATE | StandardOpenOption.DELETE_ON_CLOSE |
             StandardOpenOption.CREATE_NEW | StandardOpenOption.DSYNC | StandardOpenOption.SYNC => throw new UnsupportedOperationException()
      }
    }

    OssFileReadChannel(ossClient, 0, path.asInstanceOf[OssStoragePath])
  }

  def doesObjectExist(bucket: String, name: String): Boolean = {
    val req = new GenericRequest(bucket, name)
    req.setLogEnabled(false)
    ossClient.doesBucketExist(req)
  }

  override def createDirectory(dir: Path, attrs: FileAttribute[_]*): Unit = {}

  override def deleteIfExists(path: Path): Boolean = {
    val ossPath = OssStoragePath.checkPath(path)

    if (ossPath.seemsLikeDirectory) {
      if (headPrefix(ossPath)) {
        throw new UnsupportedOperationException("Can not delete a non-empty directory")
      }

      return true
    }

    val exist = OssStorageRetry.from(
      () => {
        val request = new GenericRequest(ossPath.bucket, ossPath.key)
        request.setLogEnabled(false)
        ossClient.doesObjectExist(request)
      }
    )

    if (!exist) {
      return false
    }

    OssStorageRetry.from(
      () => ossClient.deleteObject(ossPath.bucket, ossPath.key)
    )

    true
  }

  override def delete(path: Path): Unit = {
    if (!deleteIfExists(path)) {
      throw new NoSuchFileException(s"File $path not exists")
    }
  }

  /*
   * XXX: Can only copy files whose size is below 1GB currently.
   */

  override def copy(source: Path, target: Path, options: CopyOption*): Unit = {
    val srcOssPath = OssStoragePath.checkPath(source)
    val targetOssPath= OssStoragePath.checkPath(target)

    // ignore all options currently.
    if (srcOssPath == targetOssPath) {
      return
    }

    val objectMetadata: ObjectMetadata = ossClient.getObjectMetadata (srcOssPath.bucket, srcOssPath.key)
    // get object length
    val contentLength: Long = objectMetadata.getContentLength
    if (contentLength < OssStorageFileSystemProvider.maxCopySize ) {
      val _ = OssStorageRetry.from(
        () => ossClient.copyObject(srcOssPath.bucket, srcOssPath.key, targetOssPath.bucket, targetOssPath.key)
      )
    } else {
      val _ = OssStorageRetry.from(
        () => copyLargeObject(srcOssPath, targetOssPath)
      )
    }
  }

  override def move(source: Path, target: Path, options: CopyOption*): Unit = {
    copy(source, target, options: _*)

    val _ = deleteIfExists(source)
  }

  override def isSameFile(path: Path, path2: Path): Boolean = {
    OssStoragePath.checkPath(path).equals(OssStoragePath.checkPath(path2))
  }

  override def isHidden(path: Path): Boolean = {
    false
  }

  override def getFileStore(path: Path): FileStore = throw new UnsupportedOperationException()

  override def checkAccess(path: Path, modes: AccessMode*): Unit = {
    for (mode <- modes) {
      mode match {
        case AccessMode.READ | AccessMode.WRITE =>
        case AccessMode.EXECUTE => throw new AccessDeniedException(mode.toString)
      }
    }

    val ossPath = OssStoragePath.checkPath(path)
    // directory always exists.
    if (ossPath.seemsLikeDirectory) {
      return
    }

    val exist = OssStorageRetry.from(
      () => {
        val request = new GenericRequest(ossPath.bucket, ossPath.key)
        request.setLogEnabled(false)
        ossClient.doesObjectExist(request)
      }
    )

    if (!exist) {
      throw new NoSuchFileException(path.toString)
    }
  }

  override def getFileAttributeView[V <: FileAttributeView](path: Path, `type`: Class[V], options: LinkOption*): V = {
    if (`type` != classOf[OssStorageFileAttributesView] && `type` != classOf[BasicFileAttributeView] ) {
      throw new UnsupportedOperationException(`type`.getSimpleName)
    }

    val ossPath = OssStoragePath.checkPath(path)

    OssStorageFileAttributesView(ossClient, ossPath).asInstanceOf[V]
  }

  override def readAttributes(path: Path, attributes: String, options: LinkOption*): util.Map[String, AnyRef] = {
    throw new UnsupportedOperationException()
  }

  override def readAttributes[A <: BasicFileAttributes](path: Path, `type`: Class[A], options: LinkOption*): A = {
    if (`type` != classOf[OssStorageFileAttributes] && `type` != classOf[BasicFileAttributes] ) {
      throw new UnsupportedOperationException(`type`.getSimpleName)
    }

    val ossPath = OssStoragePath.checkPath(path)

    if (ossPath.seemsLikeDirectory) {
      return new OssStorageDirectoryAttributes(ossPath).asInstanceOf[A]
    }

    val exists = OssStorageRetry.from(
      () => {
        val request = new GenericRequest(ossPath.bucket, ossPath.key)
        request.setLogEnabled(false)
        ossClient.doesObjectExist(request)
      }
    )

    if (!exists) {
      throw new NoSuchFileException(ossPath.toString)
    }

    val objectMeta = OssStorageRetry.from(
      () => ossClient.getObjectMetadata(ossPath.bucket, ossPath.key)
    )

    OssStorageObjectAttributes(objectMeta, ossPath).asInstanceOf[A]
  }

  override def newDirectoryStream(dir: Path, filter: DirectoryStream.Filter[_ >: Path]): DirectoryStream[Path] = {
    val ossPath = OssStoragePath.checkPath(dir)

    new OssStorageDirectoryStream(ossClient, ossPath, filter)
  }

  override def setAttribute(path: Path, attribute: String, value: scala.Any, options: LinkOption*): Unit = throw new UnsupportedOperationException()

  private[this] def headPrefix(path: Path): Boolean = {
    val ossPath = OssStoragePath.checkPath(path)

    val listRequest = new ListObjectsRequest(ossPath.bucket)
    listRequest.setPrefix(ossPath.key)

    val listResult = OssStorageRetry.from(
      () => ossClient.listObjects(listRequest)
    )

    listResult.getObjectSummaries.iterator().hasNext
  }

  private[this] def copyLargeObject(src: OssStoragePath, dst: OssStoragePath): Unit = {

    val objectMetadata: ObjectMetadata = ossClient.getObjectMetadata (src.bucket, src.key)
    // get object length
    val contentLength: Long = objectMetadata.getContentLength

    // set part size 10MB。
    val partSize: Long = OssStorageFileSystemProvider.partSize

    // get the total part count
    var partCount: Int = (contentLength / partSize).toInt
    if (contentLength % partSize != 0) {
      partCount += 1
    }

    // init copy task
    val initiateMultipartUploadRequest: InitiateMultipartUploadRequest = new InitiateMultipartUploadRequest (dst.bucket, dst.key)
    val initiateMultipartUploadResult: InitiateMultipartUploadResult = ossClient.initiateMultipartUpload (initiateMultipartUploadRequest)
    val uploadId: String = initiateMultipartUploadResult.getUploadId

    // copy by part
    val partETags: util.List[PartETag] = new util.ArrayList[PartETag]
    var i: Int = 0
    while (i < partCount) {
      // get the part size
      val skipBytes: Long = partSize * i
      val size: Long = if (partSize < contentLength - skipBytes) partSize else (contentLength - skipBytes)
      // create and set UploadPartCopyRequest
      val uploadPartCopyRequest: UploadPartCopyRequest = new UploadPartCopyRequest(src.bucket, src.key, dst.bucket, dst.key)
      uploadPartCopyRequest.setUploadId(uploadId)
      uploadPartCopyRequest.setPartSize(size)
      uploadPartCopyRequest.setBeginIndex(skipBytes)
      uploadPartCopyRequest.setPartNumber(i + 1)
      val uploadPartCopyResult: UploadPartCopyResult = ossClient.uploadPartCopy(uploadPartCopyRequest)
      // save the ETags
      partETags.add(uploadPartCopyResult.getPartETag)
      i += 1
    }

    // commit the upload request
    val completeMultipartUploadRequest: CompleteMultipartUploadRequest = new CompleteMultipartUploadRequest (dst.bucket, dst.key, uploadId, partETags)
    val _ = ossClient.completeMultipartUpload(completeMultipartUploadRequest)
  }
}
