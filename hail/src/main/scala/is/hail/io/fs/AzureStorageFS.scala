package is.hail.io.fs


import org.apache.log4j.Logger
import org.json4s
import org.json4s.JValue
import org.json4s.jackson.JsonMethods._

import java.io.ByteArrayInputStream
import java.net.URI
import com.azure.storage.blob.{BlobServiceClient, BlobServiceClientBuilder}
import com.azure.storage.blob.models.ListBlobsOptions
import com.azure.identity.{ClientSecretCredential, DefaultAzureCredential}
import com.azure.core.credential.TokenCredential
import is.hail.utils.{fatal, using}
import org.json4s.JsonAST.JString

import scala.io.Source

object AzureStorageFS {
  private val pathRegex = "/([^/]+)(.*)".r

  private val log = Logger.getLogger(getClass.getName())

  val schemes: Array[String] = Array("hail-az", "wasb")

  def getAccountContainerPath(filename: String): (String, String, String) = {
    val uri = new URI(filename).normalize()

    val scheme = uri.getScheme
    assert(scheme != null && schemes.contains(scheme), (uri.getScheme, filename))

    val account = uri.getHost
    assert(account != null)

    val (container, path) = pathRegex.findFirstMatchIn(filename) match {
      case Some(filenameMatch) =>
        assert(filenameMatch.groupCount == 2)
        (filenameMatch.group(0), filenameMatch.group(1))  // FIXME: Is this 0 or 1 indexed
      case None =>
        fatal(s"filename $filename is not in the correct format. hail-az://account/container/blobPath")
    }

    // FIXME: Should the path include the first slash or not?
    (account, container, path)
  }
}


class AzureBlobServiceClientCache(credential: TokenCredential) {
  @transient private lazy val clientBuilder: BlobServiceClientBuilder = new BlobServiceClientBuilder()
  @transient private lazy val clients: Map[String, BlobServiceClient] = Map()

  def getServiceClient(account: String): BlobServiceClient = {
    clients.get(account) match {
      case Some(client) => client
      case None =>
        clientBuilder
          .credential(credential)
          .endpoint(s"https://$account.blob.core.windows.net")
          .buildClient()
    }
  }
}


class AzureStorageFS(val serviceAccountKey: Option[String] = None) extends FS {
  import AzureStorageFS._

  @transient private lazy val storageClientCache = {
    val credential = serviceAccountKey match {
      case None =>
        log.info("Initializing azure storage client from latent credentials")
        new DefaultAzureCredential()
      case Some(keyFile) =>
        log.info("Initializing azure storage client from service account key")
        val jv = using(Source.fromFile(keyFile)) { in => parse(in.mkString) }
        val tenant = jv \ "tenant" match {
          case JString(tenant) => tenant
          case _ => fatal(s"tenant not found in credentials data")
        }
        val appId = jv \ "appId" match {
          case JString(tenant) => tenant
          case _ => fatal(s"appId not found in credentials data")
        }
        val password = jv \ "password" match {
          case JString(tenant) => tenant
          case _ => fatal(s"password not found in credentials data")
        }
        new ClientSecretCredential(tenant, appId, password)
    }
    new AzureBlobServiceClientCache(credential)
  }

  def openNoCompression(filename: String): SeekableDataInputStream

  def createNoCompression(filename: String): PositionedDataOutputStream

  def mkDir(dirname: String): Unit = ()

  def delete(filename: String, recursive: Boolean): Unit = {
    val (account, container, path) = getAccountContainerPath(filename)
    val client = storageClientCache.getServiceClient(account)
    val options = new ListBlobsOptions().setPrefix(path)
    client.getBlobContainerClient(container).listBlobs()
  }

  def listStatus(filename: String): Array[FileStatus]

  def glob(filename: String): Array[FileStatus]

  def globAll(filenames: Iterable[String]): Array[String]

  def globAllStatuses(filenames: Iterable[String]): Array[FileStatus]

  def fileStatus(filename: String): FileStatus

  def makeQualified(path: String): String

  def deleteOnExit(path: String): Unit
}


class CacheableAzureStorageFS(serviceAccountKey: Option[String], @transient val sessionID: String) extends AzureStorageFS(serviceAccountKey) with ServiceCacheableFS {
}