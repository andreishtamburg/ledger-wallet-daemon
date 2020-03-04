package co.ledger.wallet.daemon.clients

import java.net.URL

import co.ledger.wallet.daemon.configurations.DaemonConfiguration
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalNotification}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.service.{Backoff, RetryBudget}
import com.twitter.finagle.{Http, Service}
import com.twitter.inject.Logging
import com.twitter.util.Duration

import scala.util.Try

class ScalaHttpClientPool extends Logging {

  import ScalaHttpClientPool._

  // FIXME : Make configurable cache pool ttl and cache size
  private val connectionPools: LoadingCache[Host, Service[Request, Response]] =
    CacheBuilder.newBuilder()
      .maximumSize(50)
      .removalListener((notification: RemovalNotification[Host, Service[Request, Response]]) => {
        info(s"Connection pool for Host ${notification.getKey} has been removed from cache due to " +
          s"${notification.getCause.name} cause, was evicted = ${notification.wasEvicted}")
        notification.getValue.close()
      })
      .build[Host, Service[Request, Response]](new CacheLoader[Host, Service[Request, Response]] {
        def load(host: Host): Service[Request, Response] = {
          serviceFor(host) match {
            case Right(service) => service
            case Left(th) => throw th
          }
        }
      })

  def poolCacheSize: Long = connectionPools.size()

  def isHostCached(host: Host): Boolean = connectionPools.asMap().containsKey(host)

  def execute(host: Host, request: Request): com.twitter.util.Future[Response] =
    connectionPools.get(host)(request).map(response => {
      info(s"Received from ${request.uri} status=${response.status.code} error=${isOnError(response.status.code)} - statusText=${response.status.reason}")
      response
    })
}

object ScalaHttpClientPool {
  val PROXY_BUFFER_SIZE: Int = 4 * 4096
  val HTTP_DEFAULT_PORT: Int = 80
  val HTTPS_DEFAULT_PORT: Int = 443

  case class Host(hostName: String, protocol: String, port: Int)

  private val budget: RetryBudget = RetryBudget(
    ttl = Duration.fromSeconds(DaemonConfiguration.explorer.client.retryTtl),
    minRetriesPerSec = DaemonConfiguration.explorer.client.retryMin,
    percentCanRetry = DaemonConfiguration.explorer.client.retryPercent
  )
  // FIXME Client sharing ?
  private val client = Http.client
    .withRetryBudget(budget)
    .withRetryBackoff(Backoff.linear(
      Duration.fromMilliseconds(DaemonConfiguration.explorer.client.retryBackoff),
      Duration.fromMilliseconds(DaemonConfiguration.explorer.client.retryBackoff)))
    .withSessionPool.maxSize(DaemonConfiguration.explorer.client.connectionPoolSize)
    .withSessionPool.ttl(Duration.fromSeconds(DaemonConfiguration.explorer.client.connectionTtl))

  def serviceFor(host: Host): Either[Throwable, Service[Request, Response]] = {
    Try(DaemonConfiguration.proxy match {
      case Some(proxy) => tls(host, client).withTransport.httpProxyTo(s"${host.hostName}:${host.port}")
        .newService(s"${proxy.host}:${proxy.port}")
      case None => tls(host, client).newService(s"${host.hostName}:${host.port}")
    }).toEither
  }

  def tls(host: Host, client: Http.Client): Http.Client =
    host.protocol match {
      case "https" => client.withTls(host.hostName)
      case _ => client
    }

  def resolvePort(url: URL): Int = url.getPort match {
    case port if port > 0 => port
    case _ => url.getProtocol match {
      case "https" => HTTPS_DEFAULT_PORT
      case _ => HTTP_DEFAULT_PORT
    }
  }

  def isOnError(statusCode: Int): Boolean = {
    !(statusCode >= 200 && statusCode < 400)
  }

  def urlToHost(url: URL): Host = Host(url.getHost, url.getProtocol, resolvePort(url))
}
