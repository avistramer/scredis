package scredis.io

import java.util.concurrent.locks.ReentrantLock

import akka.actor.{ActorSystem, Props}
import scredis.{RedisConfigDefaults, Server}
import scredis.exceptions.RedisIOException
import scredis.protocol.Request
import scredis.util.UniqueNameGenerator

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try

class ClusterBlockingConnection(
  system: ActorSystem,
  nodes: Seq[Server] = RedisConfigDefaults.Redis.ClusterNodes,
  maxRetries: Int = 4,
  receiveTimeoutOpt: Option[FiniteDuration] = RedisConfigDefaults.IO.ReceiveTimeoutOpt,
  connectTimeout: FiniteDuration = RedisConfigDefaults.IO.ConnectTimeout,
  maxWriteBatchSize: Int = RedisConfigDefaults.IO.MaxWriteBatchSize,
  tcpSendBufferSizeHint: Int = RedisConfigDefaults.IO.TCPSendBufferSizeHint,
  tcpReceiveBufferSizeHint: Int = RedisConfigDefaults.IO.TCPReceiveBufferSizeHint,
  akkaListenerDispatcherPath: String = RedisConfigDefaults.IO.Akka.ListenerDispatcherPath,
  akkaIODispatcherPath: String = RedisConfigDefaults.IO.Akka.IODispatcherPath,
  akkaDecoderDispatcherPath: String = RedisConfigDefaults.IO.Akka.DecoderDispatcherPath,
  tryAgainWait: FiniteDuration = RedisConfigDefaults.IO.Cluster.TryAgainWait,
  clusterDownWait: FiniteDuration = RedisConfigDefaults.IO.Cluster.ClusterDownWait
)
extends ClusterConnection(
  system = system,
  nodes = nodes,
  maxRetries = maxRetries,
  receiveTimeoutOpt = receiveTimeoutOpt,
  connectTimeout = connectTimeout,
  maxWriteBatchSize = maxWriteBatchSize,
  tcpSendBufferSizeHint = tcpReceiveBufferSizeHint,
  akkaListenerDispatcherPath = akkaListenerDispatcherPath,
  akkaIODispatcherPath = akkaIODispatcherPath,
  tryAgainWait = tryAgainWait,
  clusterDownWait = clusterDownWait
) with BlockingConnection {

  private val lock = new ReentrantLock()

  private def withLock[A](f: => A): A = {
    if (lock.tryLock) {
      try {
        f
      } finally {
        lock.unlock()
      }
    } else {
      throw RedisIOException("Trying to send request on a blocked cluster connection")
    }
  }

  override protected[scredis] def sendBlocking[A](request: Request[A])(
    implicit timeout: Duration
  ): Try[A] = withLock {
    logger.debug(s"Sending blocking request: $request")
    val future = send(request)
    Try(Await.result(future, timeout))
  }

}