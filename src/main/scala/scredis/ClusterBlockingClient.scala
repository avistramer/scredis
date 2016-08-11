package scredis

import com.typesafe.config.Config

import akka.actor.ActorSystem

import scredis.io.ClusterBlockingConnection
import scredis.protocol.requests.ConnectionRequests.Quit
import scredis.commands._

import scala.util.Try
import scala.concurrent.duration._

class ClusterBlockingClient private[scredis](
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
  )(implicit system: ActorSystem) extends ClusterBlockingConnection(
  system = system,
  nodes = nodes,
  maxRetries = maxRetries,
  receiveTimeoutOpt = receiveTimeoutOpt,
  connectTimeout = connectTimeout,
  maxWriteBatchSize = maxWriteBatchSize,
  tcpSendBufferSizeHint = tcpSendBufferSizeHint,
  tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
  akkaListenerDispatcherPath = akkaListenerDispatcherPath,
  akkaIODispatcherPath = akkaIODispatcherPath,
  akkaDecoderDispatcherPath = akkaDecoderDispatcherPath,
  tryAgainWait = tryAgainWait,
  clusterDownWait = clusterDownWait
) with BlockingListCommands {

  /**
    * Constructs a $clusterBlockingClient instance from a [[scredis.RedisConfig]]
    *
    * @param config [[scredis.RedisConfig]]
    * @return the constructed $client
    */
  def this(config: RedisConfig)(implicit system: ActorSystem) = this(
    nodes = config.Redis.ClusterNodes,
    maxRetries = 4,
    receiveTimeoutOpt = config.IO.ReceiveTimeoutOpt,
    connectTimeout = config.IO.ConnectTimeout,
    maxWriteBatchSize = config.IO.MaxWriteBatchSize,
    tcpSendBufferSizeHint = config.IO.TCPSendBufferSizeHint,
    tcpReceiveBufferSizeHint = config.IO.TCPReceiveBufferSizeHint,
    akkaListenerDispatcherPath = config.IO.Akka.ListenerDispatcherPath,
    akkaIODispatcherPath = config.IO.Akka.IODispatcherPath,
    akkaDecoderDispatcherPath = config.IO.Akka.DecoderDispatcherPath,
    tryAgainWait = config.IO.Cluster.TryAgainWait,
    clusterDownWait = config.IO.Cluster.ClusterDownWait
  )

  /**
    * Constructs a $clusterBlockingClient instance from a $tc
    *
    * @note The config must contain the scredis object at its root.
    * This constructor is equivalent to {{{
    * new Client(config, "scredis")
    * }}}
    *
    * @param config $tc
    * @return the constructed $client
    */
  def this(config: Config)(implicit system: ActorSystem) = this(RedisConfig(config))

  /**
    * Constructs a $client instance from a config file.
    *
    * @note The config file must contain the scredis object at its root.
    * This constructor is equivalent to {{{
    * new Client(configName, "scredis")
    * }}}
    *
    * @param configName config filename
    * @return the constructed $client
    */
  def this(configName: String)(implicit system: ActorSystem) = this(RedisConfig(configName))

  /**
    * Constructs a $client instance from a config file and using the provided path.
    *
    * @note The path must include to the scredis object, e.g. x.y.scredis
    *
    * @param configName config filename
    * @param path path pointing to the scredis config object
    * @return the constructed $client
    */
  def this(configName: String, path: String)(implicit system: ActorSystem) = this(
    RedisConfig(configName, path)
  )

  /**
    * Closes the connection.
    *
    * @since 1.0.0
    */
  def quit()(implicit timeout: Duration): Try[Unit] = sendBlocking(Quit())

}

/**
  * The companion object provides additional friendly constructors.
  *
  * @define client [[scredis.ClusterBlockingClient]]
  * @define tc com.typesafe.Config
  */
object ClusterBlockingClient {

  /**
    * Constructs a $clusterBlockingClient instance using provided parameters.
    *
    * @param nodes list of server nodes, used as seed nodes to initially connect to the cluster.
    * @param maxRetries maximum number of retries and redirects to perform for a single command
    * @param receiveTimeoutOpt optional batch receive timeout
    * @param connectTimeout connection timeout
    * @param maxWriteBatchSize max number of bytes to send as part of a batch
    * @param tcpSendBufferSizeHint size hint of the tcp send buffer, in bytes
    * @param tcpReceiveBufferSizeHint size hint of the tcp receive buffer, in bytes
    * @param akkaListenerDispatcherPath path to listener dispatcher definition
    * @param akkaIODispatcherPath path to io dispatcher definition
    * @param akkaDecoderDispatcherPath path to decoder dispatcher definition
    * @param tryAgainWait time to wait after a TRYAGAIN response by a cluster node
    * @param clusterDownWait time to wait for a retry after CLUSTERDOWN response
    * @return the constructed $clusterBlockingClient
    */
  def apply(
     nodes: Seq[Server] = RedisConfigDefaults.Redis.ClusterNodes,
     maxRetries: Int = 4,
     receiveTimeoutOpt: Option[FiniteDuration] = RedisConfigDefaults.IO.ReceiveTimeoutOpt,
     connectTimeout: FiniteDuration = RedisConfigDefaults.IO.ConnectTimeout,
     maxWriteBatchSize: Int = RedisConfigDefaults.IO.MaxWriteBatchSize,
     tcpSendBufferSizeHint: Int = RedisConfigDefaults.IO.TCPSendBufferSizeHint,
     tcpReceiveBufferSizeHint: Int = RedisConfigDefaults.IO.TCPReceiveBufferSizeHint,
     actorSystemName: String = RedisConfigDefaults.IO.Akka.ActorSystemName,
     akkaListenerDispatcherPath: String = RedisConfigDefaults.IO.Akka.ListenerDispatcherPath,
     akkaIODispatcherPath: String = RedisConfigDefaults.IO.Akka.IODispatcherPath,
     akkaDecoderDispatcherPath: String = RedisConfigDefaults.IO.Akka.DecoderDispatcherPath,
     tryAgainWait: FiniteDuration = RedisConfigDefaults.IO.Cluster.TryAgainWait,
     clusterDownWait: FiniteDuration = RedisConfigDefaults.IO.Cluster.ClusterDownWait
   )(implicit system: ActorSystem) = new ClusterBlockingClient(
    nodes = nodes,
    maxRetries = maxRetries,
    receiveTimeoutOpt = receiveTimeoutOpt,
    connectTimeout = connectTimeout,
    maxWriteBatchSize = maxWriteBatchSize,
    tcpSendBufferSizeHint = tcpSendBufferSizeHint,
    tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
    akkaListenerDispatcherPath = akkaListenerDispatcherPath,
    akkaIODispatcherPath = akkaIODispatcherPath,
    akkaDecoderDispatcherPath = akkaDecoderDispatcherPath,
    tryAgainWait = tryAgainWait,
    clusterDownWait = clusterDownWait
  )

  /**
    * Constructs a $clusterBlockingClient instance from a [[scredis.RedisConfig]]
    *
    * @param config [[scredis.RedisConfig]]
    * @return the constructed $clusterBlockingClient
    */
  def apply(config: RedisConfig)(implicit system: ActorSystem): ClusterBlockingClient = new ClusterBlockingClient(config)

  /**
    * Constructs a $clusterBlockingClient instance from a $tc
    *
    * @note The config must contain the scredis object at its root.
    * This constructor is equivalent to {{{
    * Client(config, "scredis")
    * }}}
    *
    * @param config $tc
    * @return the constructed $client
    */
  def apply(config: Config)(
    implicit system: ActorSystem
  ): ClusterBlockingClient = new ClusterBlockingClient(config)

  /**
    * Constructs a $clusterBlockingClient instance from a config file.
    *
    * @note The config file must contain the scredis object at its root.
    * This constructor is equivalent to {{{
    * Client(configName, "scredis")
    * }}}
    *
    * @param configName config filename
    * @return the constructed $clusterBlockingClient
    */
  def apply(configName: String)(implicit system: ActorSystem): ClusterBlockingClient = new ClusterBlockingClient(configName)

  /**
    * Constructs a $clusterBlockingClient instance from a config file and using the provided path.
    *
    * @note The path must include to the scredis object, e.g. x.y.scredis
    *
    * @param configName config filename
    * @param path path pointing to the scredis config object
    * @return the constructed $clusterBlockingClient
    */
  def apply(configName: String, path: String)(implicit system: ActorSystem): ClusterBlockingClient = new ClusterBlockingClient(configName, path)

}