package scredis

import akka.actor._
import com.typesafe.config.Config
import scredis.commands._
import scredis.io.{ClusterConnection, Connection}
import scredis.protocol.requests.ConnectionRequests.Quit
import scredis.util.UniqueNameGenerator

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Defines a `RedisCluster` [[scredis.Client]] supporting all non-blocking commands that can be addressed to either
  * any cluster node or be automatically routed to the correct node.
  *
  * @define e [[scredis.exceptions.RedisErrorResponseException]]
  * @define redisCluster [[scredis.RedisCluster]]
  * @define typesafeConfig com.typesafe.Config
  */
class RedisCluster private[scredis](
    systemOrName: Either[ActorSystem, String],
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
    system = systemOrName match {
      case Left(system) => system
      case Right(name) => ActorSystem(UniqueNameGenerator.getUniqueName(name))
    },
    nodes = nodes,
    maxRetries = maxRetries,
    receiveTimeoutOpt = receiveTimeoutOpt,
    connectTimeout = connectTimeout,
    maxWriteBatchSize = maxWriteBatchSize,
    tcpSendBufferSizeHint = tcpSendBufferSizeHint,
    tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
    akkaListenerDispatcherPath = akkaListenerDispatcherPath,
    akkaIODispatcherPath = akkaIODispatcherPath,
    tryAgainWait = tryAgainWait,
    clusterDownWait = clusterDownWait
  ) with Connection
  with ClusterCommands
  with HashCommands
  with HyperLogLogCommands
  with KeyCommands
  with ListCommands
  with PubSubCommands
  with ScriptingCommands
  with SetCommands
  with SortedSetCommands
  with StringCommands
  //with SubscriberCommands
  with TransactionCommands
{
  override implicit val dispatcher: ExecutionContext =
    ExecutionContext.Implicits.global // TODO perhaps implement our own

  private var shouldShutdownSubscriberClient = false
  private var shouldShutdownBlockingClient = false

  /**
    * Constructs a $redisCluster instance from a [[scredis.RedisConfig]].
    *
    * @return the constructed $redisCluster
    */
  def this(config: RedisConfig) = this(
    systemOrName = Right(config.IO.Akka.ActorSystemName),
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
    * Constructs a $redisCluster instance using the default config.
    *
    * @return the constructed $redisCluster
    */
  def this() = this(RedisConfig())

  /**
    * Lazily initialized [[scredis.BlockingClient]].
    */
  lazy val blocking = {
    shouldShutdownBlockingClient = true
    ClusterBlockingClient(
      nodes = nodes,
      maxRetries = maxRetries,
      connectTimeout = connectTimeout,
      maxWriteBatchSize = maxWriteBatchSize,
      tcpSendBufferSizeHint = tcpSendBufferSizeHint,
      tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
      akkaListenerDispatcherPath = akkaListenerDispatcherPath,
      akkaIODispatcherPath = akkaIODispatcherPath,
      akkaDecoderDispatcherPath = akkaDecoderDispatcherPath,
      tryAgainWait = tryAgainWait,
      clusterDownWait = clusterDownWait
    )(system)
  }

  /**
    * Lazily initialized [[scredis.SubscriberClient]].
    */
  lazy val subscriber = {
    shouldShutdownSubscriberClient = true
    val server = connections.head._1
    SubscriberClient(
      host = server.host,
      port = server.port,
      connectTimeout = connectTimeout,
      receiveTimeoutOpt = receiveTimeoutOpt,
      maxWriteBatchSize = maxWriteBatchSize,
      tcpSendBufferSizeHint = tcpSendBufferSizeHint,
      tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
      akkaListenerDispatcherPath = akkaListenerDispatcherPath,
      akkaIODispatcherPath = akkaIODispatcherPath,
      akkaDecoderDispatcherPath = akkaDecoderDispatcherPath
    )(system)
  }

  /**
    * Closes the connection.
    */
  def quit(): Future[Unit] = {
    if (shouldShutdownBlockingClient) {
      try {
        blocking.quit()(5 seconds)
      } catch {
        case e: Throwable => logger.error("Could not shutdown blocking client", e)
      }
    }
    val future = if (shouldShutdownSubscriberClient) {
      subscriber.quit().map { _ =>
        subscriber.awaitTermination(3 seconds)
      }
    } else {
      Future.successful(())
    }
    future.recover {
      case e: Throwable => logger.error("Could not shutdown subscriber client", e)
    }.flatMap { _ =>
      send(Quit())
    }.map { _ =>
      systemOrName match {
        case Left(system) => // Do not shutdown provided ActorSystem
        case Right(name) => system.terminate()
      }
    }
  }

}


object RedisCluster {

  /**
    * Constructs a $redisCluster instance using provided parameters.
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
    * @return the constructed $redisCluster
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
  ) = new RedisCluster(
    systemOrName = Right(actorSystemName),
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
  )


  /**
    * Constructs a $redisCluster instance with given seed nodes, using the default config for all other parameters.
    *
    * @return the constructed $redisCluster
    */
  def apply(node: Server, nodes: Server*): RedisCluster = RedisCluster( nodes = node +: nodes)

  /**
    * Constructs a $redisCluster instance from a [[scredis.RedisConfig]].
    *
    * @param config a [[scredis.RedisConfig]]
    * @return the constructed $redisCluster
    */
  def apply(config: RedisConfig) = new RedisCluster(config)

  /**
    * Constructs a $redisCluster instance from a $tc.
    *
    * @note The config must contain the scredis object at its root.
    *
    * @param config a $typesafeConfig
    * @return the constructed $redis
    */
  def apply(config: Config) = new RedisCluster(RedisConfig(config))


  /**
    * Constructs a $redisCluster instance from a config file.
    *
    * @note The config file must contain the scredis object at its root.
    * This constructor is equivalent to {{{
    * Redis(configName, "scredis")
    * }}}
    *
    * @param configName config filename
    * @return the constructed $redis
    */
  def apply(configName: String): RedisCluster = new RedisCluster(RedisConfig(configName))

  /**
    * Constructs a $redisCluster instance from a config file and using the provided path.
    *
    * @note The path must include to the scredis object, e.g. x.y.scredis
    *
    * @param configName config filename
    * @param path path pointing to the scredis config object
    * @return the constructed $redis
    */
  def apply(configName: String, path: String): RedisCluster = new RedisCluster(RedisConfig(configName, path))

  def withActorSystem(config: Config)(implicit system: ActorSystem): RedisCluster = withActorSystem(RedisConfig(config))

  def withActorSystem(config: RedisConfig)(implicit system: ActorSystem): RedisCluster = new RedisCluster(
    systemOrName = Left(system),
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

}
