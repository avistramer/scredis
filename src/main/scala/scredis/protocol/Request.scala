package scredis.protocol

import scredis.exceptions._

import scala.util.{ Try, Success, Failure }
import scala.concurrent.Promise

import java.nio.ByteBuffer

abstract class Request[A](command: Command, args: Any*) {
  private val promise = Promise[A]()
  private var _encoded: ByteBuffer = null
  val future = promise.future
  
  private[scredis] def encode(): Unit = {
    _encoded = command.encode(args.toList)
  }
  
  private[scredis] def encoded: ByteBuffer = _encoded
  
  private[scredis] def complete(response: Response): Unit = {
    response match {
      case ErrorResponse(message) => promise.failure(RedisErrorResponseException(message))
      case response => try {
        promise.success(decode(response))
      } catch {
        case e: Throwable => promise.failure(
          RedisProtocolException(s"Unexpected response: $response", e)
        )
      }
    }
    Protocol.release()
  }
  
  private[scredis] def failure(throwable: Throwable): Unit = {
    promise.failure(throwable)
    Protocol.release()
  }
  
  def decode: PartialFunction[Response, A]
  def hasArguments = args.size > 0
  
}