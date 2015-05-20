package kvstore

import akka.actor.{Cancellable, Props, Actor, ActorRef}
import scala.concurrent.duration._

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {

  import Replicator._
  import Replica._
  import context.dispatcher

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var retries: Map[Long, Cancellable] = Map()

  var _seqCounter = 0L

  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def receive: Receive = {
    case replicate: Replicate =>
      val seq: Long = nextSeq
      startRetrying(Snapshot(replicate.key, replicate.valueOption, seq), replica)
      acks = acks + (seq ->(sender(), replicate))
    case SnapshotAck(key, id) =>
      stopRetrying(id)
      acks.get(id) foreach { case (initiator, replicate) =>
        initiator ! Replicated(replicate.key, replicate.id)
      }
  }

  def startRetrying(snapshot: Snapshot, actor: ActorRef): Cancellable = {
    context.system.scheduler.schedule(0.millis, 100.millis, actor, snapshot)
  }

  def stopRetrying(id: Long) = {
    retries.get(id).foreach { cancellable =>
      cancellable.cancel()
      retries = retries - id
    }
  }

}
