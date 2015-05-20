package kvstore

import akka.actor._
import akka.persistence.AtLeastOnceDelivery
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ask, pipe}
import scala.concurrent.duration._
import akka.util.Timeout

object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with Stash {

  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  var kv = Map.empty[String, String]

  var secondaries = Map.empty[ActorRef, ActorRef]

  val persistence: ActorRef = context.actorOf(persistenceProps, "persistence")

  override def preStart(): Unit = {
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary =>
      context.become(replica())
  }

  val serveGet: Receive = {
    case Get(key, id) =>
      val value = kv.get(key)
      sender ! GetResult(key, value, id)
  }
  
  
  val leader: Receive = serveGet orElse {
    case insert: Insert =>
      savePrimary(Persist(insert.key, Some(insert.value), insert.id)) { persisted =>
        kv = kv + (insert.key -> insert.value)
      }

    case remove: Remove =>
      savePrimary(Persist(remove.key, None, remove.id)) { persisted =>
        kv = kv - remove.key
      }

    case Replicas(replicas) =>
      handleReplicasChange(replicas)
  }

  def replica(lastSeq: Long = -1): Receive = serveGet orElse {
    case Snapshot(key, valueOpt, seq) =>
      if (seq <= lastSeq) {
        sender ! SnapshotAck(key, seq)
      }

      if ((lastSeq + 1L) == seq) {
        if (valueOpt.isEmpty) kv = kv - key
        else kv = kv + (key -> valueOpt.get)

        val ackTo = sender
        persist(Persist(key, valueOpt, seq)) { persisted =>
          context become replica(seq)
          ackTo ! SnapshotAck(persisted.key, persisted.id)
        }
      }

    case op: Operation =>
      sender ! OperationFailed(op.id)
  }

  def persisting(job: Cancellable, ackTo: ActorRef)(onDone: Persisted => Unit): Receive = serveGet orElse {
    case persisted: Persisted =>
      job.cancel()
      onDone(persisted)

    case _ => stash()
  }

  def awaitingAcks(ackTo: ActorRef, ack: OperationAck, pending: Set[ActorRef], timeout: Cancellable): Receive = serveGet orElse {
    case Replicated(key, id) =>
      val newPending = pending - sender

      if (newPending.isEmpty) {
        ackTo ! ack
        timeout.cancel()
        context become leader
        unstashAll()
      } else {
        context become awaitingAcks(ackTo, ack, newPending, timeout)
      }
    case Replicas(replicas) =>

      val leftReplicasReplicators = (secondaries.keySet -- replicas).filterNot(_ == self).map(secondaries(_))
      val newPending = pending -- leftReplicasReplicators

      if (newPending.isEmpty) {
        ackTo ! ack
        timeout.cancel()
        context become leader
        unstashAll()
      } else {
        context become awaitingAcks(ackTo, ack, newPending, timeout)
      }
      stash()
    case msg => stash()
  }

  def handleReplicasChange(replicas: Set[ActorRef]): (Set[ActorRef], Set[ActorRef]) = {
    val newReplicas = (replicas -- secondaries.keySet) - self

    val newReplicasAndReplicators = newReplicas.map { newReplica =>
      val replicator = context.actorOf(Replicator.props(newReplica), s"replicator-${newReplica.path.name}")
      (newReplica, replicator)
    }

    val replicasLeft = (secondaries.keySet -- replicas) - self

    replicasLeft.foreach { replica =>
      val replicator = secondaries(replica)
      secondaries = secondaries - replica
      context.stop(replicator)
    }

    newReplicasAndReplicators.foreach { case (newReplica, replicator) =>
      secondaries = secondaries + (newReplica -> replicator)
    }

    replicateWholeState(newReplicas)

    (newReplicas, replicasLeft)
  }

  def replicateWholeState(replicas: Set[ActorRef]): Unit = {
    replicas.foreach { replica =>
      kv.foreach { case (k, v) =>
        val replicator = secondaries.get(replica)
        replicator foreach { rep =>
          rep ! Replicate(k, Some(v), 0)
        }
      }
    }
  }

  def persist(persist: Persist)(callback: Persisted => Unit): Unit = {
    val job = context.system.scheduler.schedule(0.milliseconds, 100.milliseconds, persistence, persist)

    context become persisting(job, sender) { persisted =>
      callback(persisted)
      unstashAll()
    }
  }

  def savePrimary(persist: Persist)(callback: Persisted => Unit): Unit = {
    val ackTo = sender
    val ack = OperationAck(persist.id)

    val timeout = context.system.scheduler.scheduleOnce(1.second) {
      ackTo ! OperationFailed(persist.id)
      context become leader
    }

    this.persist(persist) { persisted =>
      timeout.cancel()
      callback(persisted)

      if (secondaries.nonEmpty) {
        val ackTimeout = context.system.scheduler.scheduleOnce(1.second) {
          ackTo ! OperationFailed(persist.id)
          context become leader
        }

        context become awaitingAcks(ackTo, ack, secondaries.values.toSet, ackTimeout)
        secondaries.values.foreach { replicator =>
          replicator ! Replicate(persist.key, kv.get(persist.key), persist.id)
        }
      } else {
        ackTo ! ack
        context become leader
      }
    }
  }

}

