/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import akka.event.LoggingReceive
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with Stash {

  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = LoggingReceive {
    case op: Operation => root ! op
    case GC =>
      val oldRoot = root
      root = createRoot
      oldRoot ! CopyTo(root)
      context become garbageCollecting(root)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC =>
    case CopyFinished =>
      context.unbecome()
      unstashAll()
    case msg => stash()
  }

}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = LoggingReceive {
    case insert: Insert => handleInsert(insert)
    case contains: Contains => handleContains(contains)
    case remove: Remove => handleRemove(remove)
    case copyTo: CopyTo => handleCopyTo(copyTo)
  }

  def direction(elem: Int): Position = if (elem < this.elem) Left else Right

  def handleCopyTo(copyTo: CopyTo): Unit = {
    if (!removed) copyTo.treeNode ! Insert(self, -1, elem)

    val children = subtrees.values
    children.foreach(_ ! copyTo)

    context become copying(children.toSet, removed)

    if (children.isEmpty) onDoneCopying()
  }

  def handleRemove(remove: Remove): Unit = {
    if (remove.elem == elem) {
      removed = true
      remove.requester ! OperationFinished(remove.id)
    } else {
      val dir = direction(remove.elem)
      val actor = subtrees.get(dir)

      if (actor.isDefined) actor.get ! remove
      else remove.requester ! OperationFinished(remove.id)
    }
  }

  def handleContains(contains: Contains): Unit = {
    if (contains.elem == elem) {
      contains.requester ! ContainsResult(contains.id, !removed)
    } else {
      val dir = direction(contains.elem)
      val actor = subtrees.get(dir)

      if (actor.isDefined) actor.get ! contains
      else contains.requester ! ContainsResult(contains.id, result = false)
    }
  }

  def handleInsert(insert: Insert): Unit = {
    if (insert.elem == elem) {
      removed = false
      insert.requester ! OperationFinished(insert.id)
    } else {
      val dir = direction(insert.elem)
      val actorOpt = subtrees.get(dir)

      if (actorOpt.isEmpty) {
        val newActor = context.actorOf(props(insert.elem, initiallyRemoved = false), s"n:${insert.elem}")
        subtrees = subtrees + (dir -> newActor)
        insert.requester ! OperationFinished(insert.id)
      } else {
        actorOpt.get ! insert
      }
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], confirmed: Boolean): Receive = {
    case _: OperationFinished =>
      context become copying(expected, confirmed = true)
      if (expected.isEmpty) onDoneCopying()
    case CopyFinished =>
      val newExpected = expected - sender()
      context become copying(newExpected, confirmed)
      if (newExpected.isEmpty && confirmed) onDoneCopying()
  }

  def onDoneCopying() = {
    context.parent ! CopyFinished
    self ! PoisonPill
  }

}
