package mesosphere.marathon
package core.launchqueue.impl

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.{Done, NotUsed}
import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props, Stash, Status, SupervisorStrategy}
import akka.event.LoggingReceive
import akka.pattern.pipe
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.instance.update.InstanceChange
import mesosphere.marathon.core.instance.update.InstanceUpdateOperation.RescheduleReserved
import mesosphere.marathon.core.instance.{Goal, Instance}
import mesosphere.marathon.core.launchqueue.LaunchQueueConfig
import mesosphere.marathon.core.launchqueue.impl.LaunchQueueActor.{AddFinished, QueuedAdd}
import mesosphere.marathon.core.launchqueue.impl.LaunchQueueDelegate.Add
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.{EnvVarString, PathId, RunSpec}

import scala.async.Async.{async, await}
import scala.collection.mutable.Queue
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

private class InstanceNumberFinder(runSpec: RunSpec, instances: Seq[Instance]) {
  val mustUseStableNumber: Boolean = if (runSpec != null)
    (runSpec.env.getOrElse("MUST_REUSE_ID", null) == EnvVarString("TRUE")) else false
  var found: Int = 0
  val usedIds = if (mustUseStableNumber)
    instances.filter(i => !(i.hasReservation && i.state.condition.isTerminal && i.state.goal == Goal.Stopped))
      .map(instance => instance.instanceId.instanceReusableNumber)
      .filter(_ > 0).toSet
  else null
  def next(): Int = {
    if (mustUseStableNumber) {
      do
        found = found + 1
      while (usedIds.contains(found))
    }
    found
  }
}

private[launchqueue] object LaunchQueueActor {
  def props(
    config: LaunchQueueConfig,
    instanceTracker: InstanceTracker,
    groupManager: GroupManager,
    runSpecActorProps: RunSpec => Props,
    delayUpdates: Source[RateLimiter.DelayUpdate, NotUsed]): Props = {
    Props(new LaunchQueueActor(config, instanceTracker, groupManager, runSpecActorProps, delayUpdates))
  }

  case class FullCount(appId: PathId)
  private case class QueuedAdd(sender: ActorRef, add: Add)
  private case class AddFinished(queuedAdd: QueuedAdd)
}

/**
  * An actor-based implementation of the LaunchQueue interface.
  *
  * The methods of that interface are translated to messages in the [[LaunchQueueDelegate]] implementation.
  */
private[impl] class LaunchQueueActor(
    launchQueueConfig: LaunchQueueConfig,
    instanceTracker: InstanceTracker,
    groupManager: GroupManager,
    runSpecActorProps: RunSpec => Props,
    delayUpdates: Source[RateLimiter.DelayUpdate, NotUsed]
) extends Actor with Stash with StrictLogging {
  import LaunchQueueDelegate._

  /** Currently active actors by pathId. */
  var launchers = Map.empty[PathId, ActorRef]
  /** Maps actorRefs to the PathId they handle. */
  var launcherRefs = Map.empty[ActorRef, PathId]

  /** Serial ID to ensure unique names for children actors. */
  var childSerial = 0

  // See [[receiveHandlePurging]]
  /** A message with a sender for later processing. */
  case class DeferredMessage(sender: ActorRef, message: Any)

  private[this] val queuedAddOperations = Queue.empty[QueuedAdd]
  private[this] var processingAddOperation = false

  /** The timeout for asking any children of this actor. */
  implicit val askTimeout: Timeout = launchQueueConfig.launchQueueRequestTimeout().milliseconds

  override def preStart(): Unit = {
    super.preStart()

    import akka.pattern.pipe
    import context.dispatcher
    instanceTracker.instancesBySpec().pipeTo(self)

    // Using an actorMaterializer that encompasses this context will cause the stream to auto-terminate when this actor does
    implicit val materializer = ActorMaterializer()(context)
    delayUpdates.runWith(
      Sink.actorRef(
        self,
        Status.Failure(new RuntimeException("The delay updates stream closed"))))
  }

  override def receive: Receive = initializing

  def initializing: Receive = {
    case instances: InstanceTracker.InstancesBySpec =>

      instances.instancesMap.collect {
        case (id, specInstances) if specInstances.instances.exists(_.isScheduled) =>
          groupManager.runSpec(id)
      }
        .flatten
        .foreach { scheduledRunSpec =>
          launchers.getOrElse(scheduledRunSpec.id, createAppTaskLauncher(scheduledRunSpec))
        }

      context.become(initialized)

      unstashAll()

    case Status.Failure(cause) =>
      // escalate this failure
      throw new IllegalStateException("while loading instances", cause)

    case _: AnyRef =>
      stash()
  }

  def initialized: Receive = LoggingReceive {
    Seq(
      receiveInstanceUpdate,
      receiveHandleNormalCommands
    ).reduce(_.orElse[Any, Unit](_))
  }

  /**
    * Fetch an existing actorRef, or create a new actor if the given instance is scheduled.
    * We don't need an actor ref to handle update for non-scheduled instances.
    */
  private def actorRefFor(instance: Instance): Option[ActorRef] = {
    launchers.get(instance.runSpecId).orElse {
      if (instance.isScheduled) {
        logger.info(s"No active taskLauncherActor for scheduled ${instance.instanceId}, will create one.")
        groupManager.runSpec(instance.runSpecId).map(createAppTaskLauncher)
      } else None
    }
  }

  private[this] def receiveInstanceUpdate: Receive = {
    case update: InstanceChange =>
      actorRefFor(update.instance) match {
        case Some(actorRef) => actorRef.forward(update)
        case None => sender() ! Done
      }
  }

  @SuppressWarnings(Array("all")) // async/await
  private[this] def receiveHandleNormalCommands: Receive = {
    case add @ Add(spec, count) =>
      logger.debug(s"Adding $count instances for the ${spec.configRef}")
      // we cannot process more Add requests for one runSpec in parallel because it leads to race condition.
      // See MARATHON-8320 for details. The queue handling is helping us ensure we add an instance at a time.

      if (queuedAddOperations.isEmpty && !processingAddOperation) {
        // start processing the just received operation
        processNextAdd(QueuedAdd(sender(), add))
      } else {
        queuedAddOperations += QueuedAdd(sender(), add)
      }

    case AddFinished(queuedAdd) =>
      queuedAdd.sender ! Done

      logger.info(s"Finished processing $queuedAdd and sent done to sender.")

      processingAddOperation = false

      if (queuedAddOperations.nonEmpty) {
        processNextAdd(queuedAddOperations.dequeue())
      }

    case msg @ RateLimiter.DelayUpdate(app, _) =>
      launchers.get(app.id).foreach(_.forward(msg))

    case Status.Failure(cause) =>
      // escalate this failure
      throw new IllegalStateException("after initialized", cause)
  }

  @SuppressWarnings(Array("all")) /* async/await */
  private def processNextAdd(queuedItem: QueuedAdd): Unit = {
    logger.debug(s"Processing new queue item: $queuedItem")
    import context.dispatcher
    processingAddOperation = true

    val future = async {
      val runSpec: RunSpec = queuedItem.add.spec
      // Trigger TaskLaunchActor creation and sync with instance tracker.
      launchers.getOrElse(runSpec.id, createAppTaskLauncher(runSpec))
      val allInstances = await(instanceTracker.specInstances(pathId = runSpec.id))
      // Reuse resident instances that are stopped.
      val instanceNumberFinder = new InstanceNumberFinder(runSpec, allInstances)
      val existingReservedStoppedInstances = allInstances
        .filter(i => i.hasReservation && i.state.condition.isTerminal && i.state.goal == Goal.Stopped) // resident to relaunch
        .take(queuedItem.add.count)
      await(Future.sequence(existingReservedStoppedInstances.map { instance => instanceTracker.process(RescheduleReserved(instance.instanceId, runSpec)) }))

      logger.debug(s"Rescheduled existing instances for ${runSpec.id}")

      // Schedule additional resident instances or all ephemeral instances
      val instancesToSchedule = existingReservedStoppedInstances.length.until(queuedItem.add.count).map {
        _ => Instance.scheduled(runSpec, Instance.Id.forRunSpec(runSpec.id, instanceNumberFinder.next()))
      }
      if (instancesToSchedule.nonEmpty) {
        await(instanceTracker.schedule(instancesToSchedule))
      }
      logger.info(s"Scheduling ${instancesToSchedule.length} new instances (first five: ${instancesToSchedule.take(5)} ) " +
        s"and rescheduling (${existingReservedStoppedInstances.length}) reserved instances due to LaunchQueue.Add for ${runSpec.id}")

      AddFinished(queuedItem)
    }
    future.pipeTo(self)
  }

  private[this] def createAppTaskLauncher(app: RunSpec): ActorRef = {
    val actorRef = context.actorOf(runSpecActorProps(app), s"$childSerial-${app.id.safePath}")
    childSerial += 1
    launchers += app.id -> actorRef
    launcherRefs += actorRef -> app.id
    context.watch(actorRef)
    actorRef
  }

  override def postStop(): Unit = {
    super.postStop()

    // Answer all outstanding requests.
    queuedAddOperations.foreach { item =>
      item.sender ! Status.Failure(new IllegalStateException("LaunchQueueActor stopped"))
    }
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case NonFatal(e) => Stop
    case m: Any => SupervisorStrategy.defaultDecider(m)
  }
}
