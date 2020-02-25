package mesosphere.marathon
package core.deployment.impl

import akka.Done
import akka.actor._
import akka.event.EventStream
import akka.pattern._
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.deployment.impl.DeploymentActor.{GetInstancesHealth, InstancesHealth}
import mesosphere.marathon.core.event._
import mesosphere.marathon.core.instance.{Goal, GoalChangeReason, Instance}
import mesosphere.marathon.core.launchqueue.LaunchQueue
import mesosphere.marathon.core.readiness.ReadinessCheckExecutor
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.RunSpec

import scala.async.Async.{async, await}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class TaskReplaceActor(
    val deploymentManagerActor: ActorRef,
    val status: DeploymentStatus,
    val launchQueue: LaunchQueue,
    val instanceTracker: InstanceTracker,
    val eventBus: EventStream,
    val readinessCheckExecutor: ReadinessCheckExecutor,
    val runSpec: RunSpec,
    promise: Promise[Unit]) extends Actor with Stash with ReadinessBehavior with StrictLogging {
  import TaskReplaceActor._

  def deploymentId = status.plan.id
  val syncDelay = 1.seconds

  @SuppressWarnings(Array("all")) // async/await
  override def preStart(): Unit = {
    super.preStart()
    // subscribe to all needed events
    eventBus.subscribe(self, classOf[InstanceChanged])
    eventBus.subscribe(self, classOf[InstanceHealthChanged])

    // reconcile the state from a possible previous run
    // this is necessary to have a clear view of instances with readiness status
    val currentInstances = instanceTracker.specInstancesSync(runSpec.id, readAfterWrite = true)
    val (instancesAlreadyStarted, _oldInstances) = currentInstances.partition(_.runSpecVersion == runSpec.version)
    reconcileAlreadyStartedInstances(instancesAlreadyStarted)

    // reset the launch queue delay
    logger.info("Resetting the backoff delay before restarting the runSpec")
    launchQueue.resetDelay(runSpec)

    // sending a message to DeploymentActor (instead of letting ReadinessBehavior getting events) allows to have immediate knowledge of
    // health status of all existing instances when deployment starts. Otherwise, we would not have enough information
    // about instance health until we've received enough events.
    deploymentManagerActor ! GetInstancesHealth(runSpec.id)
  }

  override def postStop(): Unit = {
    eventBus.unsubscribe(self)
    super.postStop()
  }

  override def receive: Receive = readinessBehavior orElse customReceive

  private def customReceive: Receive = {
    case Done =>
      logger.info(s"${deploymentId} Launching tasks to replace killed instances is done")

    case Status.Failure(cause) =>
      // escalate this failure
      throw new IllegalStateException("while loading tasks", cause)

    case InstancesHealth(healthFuture) =>
      healthFuture.onComplete {
        case Failure(t) => logger.error(s"Fail to get instances health: ${t.getMessage}", t)
        case Success(health) => {
          // All existing instances of this app independent of the goal.
          //
          // Killed resident tasks are not expunged from the instances list. Ignore
          // them. LaunchQueue takes care of launching instances against reservations
          // first
          val currentInstances = instanceTracker.specInstancesSync(runSpec.id, readAfterWrite = true)
          val (instancesAlreadyStarted, oldInstances) = currentInstances.partition(_.runSpecVersion == runSpec.version)

          // Old and new instances that have the Goal.Running & are considered healthy according to Mesos & Marathon health checks
          val consideredHealthyInstances = currentInstances
            .filter(i => i.state.goal == Goal.Running && i.consideredHealthy)
            .filter(i => health.getOrElse(i.instanceId, Seq.empty).filterNot(_.alive).isEmpty)
            .filter(i => readyInstances.contains(i.instanceId))

          var restartStrategy = computeRestartStrategy(runSpec, consideredHealthyInstances.size)

          // following condition addresses cases where we have extra-instances due to previous deployment adding extra-instances
          // and deployment is force-updated
          if (runSpec.instances < currentInstances.size) {
            val nrToKillImmediately = math.max(currentInstances.size - runSpec.instances, restartStrategy.nrToKillImmediately)

            restartStrategy = RestartStrategy(nrToKillImmediately, restartStrategy.maxCapacity)
            logger.info(s"runSpec.instances < currentInstances: Allowing killing all ${restartStrategy.nrToKillImmediately} extra-instances")
          }

          // kill old instances to free some capacity
          val oldActiveInstances = oldInstances.filter(_.state.goal == Goal.Running)

          // instance to kill sorted by decreasing order of priority
          // we always prefer to kill unhealthy tasks first
          val toKillOrdered = oldActiveInstances.sortWith((i1, i2) => {
            (consideredHealthyInstances.contains(i1.instanceId), consideredHealthyInstances.contains(i2.instanceId)) match {
              case (_, false) => false
              case _ => true
            }
          })
          val toKill: mutable.Queue[Instance.Id] = toKillOrdered.map(_.instanceId).to[mutable.Queue]

          for (_ <- 0 until restartStrategy.nrToKillImmediately) killNextOldInstance(toKill)

          // start new instances, if possible
          val leftCapacity = math.max(0, restartStrategy.maxCapacity - currentInstances.count(_.state.goal == Goal.Running))
          val instancesNotStartedYet = math.max(0, runSpec.instances - instancesAlreadyStarted.size)
          val instancesToStartNow = math.min(instancesNotStartedYet, leftCapacity)
          logger.info(s"Deployment $deploymentId: leftCapacity = $leftCapacity, " +
            s"instancesStarted = ${instancesAlreadyStarted.size}, instancesNotStartedYet = $instancesNotStartedYet and instancesToStartNow = $instancesToStartNow")
          launchInstances(instancesToStartNow).pipeTo(self)

          // it might be possible, that we come here, but nothing is left to do
          checkFinished(oldActiveInstances.size)

          // reschedule next check
          context.system.scheduler.scheduleOnce(syncDelay, deploymentManagerActor, GetInstancesHealth(runSpec.id))
        }
      }
  }

  // make sure we have up to date information about existing instance
  def reconcileAlreadyStartedInstances(instancesAlreadyStarted: Seq[Instance]): Unit = {
    logger.info(s"Deployment $deploymentId: Reconciling instances during ${runSpec.id} deployment: found ${instancesAlreadyStarted.size} already started instances ")
    instancesAlreadyStarted.foreach(reconcileHealthAndReadinessCheck)
  }

  def launchInstances(instancesToStartNow: Int): Future[Done] = {
    if (instancesToStartNow > 0) {
      launchQueue.add(runSpec, instancesToStartNow)
    } else {
      Future.successful(Done)
    }
  }

  @SuppressWarnings(Array("all")) // async/await
  def killNextOldInstance(toKill: mutable.Queue[Instance.Id]): Unit = {
    if (toKill.nonEmpty) {
      val dequeued = toKill.dequeue()
      async {
        await(instanceTracker.get(dequeued)) match {
          case None =>
            logger.warn(s"Deployment $deploymentId: Was about to kill instance $dequeued but it did not exist in the instance tracker anymore.")
          case Some(nextOldInstance) =>
            logger.info(s"Deployment $deploymentId: Killing old ${nextOldInstance.instanceId}")

            val goal = if (runSpec.isResident) Goal.Stopped else Goal.Decommissioned
            await(instanceTracker.setGoal(nextOldInstance.instanceId, goal, GoalChangeReason.Upgrading))
        }
      }
    }
  }

  def instanceConditionChanged(instanceId: Instance.Id): Unit = {
    // nothing to do
  }

  def checkFinished(oldActiveInstanceCount: Int): Unit = {
    if (targetCountReached(runSpec.instances) && oldActiveInstanceCount == 0) {
      logger.info(s"Deployment $deploymentId: All new instances for $pathId are ready and all old instances have been killed")
      promise.trySuccess(())
      context.stop(self)
    } else {
      logger.info(s"Deployment $deploymentId: For run spec: [${runSpec.id}] there are [${healthyInstances.size}] healthy and " +
        s"[${readyInstances.size}] ready new instances and " +
        s"[${oldActiveInstanceCount}] old instances. " +
        s"Target count is ${runSpec.instances}.")
    }
  }
}

object TaskReplaceActor extends StrictLogging {

  object CheckFinished

  //scalastyle:off
  def props(
    deploymentManagerActor: ActorRef,
    status: DeploymentStatus,
    launchQueue: LaunchQueue,
    instanceTracker: InstanceTracker,
    eventBus: EventStream,
    readinessCheckExecutor: ReadinessCheckExecutor,
    app: RunSpec,
    promise: Promise[Unit]): Props = Props(
    new TaskReplaceActor(deploymentManagerActor, status, launchQueue, instanceTracker, eventBus,
      readinessCheckExecutor, app, promise)
  )

  /** Encapsulates the logic how to get a Restart going */
  private[impl] case class RestartStrategy(nrToKillImmediately: Int, maxCapacity: Int)

  private[impl] def computeRestartStrategy(runSpec: RunSpec, consideredHealthyInstancesCount: Int): RestartStrategy = {
    // in addition to a spec which passed validation, we require:
    require(runSpec.instances > 0, s"instances must be > 0 but is ${runSpec.instances}")
    require(consideredHealthyInstancesCount >= 0, s"running instances count must be >=0 but is $consideredHealthyInstancesCount")

    val minHealthy = (runSpec.instances * runSpec.upgradeStrategy.minimumHealthCapacity).ceil.toInt
    var maxCapacity = (runSpec.instances * (1 + runSpec.upgradeStrategy.maximumOverCapacity)).toInt
    var nrToKillImmediately = math.max(0, consideredHealthyInstancesCount - minHealthy)

    if (minHealthy == maxCapacity && maxCapacity <= consideredHealthyInstancesCount) {
      if (runSpec.isResident) {
        // Kill enough instances so that we end up with one instance below minHealthy.
        // TODO: We need to do this also while restarting, since the kill could get lost.
        nrToKillImmediately = consideredHealthyInstancesCount - minHealthy + 1
        logger.info(
          "maxCapacity == minHealthy for resident app: " +
            s"adjusting nrToKillImmediately to $nrToKillImmediately in order to prevent over-capacity for resident app"
        )
      } else {
        logger.info("maxCapacity == minHealthy: Allow temporary over-capacity of one instance to allow restarting")
        maxCapacity += 1
      }
    }

    logger.info(s"For minimumHealthCapacity ${runSpec.upgradeStrategy.minimumHealthCapacity} of ${runSpec.id.toString} leave " +
      s"$minHealthy instances running, maximum capacity $maxCapacity, killing $nrToKillImmediately of " +
      s"$consideredHealthyInstancesCount running instances immediately. (RunSpec version ${runSpec.version})")

    assume(nrToKillImmediately >= 0, s"nrToKillImmediately must be >=0 but is $nrToKillImmediately")
    assume(maxCapacity > 0, s"maxCapacity must be >0 but is $maxCapacity")
    def canStartNewInstances: Boolean = minHealthy < maxCapacity || consideredHealthyInstancesCount - nrToKillImmediately < maxCapacity
    assume(canStartNewInstances, "must be able to start new instances")

    RestartStrategy(nrToKillImmediately = nrToKillImmediately, maxCapacity = maxCapacity)
  }
}

