package mesosphere.marathon.event.http

import akka.actor._
import akka.pattern.ask
import mesosphere.marathon.api.v2.json.Formats._
import mesosphere.marathon.event._
import mesosphere.marathon.event.http.HttpEventActor._
import mesosphere.marathon.event.http.SubscribersKeeperActor.GetSubscribers
import mesosphere.marathon.metrics.Metrics
import spray.client.pipelining.{ sendReceive, _ }
import spray.http.{ HttpRequest, HttpResponse }
import spray.httpx.PlayJsonSupport

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success
import System.currentTimeMillis

object HttpEventActor {
  case class NotificationFailed(url: String)
  case class NotificationSuccess(url: String)
  case class NotificationSlow(url: String)

  case class EventNotificationLimit(failed: Long, deadLine: Option[Deadline]) {
    def nextFailed: EventNotificationLimit = {
      val next = failed + 1
      EventNotificationLimit(next, Some(math.pow(2, next.toDouble).seconds.fromNow))
    }
    def notLimited: Boolean = deadLine.fold(true)(_.isOverdue())
    def limited: Boolean = !notLimited
  }
  val NoLimit = EventNotificationLimit(0, None)
}

class HttpEventActor(conf: HttpEventConfiguration, val subscribersKeeper: ActorRef, metrics: Metrics)
    extends Actor with ActorLogging with PlayJsonSupport {

  implicit val ec = HttpEventModule.executionContext
  implicit val timeout = HttpEventModule.timeout
  val pipeline: HttpRequest => Future[HttpResponse] = addHeader("Accept", "application/json") ~> sendReceive
  var limiter = Map.empty[String, EventNotificationLimit].withDefaultValue(NoLimit)

  //metrics
  val metricOutstandingRequests = metrics.counter("service.mesosphere.marathon.event.http.outstanding-requests")
  val metricSendEvent = metrics.meter("service.mesosphere.marathon.event.http.send")
  val metricDroppedEvent = metrics.meter("service.mesosphere.marathon.event.http.dropped-events")
  val metricResponseTime = metrics.timer("service.mesosphere.marathon.event.http.response-time")

  def receive: Receive = {
    case event: MarathonEvent     => broadcast(event)
    case NotificationSuccess(url) => limiter += url -> NoLimit
    case NotificationSlow(url)    => limiter += url -> limiter(url).nextFailed
    case NotificationFailed(url)  => limiter += url -> limiter(url).nextFailed
    case _                        => log.warning("Message not understood!")
  }

  def broadcast(event: MarathonEvent): Unit = {
    metricSendEvent.mark()
    log.info("POSTing to all endpoints.")
    val me = self
    (subscribersKeeper ? GetSubscribers).mapTo[EventSubscribers].foreach { subscribers =>
      val (active, limited) = subscribers.urls.partition(limiter(_).notLimited)
      if (limited.nonEmpty) log.info(s"Will not send event ${event.eventType} to unresponsive hosts: $limited")
      metricDroppedEvent.mark(limited.size)
      active.foreach(post(_, event, me))
    }
  }

  def post(url: String, event: MarathonEvent, eventActor: ActorRef): Unit = {
    log.info("Sending POST to:" + url)

    metricOutstandingRequests.inc()
    val request = Post(url, eventToJson(event))
    val response = pipeline(request)
    val start = currentTimeMillis()

    response.onComplete {
      case _ =>
        metricOutstandingRequests.dec()
        metricResponseTime.updateMillis(currentTimeMillis() - start)
    }
    response.onComplete {
      case Success(res) =>
        if (res.status.isFailure) log.debug(s"No success response for post $event to $url")
        val inTime = (currentTimeMillis() - start) < conf.httpEventCallbackSlowConsumerTimeout()
        eventActor ! (if (inTime) NotificationSuccess(url) else NotificationSlow(url))
      case _ =>
        log.warning(s"Failed to post $event to $url")
        eventActor ! NotificationFailed(url)
    }
  }
}

