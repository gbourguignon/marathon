package mesosphere.marathon.event.http

import akka.actor.{ Actor, ActorSystem, Props }
import akka.testkit.{ EventFilter, TestActorRef }
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.ConfigFactory
import mesosphere.marathon.MarathonSpec
import mesosphere.marathon.event.EventStreamAttached
import mesosphere.marathon.event.http.HttpEventActor.EventNotificationLimit
import mesosphere.marathon.event.http.SubscribersKeeperActor.GetSubscribers
import mesosphere.marathon.integration.setup.WaitTestSupport.waitUntil
import mesosphere.marathon.metrics.Metrics
import mesosphere.util.Mockito
import org.scalatest.{ GivenWhenThen, Matchers }
import spray.http.{ HttpRequest, HttpResponse, StatusCode }

import scala.concurrent.Future
import scala.concurrent.duration._

class HttpEventActorTest extends MarathonSpec with Mockito with GivenWhenThen with Matchers {

  test("A message is broadcast to all subscribers") {
    Given("A HttpEventActor with 2 subscribers")
    val aut = TestActorRef(new NoHttpEventActor(Set("host1", "host2")))

    When("An event is send to the actor")
    aut ! EventStreamAttached("remote")

    Then("The message is broadcast to both subscribers")
    waitUntil("Wait for 2 subscribers to get notified", 1.second) {
      aut.underlyingActor.requests.size == 2
    }
  }

  test("If a message is send to non existing subscribers") {
    Given("A HttpEventActor with 2 subscribers")
    val aut = TestActorRef(new NoHttpEventActor(Set("host1", "host2")))
    responseAction = () => throw new RuntimeException("Can not connect")

    When("An event is send to the actor")
    aut ! EventStreamAttached("remote")

    Then("The callback listener is rate limited")
    waitUntil("Wait for rate limiting 2 subscribers", 1.second) {
      aut.underlyingActor.limiter("host1").deadLine.isDefined && aut.underlyingActor.limiter("host2").deadLine.isDefined
    }
  }

  test("If a message is send to a slow subscriber") {
    Given("A HttpEventActor with 1 subscriber")
    val aut = TestActorRef(new NoHttpEventActor(Set("host1")))
    responseAction = () => { Thread.sleep(100); response }

    When("An event is send to the actor")
    aut ! EventStreamAttached("remote")

    Then("The callback listener is rate limited")
    waitUntil("Wait for rate limiting 1 subscriber", 5.second) {
      aut.underlyingActor.limiter("host1").deadLine.isDefined
    }
  }

  test("A rate limited subscriber will not be notified") {
    Given("A HttpEventActor with 2 subscribers")
    val aut = TestActorRef(new NoHttpEventActor(Set("host1", "host2")))
    aut.underlyingActor.limiter += "host1" -> EventNotificationLimit(23, Some(100.seconds.fromNow))

    When("An event is send to the actor")
    Then("Only one subscriber is limited")
    EventFilter.info(start = "Will not send event event_stream_attached to unresponsive hosts: Set(host1)") intercept {
      aut ! EventStreamAttached("remote")
    }

    And("The message is send to the other subscriber")
    waitUntil("Wait for 1 subscribers to get notified", 1.second) {
      aut.underlyingActor.requests.size == 1
    }
  }

  var conf: HttpEventConfiguration = _
  var response: HttpResponse = _
  var statusCode: StatusCode = _
  var responseAction = () => response
  val metrics = new Metrics(new MetricRegistry)
  implicit val system = ActorSystem("test-system",
    ConfigFactory.parseString("""akka.loggers = ["akka.testkit.TestEventListener"]""")
  )

  before {
    conf = mock[HttpEventConfiguration]
    conf.slowConsumerTimeout returns 1
    statusCode = mock[StatusCode]
    statusCode.isSuccess returns true
    response = mock[HttpResponse]
    response.status returns statusCode
  }

  class NoHttpEventActor(subscribers: Set[String])
      extends HttpEventActor(conf, TestActorRef(Props(new ReturnSubscribersTestActor(subscribers))), metrics) {
    var requests = List.empty[HttpRequest]
    override val pipeline: (HttpRequest) => Future[HttpResponse] = { request =>
      requests ::= request
      Future(responseAction())
    }
  }

  class ReturnSubscribersTestActor(subscribers: Set[String]) extends Actor {
    override def receive: Receive = {
      case GetSubscribers => sender ! EventSubscribers(subscribers)
    }
  }
}
