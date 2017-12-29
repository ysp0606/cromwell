package cromwell.services.metadata.impl.pubsub

import akka.actor.ActorInitializationException
import com.google.api.client.auth.oauth2.Credential
import com.google.api.services.pubsub.model.Topic
import com.typesafe.config.{Config, ConfigFactory}
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO.PubSubMessage
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.{ExecutionContext, Future}

class PubSubMetadataServiceActorSpec extends FlatSpec with Matchers {
  import PubSubMetadataServiceActorSpec._

  // Build w/ empty config should throw exception
  "A PubSubMetadataActor with an empty serviceConfig" should "fail to build" in {
    an [ActorInitializationException] should be thrownBy new PubSubMetadataServiceActor(emptyConfig, emptyConfig)
  }

  // Build successful w/ subscription should emit topic & subscription
  // Publish to successful w/ subscription should log
  // publish w/ ack to successful w/ subscription should respond

  // Build successful w/o subscription should emit topic & no subscription (explicitly check)
  // Publish to successful w/o subscription should log
  // publish w/ ack to successful w/o subscription should respond

  // Build Failure should fail on create topic

  // Build FailToPublish should emit topic
  // Publish to failtopublish should log error
  // Publish w/ ack to failtopublish should respond
}

object PubSubMetadataServiceActorSpec {
  /** A variant of PubSubMetadataServiceActor with a GooglePubSubDAO which will always return success */
  class SuccessfulMockPubSubMetadataServiceActor(serviceConfig: Config, globalConfig: Config)
    extends PubSubMetadataServiceActor(serviceConfig, globalConfig) {

    override def createPubSubConnection(): GooglePubSubDAO = new SuccessfulMockGooglePubSubDao
  }

  /** A variant of PubSubMetadataServiceActor with a GooglePubSubDAO which will always return failure */
  class FailingMockPubSubMetadataServiceActor(serviceConfig: Config, globalConfig: Config)
    extends PubSubMetadataServiceActor(serviceConfig, globalConfig) {

    override def createPubSubConnection(): GooglePubSubDAO = new FailingMockGooglePubSubDao
  }

  /** A variant of PubSubMetadataServiceActor which will fail on message publication */
  class FailToPublishMockPubSubMetadataServiceActor(serviceConfig: Config, globalConfig: Config)
    extends PubSubMetadataServiceActor(serviceConfig, globalConfig) {

    override def createPubSubConnection(): GooglePubSubDAO = new FailToPublishMockGooglePubSubDao
  }

  trait MockGooglePubSubDao extends GooglePubSubDAO {
    override implicit val executionContext = ExecutionContext.global

    override def createTopic(topicName: String): Future[Boolean]
    override def createSubscription(topicName: String, subscriptionName: String): Future[Boolean]
    override def publishMessages(topicName: String, messages: Seq[String]): Future[Unit]

    // The following aren't used so leaving them empty
    override def deleteTopic(topicName: String): Future[Boolean] = ???
    override def getTopic(topicName: String)(implicit executionContext: ExecutionContext): Future[Option[Topic]] = ???
    override def deleteSubscription(subscriptionName: String): Future[Boolean] = ???
    override def acknowledgeMessages(subscriptionName: String, messages: Seq[PubSubMessage]): Future[Unit] = ???
    override def acknowledgeMessagesById(subscriptionName: String, ackIds: Seq[String]): Future[Unit] = ???
    override def pullMessages(subscriptionName: String, maxMessages: Int): Future[Seq[PubSubMessage]] = ???
    override def getPubSubServiceAccountCredential: Credential = ???
  }

  class SuccessfulMockGooglePubSubDao extends MockGooglePubSubDao {
    override def createTopic(topicName: String): Future[Boolean] = Future.successful(true)
    override def createSubscription(topicName: String, subscriptionName: String): Future[Boolean] = Future.successful(true)
    override def publishMessages(topicName: String, messages: Seq[String]): Future[Unit] = Future.successful(())
  }

  class FailingMockGooglePubSubDao extends MockGooglePubSubDao {
    override def createTopic(topicName: String): Future[Boolean] = Future.successful(false)
    override def createSubscription(topicName: String, subscriptionName: String): Future[Boolean] = Future.successful(false)
    override def publishMessages(topicName: String, messages: Seq[String]): Future[Unit] = Future.successful(())
  }

  class FailToPublishMockGooglePubSubDao extends FailingMockGooglePubSubDao {
    override def publishMessages(topicName: String, messages: Seq[String]): Future[Unit] = Future.successful(())
  }

  // This doesn't include a project so should be a failure
  val emptyConfig = ConfigFactory.parseString(
    """
      |
    """.stripMargin
  )

  val configWithSubscription = ConfigFactory.parseString(
    """
      |project = "foo"
      |topic = "bar"
      |subscription = "baz"
    """.stripMargin
  )

  val configWithoutSubscription = ConfigFactory.parseString(
    """
      |project = "foo"
      |topic = "bar"
    """.stripMargin
  )
}