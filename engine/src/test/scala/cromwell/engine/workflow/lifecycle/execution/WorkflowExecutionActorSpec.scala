package cromwell.engine.workflow.lifecycle.execution

import akka.actor.Actor
import akka.testkit.{EventFilter, TestActorRef, TestDuration}
import com.typesafe.config.ConfigFactory
import cromwell.backend.AllBackendInitializationData
import cromwell.core.WorkflowId
import cromwell.engine.backend.{BackendConfigurationEntry, CromwellBackends}
import cromwell.engine.workflow.WorkflowDescriptorBuilder
import cromwell.engine.workflow.lifecycle.execution.WorkflowExecutionActor.ExecuteWorkflowCommand
import cromwell.services.ServiceRegistryActor
import cromwell.util.SampleWdl
import cromwell.{AlwaysHappyJobStoreActor, CromwellTestkitSpec, EmptyCallCacheReadActor}
import org.scalatest.BeforeAndAfter

import scala.concurrent.duration._

class WorkflowExecutionActorSpec extends CromwellTestkitSpec with BeforeAndAfter with WorkflowDescriptorBuilder {

  override implicit val actorSystem = system

  def mockServiceRegistryActor = TestActorRef(new Actor {
    override def receive = {
      case _ => // No action
    }
  })

  val stubbedConfig = ConfigFactory.load().getConfig("backend.providers.Mock").getConfig("config")

  val runtimeSection =
    """
      |runtime {
      | backend: "Mock"
      |}
    """.stripMargin

  "WorkflowExecutionActor" should {
    "retry a job 2 times and succeed in the third attempt" in {
      import spray.json._
      val serviceRegistryActor = system.actorOf(ServiceRegistryActor.props(ConfigFactory.load()))
      val jobStoreActor = system.actorOf(AlwaysHappyJobStoreActor.props)
      val MockBackendConfigEntry = BackendConfigurationEntry(
        name = "Mock",
        lifecycleActorFactoryClass = "cromwell.engine.backend.mock.RetryableBackendLifecycleActorFactory",
        stubbedConfig
      )
      CromwellBackends.initBackends(List(MockBackendConfigEntry), system)

      val workflowId = WorkflowId.randomId()
      val engineWorkflowDescriptor = createMaterializedEngineWorkflowDescriptor(workflowId, SampleWdl.HelloWorld.asWorkflowSources(runtime = runtimeSection))
      val callCacheReadActor = system.actorOf(EmptyCallCacheReadActor.props)

      val workflowExecutionActor = system.actorOf(
        WorkflowExecutionActor.props(workflowId, engineWorkflowDescriptor,serviceRegistryActor, jobStoreActor,
          callCacheReadActor, AllBackendInitializationData.empty, restarting = false),
        "WorkflowExecutionActor")

      EventFilter.info(pattern = ".*Final Outputs", occurrences = 1).intercept {
        EventFilter.info(pattern = "Starting calls: hello.hello", occurrences = 3).intercept {
          workflowExecutionActor ! ExecuteWorkflowCommand
        }
      }

      val isFatal = (_: Throwable) => false

      // Sleep a bit, and let the metadata do its thing in the background.
      Thread.sleep(3.seconds.dilated.toMillis)

      eventually(isFatal) {
        val metadata = getWorkflowMetadata(workflowId, serviceRegistryActor, None)
        val attempts: Vector[JsValue] = metadata.fields("calls").asJsObject.fields("hello.hello").asInstanceOf[JsArray].elements

        attempts.length shouldBe 3
        attempts.head.asJsObject.fields("attempt") shouldBe JsNumber(1)
        attempts.head.asJsObject.fields("executionStatus") shouldBe JsString("Preempted")
        attempts(1).asJsObject.fields("attempt") shouldBe JsNumber(2)
        attempts(1).asJsObject.fields("executionStatus") shouldBe JsString("Preempted")
        attempts(2).asJsObject.fields("attempt") shouldBe JsNumber(3)
      }
      system.stop(serviceRegistryActor)
    }

    "execute a workflow with scatters" in {
      val serviceRegistry = mockServiceRegistryActor
      val jobStore = system.actorOf(AlwaysHappyJobStoreActor.props)
      val callCacheReadActor = system.actorOf(EmptyCallCacheReadActor.props)

      val MockBackendConfigEntry = BackendConfigurationEntry(
        name = "Mock",
        lifecycleActorFactoryClass = "cromwell.engine.backend.mock.DefaultBackendLifecycleActorFactory",
        stubbedConfig
      )
      CromwellBackends.initBackends(List(MockBackendConfigEntry), system)

      val workflowId = WorkflowId.randomId()
      val engineWorkflowDescriptor = createMaterializedEngineWorkflowDescriptor(workflowId, SampleWdl.SimpleScatterWdl.asWorkflowSources(runtime = runtimeSection))
      val workflowExecutionActor = system.actorOf(
        WorkflowExecutionActor.props(workflowId, engineWorkflowDescriptor, serviceRegistry, jobStore,
          callCacheReadActor, AllBackendInitializationData.empty, restarting = false),
        "WorkflowExecutionActor")

      val scatterLog = "Starting calls: scatter0.inside_scatter:0:1, scatter0.inside_scatter:1:1, scatter0.inside_scatter:2:1, scatter0.inside_scatter:3:1, scatter0.inside_scatter:4:1"

      EventFilter.info(pattern = ".*Final Outputs", occurrences = 1).intercept {
        EventFilter.info(pattern = scatterLog, occurrences = 1).intercept {
          EventFilter.info(pattern = "Starting calls: scatter0.outside_scatter:NA:1", occurrences = 1).intercept {
            workflowExecutionActor ! ExecuteWorkflowCommand
          }
        }
      }
      system.stop(serviceRegistry)
    }
  }
}
