package mesosphere.marathon
package tasks

import mesosphere.UnitTest
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.PathId
import mesosphere.marathon.state.PathId._
import org.apache.mesos.Protos.TaskID
import org.scalatest.Inside

class TaskIdTest extends UnitTest with Inside {
  "TaskIds" should {
    "AppIds can be converted to TaskIds and back to AppIds" in {
      val appId = "/test/foo/bla/rest".toPath
      val taskId = Task.Id.forRunSpec(appId)
      taskId.runSpecId should equal(appId)
    }

    "Old TaskIds can be converted" in {
      val taskId = Task.Id(TaskID.newBuilder().setValue("app_682ebe64-0771-11e4-b05d-e0f84720c54e").build)
      taskId.runSpecId should equal("app".toRootPath)
    }

    "Old TaskIds can be converted even if they have dots in them" in {
      val taskId = Task.Id(TaskID.newBuilder().setValue("app.foo.bar_682ebe64-0771-11e4-b05d-e0f84720c54e").build)
      taskId.runSpecId should equal("app.foo.bar".toRootPath)
    }

    "Old TaskIds can be converted even if they have underscores in them" in {
      val taskId = Task.Id(TaskID.newBuilder().setValue("app_foo_bar_682ebe64-0771-11e4-b05d-e0f84720c54e").build)
      taskId.runSpecId should equal("/app/foo/bar".toRootPath)
    }

    "TaskIds with encoded InstanceIds could be encoded" in {
      val taskId = Task.Id(TaskID.newBuilder().setValue("test_foo_bla_rest.instance-62d0f03f-79aa-11e6-a1a0-660c139c5e15.$anon").build)
      taskId.runSpecId should equal("/test/foo/bla/rest".toRootPath)
      taskId.instanceId.idString should equal("test_foo_bla_rest.instance-62d0f03f-79aa-11e6-a1a0-660c139c5e15")
    }

    "TaskIds with encoded InstanceIds could be encoded even with crucial path ids" in {
      val taskId = Task.Id(TaskID.newBuilder().setValue("test_foo.instance-_bla_rest.instance-62d0f03f-79aa-11e6-a1a0-660c139c5e15.$anon").build)
      taskId.runSpecId should equal("/test/foo.instance-/bla/rest".toRootPath)
      taskId.instanceId.idString should equal("test_foo.instance-_bla_rest.instance-62d0f03f-79aa-11e6-a1a0-660c139c5e15")
    }

    "TaskIds without specific instanceId should use taskId as instanceId" in {
      val taskId = Task.Id(TaskID.newBuilder().setValue("test_foo_bla_rest.62d0f03f-79aa-11e6-a1a0-660c139c5e15").build)
      taskId.runSpecId should equal("/test/foo/bla/rest".toRootPath)
      taskId.instanceId.idString should equal("test_foo_bla_rest.marathon-62d0f03f-79aa-11e6-a1a0-660c139c5e15")
    }

    "TaskIds for resident tasks can be created from legacy taskIds" in {
      val originalId = Task.Id.forRunSpec(PathId("/app"))
      originalId shouldBe a[Task.LegacyId]

      val newTaskId = Task.Id.forResidentTask(originalId)
      // this is considered the first attempt
      newTaskId shouldBe a[Task.LegacyResidentId]
      inside(newTaskId) {
        case Task.LegacyResidentId(_, _, _, attempt) =>
          attempt shouldBe 1
      }

      originalId shouldNot equal(newTaskId)
      originalId.instanceId shouldEqual newTaskId.instanceId
    }

    "TaskIds for resident tasks can be incremented" in {
      val taskIdString = "app.test.23.b6ff5fa5-7714-11e7-a55c-5ecf1c4671f6.41"
      val originalId = Task.Id(taskIdString)
      originalId shouldBe a[Task.LegacyResidentId]
      inside(originalId) {
        case Task.LegacyResidentId(_, _, _, attempt) =>
          attempt should be(41)
      }

      val newTaskId = Task.Id.forResidentTask(originalId)
      newTaskId shouldBe a[Task.LegacyResidentId]
      inside(newTaskId) {
        case Task.LegacyResidentId(_, _, _, attempt) =>
          attempt shouldBe 42
      }

      originalId shouldNot equal(newTaskId)
      originalId.instanceId shouldEqual newTaskId.instanceId
    }

    "TaskId.reservationId is the same as task id when task id is without attempt counter" in {
      val originalId = Task.Id.forRunSpec(PathId("/app/test/23"))
      val reservationId = originalId.reservationId

      reservationId shouldEqual originalId.idString
    }

    "TaskId.reservationId removes attempt from app task id" in {
      val originalId = Task.Id.forRunSpec(PathId("/app/test/23"))

      val residentTaskId = Task.Id.forResidentTask(originalId)
      residentTaskId.instanceId shouldEqual originalId.instanceId
      residentTaskId.reservationId shouldEqual originalId.reservationId

      val anotherResidentTaskId = Task.Id.forResidentTask(residentTaskId)
      anotherResidentTaskId.instanceId shouldEqual originalId.instanceId
      anotherResidentTaskId.reservationId shouldEqual originalId.reservationId
    }

    "TaskId.reservationId removes attempt and container name from pod task id" in {
      val originalId = Task.Id.forInstanceId(Instance.Id.forRunSpec(PathId("/app/test/23")), None)

      val residentTaskId = Task.Id.forResidentTask(originalId)
      residentTaskId.instanceId shouldEqual originalId.instanceId

      val anotherResidentTaskId = Task.Id.forResidentTask(residentTaskId)
      anotherResidentTaskId.instanceId shouldEqual originalId.instanceId

      anotherResidentTaskId.reservationId shouldEqual residentTaskId.reservationId
    }

    "TaskId.reservationId works as expected for all types of task ids" in {
      val appTaskId = Task.Id("app.4455cb85-0c16-490d-b84e-481f8321ff0a")
      appTaskId shouldBe a[Task.LegacyId]
      appTaskId.reservationId shouldEqual "app.4455cb85-0c16-490d-b84e-481f8321ff0a"

      val appResidentTaskIdWithAttempt = Task.Id("app.4455cb85-0c16-490d-b84e-481f8321ff0a.1")
      appResidentTaskIdWithAttempt shouldBe a[Task.LegacyResidentId]
      appResidentTaskIdWithAttempt.reservationId shouldEqual "app.4455cb85-0c16-490d-b84e-481f8321ff0a"

      val podTaskIdWithContainerName = Task.Id("app.instance-4455cb85-0c16-490d-b84e-481f8321ff0a.ct")
      podTaskIdWithContainerName shouldBe a[Task.EphemeralOrReservedTaskId]
      podTaskIdWithContainerName.reservationId shouldEqual "app.instance-4455cb85-0c16-490d-b84e-481f8321ff0a"

      val podTaskIdWithContainerNameAndAttempt = Task.Id("app.instance-4455cb85-0c16-490d-b84e-481f8321ff0a.ct.1")
      podTaskIdWithContainerNameAndAttempt shouldBe a[Task.ResidentTaskId]
      podTaskIdWithContainerNameAndAttempt.reservationId shouldEqual "app.instance-4455cb85-0c16-490d-b84e-481f8321ff0a"
    }

    "Numbered TaskId.reservationId remove number for all types of task ids" in {
      val uuid = "4455cb85-0c16-490d-b84e-481f8321ff0a"
      val numbered_uuid = f"0001.$uuid"
      val appTaskId = Task.Id(f"app.$numbered_uuid")
      appTaskId shouldBe a[Task.LegacyId]
      appTaskId.idString shouldEqual f"app.$uuid"
      appTaskId.reservationId shouldEqual f"app.$uuid"

      val appResidentTaskIdWithAttempt = Task.Id(f"app.$numbered_uuid.1")
      appResidentTaskIdWithAttempt shouldBe a[Task.LegacyResidentId]
      appResidentTaskIdWithAttempt.idString shouldEqual f"app.$uuid.1"
      appResidentTaskIdWithAttempt.reservationId shouldEqual f"app.$uuid"

      val podTaskIdWithContainerName = Task.Id(f"app.instance-$numbered_uuid.ct")
      podTaskIdWithContainerName shouldBe a[Task.EphemeralOrReservedTaskId]
      podTaskIdWithContainerName.idString shouldEqual f"app.instance-$uuid.ct"
      podTaskIdWithContainerName.reservationId shouldEqual f"app.instance-$uuid"

      val podTaskIdWithContainerNameAndAttempt = Task.Id(f"app.instance-$numbered_uuid.ct.1")
      podTaskIdWithContainerNameAndAttempt shouldBe a[Task.ResidentTaskId]
      podTaskIdWithContainerNameAndAttempt.idString shouldEqual f"app.instance-$uuid.ct.1"
      podTaskIdWithContainerNameAndAttempt.reservationId shouldEqual f"app.instance-$uuid"
    }
  }
}
