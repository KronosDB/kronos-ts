import { describe, expect, it } from "bun:test"
import { createRabbitMqTopologyNames } from "../topology.js"

describe("RabbitMQ topology names", () => {
  it("uses serviceName for durable command queues and instanceId for reply queues", () => {
    const topology = createRabbitMqTopologyNames({
      serviceName: "faculty-service",
      instanceId: "pod-1",
    })

    expect(topology.commandsExchange).toBe("kronos.commands")
    expect(topology.commandRoutingKey("faculty.SendNotification")).toBe("faculty.SendNotification")
    expect(topology.commandQueue("faculty.SendNotification")).toBe(
      "kronos.commands.faculty-service.faculty.SendNotification",
    )
    expect(topology.replyQueue()).toBe("kronos.replies.faculty-service.pod-1")
  })

  it("sanitizes queue segments", () => {
    const topology = createRabbitMqTopologyNames({
      serviceName: "faculty service",
      instanceId: "pod/1",
    })

    expect(topology.commandQueue("faculty.Send Notification")).toBe(
      "kronos.commands.faculty_service.faculty.Send_Notification",
    )
    expect(topology.replyQueue()).toBe("kronos.replies.faculty_service.pod_1")
  })
})
