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
    expect(topology.queriesExchange).toBe("kronos.queries")
    expect(topology.queryRoutingKey("faculty.GetSchedule")).toBe("faculty.GetSchedule")
    expect(topology.queryQueue("faculty.GetSchedule")).toBe(
      "kronos.queries.faculty-service.faculty.GetSchedule",
    )
    expect(topology.subscribersGossipExchange).toBe("kronos.subscribers.gossip")
    expect(topology.subscribersDirectExchange).toBe("kronos.subscribers.direct")
    expect(topology.subscribersGossipQueue()).toBe(
      "kronos.subscribers.gossip.faculty-service.pod-1",
    )
    expect(topology.subscribersDirectQueue()).toBe(
      "kronos.subscribers.direct.faculty-service.pod-1",
    )
    expect(topology.commandReplyQueue()).toBe("kronos.replies.faculty-service.pod-1")
    expect(topology.queryReplyQueue()).toBe("kronos.query-replies.faculty-service.pod-1")
  })

  it("sanitizes queue segments", () => {
    const topology = createRabbitMqTopologyNames({
      serviceName: "faculty service",
      instanceId: "pod/1",
    })

    expect(topology.commandQueue("faculty.Send Notification")).toBe(
      "kronos.commands.faculty_service.faculty.Send_Notification",
    )
    expect(topology.commandReplyQueue()).toBe("kronos.replies.faculty_service.pod_1")
    expect(topology.queryReplyQueue()).toBe("kronos.query-replies.faculty_service.pod_1")
    expect(topology.queryQueue("faculty.Get Schedule")).toBe(
      "kronos.queries.faculty_service.faculty.Get_Schedule",
    )
    expect(topology.subscribersGossipQueue()).toBe(
      "kronos.subscribers.gossip.faculty_service.pod_1",
    )
    expect(topology.subscribersDirectQueue()).toBe(
      "kronos.subscribers.direct.faculty_service.pod_1",
    )
  })
})
