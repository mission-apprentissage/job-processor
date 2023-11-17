import { MongoClient, ObjectId } from "mongodb";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { initJobProcessor } from "../setup.ts";
import { heartbeatEvent, startHeartbeat, workerId } from "./heartbeat.ts";
import { getWorkerCollection } from "../data/actions.ts";
import { IWorker } from "../index.ts";

describe("heartbeat", () => {
  let client: MongoClient | null;
  const startDate = new Date("2023-11-17T11:00:00.000Z");
  const otherWorkers: IWorker[] = [
    {
      _id: new ObjectId(),
      hostname: "a",
      lastSeen: new Date(startDate.getTime() - 1_000),
    },
    {
      _id: new ObjectId(),
      hostname: "b",
      lastSeen: new Date(startDate.getTime() - 500),
    },
  ];

  beforeAll(async () => {
    client = new MongoClient("mongodb://127.0.0.1:27018");
    await client.connect();
    await initJobProcessor({
      logger: {
        debug: vi.fn() as any,
        info: vi.fn() as any,
        error: vi.fn() as any,
        child: vi.fn() as any,
      },
      db: client.db(),
      jobs: {},
      crons: {},
    });

    return async () => {
      await client?.close();
    };
  });

  beforeEach(async () => {
    await getWorkerCollection().deleteMany({});
    await getWorkerCollection().insertMany(otherWorkers);

    vi.useFakeTimers();
    vi.setSystemTime(startDate);

    return () => {
      vi.useRealTimers();
    };
  });

  it("should manage heartbeat lifecycle", async () => {
    const abortController = new AbortController();
    await startHeartbeat(abortController.signal);
    expect(workerId).toEqual(expect.any(ObjectId));

    // Once started, it should be referenced in worker collection without affecting other workers
    let workers = await getWorkerCollection()
      .find({}, { sort: { lastSeen: 1 } })
      .toArray();
    expect(workers).toHaveLength(3);
    expect(workers[0]).toEqual(otherWorkers[0]);
    expect(workers[1]).toEqual(otherWorkers[1]);
    expect(workers[2]).toEqual({
      _id: workerId,
      hostname: expect.any(String),
      lastSeen: startDate,
    });

    // It should have registered interval to update timer
    expect(vi.getTimerCount()).toBe(1);

    // Execute next interval
    await vi.runOnlyPendingTimersAsync();
    await new Promise((resolve) => heartbeatEvent.once("ping", resolve));

    // Expect lastSeen has been updated
    workers = await getWorkerCollection().find({}).toArray();
    expect(workers).toHaveLength(3);
    expect(workers[0]).toEqual(otherWorkers[0]);
    expect(workers[1]).toEqual(otherWorkers[1]);
    expect(workers[2]).toEqual({
      _id: workerId,
      hostname: expect.any(String),
      lastSeen: new Date(startDate.getTime() + 30_000),
    });

    // Execute next interval
    await vi.runOnlyPendingTimersAsync();
    await new Promise((resolve) => heartbeatEvent.once("ping", resolve));

    // Expect lastSeen has been updated
    workers = await getWorkerCollection().find({}).toArray();
    expect(workers).toHaveLength(3);
    expect(workers[0]).toEqual(otherWorkers[0]);
    expect(workers[1]).toEqual(otherWorkers[1]);
    expect(workers[2]).toEqual({
      _id: workerId,
      hostname: expect.any(String),
      lastSeen: new Date(startDate.getTime() + 60_000),
    });

    abortController.abort();
    await new Promise((resolve) => heartbeatEvent.once("stop", resolve));
    expect(vi.getTimerCount()).toBe(0);

    workers = await getWorkerCollection().find({}).toArray();
    expect(workers).toHaveLength(2);
    expect(workers[0]).toEqual(otherWorkers[0]);
    expect(workers[1]).toEqual(otherWorkers[1]);
  });

  it("should exit when heartbeat failed", async () => {
    const abortController = new AbortController();
    await startHeartbeat(abortController.signal);
    expect(workerId).toEqual(expect.any(ObjectId));

    await getWorkerCollection().deleteOne({ _id: workerId });

    // Execute next interval
    await vi.runOnlyPendingTimersAsync();
    await expect(
      new Promise((resolve) => {
        heartbeatEvent.once("kill", resolve);
      }),
    ).resolves.toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
  });
});
