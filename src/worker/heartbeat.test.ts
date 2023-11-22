import { MongoClient, ObjectId } from "mongodb";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { initJobProcessor } from "../setup.ts";
import {
  heartbeatEvent,
  startHeartbeat,
  startSyncHeartbeat,
  workerId,
} from "./heartbeat.ts";
import { getWorkerCollection } from "../data/actions.ts";
import { IWorker } from "../index.ts";

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
  client = new MongoClient(
    `mongodb://127.0.0.1:27018/${process.env["VITEST_POOL_ID"]}_${process.env["VITEST_WORKER_ID"]}`,
  );
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

describe("startHeartbeat", () => {
  it("should manage heartbeat lifecycle", async () => {
    const abortController = new AbortController();
    await startHeartbeat(true, abortController.signal);
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
    let onPing = new Promise((resolve) => heartbeatEvent.once("ping", resolve));
    await vi.runOnlyPendingTimersAsync();
    await onPing;

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
    onPing = new Promise((resolve) => heartbeatEvent.once("ping", resolve));
    await vi.runOnlyPendingTimersAsync();
    await onPing;

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

  it("when isWorker=true should exit on heartbeat error", async () => {
    const abortController = new AbortController();
    await startHeartbeat(true, abortController.signal);
    expect(workerId).toEqual(expect.any(ObjectId));

    await getWorkerCollection().deleteOne({ _id: workerId });

    // Execute next interval
    const onKill = new Promise((resolve) => {
      heartbeatEvent.once("kill", resolve);
    });
    await vi.runOnlyPendingTimersAsync();
    await expect(onKill).resolves.toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("when isWorker=false should keep trying on heartbeat error", async () => {
    const abortController = new AbortController();
    await startHeartbeat(false, abortController.signal);
    expect(workerId).toEqual(expect.any(ObjectId));

    await getWorkerCollection().deleteOne({ _id: workerId });

    // Execute next interval
    const onPing = new Promise((resolve) =>
      heartbeatEvent.once("ping", resolve),
    );
    await vi.runOnlyPendingTimersAsync();
    await expect(onPing).resolves.toBeUndefined();
    expect(vi.getTimerCount()).toBe(1);

    // Expect worker has been re-created
    let workers = await getWorkerCollection()
      .find({}, { sort: { lastSeen: 1 } })
      .toArray();
    expect(workers).toHaveLength(3);
    expect(workers[0]).toEqual(otherWorkers[0]);
    expect(workers[1]).toEqual(otherWorkers[1]);
    expect(workers[2]).toEqual({
      _id: workerId,
      hostname: expect.any(String),
      lastSeen: new Date(startDate.getTime() + 30_000),
    });

    const onStop = new Promise((resolve) =>
      heartbeatEvent.once("stop", resolve),
    );
    abortController.abort();
    await onStop;
    expect(vi.getTimerCount()).toBe(0);

    workers = await getWorkerCollection().find({}).toArray();
    // When not ran in worker mode, we should not delete worker
    expect(workers).toHaveLength(3);
    expect(workers[0]).toEqual(otherWorkers[0]);
    expect(workers[1]).toEqual(otherWorkers[1]);
    expect(workers[2]).toEqual({
      _id: workerId,
      hostname: expect.any(String),
      lastSeen: new Date(startDate.getTime() + 30_000),
    });
  });
});

describe("startSyncHeartbeat", () => {
  it("should manage concurrency", async () => {
    let worker = await getWorkerCollection().findOne({ _id: workerId });
    expect(worker).toBe(null);

    // 2 Jobs starting concurrently
    const [finallyCb1, finallyCb2] = await Promise.all([
      startSyncHeartbeat(),
      startSyncHeartbeat(),
    ]);

    // Once started, worker should exists
    worker = await getWorkerCollection().findOne({ _id: workerId });
    expect(worker?.lastSeen).toEqual(startDate);

    // It should have registered interval to update timer
    expect(vi.getTimerCount()).toBe(1);

    // Execute next interval
    let onPing = new Promise((resolve) => heartbeatEvent.once("ping", resolve));
    await vi.runOnlyPendingTimersAsync();
    await onPing;

    // Expect lastSeen has been updated
    worker = await getWorkerCollection().findOne({ _id: workerId });
    expect(worker?.lastSeen).toEqual(new Date(startDate.getTime() + 30_000));

    // Job #2 stops
    finallyCb2();

    // Heartbeat should continue
    expect(vi.getTimerCount()).toBe(1);

    // Execute next interval
    onPing = new Promise((resolve) => heartbeatEvent.once("ping", resolve));
    await vi.runOnlyPendingTimersAsync();
    await onPing;

    // Expect lastSeen has been updated
    worker = await getWorkerCollection().findOne({ _id: workerId });
    expect(worker?.lastSeen).toEqual(new Date(startDate.getTime() + 60_000));

    // New job starting
    const finallyCb3 = await startSyncHeartbeat();

    // Execute next interval
    onPing = new Promise((resolve) => heartbeatEvent.once("ping", resolve));
    await vi.runOnlyPendingTimersAsync();
    await onPing;

    // Expect lastSeen has been updated
    worker = await getWorkerCollection().findOne({ _id: workerId });
    expect(worker?.lastSeen).toEqual(new Date(startDate.getTime() + 90_000));

    // Job #1 stops
    finallyCb1();

    // Heartbeat should continue
    expect(vi.getTimerCount()).toBe(1);

    // Execute next interval
    onPing = new Promise((resolve) => heartbeatEvent.once("ping", resolve));
    await vi.runOnlyPendingTimersAsync();
    await onPing;

    // Expect lastSeen has been updated
    worker = await getWorkerCollection().findOne({ _id: workerId });
    expect(worker?.lastSeen).toEqual(new Date(startDate.getTime() + 120_000));

    let onStop = new Promise((resolve) => heartbeatEvent.once("stop", resolve));
    // Job #3 stops
    finallyCb3();

    // It should stop
    await onStop;

    expect(vi.getTimerCount()).toBe(0);

    worker = await getWorkerCollection().findOne({ _id: workerId });
    expect(worker).not.toBe(null);

    // 2 Jobs starting concurrently
    const [finallyCb4, finallyCb5] = await Promise.all([
      startSyncHeartbeat(),
      startSyncHeartbeat(),
    ]);

    // Heartbeat should restart
    expect(vi.getTimerCount()).toBe(1);

    // Expect worker has been created
    worker = await getWorkerCollection().findOne({ _id: workerId });
    expect(worker?.lastSeen).toEqual(new Date(startDate.getTime() + 120_000));

    onStop = new Promise((resolve) => heartbeatEvent.once("stop", resolve));
    // Jobs stopping concurrently
    finallyCb4();
    finallyCb5();
    await onStop;

    expect(vi.getTimerCount()).toBe(0);
  });
});
