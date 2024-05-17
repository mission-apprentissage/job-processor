import { MongoClient, ObjectId } from "mongodb";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { initJobProcessor } from "../setup.ts";
import {
  detectExitedJobs,
  getCronTaskJob,
  getJobCollection,
  getSimpleJob,
  getWorkerCollection,
  pickNextJob,
} from "../data/actions.ts";
import { IJobsCronTask, IJobsSimple, IWorker } from "../index.ts";
import { workerId } from "../worker/heartbeat.ts";

let client: MongoClient | null;

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

const now = new Date("2023-11-17T11:00:00.000Z");
const future = new Date("2023-11-17T11:05:00.000Z");
const past = new Date("2023-11-17T10:55:00.000Z");
const agesAgo = new Date("2023-01-01T00:00:00.000Z");

beforeEach(async () => {
  await getJobCollection().deleteMany({});

  vi.useFakeTimers();
  vi.setSystemTime(now);

  return () => {
    vi.useRealTimers();
  };
});

function createSimpleJob(
  data: Pick<IJobsSimple, "status" | "scheduled_for"> &
    Partial<Pick<IJobsSimple, "worker_id" | "started_at">>,
): IJobsSimple {
  const now = new Date();

  return {
    _id: new ObjectId(),
    name: "hello",
    type: "simple",
    sync: false,
    payload: { name: "Moroine" },
    output: null,
    started_at: null,
    ended_at: null,
    updated_at: now,
    created_at: now,
    worker_id: null,
    ...data,
  };
}

function createCronTaskJob(
  data: Pick<IJobsCronTask, "status" | "scheduled_for"> &
    Partial<Pick<IJobsCronTask, "worker_id" | "started_at">>,
): IJobsCronTask {
  const now = new Date();

  return {
    _id: new ObjectId(),
    name: "hello",
    type: "cron_task",
    started_at: null,
    ended_at: null,
    updated_at: now,
    created_at: now,
    worker_id: null,
    ...data,
  };
}

function createWorker(
  data: Pick<IWorker, "hostname" | "lastSeen"> & Partial<Pick<IWorker, "_id">>,
): IWorker {
  return {
    _id: new ObjectId(),
    ...data,
  };
}

describe("pickNextJob", () => {
  describe("when no jobs are scheduled", async () => {
    it("should return null", async () => {
      await getJobCollection().insertMany([
        createSimpleJob({ status: "finished", scheduled_for: past }),
        createSimpleJob({ status: "pending", scheduled_for: future }),
        createSimpleJob({ status: "errored", scheduled_for: past }),
        createSimpleJob({
          status: "running",
          scheduled_for: future,
          worker_id: new ObjectId(),
        }),
        createCronTaskJob({ status: "finished", scheduled_for: past }),
        createCronTaskJob({ status: "pending", scheduled_for: future }),
        createCronTaskJob({ status: "errored", scheduled_for: past }),
        createCronTaskJob({
          status: "running",
          scheduled_for: future,
          worker_id: new ObjectId(),
        }),
      ]);

      expect(await pickNextJob()).toBe(null);
    });
  });

  describe("when jobs are scheduled", async () => {
    it("should return oldest scheduled and update status + worker_id", async () => {
      const jobs = [
        createSimpleJob({ status: "pending", scheduled_for: future }),
        createCronTaskJob({ status: "pending", scheduled_for: future }),
        createSimpleJob({ status: "pending", scheduled_for: past }),
        createCronTaskJob({ status: "pending", scheduled_for: agesAgo }),
      ] as const;
      await getJobCollection().insertMany([...jobs]);

      expect(await pickNextJob()).toEqual(jobs[3]);
      expect(await getCronTaskJob(jobs[3]._id)).toEqual({
        ...jobs[3],
        worker_id: workerId,
        status: "running",
        started_at: now,
      });
    });

    it("should return paused jobs but preserve old started_at", async () => {
      const jobs = [
        createSimpleJob({
          status: "paused",
          scheduled_for: agesAgo,
          started_at: past,
        }),
      ] as const;
      await getJobCollection().insertMany([...jobs]);

      expect(await pickNextJob()).toEqual(jobs[0]);
      expect(await getSimpleJob(jobs[0]._id)).toEqual({
        ...jobs[0],
        worker_id: workerId,
        status: "running",
        started_at: past,
      });
    });
  });
});

describe("detectExitedJobs", () => {
  it("should marked them as failed", async () => {
    const activeWorkers = [
      createWorker({ hostname: "active", lastSeen: past }),
      createWorker({ hostname: "self", lastSeen: past, _id: workerId }),
    ] as const;

    const newlyActiveWorkerId = new ObjectId();
    const removedWorkerIdNotSoLongAgo = new ObjectId();
    const removedWorkerId = new ObjectId();

    const jobs = [
      createSimpleJob({ status: "pending", scheduled_for: past }),
      createSimpleJob({
        status: "running",
        scheduled_for: past,
        started_at: past,
        worker_id: removedWorkerIdNotSoLongAgo,
      }),
      createSimpleJob({
        status: "running",
        scheduled_for: agesAgo,
        started_at: new Date(past.getTime() - 1_000),
        worker_id: removedWorkerId,
      }),
      createSimpleJob({
        status: "running",
        scheduled_for: past,
        started_at: agesAgo,
        worker_id: activeWorkers[0]._id,
      }),
      createCronTaskJob({
        status: "running",
        scheduled_for: past,
        started_at: now,
        worker_id: newlyActiveWorkerId,
      }),
    ] as const;
    await getJobCollection().insertMany([...jobs]);

    await getWorkerCollection().insertMany([...activeWorkers]);

    const expectedJobs = [
      jobs[0],
      // Less than 5min ago, let it be for now
      jobs[1],
      {
        ...jobs[2],
        status: "errored",
        output: {
          duration: "--",
          result: null,
          error: "Worker crashed unexpectly",
        },
        ended_at: now,
      },
      jobs[3],
      jobs[4],
    ];

    expect(await detectExitedJobs()).toEqual(expectedJobs[2]);
    expect(await detectExitedJobs()).toEqual(null);

    expect(await getJobCollection().find({}).toArray()).toEqual(expectedJobs);
  });
});
