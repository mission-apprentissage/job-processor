import { MongoClient, ObjectId } from "mongodb";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import type { IJobsCronTask, IJobsSimple, IWorker } from "../index.ts";
import { workerId } from "../worker/workerId.ts";
import { getOptions } from "../options.ts";
import {
  configureDb,
  createJobCronTask,
  createJobSimple,
  detectExitedJobs,
  getCronTaskJob,
  getJobCollection,
  getSimpleJob,
  getWorkerCollection,
  pickNextJob,
} from "./actions.ts";

let client: MongoClient | null;

vi.mock("../options.ts");
beforeAll(async () => {
  client = new MongoClient(
    `mongodb://127.0.0.1:27019/${process.env["VITEST_POOL_ID"]}`,
  );
  await client.connect();
  vi.mocked(getOptions).mockReturnValue({
    logger: {
      debug: vi.fn() as any,
      info: vi.fn() as any,
      warn: vi.fn() as any,
      error: vi.fn() as any,
      child: vi.fn() as any,
    },
    db: client.db(),
    jobs: {
      anyOne: {
        handler: vi.fn() as any,
        tag: null,
      },
      onlyA: {
        handler: vi.fn() as any,
        tag: "A",
      },
      sameNameTagDiff: {
        handler: vi.fn() as any,
        tag: "B",
      },
    },
    crons: {
      anyOne: {
        cron_string: "* * * * *",
        handler: vi.fn() as any,
      },
      onlyB: {
        cron_string: "* * * * *",
        handler: vi.fn() as any,
        tag: "B",
      },
      sameNameTagDiff: {
        cron_string: "* * * * *",
        handler: vi.fn() as any,
        tag: "A",
      },
    },
  });

  await configureDb();

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
    Partial<Pick<IJobsSimple, "worker_id" | "started_at" | "name">>,
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
    concurrency: { mode: "concurrent" },
    ...data,
  };
}

function createCronTaskJob(
  data: Pick<IJobsCronTask, "status" | "scheduled_for"> &
    Partial<Pick<IJobsCronTask, "worker_id" | "started_at" | "name">>,
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
    concurrency: { mode: "concurrent" },
    ...data,
  };
}

function createWorker(
  data: Pick<IWorker, "hostname" | "lastSeen"> & Partial<Pick<IWorker, "_id">>,
): IWorker {
  return {
    _id: new ObjectId(),
    tags: null,
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

      expect(await pickNextJob()).toEqual({
        ...jobs[3],
        worker_id: workerId,
        status: "running",
        started_at: now,
      });
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

      expect(await pickNextJob()).toEqual({
        ...jobs[0],
        worker_id: workerId,
        status: "running",
        started_at: past,
      });
      expect(await getSimpleJob(jobs[0]._id)).toEqual({
        ...jobs[0],
        worker_id: workerId,
        status: "running",
        started_at: past,
      });
    });

    it("should pick tagged job if workerTags is null", async () => {
      const jobs = [
        createSimpleJob({
          status: "pending",
          scheduled_for: past,
          name: "onlyA",
        }),
        createCronTaskJob({
          status: "pending",
          scheduled_for: agesAgo,
          name: "onlyB",
        }),
      ] as const;
      await getJobCollection().insertMany([...jobs]);

      expect(await pickNextJob()).toEqual({
        ...jobs[1],
        worker_id: workerId,
        status: "running",
        started_at: now,
      });
      expect(await pickNextJob()).toEqual({
        ...jobs[0],
        worker_id: workerId,
        status: "running",
        started_at: now,
      });
      expect(await pickNextJob()).toBe(null);
    });
  });

  describe("when worker tags are set", async () => {
    beforeEach(async () => {
      vi.mocked(getOptions).mockReturnValue({
        ...getOptions(),
        workerTags: ["A"],
      });
    });

    it("should pick jobs with null tag or matching tag", async () => {
      const jobs = [
        createSimpleJob({
          status: "pending",
          scheduled_for: past,
          name: "onlyA",
        }),
        createSimpleJob({
          status: "pending",
          scheduled_for: past,
          name: "anyOne",
        }),
        createSimpleJob({
          status: "pending",
          scheduled_for: agesAgo,
          name: "sameNameTagDiff",
        }),
        createCronTaskJob({
          status: "pending",
          scheduled_for: agesAgo,
          name: "anyOne",
        }),
        createCronTaskJob({
          status: "pending",
          scheduled_for: agesAgo,
          name: "onlyB",
        }),
        createCronTaskJob({
          status: "pending",
          scheduled_for: agesAgo,
          name: "sameNameTagDiff",
        }),
      ] as const;
      await getJobCollection().insertMany([...jobs]);

      const todo = [];
      let next = await pickNextJob();
      while (next !== null) {
        todo.push(next._id.toString());
        next = await pickNextJob();
      }
      todo.sort();

      const expected = [
        jobs[0]._id.toString(),
        jobs[1]._id.toString(),
        jobs[3]._id.toString(),
        jobs[5]._id.toString(),
      ];
      expected.sort();

      expect(todo).toEqual(expected);
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

describe("concurrency feature", () => {
  describe("simple jobs", () => {
    it("should skip job when concurrency mode is exclusive and conflict exists", async () => {
      vi.mocked(getOptions).mockReturnValue({
        ...getOptions(),
        jobs: {
          exclusiveJob: {
            handler: vi.fn() as any,
            concurrency: { mode: "exclusive" },
            tag: null,
          },
        },
      });

      const job1 = await createJobSimple({
        name: "exclusiveJob",
        payload: {},
        scheduled_for: now,
        sync: false,
      });

      // Mark first job as running
      await getJobCollection().updateOne(
        { _id: job1._id },
        { $set: { status: "running", worker_id: workerId } },
      );

      // Try to create another job with same name
      // With exclusive mode, should be created as "skipped" immediately
      const job2 = await createJobSimple({
        name: "exclusiveJob",
        payload: { name: "Moroine" },
        scheduled_for: now,
        sync: false,
      });

      // Job2 should be created as "skipped" (not pending)
      expect(job2.status).toBe("skipped");
      expect(job2.output?.skip_metadata).toMatchObject({
        reason: "noConcurrent_conflict",
        conflicting_job_id: job1._id,
      });
    });

    it("should allow concurrent jobs when concurrency mode is concurrent", async () => {
      vi.mocked(getOptions).mockReturnValue({
        ...getOptions(),
        jobs: {
          concurrentJob: {
            handler: vi.fn() as any,
            concurrency: { mode: "concurrent" },
            tag: null,
          },
        },
      });

      const job1 = await createJobSimple({
        name: "concurrentJob",
        payload: {},
        scheduled_for: now,
        sync: false,
      });

      // Mark first job as running
      await getJobCollection().updateOne(
        { _id: job1._id },
        { $set: { status: "running", worker_id: workerId } },
      );

      // Try to create another job with same name
      const job2 = await createJobSimple({
        name: "concurrentJob",
        payload: {},
        scheduled_for: now,
        sync: false,
      });

      // Job2 should be created as "pending" (concurrent allowed)
      expect(job2.status).toBe("pending");
      expect(job2._id.toString()).not.toBe(job1._id.toString());
    });
  });

  describe("CRON tasks", () => {
    it("should skip CRON task when concurrency mode is exclusive and conflict exists", async () => {
      vi.mocked(getOptions).mockReturnValue({
        ...getOptions(),
        crons: {
          exclusiveCron: {
            cron_string: "* * * * *",
            handler: vi.fn() as any,
            concurrency: { mode: "exclusive" },
            tag: null,
          },
        },
      });

      const task1 = await createJobCronTask({
        name: "exclusiveCron",
        scheduled_for: now,
      });

      // Mark first task as running
      await getJobCollection().updateOne(
        { _id: task1._id },
        { $set: { status: "running", worker_id: workerId } },
      );

      // Try to create another task with same name
      const task2 = await createJobCronTask({
        name: "exclusiveCron",
        scheduled_for: now,
      });

      // Task2 should be created as "skipped"
      expect(task2.status).toBe("skipped");
      expect(task2.output?.skip_metadata).toMatchObject({
        reason: "noConcurrent_conflict",
        conflicting_job_id: task1._id,
      });
    });
  });
});
