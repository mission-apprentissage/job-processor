import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { MongoClient, ObjectId } from "mongodb";
import { getOptions } from "../options.ts";
import { getJobCollection, getSignalCollection } from "../data/actions.ts";
import { workerId } from "../worker/workerId.ts";
import { sleep } from "../../utils/sleep.ts";
import type { IJobsSimple } from "../../common/model.ts";
import {
  clearJobKillSignal,
  getJobKillSignal,
  killJob,
  listenSignalCollection,
} from "./signal.ts";

let client: MongoClient | null;

vi.mock("../options.ts");

describe.each([
  ["standalone", `mongodb://127.0.0.1:27019/${process.env["VITEST_POOL_ID"]}`],
  [
    "replica set",
    `mongodb://root:password@127.0.0.1:27020/${process.env["VITEST_POOL_ID"]}?authSource=admin&directConnection=true`,
  ],
])("with MongoDB %s", (_title, url) => {
  beforeAll(async () => {
    client = new MongoClient(url);
    await client.connect();
    vi.mocked(getOptions).mockReturnValue({
      logger: {
        debug: vi.fn() as any,
        info: vi.fn() as any,
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

    return async () => {
      await client?.close();
    };
  });

  describe("listenSignalCollection", () => {
    beforeEach(async () => {
      await getSignalCollection().deleteMany({});
      await getJobCollection().deleteMany({});
      process.env["TEST"] = "true";
    });

    it("should process job kill signal for running job", async () => {
      const controller = new AbortController();
      await listenSignalCollection(controller.signal);

      const jobId = new ObjectId();
      await getJobCollection().insertOne({
        _id: jobId,
        type: "simple",
        name: "anyOne",
        payload: {},
        status: "running",
        worker_id: workerId,
        created_at: new Date(),
        updated_at: new Date(),
        started_at: new Date(),
        scheduled_for: new Date(),
        sync: false,
      });

      const jobSignal = getJobKillSignal(jobId);

      await getSignalCollection().insertOne({
        _id: new ObjectId(),
        type: "kill",
        job_id: jobId,
        worker_id: workerId,
        ack: false,
        created_at: new Date(),
      });
      await sleep(100);
      expect.soft(jobSignal.aborted).toBe(true);
      controller.abort();
    });

    it("should process job kill signal for running job with jobSignal not yet created", async () => {
      const controller = new AbortController();
      await listenSignalCollection(controller.signal);

      const jobId = new ObjectId();
      await getJobCollection().insertOne({
        _id: jobId,
        type: "simple",
        name: "anyOne",
        payload: {},
        status: "running",
        worker_id: workerId,
        created_at: new Date(),
        updated_at: new Date(),
        started_at: new Date(),
        scheduled_for: new Date(),
        sync: false,
      });

      await getSignalCollection().insertOne({
        _id: new ObjectId(),
        type: "kill",
        job_id: jobId,
        worker_id: workerId,
        ack: false,
        created_at: new Date(),
      });

      await sleep(100);
      const jobSignal = getJobKillSignal(jobId);
      await sleep(100);

      expect.soft(jobSignal.aborted).toBe(true);
      controller.abort();
    });

    it.each<[IJobsSimple["status"]]>([["pending"], ["paused"]])(
      "should not process job kill signal for job is %s status",
      async (status) => {
        const controller = new AbortController();
        await listenSignalCollection(controller.signal);

        const jobId = new ObjectId();
        await getJobCollection().insertOne({
          _id: jobId,
          type: "simple",
          name: "anyOne",
          payload: {},
          status,
          worker_id: null,
          created_at: new Date(),
          updated_at: new Date(),
          started_at: null,
          scheduled_for: new Date(),
          sync: false,
        });

        await getSignalCollection().insertOne({
          _id: new ObjectId(),
          type: "kill",
          job_id: jobId,
          worker_id: workerId,
          ack: false,
          created_at: new Date(),
        });
        await sleep(100);
        const job = await getJobCollection().findOne({ _id: jobId });
        expect.soft(job?.status).toBe("killed");
        controller.abort();
      },
    );

    it("should not process job kill signal for job running on another worker", async () => {
      const controller = new AbortController();
      await listenSignalCollection(controller.signal);

      const jobId = new ObjectId();
      const otherWorkerId = new ObjectId();
      await getJobCollection().insertOne({
        _id: jobId,
        type: "simple",
        name: "anyOne",
        payload: {},
        status: "running",
        worker_id: otherWorkerId,
        created_at: new Date(),
        updated_at: new Date(),
        started_at: new Date(),
        scheduled_for: new Date(),
        sync: false,
      });

      const jobSignal = getJobKillSignal(jobId);
      await getSignalCollection().insertOne({
        _id: new ObjectId(),
        type: "kill",
        job_id: jobId,
        worker_id: otherWorkerId,
        ack: false,
        created_at: new Date(),
      });
      await sleep(100);
      expect.soft(jobSignal.aborted).toBe(false); // should not be aborted
      controller.abort();
    });

    it.each<[IJobsSimple["status"]]>([["errored"], ["finished"], ["killed"]])(
      "should ignore job kill signal for job is %s status",
      async (status) => {
        const controller = new AbortController();
        await listenSignalCollection(controller.signal);
        const jobId = new ObjectId();
        await getJobCollection().insertOne({
          _id: jobId,
          type: "simple",
          name: "anyOne",
          payload: {},
          status,
          worker_id: workerId,
          created_at: new Date(),
          updated_at: new Date(),
          started_at: new Date(),
          ended_at: new Date(),
          scheduled_for: new Date(),
          sync: false,
        });

        await getSignalCollection().insertOne({
          _id: new ObjectId(),
          type: "kill",
          job_id: jobId,
          worker_id: workerId,
          ack: false,
          created_at: new Date(),
        });
        await sleep(100);
        const job = await getJobCollection().findOne({ _id: jobId });
        expect.soft(job?.status).toBe(status);
        controller.abort();
      },
    );

    it("should handle job exited before signal processed", async () => {
      const controller = new AbortController();
      await listenSignalCollection(controller.signal);

      const jobId = new ObjectId();
      await getJobCollection().insertOne({
        _id: jobId,
        type: "simple",
        name: "anyOne",
        payload: {},
        status: "running",
        worker_id: workerId,
        created_at: new Date(),
        updated_at: new Date(),
        started_at: new Date(),
        scheduled_for: new Date(),
        sync: false,
      });

      await getSignalCollection().insertOne({
        _id: new ObjectId(),
        type: "kill",
        job_id: jobId,
        worker_id: workerId,
        ack: false,
        created_at: new Date(),
      });
      await sleep(100);
      const jobSignal = getJobKillSignal(jobId);
      clearJobKillSignal(jobId);
      await sleep(100);
      expect.soft(jobSignal.aborted).toBe(false);
      controller.abort();
    });
  });

  describe("killJob", () => {
    beforeEach(async () => {
      await getSignalCollection().deleteMany({});
      await getJobCollection().deleteMany({});
      process.env["TEST"] = "true";
    });

    it("should kill a running job", async () => {
      const jobId = new ObjectId();
      await getJobCollection().insertOne({
        _id: jobId,
        type: "simple",
        name: "anyOne",
        payload: {},
        status: "running",
        worker_id: workerId,
        created_at: new Date(),
        updated_at: new Date(),
        started_at: new Date(),
        scheduled_for: new Date(),
        sync: false,
      });

      const jobSignal = getJobKillSignal(jobId);

      await killJob(jobId);
      await sleep(100);
      expect.soft(jobSignal.aborted).toBe(true);
    });

    it.each<[IJobsSimple["status"]]>([["pending"], ["paused"]])(
      "should kill a job in %s status",
      async (status) => {
        const jobId = new ObjectId();
        await getJobCollection().insertOne({
          _id: jobId,
          type: "simple",
          name: "anyOne",
          payload: {},
          status,
          worker_id: null,
          created_at: new Date(),
          updated_at: new Date(),
          started_at: null,
          scheduled_for: new Date(),
          sync: false,
        });

        await killJob(jobId);
        const job = await getJobCollection().findOne({ _id: jobId });
        expect.soft(job?.status).toBe("killed");
      },
    );

    it("should send kill signal for job running on another worker", async () => {
      const jobId = new ObjectId();
      const otherWorkerId = new ObjectId();
      await getJobCollection().insertOne({
        _id: jobId,
        type: "simple",
        name: "anyOne",
        payload: {},
        status: "running",
        worker_id: otherWorkerId,
        created_at: new Date(),
        updated_at: new Date(),
        started_at: new Date(),
        scheduled_for: new Date(),
        sync: false,
      });

      const killJobPromise = killJob(jobId);
      const signal = vi.waitUntil(async () => {
        return getSignalCollection().findOne({
          job_id: jobId,
          worker_id: otherWorkerId,
        });
      });
      await expect.soft(killJobPromise).resolves.toBeUndefined();
      expect.soft(signal).toBeDefined();
    });

    it.each<[IJobsSimple["status"]]>([["errored"], ["finished"], ["killed"]])(
      "should ignore killing a job in %s status",
      async (status) => {
        const jobId = new ObjectId();
        await getJobCollection().insertOne({
          _id: jobId,
          type: "simple",
          name: "anyOne",
          payload: {},
          status,
          worker_id: workerId,
          created_at: new Date(),
          updated_at: new Date(),
          started_at: new Date(),
          ended_at: new Date(),
          scheduled_for: new Date(),
          sync: false,
        });

        await killJob(jobId);
        await sleep(100);
        const job = await getJobCollection().findOne({ _id: jobId });
        expect.soft(job?.status).toBe(status);
      },
    );
  });
});
