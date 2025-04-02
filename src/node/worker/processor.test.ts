import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { processorEventEmitter, runJobProcessor } from "./processor.ts";
import { MongoClient } from "mongodb";
import { initJobProcessor } from "../setup.ts";
import { getJobCollection, getWorkerCollection } from "../data/actions.ts";
import { addJob } from "../index.ts";

describe("runJobProcessor", () => {
  let client: MongoClient | null;
  const jobMock = vi.fn();
  const onExitMock = vi.fn();

  beforeAll(async () => {
    client = new MongoClient(
      `mongodb://127.0.0.1:27019/${process.env["VITEST_POOL_ID"]}_${process.env["VITEST_WORKER_ID"]}`,
    );
    await client.connect();
    const logger = {
      debug: vi.fn() as any,
      info: vi.fn() as any,
      error: vi.fn() as any,
      child: () => logger,
    };
    await initJobProcessor({
      logger,
      db: client.db(),
      jobs: {
        test: {
          handler: jobMock,
        },
        testWithExitHandler: {
          handler: jobMock,
          onJobExited: onExitMock,
        },
      },
      crons: {},
    });

    return async () => {
      await client?.close();
    };
  });

  beforeEach(async () => {
    await getJobCollection().deleteMany({});
    await getWorkerCollection().deleteMany({});
  });

  it("should run jobs", async () => {
    const ctrl = new AbortController();

    vi.mocked(jobMock).mockResolvedValue("success");
    await addJob({ name: "test", queued: true });

    const onContinue = new Promise((resolve) =>
      processorEventEmitter.once("continue", resolve),
    );

    const teardown = runJobProcessor(ctrl.signal);
    await onContinue;

    expect(await getJobCollection().find({ name: "test" }).toArray()).toEqual([
      {
        _id: expect.anything(),
        created_at: expect.any(Date),
        name: "test",
        payload: null,
        scheduled_for: expect.any(Date),
        status: "finished",
        sync: false,
        type: "simple",
        updated_at: expect.any(Date),
        worker_id: null,
        started_at: expect.any(Date),
        ended_at: expect.any(Date),
        output: {
          duration: expect.any(String),
          error: null,
          result: "success",
        },
      },
    ]);

    ctrl.abort();
    await teardown;
  });

  it("should handle job failure", async () => {
    const ctrl = new AbortController();

    const cause = new Error("cause");
    vi.mocked(jobMock).mockRejectedValueOnce(new Error("failure", { cause }));
    await addJob({ name: "test", queued: true });

    const onContinue = new Promise((resolve) =>
      processorEventEmitter.once("continue", resolve),
    );

    const teardown = runJobProcessor(ctrl.signal);
    await onContinue;

    expect(await getJobCollection().find({ name: "test" }).toArray()).toEqual([
      {
        _id: expect.anything(),
        created_at: expect.any(Date),
        name: "test",
        payload: null,
        scheduled_for: expect.any(Date),
        status: "errored",
        sync: false,
        type: "simple",
        updated_at: expect.any(Date),
        worker_id: null,
        started_at: expect.any(Date),
        ended_at: expect.any(Date),
        output: {
          duration: expect.any(String),
          result: null,
          error: expect.stringMatching(/Error: failure/),
        },
      },
    ]);

    ctrl.abort();
    await teardown;
  });

  it("should handle job failure", async () => {
    const ctrl = new AbortController();

    const cause = new Error("cause");
    vi.mocked(jobMock).mockRejectedValueOnce(new Error("failure", { cause }));
    await addJob({ name: "test", queued: true });

    const onContinue = new Promise((resolve) =>
      processorEventEmitter.once("continue", resolve),
    );

    const teardown = runJobProcessor(ctrl.signal);
    await onContinue;

    expect(await getJobCollection().find({ name: "test" }).toArray()).toEqual([
      {
        _id: expect.anything(),
        created_at: expect.any(Date),
        name: "test",
        payload: null,
        scheduled_for: expect.any(Date),
        status: "errored",
        sync: false,
        type: "simple",
        updated_at: expect.any(Date),
        worker_id: null,
        started_at: expect.any(Date),
        ended_at: expect.any(Date),
        output: {
          duration: expect.any(String),
          result: null,
          error: expect.stringMatching(/Error: failure/),
        },
      },
    ]);

    ctrl.abort();
    await teardown;
  });
});
