import { Mock, afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { updateJob } from "../data/actions.ts";
import { executeJob } from "./worker.ts";
import { JobProcessorOptions, getOptions } from "../setup.ts";
import { ObjectId } from "mongodb";
import { IJobsSimple } from "../data/model.ts";

vi.mock("../setup.ts", async (importOriginal) => {
  const mod = await importOriginal();
  return {
    // @ts-expect-error not properly typed
    ...mod,
    getLogger: vi.fn().mockReturnValue({
      debug: vi.fn(),
      info: vi.fn(),
      error: vi.fn(),
      child: vi.fn().mockReturnThis(),
    }),
    getOptions: vi.fn(),
  };
});

vi.mock("../data/actions.ts", async (importOriginal) => {
  const mod = await importOriginal();
  return {
    // @ts-expect-error not properly typed
    ...mod,
    pickNextJob: vi.fn(),
    updateJob: vi.fn(),
  };
});

describe("worker", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("should execute properly on success", async () => {
    const options: JobProcessorOptions = {
      db: vi.fn() as any,
      logger: vi.fn() as any,
      crons: {},
      jobs: {
        hello: {
          handler: async (j: any) => {
            vi.advanceTimersByTime(2_000);

            return `Hello ${j.payload.name}`;
          },
        },
      },
    };
    const now = new Date();

    const job: IJobsSimple = {
      _id: new ObjectId(),
      name: "hello",
      type: "simple",
      status: "pending",
      sync: false,
      payload: { name: "Moroine" },
      output: null,
      scheduled_for: now,
      started_at: null,
      ended_at: null,
      updated_at: now,
      created_at: now,
    };

    const jobUpdates: Partial<IJobsSimple>[] = [];

    vi.advanceTimersByTime(3_000);

    (getOptions as Mock).mockReturnValue(options);
    (updateJob as Mock).mockImplementation(
      (id: ObjectId, data: Partial<IJobsSimple>) => {
        expect(id).toBe(job._id);
        jobUpdates.push(data);
      },
    );

    const abortController = new AbortController();

    await expect.soft(executeJob(job, abortController.signal)).resolves.toBe(0);
    expect(jobUpdates).toEqual([
      { status: "running", started_at: expect.anything() },
      {
        status: "finished",
        output: {
          duration: "2 seconds",
          result: "Hello Moroine",
          error: null,
        },
        ended_at: expect.anything(),
      },
    ]);
    expect(jobUpdates[0]?.started_at?.getTime()).toBeGreaterThanOrEqual(
      now.getTime() + 3000,
    );
    expect(jobUpdates[0]?.started_at?.getTime()).toBeLessThanOrEqual(
      now.getTime() + 3100,
    );
    expect(jobUpdates[1]?.ended_at?.getTime()).toBeGreaterThanOrEqual(
      now.getTime() + 5000,
    );
    expect(jobUpdates[1]?.ended_at?.getTime()).toBeLessThanOrEqual(
      now.getTime() + 5100,
    );
  });

  it("should report error on failure", async () => {
    const options: JobProcessorOptions = {
      db: vi.fn() as any,
      logger: vi.fn() as any,
      crons: {},
      jobs: {
        hello: {
          handler: async () => {
            vi.advanceTimersByTime(2_000);

            throw new Error("Ooops");
          },
        },
      },
    };
    const now = new Date();

    const job: IJobsSimple = {
      _id: new ObjectId(),
      name: "hello",
      type: "simple",
      status: "pending",
      sync: false,
      payload: { name: "Moroine" },
      output: null,
      scheduled_for: now,
      started_at: null,
      ended_at: null,
      updated_at: now,
      created_at: now,
    };

    const jobUpdates: Partial<IJobsSimple>[] = [];

    vi.advanceTimersByTime(3_000);

    (getOptions as Mock).mockReturnValue(options);
    (updateJob as Mock).mockImplementation(
      (id: ObjectId, data: Partial<IJobsSimple>) => {
        expect(id).toBe(job._id);
        jobUpdates.push(data);
      },
    );

    const abortController = new AbortController();

    expect.soft(await executeJob(job, abortController.signal)).toBe(1);
    expect(jobUpdates).toEqual([
      { status: "running", started_at: expect.anything() },
      {
        status: "errored",
        output: {
          duration: "2 seconds",
          result: undefined,
          error: expect.stringContaining("Error: Ooops"),
        },
        ended_at: expect.anything(),
      },
    ]);
    expect(jobUpdates[0]?.started_at?.getTime()).toBeGreaterThanOrEqual(
      now.getTime() + 3000,
    );
    expect(jobUpdates[0]?.started_at?.getTime()).toBeLessThanOrEqual(
      now.getTime() + 3100,
    );
    expect(jobUpdates[1]?.ended_at?.getTime()).toBeGreaterThanOrEqual(
      now.getTime() + 5000,
    );
    expect(jobUpdates[1]?.ended_at?.getTime()).toBeLessThanOrEqual(
      now.getTime() + 5100,
    );
  });
});
