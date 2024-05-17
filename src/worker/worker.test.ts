import { Mock, afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { updateJob, getSimpleJob, getCronTaskJob } from "../data/actions.ts";
import { executeJob, reportJobCrash } from "./worker.ts";
import { JobProcessorOptions, getOptions } from "../setup.ts";
import { ObjectId } from "bson";
import { IJobsCronTask, IJobsSimple } from "../data/model.ts";
import { workerId } from "./heartbeat.ts";

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
    getSimpleJob: vi.fn(),
    getCronTaskJob: vi.fn(),
  };
});

describe("executeJob", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  describe("when simple job", () => {
    it("should execute properly on success", async () => {
      const onJobExited = vi.fn();
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
            onJobExited,
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
        worker_id: null,
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
      (getSimpleJob as Mock).mockImplementation((id: ObjectId) => {
        expect(id).toBe(job._id);

        return jobUpdates.reduce((acc, update) => {
          return { ...acc, ...update };
        }, job);
      });

      const abortController = new AbortController();

      await expect
        .soft(executeJob(job, abortController.signal))
        .resolves.toBe(0);
      expect(jobUpdates).toEqual([
        {
          status: "running",
          started_at: expect.anything(),
          worker_id: workerId,
        },
        {
          status: "finished",
          output: {
            duration: "2 seconds",
            result: "Hello Moroine",
            error: null,
          },
          ended_at: expect.anything(),
          worker_id: null,
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
      expect(onJobExited).toHaveBeenCalledOnce();
      expect(onJobExited).toHaveBeenCalledWith({
        ...job,
        ...jobUpdates[0],
        ...jobUpdates[1],
      });
    });

    it("should report error on failure", async () => {
      const onJobExited = vi.fn();
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
            onJobExited,
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
        worker_id: null,
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
      (getSimpleJob as Mock).mockImplementation((id: ObjectId) => {
        expect(id).toBe(job._id);

        return jobUpdates.reduce((acc, update) => {
          return { ...acc, ...update };
        }, job);
      });

      const abortController = new AbortController();

      expect.soft(await executeJob(job, abortController.signal)).toBe(1);
      expect(jobUpdates).toEqual([
        {
          status: "running",
          started_at: expect.anything(),
          worker_id: workerId,
        },
        {
          status: "errored",
          output: {
            duration: "2 seconds",
            result: undefined,
            error: expect.stringContaining("Error: Ooops"),
          },
          ended_at: expect.anything(),
          worker_id: null,
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
      expect(onJobExited).toHaveBeenCalledOnce();
      expect(onJobExited).toHaveBeenCalledWith({
        ...job,
        ...jobUpdates[0],
        ...jobUpdates[1],
      });
    });
  });

  describe("when cron task", () => {
    it("should execute properly on success", async () => {
      const onJobExited = vi.fn();
      const options: JobProcessorOptions = {
        db: vi.fn() as any,
        logger: vi.fn() as any,
        crons: {
          hello: {
            cron_string: "*",
            handler: async () => {
              vi.advanceTimersByTime(2_000);

              return `Hello`;
            },
            onJobExited,
          },
        },
        jobs: {},
      };
      const now = new Date();

      const job: IJobsCronTask = {
        _id: new ObjectId(),
        name: "hello",
        type: "cron_task",
        status: "pending",
        scheduled_for: now,
        started_at: null,
        ended_at: null,
        updated_at: now,
        created_at: now,
        worker_id: null,
      };

      const jobUpdates: Partial<IJobsCronTask>[] = [];

      vi.advanceTimersByTime(3_000);

      (getOptions as Mock).mockReturnValue(options);
      (updateJob as Mock).mockImplementation(
        (id: ObjectId, data: Partial<IJobsCronTask>) => {
          expect(id).toBe(job._id);
          jobUpdates.push(data);
        },
      );
      (getCronTaskJob as Mock).mockImplementation((id: ObjectId) => {
        expect(id).toBe(job._id);

        return jobUpdates.reduce((acc, update) => {
          return { ...acc, ...update };
        }, job);
      });

      const abortController = new AbortController();

      await expect
        .soft(executeJob(job, abortController.signal))
        .resolves.toBe(0);
      expect(jobUpdates).toEqual([
        {
          status: "running",
          started_at: expect.anything(),
          worker_id: workerId,
        },
        {
          status: "finished",
          output: {
            duration: "2 seconds",
            result: "Hello",
            error: null,
          },
          ended_at: expect.anything(),
          worker_id: null,
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
      expect(onJobExited).toHaveBeenCalledOnce();
      expect(onJobExited).toHaveBeenCalledWith({
        ...job,
        ...jobUpdates[0],
        ...jobUpdates[1],
      });
    });

    it("should report error on failure", async () => {
      const onJobExited = vi.fn();
      const options: JobProcessorOptions = {
        db: vi.fn() as any,
        logger: vi.fn() as any,
        crons: {
          hello: {
            handler: async () => {
              vi.advanceTimersByTime(2_000);

              throw new Error("Ooops");
            },
            onJobExited,
            cron_string: "*",
          },
        },
        jobs: {},
      };
      const now = new Date();

      const job: IJobsCronTask = {
        _id: new ObjectId(),
        name: "hello",
        type: "cron_task",
        status: "pending",
        scheduled_for: now,
        started_at: null,
        ended_at: null,
        updated_at: now,
        created_at: now,
        worker_id: null,
      };

      const jobUpdates: Partial<IJobsCronTask>[] = [];

      vi.advanceTimersByTime(3_000);

      (getOptions as Mock).mockReturnValue(options);
      (updateJob as Mock).mockImplementation(
        (id: ObjectId, data: Partial<IJobsCronTask>) => {
          expect(id).toBe(job._id);
          jobUpdates.push(data);
        },
      );
      (getCronTaskJob as Mock).mockImplementation((id: ObjectId) => {
        expect(id).toBe(job._id);

        return jobUpdates.reduce((acc, update) => {
          return { ...acc, ...update };
        }, job);
      });

      const abortController = new AbortController();

      expect.soft(await executeJob(job, abortController.signal)).toBe(1);
      expect(jobUpdates).toEqual([
        {
          status: "running",
          started_at: expect.anything(),
          worker_id: workerId,
        },
        {
          status: "errored",
          output: {
            duration: "2 seconds",
            result: undefined,
            error: expect.stringContaining("Error: Ooops"),
          },
          ended_at: expect.anything(),
          worker_id: null,
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
      expect(onJobExited).toHaveBeenCalledOnce();
      expect(onJobExited).toHaveBeenCalledWith({
        ...job,
        ...jobUpdates[0],
        ...jobUpdates[1],
      });
    });
  });
});

describe("reportJobCrash", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  describe("when simple job", () => {
    describe("without onJobExited", () => {
      it("should do nothing", async () => {
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
        (getOptions as Mock).mockReturnValue(options);

        const now = new Date();

        const job: IJobsSimple = {
          _id: new ObjectId(),
          name: "hello",
          type: "simple",
          status: "errored",
          sync: false,
          payload: { name: "Moroine" },
          output: {
            duration: "--",
            result: null,
            error: "Worker crashed unexpectly",
          },
          scheduled_for: new Date(now.getTime() - 900_000),
          started_at: new Date(now.getTime() - 900_000),
          ended_at: now,
          updated_at: now,
          created_at: new Date(now.getTime() - 900_000),
          worker_id: null,
        };

        await expect(reportJobCrash(job)).resolves.toBeUndefined();
      });
    });

    describe("with onJobExited", () => {
      it("should call onJobExited", async () => {
        const onJobExited = vi.fn();
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
              onJobExited,
            },
          },
        };
        (getOptions as Mock).mockReturnValue(options);

        const now = new Date();

        const job: IJobsSimple = {
          _id: new ObjectId(),
          name: "hello",
          type: "simple",
          status: "errored",
          sync: false,
          payload: { name: "Moroine" },
          output: {
            duration: "--",
            result: null,
            error: "Worker crashed unexpectly",
          },
          scheduled_for: new Date(now.getTime() - 900_000),
          started_at: new Date(now.getTime() - 900_000),
          ended_at: now,
          updated_at: now,
          created_at: new Date(now.getTime() - 900_000),
          worker_id: null,
        };

        await expect(reportJobCrash(job)).resolves.toBeUndefined();
        expect(onJobExited).toHaveBeenCalledOnce();
        expect(onJobExited).toHaveBeenCalledWith(job);
      });

      it("should not throw when onJobExited error", async () => {
        const onJobExited = vi.fn().mockRejectedValue(new Error("Ooops"));
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
              onJobExited,
            },
          },
        };
        (getOptions as Mock).mockReturnValue(options);

        const now = new Date();

        const job: IJobsSimple = {
          _id: new ObjectId(),
          name: "hello",
          type: "simple",
          status: "errored",
          sync: false,
          payload: { name: "Moroine" },
          output: {
            duration: "--",
            result: null,
            error: "Worker crashed unexpectly",
          },
          scheduled_for: new Date(now.getTime() - 900_000),
          started_at: new Date(now.getTime() - 900_000),
          ended_at: now,
          updated_at: now,
          created_at: new Date(now.getTime() - 900_000),
          worker_id: null,
        };

        await expect(reportJobCrash(job)).resolves.toBeUndefined();
        expect(onJobExited).toHaveBeenCalledOnce();
        expect(onJobExited).toHaveBeenCalledWith(job);
      });
    });
  });

  describe("when simple job", () => {
    describe("without onJobExited", () => {
      it("should do nothing", async () => {
        const options: JobProcessorOptions = {
          db: vi.fn() as any,
          logger: vi.fn() as any,
          crons: {
            hello: {
              cron_string: "*",
              handler: async () => {
                vi.advanceTimersByTime(2_000);

                return `Hello`;
              },
            },
          },
          jobs: {},
        };
        (getOptions as Mock).mockReturnValue(options);

        const now = new Date();

        const job: IJobsCronTask = {
          _id: new ObjectId(),
          name: "hello",
          type: "cron_task",
          status: "errored",
          output: {
            duration: "--",
            result: null,
            error: "Worker crashed unexpectly",
          },
          scheduled_for: new Date(now.getTime() - 900_000),
          started_at: new Date(now.getTime() - 900_000),
          ended_at: now,
          updated_at: now,
          created_at: new Date(now.getTime() - 900_000),
          worker_id: null,
        };

        await expect(reportJobCrash(job)).resolves.toBeUndefined();
      });
    });

    describe("with onJobExited", () => {
      it("should call onJobExited", async () => {
        const onJobExited = vi.fn();
        const options: JobProcessorOptions = {
          db: vi.fn() as any,
          logger: vi.fn() as any,
          crons: {
            hello: {
              cron_string: "*",
              handler: async () => {
                vi.advanceTimersByTime(2_000);

                return `Hello`;
              },
              onJobExited,
            },
          },
          jobs: {},
        };
        (getOptions as Mock).mockReturnValue(options);

        const now = new Date();

        const job: IJobsCronTask = {
          _id: new ObjectId(),
          name: "hello",
          type: "cron_task",
          status: "errored",
          output: {
            duration: "--",
            result: null,
            error: "Worker crashed unexpectly",
          },
          scheduled_for: new Date(now.getTime() - 900_000),
          started_at: new Date(now.getTime() - 900_000),
          ended_at: now,
          updated_at: now,
          created_at: new Date(now.getTime() - 900_000),
          worker_id: null,
        };

        await expect(reportJobCrash(job)).resolves.toBeUndefined();
        expect(onJobExited).toHaveBeenCalledOnce();
        expect(onJobExited).toHaveBeenCalledWith(job);
      });

      it("should not throw when onJobExited error", async () => {
        const onJobExited = vi.fn().mockRejectedValue(new Error("Ooops"));
        const options: JobProcessorOptions = {
          db: vi.fn() as any,
          logger: vi.fn() as any,
          crons: {
            hello: {
              cron_string: "*",
              handler: async () => {
                vi.advanceTimersByTime(2_000);

                return `Hello`;
              },
              onJobExited,
            },
          },
          jobs: {},
        };
        (getOptions as Mock).mockReturnValue(options);

        const now = new Date();

        const job: IJobsCronTask = {
          _id: new ObjectId(),
          name: "hello",
          type: "cron_task",
          status: "errored",
          output: {
            duration: "--",
            result: null,
            error: "Worker crashed unexpectly",
          },
          scheduled_for: new Date(now.getTime() - 900_000),
          started_at: new Date(now.getTime() - 900_000),
          ended_at: now,
          updated_at: now,
          created_at: new Date(now.getTime() - 900_000),
          worker_id: null,
        };

        await expect(reportJobCrash(job)).resolves.toBeUndefined();
        expect(onJobExited).toHaveBeenCalledOnce();
        expect(onJobExited).toHaveBeenCalledWith(job);
      });
    });
  });
});
