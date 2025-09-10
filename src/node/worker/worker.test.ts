import type { MatchKeysAndValues } from "mongodb";
import { ObjectId } from "mongodb";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { IJob, IJobsCronTask, IJobsSimple } from "../../common/model.ts";
import { getCronTaskJob, getSimpleJob, updateJob } from "../data/actions.ts";
import type { JobProcessorOptions } from "../setup.ts";
import { getOptions } from "../options.ts";
import { getJobKillSignal } from "../signal/signal.ts";
import { workerId } from "./workerId.ts";
import { executeJob, reportJobCrash } from "./worker.ts";

vi.mock("../options.ts");
vi.mock("../signal/signal.ts");
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

vi.mock("./sentry.ts", async (importOriginal) => {
  const original = await importOriginal();
  return {
    // @ts-expect-error not properly typed
    ...original,
    notifySentryJobStart: vi.fn(),
    notifySentryJobEnd: vi.fn(),
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
      vi.advanceTimersByTime(3_000);
      const in3sec = new Date();

      const job: IJobsSimple = {
        _id: new ObjectId(),
        name: "hello",
        type: "simple",
        status: "running",
        sync: false,
        payload: { name: "Moroine" },
        output: null,
        scheduled_for: now,
        started_at: in3sec,
        ended_at: null,
        updated_at: now,
        created_at: now,
        worker_id: workerId,
      };

      const jobUpdates: Partial<IJobsSimple>[] = [];

      vi.mocked(getOptions).mockReturnValue(options);
      vi.mocked(updateJob).mockImplementation(
        async (id: ObjectId, data: MatchKeysAndValues<IJob>) => {
          expect(id).toBe(job._id);
          jobUpdates.push(data as IJobsSimple);
        },
      );
      vi.mocked(getSimpleJob).mockImplementation(
        async (id: ObjectId): Promise<IJobsSimple> => {
          expect(id).toBe(job._id);

          return jobUpdates.reduce<IJobsSimple>((acc, update) => {
            return { ...acc, ...update };
          }, job);
        },
      );

      const processorAbortController = new AbortController();
      const jobAbortController = new AbortController();
      vi.mocked(getJobKillSignal).mockReturnValue(jobAbortController.signal);

      await expect
        .soft(executeJob(job, processorAbortController.signal))
        .resolves.toBe(0);
      expect(getJobKillSignal).toHaveBeenCalledBefore(vi.mocked(updateJob));
      expect(getJobKillSignal).toHaveBeenCalledWith(job._id);
      expect(jobUpdates).toEqual([
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
      expect(jobUpdates[0]?.ended_at?.getTime()).toBeGreaterThanOrEqual(
        now.getTime() + 5000,
      );
      expect(jobUpdates[0]?.ended_at?.getTime()).toBeLessThanOrEqual(
        now.getTime() + 5100,
      );
      expect(onJobExited).toHaveBeenCalledOnce();
      expect(onJobExited).toHaveBeenCalledWith({
        ...job,
        ...jobUpdates[0],
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
      vi.advanceTimersByTime(3_000);
      const in3sec = new Date();

      const job: IJobsSimple = {
        _id: new ObjectId(),
        name: "hello",
        type: "simple",
        status: "running",
        sync: false,
        payload: { name: "Moroine" },
        output: null,
        scheduled_for: now,
        started_at: in3sec,
        ended_at: null,
        updated_at: now,
        created_at: now,
        worker_id: workerId,
      };

      const jobUpdates: Partial<IJobsSimple>[] = [];

      vi.mocked(getOptions).mockReturnValue(options);
      vi.mocked(updateJob).mockImplementation(
        async (id: ObjectId, data: MatchKeysAndValues<IJob>) => {
          expect(id).toBe(job._id);
          jobUpdates.push(data as IJobsSimple);
        },
      );
      vi.mocked(getSimpleJob).mockImplementation(
        async (id: ObjectId): Promise<IJobsSimple> => {
          expect(id).toBe(job._id);

          return jobUpdates.reduce<IJobsSimple>((acc, update) => {
            return { ...acc, ...update };
          }, job);
        },
      );

      const processorAbortController = new AbortController();
      const jobAbortController = new AbortController();
      vi.mocked(getJobKillSignal).mockReturnValue(jobAbortController.signal);

      expect
        .soft(await executeJob(job, processorAbortController.signal))
        .toBe(1);
      expect(getJobKillSignal).toHaveBeenCalledBefore(vi.mocked(updateJob));
      expect(getJobKillSignal).toHaveBeenCalledWith(job._id);
      expect(jobUpdates).toEqual([
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
      expect(jobUpdates[0]?.ended_at?.getTime()).toBeGreaterThanOrEqual(
        now.getTime() + 5000,
      );
      expect(jobUpdates[0]?.ended_at?.getTime()).toBeLessThanOrEqual(
        now.getTime() + 5100,
      );
      expect(onJobExited).toHaveBeenCalledOnce();
      expect(onJobExited).toHaveBeenCalledWith({
        ...job,
        ...jobUpdates[0],
      });
    });

    it("should handle job kill signal", async () => {
      const onJobExited = vi.fn();
      const options: JobProcessorOptions = {
        db: vi.fn() as any,
        logger: vi.fn() as any,
        crons: {},
        jobs: {
          hello: {
            handler: async (_j, signal) => {
              return new Promise((_resolve, reject) => {
                signal.addEventListener("abort", async () => {
                  // Simluate abort handling after a small delay
                  await new Promise((r) => setTimeout(r, 1_000));
                  reject(signal.reason);
                });
              });
            },
            onJobExited,
          },
        },
      };
      const now = new Date();
      vi.advanceTimersByTime(3_000);
      const in3sec = new Date();

      const job: IJobsSimple = {
        _id: new ObjectId(),
        name: "hello",
        type: "simple",
        status: "running",
        sync: false,
        payload: { name: "Moroine" },
        output: null,
        scheduled_for: now,
        started_at: in3sec,
        ended_at: null,
        updated_at: now,
        created_at: now,
        worker_id: workerId,
      };

      const jobUpdates: Partial<IJobsSimple>[] = [];

      vi.mocked(getOptions).mockReturnValue(options);
      vi.mocked(updateJob).mockImplementation(
        async (id: ObjectId, data: MatchKeysAndValues<IJob>) => {
          expect(id).toBe(job._id);
          jobUpdates.push(data as IJobsSimple);
        },
      );
      vi.mocked(getSimpleJob).mockImplementation(
        async (id: ObjectId): Promise<IJobsSimple> => {
          expect(id).toBe(job._id);

          return jobUpdates.reduce<IJobsSimple>((acc, update) => {
            return { ...acc, ...update };
          }, job);
        },
      );

      const processorAbortController = new AbortController();
      const jobAbortController = new AbortController();
      vi.mocked(getJobKillSignal).mockReturnValue(jobAbortController.signal);

      const executePromise = executeJob(job, processorAbortController.signal);
      vi.waitFor(() => {
        expect(vi.mocked(getJobKillSignal)).toHaveBeenCalledWith(job._id);
      });
      vi.advanceTimersByTime(500);
      jobAbortController.abort(new Error("Job killed"));

      await expect.soft(executePromise).resolves.toBe(2);

      expect(getJobKillSignal).toHaveBeenCalledBefore(vi.mocked(updateJob));
      expect(getJobKillSignal).toHaveBeenCalledWith(job._id);
      expect(jobUpdates).toEqual([
        {
          status: "killed",
          output: {
            duration: "550ms",
            result: null,
            error: "Killed",
          },
          ended_at: expect.anything(),
          worker_id: null,
        },
      ]);
      expect(onJobExited).toHaveBeenCalledOnce();
      expect(onJobExited).toHaveBeenCalledWith({
        ...job,
        ...jobUpdates[0],
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
      vi.advanceTimersByTime(3_000);
      const in3sec = new Date();

      const job: IJobsCronTask = {
        _id: new ObjectId(),
        name: "hello",
        type: "cron_task",
        status: "running",
        scheduled_for: now,
        started_at: in3sec,
        ended_at: null,
        updated_at: now,
        created_at: now,
        worker_id: workerId,
      };

      const jobUpdates: Partial<IJobsCronTask>[] = [];

      vi.mocked(getOptions).mockReturnValue(options);
      vi.mocked(updateJob).mockImplementation(
        async (id: ObjectId, data: MatchKeysAndValues<IJob>) => {
          expect(id).toBe(job._id);
          jobUpdates.push(data as IJobsCronTask);
        },
      );
      vi.mocked(getCronTaskJob).mockImplementation(
        async (id: ObjectId): Promise<IJobsCronTask> => {
          expect(id).toBe(job._id);

          return jobUpdates.reduce<IJobsCronTask>((acc, update) => {
            return { ...acc, ...update };
          }, job);
        },
      );

      const processorAbortController = new AbortController();
      const jobAbortController = new AbortController();
      vi.mocked(getJobKillSignal).mockReturnValue(jobAbortController.signal);

      await expect
        .soft(executeJob(job, processorAbortController.signal))
        .resolves.toBe(0);
      expect(getJobKillSignal).toHaveBeenCalledBefore(vi.mocked(updateJob));
      expect(getJobKillSignal).toHaveBeenCalledWith(job._id);
      expect(jobUpdates).toEqual([
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
      expect(jobUpdates[0]?.ended_at?.getTime()).toBeGreaterThanOrEqual(
        now.getTime() + 5000,
      );
      expect(jobUpdates[0]?.ended_at?.getTime()).toBeLessThanOrEqual(
        now.getTime() + 5100,
      );
      expect(onJobExited).toHaveBeenCalledOnce();
      expect(onJobExited).toHaveBeenCalledWith({
        ...job,
        ...jobUpdates[0],
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
      vi.advanceTimersByTime(3_000);
      const in3sec = new Date();

      const job: IJobsCronTask = {
        _id: new ObjectId(),
        name: "hello",
        type: "cron_task",
        status: "running",
        scheduled_for: now,
        started_at: in3sec,
        ended_at: null,
        updated_at: now,
        created_at: now,
        worker_id: workerId,
      };

      const jobUpdates: Partial<IJobsCronTask>[] = [];

      vi.mocked(getOptions).mockReturnValue(options);
      vi.mocked(updateJob).mockImplementation(
        async (id: ObjectId, data: MatchKeysAndValues<IJob>) => {
          expect(id).toBe(job._id);
          jobUpdates.push(data as IJobsCronTask);
        },
      );
      vi.mocked(getCronTaskJob).mockImplementation(async (id: ObjectId) => {
        expect(id).toBe(job._id);

        return jobUpdates.reduce<IJobsCronTask>((acc, update) => {
          return { ...acc, ...update };
        }, job);
      });

      const processorAbortController = new AbortController();
      const jobAbortController = new AbortController();
      vi.mocked(getJobKillSignal).mockReturnValue(jobAbortController.signal);

      expect
        .soft(await executeJob(job, processorAbortController.signal))
        .toBe(1);
      expect(getJobKillSignal).toHaveBeenCalledBefore(vi.mocked(updateJob));
      expect(getJobKillSignal).toHaveBeenCalledWith(job._id);
      expect(jobUpdates).toEqual([
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
      expect(jobUpdates[0]?.ended_at?.getTime()).toBeGreaterThanOrEqual(
        now.getTime() + 5000,
      );
      expect(jobUpdates[0]?.ended_at?.getTime()).toBeLessThanOrEqual(
        now.getTime() + 5100,
      );
      expect(onJobExited).toHaveBeenCalledOnce();
      expect(onJobExited).toHaveBeenCalledWith({
        ...job,
        ...jobUpdates[0],
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
        vi.mocked(getOptions).mockReturnValue(options);

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
        vi.mocked(getOptions).mockReturnValue(options);

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
        vi.mocked(getOptions).mockReturnValue(options);

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
        vi.mocked(getOptions).mockReturnValue(options);

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
        vi.mocked(getOptions).mockReturnValue(options);

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
        vi.mocked(getOptions).mockReturnValue(options);

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
