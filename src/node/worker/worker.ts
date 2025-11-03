import * as Sentry from "@sentry/node";
import { formatDuration, intervalToDuration } from "date-fns";
import type { IJobsCronTask, IJobsSimple } from "../../common/model.ts";
import { getCronTaskJob, getSimpleJob, updateJob } from "../data/actions.ts";
import { getOptions } from "../options.ts";
import type { CronDef, ILogger, JobDef } from "../setup.ts";
import { getLogger } from "../logger.ts";
import { clearJobKillSignal, getJobKillSignal } from "../signal/signal.ts";
import { notifySentryJobEnd, notifySentryJobStart } from "./sentry.ts";

function getJobSimpleDef(job: IJobsSimple): JobDef | null {
  const options = getOptions();
  return options.jobs[job.name] ?? null;
}

function getCronTaskDef(job: IJobsCronTask): CronDef | null {
  const options = getOptions();
  return options.crons[job.name] ?? null;
}

function stringifyError<T>(error: T): string | T {
  if (error instanceof Error) {
    const message =
      error.stack?.split("\n").slice(0, 3).join("\n") ?? error.message;

    if (!error.cause) return message;

    return `${message}\nCaused by: ${stringifyError(error.cause)}`;
  }

  return error;
}

async function onRunnerExit(
  job: IJobsCronTask | IJobsSimple,
  error: string | null,
  status: "finished" | "errored" | "killed" | "paused",
  result: unknown,
  jobLogger: ILogger,
) {
  const startDate = job.started_at ?? new Date();
  const endDate = new Date();
  const ts = endDate.getTime() - startDate.getTime();
  const duration =
    formatDuration(intervalToDuration({ start: startDate, end: endDate })) ||
    `${ts}ms`;

  const currentJob =
    job.type === "simple"
      ? await getSimpleJob(job._id)
      : await getCronTaskJob(job._id);

  if (currentJob?.status === "killed") {
    // Job is already killed, we don't override it
    return { status: "killed", duration };
  }

  await updateJob(job._id, {
    status,
    output: status === "paused" ? null : { duration, result, error },
    ended_at: status === "paused" ? null : endDate,
    worker_id: null,
  });
  await notifySentryJobEnd(job, !error);

  if (status === "paused") {
    // Job was paused, we don't call onJobExited
    return { status, duration };
  }

  if (job.type === "simple") {
    const onJobExited = getJobSimpleDef(job)?.onJobExited ?? null;
    if (onJobExited) {
      const updatedJob = (await getSimpleJob(job._id)) ?? job;
      try {
        await onJobExited(updatedJob);
      } catch (errored) {
        jobLogger.error(
          { error: errored, job },
          "job-processor: onJobExited failed",
        );
        Sentry.captureException(errored, { extra: { job } });
      }
    }
  } else {
    const onJobExited = getCronTaskDef(job)?.onJobExited ?? null;
    if (onJobExited) {
      const updatedJob = (await getCronTaskJob(job._id)) ?? job;
      try {
        await onJobExited(updatedJob);
      } catch (errored) {
        jobLogger.error(
          { error: errored, job },
          "job-processor: onJobExited failed",
        );
        Sentry.captureException(errored, { extra: { job } });
      }
    }
  }

  return { status, duration };
}

function getJobAbortedCb(
  job: IJobsSimple | IJobsCronTask,
  jobLogger: ILogger,
): () => Promise<void> {
  return async () => {
    try {
      // As soon as the process is abort, we update job status
      // We still wait for completion of the handler
      // but in case it didn't return in time, we still have a better status
      const resumable =
        job.type === "simple"
          ? getJobSimpleDef(job)?.resumable
          : getCronTaskDef(job)?.resumable;

      if (resumable === true) {
        await onRunnerExit(job, "Interrupted", "paused", null, jobLogger);
      } else {
        const error = new Error("[job-processor] Job processor aborted");
        Sentry.captureException(error, {
          extra: { job },
        });
        getLogger().error(
          { error, job },
          "job-processor: job processor aborted",
        );
        await onRunnerExit(job, "Interrupted", "errored", null, jobLogger);
      }
    } catch (err) {
      Sentry.captureException(err, { extra: { job } });
      getLogger().error(
        { error: err, job },
        "job-processor: job processor abort unexpected error",
      );
    }
  };
}

function getJobKillCb(
  job: IJobsSimple | IJobsCronTask,
  jobLogger: ILogger,
): () => Promise<void> {
  return async () => {
    try {
      const error = new Error("[job-processor] Job killed");
      Sentry.captureException(error, {
        extra: { job },
      });
      getLogger().error({ error, job }, "job-processor: job killed");
      await onRunnerExit(job, "Killed", "killed", null, jobLogger);
    } catch (err) {
      Sentry.captureException(err, { extra: { job } });
      jobLogger.error(
        { error: err, job },
        "job-processor: job kill unexpected error",
      );
    }
  };
}

async function runner(
  job: IJobsCronTask | IJobsSimple,
  processSignal: AbortSignal,
): Promise<number> {
  const jobLogger = getLogger().child({
    _id: job._id,
    jobName: job.name,
    created_at: job.created_at,
    updated_at: job.updated_at,
  });

  jobLogger.info("job started");
  const jobKillSignal = getJobKillSignal(job._id);

  const onKill = getJobKillCb(job, jobLogger);
  if (jobKillSignal.aborted) {
    await onKill();
    return 2;
  }

  jobKillSignal.addEventListener("abort", onKill, { once: true });

  const onAbort = getJobAbortedCb(job, jobLogger);
  if (processSignal.aborted) {
    await onAbort();
    return 2;
  }

  processSignal.addEventListener("abort", onAbort, { once: true });

  let error: string | null = null;
  let result: unknown = undefined;

  const signal = AbortSignal.any([processSignal, jobKillSignal]);

  try {
    if (signal.aborted) {
      throw signal.reason;
    }

    if (job.type === "simple") {
      const jobDef = getJobSimpleDef(job);
      if (!jobDef) {
        throw new Error("Job not found");
      }
      result = await jobDef.handler(job, signal);
    } else {
      const cronDef = getCronTaskDef(job);
      if (!cronDef) {
        throw new Error("Cron not found");
      }
      result = await cronDef.handler(signal);
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } catch (err: any) {
    if (err === signal.reason) {
      // No need to update, it's already handled by onAbort / onKill
      return 2;
    }

    Sentry.captureException(err, { extra: { job } });
    jobLogger.error(
      { err, writeErrors: err.writeErrors, error: err },
      "job error",
    );
    error = stringifyError(err) ?? "job-erorr: unknown Error";
  } finally {
    clearJobKillSignal(job._id);
  }

  jobKillSignal.removeEventListener("abort", onKill);
  processSignal.removeEventListener("abort", onAbort);

  const { status, duration } = await onRunnerExit(
    job,
    error,
    error ? "errored" : "finished",
    result,
    jobLogger,
  );

  jobLogger.info({ status, duration }, "job ended");

  return error ? 1 : 0;
}

export async function executeJob(
  job: IJobsCronTask | IJobsSimple,
  signal: AbortSignal | null,
): Promise<number> {
  return Sentry.withIsolationScope(async (scope: Sentry.Scope) => {
    scope.setContext("job", job);
    return await Sentry.startSpan(
      {
        op: "processor.job",
        name: `JOB: ${job.name}`,
      },
      async () => {
        Sentry.getCurrentScope().setTag("job", job.name);
        await notifySentryJobStart(job);
        const start = Date.now();
        try {
          const s = signal ?? new AbortController().signal;
          const result = await runner(job, s);
          return result;
        } finally {
          Sentry.setMeasurement(
            "job.execute",
            Date.now() - start,
            "millisecond",
          );
        }
      },
    );
  });
}

export async function reportJobCrash(
  job: IJobsCronTask | IJobsSimple,
): Promise<void> {
  try {
    if (job.type === "simple") {
      const jobDef = getJobSimpleDef(job);
      if (!jobDef) {
        throw new Error("Job not found");
      }
      await jobDef.onJobExited?.(job);
    } else {
      const cronDef = getCronTaskDef(job);
      if (!cronDef) {
        throw new Error("Cron not found");
      }
      await notifySentryJobEnd(job, false);
      await cronDef.onJobExited?.(job);
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } catch (err: any) {
    Sentry.captureException(err, { extra: { job } });
    getLogger().error(
      { err, writeErrors: err.writeErrors, error: err, job },
      "reportJobCrash error",
    );
  }
}
