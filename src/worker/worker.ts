import { getCronTaskJob, getSimpleJob, updateJob } from "../data/actions.ts";
import { IJobsCronTask, IJobsSimple } from "../data/model.ts";
import { CronDef, JobDef, getLogger, getOptions } from "../setup.ts";
import {
  captureException,
  runWithAsyncContext,
  getCurrentHub,
} from "@sentry/node";
import { formatDuration, intervalToDuration } from "date-fns";
import { workerId } from "./heartbeat.ts";

function getJobSimpleDef(job: IJobsSimple): JobDef | null {
  const options = getOptions();
  return options.jobs[job.name] ?? null;
}

function getCronTaskDef(job: IJobsCronTask): CronDef | null {
  const options = getOptions();
  return options.crons[job.name] ?? null;
}

async function onRunnerExit(
  startDate: Date,
  job: IJobsCronTask | IJobsSimple,
  error: string | null,
  result: unknown,
) {
  const endDate = new Date();
  const ts = endDate.getTime() - startDate.getTime();
  const duration =
    formatDuration(intervalToDuration({ start: startDate, end: endDate })) ||
    `${ts}ms`;

  const status = error ? "errored" : "finished";
  await updateJob(job._id, {
    status: error ? "errored" : "finished",
    output: { duration, result, error },
    ended_at: endDate,
    worker_id: null,
  });

  if (job.type === "simple") {
    const onJobExited = getJobSimpleDef(job)?.onJobExited ?? null;
    if (onJobExited) {
      const updatedJob = (await getSimpleJob(job._id)) ?? job;
      await onJobExited(updatedJob);
    }
  } else {
    const onJobExited = getCronTaskDef(job)?.onJobExited ?? null;
    if (onJobExited) {
      const updatedJob = (await getCronTaskJob(job._id)) ?? job;
      await onJobExited(updatedJob);
    }
  }

  return { status, duration };
}

async function runner(
  job: IJobsCronTask | IJobsSimple,
  signal: AbortSignal,
): Promise<number> {
  const jobLogger = getLogger().child({
    _id: job._id,
    jobName: job.name,
    created_at: job.created_at,
    updated_at: job.updated_at,
  });

  jobLogger.info("job started");
  const startDate = new Date();
  await updateJob(job._id, {
    status: "running",
    started_at: startDate,
    worker_id: workerId,
  });
  let error: string | null = null;
  let result: unknown = undefined;

  try {
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
    captureException(err);
    jobLogger.error(
      { err, writeErrors: err.writeErrors, error: err },
      "job error",
    );
    error = (err as Error)?.stack ?? "Unknown";
  }

  const { status, duration } = await onRunnerExit(
    startDate,
    job,
    error,
    result,
  );

  jobLogger.info({ status, duration }, "job ended");

  return error ? 1 : 0;
}

export function executeJob(
  job: IJobsCronTask | IJobsSimple,
  signal: AbortSignal | null,
): Promise<number> {
  return runWithAsyncContext(async () => {
    const hub = getCurrentHub();
    const transaction = hub?.startTransaction({
      name: `JOB: ${job.name}`,
      op: "processor.job",
    });
    hub?.configureScope((scope) => {
      scope.setSpan(transaction);
      scope.setTag("job", job.name);
      scope.setContext("job", job);
    });
    const start = Date.now();
    try {
      const s = signal ?? new AbortController().signal;
      return await runner(job, s);
    } finally {
      transaction?.setMeasurement(
        "job.execute",
        Date.now() - start,
        "millisecond",
      );
      transaction?.finish();
    }
  });
}
