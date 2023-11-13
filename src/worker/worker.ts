import { updateJob } from "../data/actions.ts";
import { IJobsCronTask, IJobsSimple } from "../data/model.ts";
import { CronDef, JobFn, getLogger, getOptions } from "../setup.ts";
import {
  captureException,
  runWithAsyncContext,
  getCurrentHub,
} from "@sentry/node";
import { formatDuration, intervalToDuration } from "date-fns";

function getJobSimpleFn(job: IJobsSimple): JobFn | null {
  const options = getOptions();
  return options.jobs[job.name] ?? null;
}

function getCronTaskFn(job: IJobsCronTask): CronDef["handler"] | null {
  const options = getOptions();
  return options.crons[job.name]?.handler ?? null;
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
  });
  let error: Error | undefined = undefined;
  let result: unknown = undefined;

  try {
    if (job.type === "simple") {
      const jobFn = getJobSimpleFn(job);
      if (!jobFn) {
        throw new Error("Job function not found");
      }
      result = await jobFn(job, signal);
    } else {
      const jobFn = getCronTaskFn(job);
      if (!jobFn) {
        throw new Error("Job function not found");
      }
      result = await jobFn(signal);
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } catch (err: any) {
    captureException(err);
    jobLogger.error(
      { err, writeErrors: err.writeErrors, error: err },
      "job error",
    );
    error = err?.stack;
  }

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
  });

  jobLogger.info({ status, duration }, "job ended");

  if (error) {
    jobLogger.error(
      { error },
      error.constructor.name === "EnvVarError" ? error.message : error,
    );
  }

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
